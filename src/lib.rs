use std::fmt::Display;
use std::fs::{self, create_dir, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::prelude::PermissionsExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::Compression;
use reqwest::Url;
use sha1::{Digest, Sha1};

pub mod client;
pub mod pack;
pub mod pkt;

use crate::client::GitClient;
use crate::pack::PackFile;

#[derive(Debug, thiserror::Error)]
pub enum GitError {
    #[error("Invalid object type")]
    InvalidObjectType,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub struct GitRepo {
    path: PathBuf,
    git_dir: PathBuf,
    object_dir: PathBuf,
    refs_dir: PathBuf,
    tags_dir: PathBuf,
}

impl GitRepo {
    pub fn new<P: AsRef<Path>>(dir: P) -> Self {
        let git_dir = dir.as_ref().join(".git");
        let object_dir = git_dir.join("objects");
        let refs_dir = git_dir.join("refs");
        let tags_dir = git_dir.join("tags");
        Self {
            path: dir.as_ref().to_owned(),
            git_dir,
            object_dir,
            refs_dir,
            tags_dir,
        }
    }

    pub fn init(&self) -> Result<()> {
        fs::create_dir(&self.git_dir).context("Creating .git directory")?;
        fs::create_dir(&self.object_dir).context("Creating .git/objects directory")?;
        fs::create_dir(&self.refs_dir).context("Creating .git/refs directory")?;
        fs::create_dir(self.refs_dir.join("heads"))
            .context("Creating .git/refs/heads directory")?;
        fs::create_dir(&self.tags_dir).context("Creating .git/tags directory")?;
        fs::write(self.git_dir.join("HEAD"), "ref: refs/heads/master\n")
            .context("creating .git/HEAD file")?;
        println!("Initialized git directory");

        Ok(())
    }

    pub fn cat_file(&self, oid: ObjectId) -> Result<()> {
        let object = self.get_object(oid)?;
        let mut stdout = io::stdout().lock();

        // Write content of the blob to stdout
        stdout.write_all(&object.content)?;

        Ok(())
    }

    pub fn hash_object(&self, file: String) -> Result<()> {
        let file_content = fs::read(file).context("Reading file to hash")?;

        let object = Object {
            object_type: ObjectType::Blob,
            content: file_content.into(),
        };

        let sha = self.store_object(object)?;
        println!("{sha}");

        Ok(())
    }

    pub fn read_tree(&self, oid: ObjectId, names_only: bool) -> Result<()> {
        let object = self.get_object(oid)?;

        ensure!(
            object.object_type == ObjectType::Tree,
            "Object is not a tree"
        );

        let mut content = object.content.clone();
        let tree = Tree::parse(&mut content)?;

        for entry in tree.entries {
            if names_only {
                println!("{}", entry.name);
            } else {
                println!("{entry}");
            }
        }

        Ok(())
    }

    pub fn write_tree(&self) -> Result<()> {
        let sha = self.write_tree_dir(&self.path)?;

        println!("{sha}");

        Ok(())
    }

    fn write_tree_dir<P: AsRef<Path>>(&self, path: P) -> Result<ObjectId> {
        let dir = fs::read_dir(path)?;

        let mut tree_entries = Vec::new();
        for e in dir {
            let e = e?;
            let ft = e.file_type()?;
            if ft.is_dir() {
                // ignore `.git/` directory
                if e.file_name() == ".git" {
                    continue;
                }
                let tree_sha = self.write_tree_dir(e.path())?;
                tree_entries.push(TreeEntry {
                    mode: "40000".to_string(),
                    object_type: ObjectType::Tree,
                    name: e.file_name().to_string_lossy().to_string(),
                    sha1: tree_sha,
                });
                // recurse
            } else if ft.is_file() {
                let perms = e.metadata()?.permissions().mode();
                let mode = if (perms & 0o100) != 0 {
                    // executable
                    "00755"
                } else {
                    // regular file
                    "00644"
                };
                let mut file = BufReader::new(File::open(e.path())?);
                let mut buf = Vec::new();
                file.read_to_end(&mut buf)?;
                let object = Object::blob(buf);
                let file_sha = self.store_object(object)?;
                tree_entries.push(TreeEntry {
                    object_type: ObjectType::Blob,
                    mode: mode.to_owned(),
                    sha1: file_sha,
                    name: e.file_name().to_string_lossy().to_string(),
                });
            } else {
                // symlink
                unimplemented!()
            }
        }

        // Sort entries by name
        tree_entries.sort_by(|a, b| a.name.cmp(&b.name));

        // Prepare content of the tree object
        let mut buf = BytesMut::new();
        for entry in tree_entries.into_iter() {
            // Each tree entry as the following format:
            // `[mode] [Object name]\0[SHA-1 in binary format]`
            if entry.object_type == ObjectType::Blob {
                buf.put_u8(b'1')
            };
            buf.put(entry.mode.as_bytes());
            buf.put_u8(b' ');
            buf.put(entry.name.as_bytes());
            buf.put_u8(0);
            let sha_binary = hex::decode(entry.sha1.as_bytes())?;
            buf.put(sha_binary.as_slice());
        }

        let tree_object = Object::tree(buf.into());
        let sha1 = self.store_object(tree_object)?;

        Ok(sha1)
    }

    pub fn commit_tree(&self, tree_oid: ObjectId, parent: ObjectId, message: String) -> Result<()> {
        let mut buf = String::new();
        let now = SystemTime::now();
        let now_seconds = now.duration_since(UNIX_EPOCH)?.as_secs();

        buf.push_str(&format!("tree {tree_oid}\n"));
        buf.push_str(&format!("parent {parent}\n"));
        buf.push_str(&format!(
            "author {} <{}> {} {}\n",
            "Joe Author", "joe.author@example.com", now_seconds, "+1000",
        ));
        buf.push_str(&format!(
            "committer {} <{}> {} {}\n",
            "Bob Committer", "bob.committer@example.com", now_seconds, "+1000",
        ));
        buf.push('\n');
        buf.push_str(&message);
        buf.push('\n');

        let object = Object::commit(buf.as_bytes().to_vec());
        let sha = self.store_object(object)?;

        println!("{sha}");

        Ok(())
    }

    pub fn clone<P: AsRef<Path>>(url: Url, dir: P) -> Result<Self> {
        println!("Cloning {url} into {}", dir.as_ref().display());
        let client = GitClient::new(url);

        // Discover refs
        println!("Discovering refs...");
        let (_capabilities, advertised) = client.discover_refs()?;
        // For now only ask for the first ref, which should be HEAD
        // TODO: ask for all the refs
        let reference = advertised.first().expect("At least 1 ref to be advertised");

        // Fetch packfile
        let mut pack_data = client.request_pack(reference.oid)?;
        let pack_file = PackFile::parse(&mut pack_data)?;

        // create the requested directory and run `git init`
        let dir = dir.as_ref();
        create_dir(dir)?;
        let repo = GitRepo::new(dir);
        repo.init()?;

        // explode packfile into loose objects
        pack_file.explode_into_repo(&repo)?;

        // create references
        println!("Creating refs:");
        let tags_dir = repo.refs_dir.join("tags");
        let branches_dir = dir.join(".git/refs/remotes/origin");
        fs::create_dir_all(&tags_dir)?;
        fs::create_dir_all(&branches_dir)?;
        let (tags, branches) = advertised
            .iter()
            .filter(|r| !r.is_peeled_tag())
            .partition::<Vec<_>, _>(|r| r.is_tag());
        for tag in tags {
            let parts = tag.name.split('/').collect::<Vec<_>>();
            let tag_name = parts.last().expect("Invalid tag name");
            println!("\tCreating tag {}", tag_name);
            let mut file = File::create(tags_dir.join(tag_name))?;
            file.write_all(format!("{}\n", tag.oid).as_bytes())?;
        }
        for branch in branches {
            let parts = branch.name.split('/').collect::<Vec<_>>();
            let branch_name = parts.last().expect("Invalid branch name");
            println!("\tCreating branch {}", branch_name);
            let mut file = File::create(branches_dir.join(branch_name))?;
            file.write_all(format!("{}\n", branch.oid).as_bytes())?;
        }

        // set HEAD ref to HEAD of remote
        let remote_head = advertised
            .iter()
            .find(|r| r.name == "HEAD")
            .ok_or(anyhow!("The remote didn't send us a HEAD reference"))?;
        let remote_head_target = advertised
            .iter()
            .find(|r| r.name != "HEAD" && r.oid == remote_head.oid)
            .ok_or(anyhow!("No ref found as target of remote HEAD"))?;
        // Create local branch for HEAD to point to
        fs::write(
            repo.git_dir.join(&remote_head_target.name),
            format!("{}\n", remote_head_target.oid),
        )?;
        // Point HEAD to that local branch
        fs::write(
            repo.git_dir.join("HEAD"),
            format!("ref: {}\n", remote_head_target.name),
        )?;

        // checkout HEAD
        repo.checkout_head()?;

        Ok(repo)
    }

    pub fn checkout_head(&self) -> Result<()> {
        let head = self.resolve_head()?;

        let Some(target_commit) = self.get_object(head)?.as_commit() else {
            bail!("HEAD doesn't point to a commit");
        };
        dbg!(&target_commit);

        self.checkout_tree_in_dir(target_commit.tree, &self.path)?;

        Ok(())
    }

    fn checkout_tree_in_dir<P: AsRef<Path>>(&self, tree: ObjectId, dir: P) -> Result<()> {
        let Some(tree) = self.get_object(tree)?.as_tree() else {
            bail!("Trying to checkout an object that's not a tree");
        };

        for entry in tree.entries {
            if entry.object_type == ObjectType::Tree {
                // directory
                let new_dir = dir.as_ref().join(entry.name);
                println!("Creating directory {}", new_dir.display());
                fs::create_dir_all(&new_dir)?;
                self.checkout_tree_in_dir(entry.sha1, &new_dir)?;
            } else {
                // file
                let file = dir.as_ref().join(entry.name);
                println!("Checking out file {}", file.display());
                let blob = self.get_object(entry.sha1)?;
                fs::write(file, blob.content)?;
            }
        }

        Ok(())
    }

    fn resolve_head(&self) -> Result<ObjectId> {
        let head = fs::read_to_string(self.git_dir.join("HEAD")).context("Failed to read HEAD")?;
        let head_ref = head
            .strip_prefix("ref: ")
            .ok_or_else(|| anyhow!("Invalid symref: {head}"))?
            .trim();
        let target_ref =
            fs::read_to_string(self.git_dir.join(head_ref)).context("Failed to read {head_ref}")?;
        let target_ref = target_ref.trim().to_string();

        ObjectId::from_str(&target_ref)
    }

    pub fn store_object(&self, object: Object) -> Result<ObjectId> {
        let header = format!("{} {}\0", object.object_type, object.content.len());

        // compute SHA1
        let mut hasher = Sha1::new();
        hasher.update(header.as_bytes());
        hasher.update(&object.content);
        let result = hasher.finalize();
        let sha1 = hex::encode(result);
        let oid = ObjectId::from_str(&sha1)?;

        let path = self.get_object_path(oid);
        let dir = path.parent().expect("object path to have a parent");
        // Create parent directory if needed
        fs::create_dir_all(dir)?;
        // Create objectfile
        let mut object_file = fs::File::options().create(true).write(true).open(path)?;
        // Wrap object file in zlib encoder
        let mut writer = flate2::write::ZlibEncoder::new(&mut object_file, Compression::fast());

        // write header
        writer.write_all(header.as_bytes())?;
        // write content
        writer.write_all(&object.content)?;

        Ok(oid)
    }

    pub fn get_object(&self, oid: ObjectId) -> Result<Object> {
        let path = self.get_object_path(oid);

        let file = fs::File::open(path)?;
        let file = BufReader::new(file);
        let reader = flate2::bufread::ZlibDecoder::new(file);
        let mut reader = BufReader::new(reader);
        let mut buf = Vec::new();

        // read header
        reader.read_until(0u8, &mut buf)?;
        let obj_type = if buf.starts_with(b"blob ") {
            ObjectType::Blob
        } else if buf.starts_with(b"tree ") {
            ObjectType::Tree
        } else if buf.starts_with(b"commit ") {
            ObjectType::Commit
        } else {
            bail!("Invalid object type");
        };

        buf.clear();
        let _ = reader.read_to_end(&mut buf)?;

        Ok(Object {
            object_type: obj_type,
            content: buf.into(),
        })
    }

    pub fn get_object_path(&self, oid: ObjectId) -> PathBuf {
        let sha = oid.to_string();
        let (dirname, filename) = sha.split_at(2);
        self.git_dir
            .join(["objects", dirname, filename].iter().collect::<PathBuf>())
    }
}

pub struct Object {
    pub object_type: ObjectType,
    pub content: Bytes,
}

impl Object {
    pub fn blob(content: Vec<u8>) -> Self {
        Self {
            object_type: ObjectType::Blob,
            content: content.into(),
        }
    }

    pub fn tree(content: Vec<u8>) -> Self {
        Self {
            object_type: ObjectType::Tree,
            content: content.into(),
        }
    }

    pub fn commit(content: Vec<u8>) -> Self {
        Self {
            object_type: ObjectType::Commit,
            content: content.into(),
        }
    }

    pub fn as_commit(&self) -> Option<Commit> {
        if let ObjectType::Commit = self.object_type {
            let mut content = self.content.clone();
            Commit::parse(&mut content).ok()
        } else {
            None
        }
    }

    pub fn as_tree(&self) -> Option<Tree> {
        if let ObjectType::Tree = self.object_type {
            let mut content = self.content.clone();
            Some(Tree::parse(&mut content).expect("Failed to parse tree object"))
        } else {
            None
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ObjectType {
    Blob,
    Tree,
    Commit,
}

impl Display for ObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ObjectType::Blob => "blob",
            ObjectType::Tree => "tree",
            ObjectType::Commit => "commit",
        };
        write!(f, "{s}")
    }
}

impl FromStr for ObjectType {
    type Err = GitError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "blob" => Ok(Self::Blob),
            "tree" => Ok(Self::Tree),
            "commit" => Ok(Self::Commit),
            _ => Err(GitError::InvalidObjectType),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub tree: ObjectId,
    // pub parent: Vec<Sha>,
    // TODO author, commiter, message....
}

impl Commit {
    pub fn parse(bytes: &mut impl Buf) -> Result<Self> {
        let mut reader = bytes.reader();
        let tree = read_prefixed_line(&mut reader, "tree ")?;

        Ok(Self {
            tree: ObjectId::from_str(&tree)?,
        })
    }
}

fn read_prefixed_line(r: &mut impl BufRead, prefix: &str) -> Result<String> {
    let mut buf = String::new();
    r.read_line(&mut buf)?;
    let data = buf.strip_prefix(prefix).expect("invalid data").trim();
    Ok(data.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tree {
    entries: Vec<TreeEntry>,
}

impl Tree {
    pub fn parse(bytes: &mut impl Buf) -> Result<Self> {
        let mut entries = Vec::new();

        while bytes.has_remaining() {
            let entry = TreeEntry::parse(bytes)?;
            entries.push(entry);
        }

        Ok(Tree { entries })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    pub mode: String,
    pub object_type: ObjectType,
    pub name: String,
    pub sha1: ObjectId,
}

impl TreeEntry {
    pub fn parse(bytes: &mut impl Buf) -> Result<Self> {
        let mut buf = Vec::new();
        let mut reader = bytes.reader();

        let n = reader.read_until(b' ', &mut buf)?;
        let mode = String::from_utf8_lossy(&buf[0..n - 1]).to_string();
        buf.clear();

        let object_type = if mode.starts_with('1') {
            ObjectType::Blob
        } else {
            ObjectType::Tree
        };

        let n = reader.read_until(0, &mut buf)?;
        let name = String::from_utf8_lossy(&buf[0..n - 1]).to_string();
        buf.clear();

        let mut sha = [0u8; 20];
        reader.read_exact(&mut sha)?;
        let sha1 = ObjectId(sha);

        Ok(TreeEntry {
            mode,
            object_type,
            name,
            sha1,
        })
    }
}

impl Display for TreeEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{} {} {}\t{}",
            if self.object_type == ObjectType::Blob {
                "1"
            } else {
                "0"
            },
            self.mode,
            self.object_type,
            self.sha1,
            self.name,
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ObjectId([u8; 20]);

impl ObjectId {
    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self> {
        ensure!(bytes.as_ref().len() == 20);
        let b: [u8; 20] = bytes.as_ref().try_into()?;
        Ok(Self(b))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl FromStr for ObjectId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        ensure!(s.len() == 40);
        let mut bytes = [0u8; 20];
        hex::decode_to_slice(s, &mut bytes)?;
        Ok(Self(bytes))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ref {
    pub oid: ObjectId,
    pub name: String,
}

impl Ref {
    pub fn is_tag(&self) -> bool {
        self.name.starts_with("refs/tags")
    }

    pub fn is_peeled_tag(&self) -> bool {
        self.name.ends_with("^{}")
    }
}
