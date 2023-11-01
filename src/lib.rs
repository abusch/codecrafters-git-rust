use std::fmt::Display;
use std::fs::{self, create_dir, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::ops::Deref;
use std::os::unix::prelude::{OsStrExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, ensure, Context, Result};
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

    pub fn cat_file(&self, sha: String) -> Result<()> {
        let object = self.get_object(&sha)?;
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

    pub fn read_tree(&self, sha: String, names_only: bool) -> Result<()> {
        let object = self.get_object(&sha)?;

        ensure!(
            object.object_type == ObjectType::Tree,
            "Object is not a tree"
        );
        let mut reader = object.content.reader();

        let mut tree_entries = Vec::new();
        loop {
            let mut buf = Vec::new();
            let n = reader.read_until(b' ', &mut buf)?;
            if n == 0 {
                // We've reached EOF
                break;
            }
            // Remove the trailing space we just read
            buf.pop();
            let object_type = if buf[0] == b'1' {
                buf.remove(0);
                ObjectType::Blob
            } else {
                ObjectType::Tree
            };
            let mode = String::from_utf8(buf).context("Invalid tree mode")?;

            let mut name = Vec::new();
            let _ = reader.read_until(0, &mut name)?;
            // Remove trailing null byte
            name.pop();

            let mut sha = [0u8; 20];
            reader.read_exact(&mut sha)?;
            let sha_ascii = hex::encode(sha);
            tree_entries.push(TreeEntry {
                mode,
                object_type,
                name,
                sha1: sha_ascii,
            });
        }

        for entry in tree_entries {
            if names_only {
                println!("{}", String::from_utf8_lossy(&entry.name));
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

    fn write_tree_dir<P: AsRef<Path>>(&self, path: P) -> Result<String> {
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
                    name: e.file_name().as_bytes().to_vec(),
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
                    name: e.file_name().as_bytes().to_vec(),
                });
            } else {
                // symlink
                unimplemented!()
            }
        }

        // Sort entries by name
        tree_entries.sort_by(|a, b| a.name.as_slice().cmp(b.name.as_slice()));

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
            buf.put(entry.name.as_slice());
            buf.put_u8(0);
            let sha_binary = hex::decode(entry.sha1)?;
            buf.put(sha_binary.as_slice());
        }

        let tree_object = Object::tree(buf.into());
        let sha1 = self.store_object(tree_object)?;

        Ok(sha1)
    }

    pub fn commit_tree(&self, tree_sha: String, parent: String, message: String) -> Result<()> {
        let mut buf = String::new();
        let now = SystemTime::now();
        let now_seconds = now.duration_since(UNIX_EPOCH)?.as_secs();

        buf.push_str(&format!("tree {tree_sha}\n"));
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
        let mut pack_data = client.request_pack(&reference.sha)?;
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
            file.write_all(format!("{}\n", tag.sha).as_bytes())?;
        }
        for branch in branches {
            let parts = branch.name.split('/').collect::<Vec<_>>();
            let branch_name = parts.last().expect("Invalid branch name");
            println!("\tCreating branch {}", branch_name);
            let mut file = File::create(branches_dir.join(branch_name))?;
            file.write_all(format!("{}\n", branch.sha).as_bytes())?;
        }

        // set HEAD ref to HEAD of remote
        let remote_head = advertised
            .iter()
            .find(|r| r.name == "HEAD")
            .ok_or(anyhow!("The remote didn't send us a HEAD reference"))?;
        let remote_head_target = advertised
            .iter()
            .find(|r| r.name != "HEAD" && r.sha == remote_head.sha)
            .ok_or(anyhow!("No ref found as target of remote HEAD"))?;
        // Create local branch for HEAD to point to
        fs::write(
            repo.git_dir.join(&remote_head_target.name),
            format!("{}\n", remote_head_target.sha),
        )?;
        // Point HEAD to that local branch
        fs::write(
            repo.git_dir.join("HEAD"),
            format!("ref: {}\n", remote_head_target.name),
        )?;

        // checkout HEAD

        Ok(repo)
    }

    pub fn store_object(&self, object: Object) -> Result<String, GitError> {
        let header = format!("{} {}\0", object.object_type, object.content.len());

        // compute SHA1
        let mut hasher = Sha1::new();
        hasher.update(header.as_bytes());
        hasher.update(&object.content);
        let result = hasher.finalize();
        let sha1 = hex::encode(result);

        let path = self.get_object_path(&sha1);
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

        Ok(sha1)
    }

    pub fn get_object(&self, sha: &str) -> Result<Object, GitError> {
        let path = self.get_object_path(sha);

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
            return Err(GitError::InvalidObjectType);
        };

        buf.clear();
        let _ = reader.read_to_end(&mut buf)?;

        Ok(Object {
            object_type: obj_type,
            content: buf.into(),
        })
    }

    pub fn get_object_path(&self, sha: &str) -> PathBuf {
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
pub struct TreeEntry {
    pub mode: String,
    pub object_type: ObjectType,
    pub name: Vec<u8>,
    pub sha1: String,
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
            String::from_utf8_lossy(&self.name)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sha(String);

impl Sha {
    pub fn new(sha: String) -> Self {
        assert_eq!(sha.len(), 40);
        Self(sha)
    }

    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(hex::encode(bytes))
    }
}

impl Deref for Sha {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

impl Display for Sha {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ref {
    pub sha: Sha,
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
