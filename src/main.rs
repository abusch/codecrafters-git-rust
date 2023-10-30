use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::{fs::PermissionsExt, prelude::OsStrExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::ensure;
use anyhow::{Context, Result};
use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use clap::Parser;
use clap::Subcommand;
use reqwest::Url;

use crate::git::Object;
use crate::git::ObjectType;
use crate::git::TreeEntry;

pub mod clone;
pub mod git;
pub mod pack;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize a new git repo
    Init,
    CatFile {
        #[arg(short = 'p', value_name = "blob_sha")]
        sha: String,
    },
    HashObject {
        #[arg(short = 'w', value_name = "file")]
        file: String,
    },
    LsTree {
        #[arg(long)]
        name_only: bool,
        sha: String,
    },
    WriteTree,
    CommitTree {
        #[arg(short, long)]
        parent: String,
        #[arg(short, long)]
        message: String,
        tree_sha: String,
    },
    Clone {
        url: Url,
        dir: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let cwd = std::env::current_dir()?;
    match args.command {
        Commands::Init => git_init(cwd)?,
        Commands::CatFile { sha } => git_cat_file(sha)?,
        Commands::HashObject { file } => git_hash_object(cwd, file)?,
        Commands::LsTree { name_only, sha } => read_tree(sha, name_only)?,
        Commands::WriteTree => write_tree()?,
        Commands::CommitTree {
            parent,
            message,
            tree_sha,
        } => commit_tree(cwd, tree_sha, parent, message)?,
        Commands::Clone { url, dir } => clone::clone(url, dir)?,
    }

    Ok(())
}

pub fn git_init<P: AsRef<Path>>(repo: P) -> Result<()> {
    let repo = repo.as_ref();
    fs::create_dir(repo.join(".git")).context("Creating .git directory")?;
    fs::create_dir(repo.join(".git/objects")).context("Creating .git/objects directory")?;
    fs::create_dir(repo.join(".git/refs")).context("Creating .git/refs directory")?;
    fs::write(repo.join(".git/HEAD"), "ref: refs/heads/master\n")
        .context("creating .git/HEAD file")?;
    println!("Initialized git directory");

    Ok(())
}

pub fn git_cat_file(sha: String) -> Result<()> {
    let object = Object::read_from_file(&sha)?;
    let mut stdout = io::stdout().lock();

    // Write content of the blob to stdout
    stdout.write_all(&object.content)?;

    Ok(())
}

pub fn git_hash_object<P: AsRef<Path>>(repo: P, file: String) -> Result<()> {
    let file_content = fs::read(file).context("Reading file to hash")?;

    let object = Object {
        object_type: ObjectType::Blob,
        content: file_content.into(),
    };

    let sha = object.write_to_file(repo)?;
    println!("{sha}");

    Ok(())
}

pub fn read_tree(sha: String, names_only: bool) -> Result<()> {
    let object = Object::read_from_file(&sha)?;

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

pub fn write_tree() -> Result<()> {
    let sha = write_tree_dir(".")?;

    println!("{sha}");

    Ok(())
}

pub fn write_tree_dir<P: AsRef<Path>>(path: P) -> Result<String> {
    let path = path.as_ref();
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
            let tree_sha = write_tree_dir(e.path())?;
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
            let file_sha = object.write_to_file(path)?;
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
    let sha1 = tree_object.write_to_file(path)?;

    Ok(sha1)
}

pub fn commit_tree<P: AsRef<Path>>(
    repo: P,
    tree_sha: String,
    parent: String,
    message: String,
) -> Result<()> {
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
    let sha = object.write_to_file(repo)?;

    println!("{sha}");

    Ok(())
}
