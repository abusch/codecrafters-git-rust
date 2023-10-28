#[allow(unused_imports)]
use std::env;
#[allow(unused_imports)]
use std::fs;
use std::io::Read;
use std::io::{self, BufRead, Write};

use anyhow::ensure;
use anyhow::{Context, Result};
use bytes::Buf;
use clap::Parser;
use clap::Subcommand;

use crate::git::Object;
use crate::git::ObjectType;
use crate::git::TreeEntry;

pub mod git;

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
}

fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Commands::Init => git_init()?,
        Commands::CatFile { sha } => git_cat_file(sha)?,
        Commands::HashObject { file } => git_hash_object(file)?,
        Commands::LsTree { name_only, sha } => read_tree(sha, name_only)?,
    }

    Ok(())
}

pub fn git_init() -> Result<()> {
    fs::create_dir(".git").context("Creating .git directory")?;
    fs::create_dir(".git/objects").context("Creating .git/objects directory")?;
    fs::create_dir(".git/refs").context("Creating .git/refs directory")?;
    fs::write(".git/HEAD", "ref: refs/heads/master\n").context("creating .git/HEAD file")?;
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

pub fn git_hash_object(file: String) -> Result<()> {
    let file_content = fs::read(file).context("Reading file to hash")?;

    let object = Object {
        object_type: ObjectType::Blob,
        content: file_content.into(),
    };

    let sha = object.write_to_file()?;
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
