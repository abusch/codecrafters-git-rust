use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use git_starter_rust::GitRepo;
use git_starter_rust::ObjectId;
use reqwest::Url;

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
        sha: ObjectId,
    },
    HashObject {
        #[arg(short = 'w', value_name = "file")]
        file: String,
    },
    LsTree {
        #[arg(long)]
        name_only: bool,
        sha: ObjectId,
    },
    WriteTree,
    CommitTree {
        #[arg(short, long)]
        parent: ObjectId,
        #[arg(short, long)]
        message: String,
        tree_sha: ObjectId,
    },
    Clone {
        url: Url,
        dir: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let cwd = std::env::current_dir()?;
    let repo = GitRepo::new(cwd);
    match args.command {
        Commands::Init => repo.init()?,
        Commands::CatFile { sha } => repo.cat_file(sha)?,
        Commands::HashObject { file } => repo.hash_object(file)?,
        Commands::LsTree { name_only, sha } => repo.read_tree(sha, name_only)?,
        Commands::WriteTree => repo.write_tree()?,
        Commands::CommitTree {
            parent,
            message,
            tree_sha,
        } => repo.commit_tree(tree_sha, parent, message)?,
        Commands::Clone { url, dir } => {
            GitRepo::clone(url, dir)?;
        }
    }

    Ok(())
}
