#[allow(unused_imports)]
use std::env;
#[allow(unused_imports)]
use std::fs;
use std::io::BufReader;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use clap::Subcommand;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialze a new git repo
    Init,
    CatFile {
        #[arg(short = 'p', value_name = "blob_sha")]
        sha: String,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Commands::Init => git_init()?,
        Commands::CatFile { sha } => git_cat_file(sha)?,
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
    let (dirname, filename) = sha.split_at(2);
    let path: PathBuf = [".git", "objects", dirname, filename].iter().collect();

    let file = fs::File::open(path).context("Failed to open blob file")?;
    let file = BufReader::new(file);
    let mut reader = flate2::bufread::ZlibDecoder::new(file);
    let mut stdout = std::io::stdout().lock();

    // Write content of the blob to stdout
    std::io::copy(&mut reader, &mut stdout).context("Writing blob content to stdout")?;

    Ok(())
}
