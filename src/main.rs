#[allow(unused_imports)]
use std::env;
#[allow(unused_imports)]
use std::fs;

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
}

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    let args = Cli::parse();
    match args.command {
        Commands::Init => git_init()?,
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
