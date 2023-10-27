#[allow(unused_imports)]
use std::env;
#[allow(unused_imports)]
use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;

use anyhow::bail;
use anyhow::{Context, Result};
use clap::Parser;
use clap::Subcommand;
use flate2::Compression;
use sha1::{Digest, Sha1};

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
    HashFile {
        #[arg(short = 'w', value_name = "file")]
        file: String,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Commands::Init => git_init()?,
        Commands::CatFile { sha } => git_cat_file(sha)?,
        Commands::HashFile { file } => git_hash_file(file)?,
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
    let reader = flate2::bufread::ZlibDecoder::new(file);
    let mut reader = BufReader::new(reader);
    let mut stdout = io::stdout().lock();
    let mut buf = Vec::with_capacity(64);

    // Read header: everything until the null byte
    reader
        .read_until(0u8, &mut buf)
        .context("Reading blob header")?;
    assert!(buf.starts_with(b"blob "), "Invalid blob file");
    // Write content of the blob to stdout
    io::copy(&mut reader, &mut stdout).context("Writing blob content to stdout")?;

    Ok(())
}

pub fn git_hash_file(file: String) -> Result<()> {
    let file_content = fs::read(file).context("Reading file to hash")?;
    let file_size = file_content.len();

    let mut hasher = Sha1::new();
    hasher.update(&file_content);
    let result = hasher.finalize();
    let sha1 = hex::encode(result);
    println!("sha1 = {sha1}");

    let (dir_name, file_name) = sha1.split_at(2);
    let blob_dir: PathBuf = [".git", "objects", dir_name].iter().collect();
    match blob_dir.try_exists() {
        // dir already exits
        Ok(true) => (),
        // dir doesn't exist: created it
        Ok(false) => {
            println!("Creating blob directory {}", blob_dir.display());
            fs::create_dir(&blob_dir).context("Creating blob directory")?;
        }
        Err(e) => bail!(e),
    }

    let blob_file_name = blob_dir.join(file_name);
    // Create blob file
    let mut blob_file = fs::File::options()
        .create(true)
        .write(true)
        .open(blob_file_name)
        .context("Creating blob file")?;
    // Wrap blob file in zlib encoder
    let mut compressed_content =
        flate2::write::ZlibEncoder::new(&mut blob_file, Compression::fast());

    let header = format!("blob {file_size}\0");
    // Write header
    compressed_content
        .write_all(header.as_bytes())
        .context("Writing blob header")?;
    // Write content
    compressed_content
        .write_all(&file_content)
        .context("Writing blob content")?;
    // Finalize stream
    compressed_content
        .finish()
        .context("Finalizing compressed stream")?;

    Ok(())
}
