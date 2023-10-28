use std::{
    fmt::Display,
    fs,
    io::BufRead,
    io::Read,
    io::{BufReader, Write},
    path::PathBuf,
    str::FromStr,
};

use bytes::Bytes;
use flate2::Compression;
use sha1::{Digest, Sha1};

#[derive(Debug, thiserror::Error)]
pub enum GitError {
    #[error("Invalid object type")]
    InvalidObjectType,
    #[error(transparent)]
    Io(#[from] std::io::Error),
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

    pub fn read_from_file(sha: &str) -> Result<Self, GitError> {
        let path = get_object_path(sha);

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

    pub fn write_to_file(self) -> Result<String, GitError> {
        let header = format!("{} {}\0", self.object_type, self.content.len());

        // compute SHA1
        let mut hasher = Sha1::new();
        hasher.update(header.as_bytes());
        hasher.update(&self.content);
        let result = hasher.finalize();
        let sha1 = hex::encode(result);

        let path = get_object_path(&sha1);
        let dir = path.parent().expect("object path to have a parent");
        match dir.try_exists() {
            // dir already exits
            Ok(true) => (),
            // dir doesn't exist: created it
            Ok(false) => {
                fs::create_dir(dir)?;
            }
            Err(e) => return Err(e.into()),
        }
        // Create objectfile
        let mut object_file = fs::File::options().create(true).write(true).open(path)?;
        // Wrap object file in zlib encoder
        let mut writer = flate2::write::ZlibEncoder::new(&mut object_file, Compression::fast());

        // write header
        writer.write_all(header.as_bytes())?;
        // write content
        writer.write_all(&self.content)?;

        Ok(sha1)
    }
}

pub fn get_object_path(sha: &str) -> PathBuf {
    let (dirname, filename) = sha.split_at(2);
    [".git", "objects", dirname, filename].iter().collect()
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
