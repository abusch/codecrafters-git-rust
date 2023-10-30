//! Module to parse pack-files

use std::{fmt::Display, io::Read, path::Path};

use anyhow::{bail, Result};
use bytes::{Buf, Bytes};
use flate2::bufread::ZlibDecoder;

pub fn parse_pack_from_file<P: AsRef<Path>>(file: P) -> Result<PackFile> {
    let mut bytes: Bytes = std::fs::read(file)?.into();
    parse_pack(&mut bytes)
}

pub fn parse_pack(bytes: &mut impl Buf) -> Result<PackFile> {
    // Read header
    let mut sig = [0; 4];
    bytes.copy_to_slice(&mut sig);
    if &sig != b"PACK" {
        bail!("Invalid pack file");
    }

    let version = bytes.get_u32();
    if version != 2 && version != 3 {
        bail!("Invalid pack file version number: {version}");
    }

    let num_objs = bytes.get_u32();

    let header = PackHeader {
        sig,
        version,
        num_objects: num_objs,
    };

    // Parse objects
    println!("Parsing objects...");
    let mut objects = Vec::new();
    for _ in 0..num_objs {
        let (typ, size) = read_var_int(bytes);
        let object_type = match typ {
            1 => PackObjectType::ObjCommit,
            2 => PackObjectType::ObjTree,
            3 => PackObjectType::ObjBlob,
            4 => PackObjectType::ObjTag,
            6 => {
                let ofs = read_var_offset(bytes);
                PackObjectType::ObjOfsDelta(ofs)
            }
            7 => {
                let sha = bytes.copy_to_bytes(20);
                let sha = hex::encode(&sha);
                PackObjectType::ObjRefDelta(sha)
            }
            _ => bail!("Invalid pack object type: {typ}"),
        };
        println!("Found object with size {size}: {object_type}");
        let mut buf = Vec::with_capacity(size as usize);
        let mut reader = ZlibDecoder::new(bytes.reader());
        reader.read_to_end(&mut buf)?;

        objects.push(PackObject {
            object_type,
            data: buf.into(),
        });
    }

    Ok(PackFile { header, objects })
}

pub struct PackFile {
    pub header: PackHeader,
    pub objects: Vec<PackObject>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PackHeader {
    pub sig: [u8; 4],
    pub version: u32,
    pub num_objects: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackObjectType {
    ObjCommit,
    ObjTree,
    ObjBlob,
    ObjTag,
    ObjOfsDelta(u64),
    ObjRefDelta(String),
}

impl Display for PackObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackObjectType::ObjCommit => write!(f, "OBJ_COMMIT"),
            PackObjectType::ObjTree => write!(f, "OBJ_TREE"),
            PackObjectType::ObjBlob => write!(f, "OBJ_BLOB"),
            PackObjectType::ObjTag => write!(f, "OBJ_TAG"),
            PackObjectType::ObjOfsDelta(ofs) => write!(f, "OBJ_OFS_DELTA({ofs})"),
            PackObjectType::ObjRefDelta(name) => write!(f, "OBJ_REF_DELTA({name})"),
        }
    }
}

// impl TryFrom<u8> for PackObjectType {
//     type Error = anyhow::Error;
//
//     fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
//         Ok(match value {
//             1 => Self::ObjCommit,
//             2 => Self::ObjTree,
//             3 => Self::ObjBlob,
//             4 => Self::ObjTag,
//             6 => Self::ObjOfsDelta,
//             7 => Self::ObjRefDelta,
//             _ => bail!("Invalid pack object type: {value}"),
//         })
//     }
// }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackObject {
    pub object_type: PackObjectType,
    pub data: Bytes,
}

/// Read a variable-length encoded integer
///
/// - The first bit of each byte indicates if another byte must be read (if 1, yes, if 0, we stop)
/// - For the first byte, the next 3 bits encode the object type, and the remaining 4 are part of
/// the integer
/// - For all subsequent bytes, the lower 7 bits are concatenated before the previous ones (i.e
/// each byte is more significant than the previous)
fn read_var_int(buf: &mut impl Buf) -> (u8, u64) {
    let mut res = 0u64;
    let mut shift_offset = 0;
    let mut typ = 0;
    loop {
        let b = buf.get_u8();
        if shift_offset == 0 {
            // first byte:
            // first 3 bits (excluding the MSB) are the type...
            typ = (b & 0b01110000) >> 4;
            // ... following 4 bits are part of the size
            res = (b & 0b00001111) as u64;
            shift_offset = 4;
        } else {
            // subsequent bytes: add the lower 7 bits to the size
            res |= ((b & 0b01111111) as u64) << shift_offset;
            shift_offset += 7;
        }

        if b <= 127 {
            break;
        }
    }

    (typ, res)
}

/// Read a variable-length encoded offset
///
/// Same as [read_var_int] except without the type.
fn read_var_offset(buf: &mut impl Buf) -> u64 {
    let mut res = 0u64;
    let mut shift_offset = 0;
    loop {
        let b = buf.get_u8();
        // add the lower 7 bits to the result
        res |= ((b & 0b01111111) as u64) << shift_offset;
        shift_offset += 7;

        if b < 127 {
            break;
        }
    }

    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        parse_pack_from_file("/Users/abusch/code/rust/yew/.git/objects/pack/pack-0eda438f06d4f311b4005e3f2511dce1c9a385de.pack").unwrap();
    }
}
