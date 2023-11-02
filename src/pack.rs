//! Module to parse pack-files

use std::fmt::Display;
use std::io::Read;
use std::path::Path;

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::bufread::ZlibDecoder;

use crate::{GitRepo, Object, ObjectId};

pub fn parse_pack_from_file<P: AsRef<Path>>(file: P) -> Result<PackFile> {
    let mut bytes: Bytes = std::fs::read(file)?.into();
    PackFile::parse(&mut bytes)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackFile {
    pub header: PackHeader,
    pub objects: Vec<PackObject>,
}

impl PackFile {
    pub fn parse(bytes: &mut impl Buf) -> Result<Self> {
        // Read header
        let header = PackHeader::parse(bytes)?;
        let num_objs = header.num_objects;

        // Parse objects
        let mut objects = Vec::new();
        for _ in 0..num_objs {
            let obj = PackObject::parse(bytes)?;
            objects.push(obj);
        }

        Ok(PackFile { header, objects })
    }

    pub fn explode_into_repo(self, repo: &GitRepo) -> Result<()> {
        // TODO: implement support for packfiles directly, i.e:
        // - store the packfile in `.git/objects/packs/`
        // - generate a `.idx` file alongside it
        // - implement lookup of objects directly from the packfile
        let mut deltas = Vec::new();
        let mut count = 0;
        // Store full objects directly
        for entry in self.objects {
            let obj = match entry.object_type {
                PackObjectType::ObjCommit => Object::commit(entry.data.into()),
                PackObjectType::ObjTree => Object::tree(entry.data.into()),
                PackObjectType::ObjBlob => Object::blob(entry.data.into()),
                PackObjectType::ObjTag => {
                    // TODO: implement annotated tags
                    println!("Tag objects not implemented!");
                    continue;
                }
                PackObjectType::ObjOfsDelta(_) => {
                    deltas.push(entry);
                    continue;
                }
                PackObjectType::ObjRefDelta(_) => {
                    deltas.push(entry);
                    continue;
                }
            };
            repo.store_object(obj)?;
            count += 1;
        }
        println!("Exploded {count} objects");

        // now apply deltas
        println!("Processing deltas");
        let mut count = 0;
        for delta in deltas {
            let PackObjectType::ObjRefDelta(base) = delta.object_type else {
                println!("Error: unsupported delta type");
                continue;
            };

            let base_object = repo.get_object(base)?;
            let mut bytes = delta.data;
            let base_size = read_var_int(&mut bytes);
            assert_eq!(
                base_size as usize,
                base_object.content.len(),
                "Base size in delta doesn't match base object size"
            );
            let target_size = read_var_int(&mut bytes);
            let target_size = if target_size == 0 {
                0x10000
            } else {
                target_size
            };
            let base_data = base_object.content;
            let mut reconstructed_data = BytesMut::with_capacity(target_size as usize);
            // println!("sha={base}, base size={base_size}, target_size={target_size}");
            while bytes.has_remaining() {
                let instr = DeltaInstruction::parse(&mut bytes)?;
                match instr {
                    DeltaInstruction::Copy { size, offset } => {
                        reconstructed_data.put(&base_data[offset..][..size])
                    }
                    DeltaInstruction::Add { size } => {
                        reconstructed_data.put(bytes.copy_to_bytes(size))
                    }
                }
            }
            // println!("reconstructed object has size {}", reconstructed_data.len());
            assert_eq!(target_size as usize, reconstructed_data.len());
            let reconstructed_object = Object {
                object_type: base_object.object_type,
                content: reconstructed_data.freeze(),
            };
            let _reconstructed_sha = repo.store_object(reconstructed_object)?;
            // println!("Reconstructed object has sha {reconstructed_sha}");
            count += 1;
        }
        println!("Reconstructed {count} objects from deltas");
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PackHeader {
    pub sig: [u8; 4],
    pub version: u32,
    pub num_objects: u32,
}

impl PackHeader {
    pub fn parse(bytes: &mut impl Buf) -> Result<Self> {
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

        Ok(PackHeader {
            sig,
            version,
            num_objects: num_objs,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackObjectType {
    ObjCommit,
    ObjTree,
    ObjBlob,
    ObjTag,
    ObjOfsDelta(u64),
    ObjRefDelta(ObjectId),
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackObject {
    pub object_type: PackObjectType,
    pub data: Bytes,
}

impl PackObject {
    pub fn parse(bytes: &mut impl Buf) -> Result<Self> {
        let (typ, size) = read_type_and_var_int(bytes);
        let object_type = match typ {
            1 => PackObjectType::ObjCommit,
            2 => PackObjectType::ObjTree,
            3 => PackObjectType::ObjBlob,
            4 => PackObjectType::ObjTag,
            6 => {
                let ofs = read_var_int(bytes);
                PackObjectType::ObjOfsDelta(ofs)
            }
            7 => {
                let sha = bytes.copy_to_bytes(20);
                PackObjectType::ObjRefDelta(ObjectId::from_bytes(&sha)?)
            }
            _ => bail!("Invalid pack object type: {typ}"),
        };
        // println!("Found object with size {size}: {object_type}");
        let mut buf = Vec::with_capacity(size as usize);
        let mut reader = ZlibDecoder::new(bytes.reader());
        reader.read_to_end(&mut buf)?;

        Ok(PackObject {
            object_type,
            data: buf.into(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaInstruction {
    Copy { size: usize, offset: usize },
    Add { size: usize },
}

impl DeltaInstruction {
    pub fn parse(bytes: &mut impl Buf) -> Result<Self> {
        let instr = bytes.get_u8();
        // println!("instr={instr:08b}");
        if instr & 128 != 0 {
            // copy instruction
            let mut offset = 0u32;
            let mut size = 0u32;
            // decode offset and size
            // bits 0, 1, 2, 3 are offset
            for i in 0..4 {
                if instr & (1 << i) != 0 {
                    offset |= (bytes.get_u8() as u32) << (i * 8);
                }
            }
            // bits 4, 5, 6 are size
            for i in 4..7 {
                if instr & (1 << i) != 0 {
                    size |= (bytes.get_u8() as u32) << ((i - 4) * 8);
                }
            }
            let offset = offset as usize;
            let size = size as usize;
            // println!("Found copy instruction size={size}, offset={offset}");
            Ok(Self::Copy { size, offset })
        } else {
            // add instruction
            let size = (instr & 127) as usize;
            // println!("Found add instruction size={size}");
            assert!(size != 0, "Delta add instruction has zero size!");
            Ok(Self::Add { size })
        }
    }
}

/// Read a variable-length encoded integer
///
/// - The first bit of each byte indicates if another byte must be read (if 1, yes, if 0, we stop)
/// - For the first byte, the next 3 bits encode the object type, and the remaining 4 are part of
/// the integer
/// - For all subsequent bytes, the lower 7 bits are concatenated before the previous ones (i.e
/// each byte is more significant than the previous)
fn read_type_and_var_int(buf: &mut impl Buf) -> (u8, u64) {
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
pub fn read_var_int(buf: &mut impl Buf) -> u64 {
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
