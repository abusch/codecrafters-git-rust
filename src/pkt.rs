use std::fmt::Display;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub enum Pkt {
    Flush,
    Data(Bytes),
}

impl Pkt {
    pub fn data(data: impl Into<Bytes>) -> Self {
        Self::Data(data.into())
    }

    pub fn flush() -> Self {
        Self::Flush
    }

    pub fn is_flush(&self) -> bool {
        match self {
            Pkt::Flush => true,
            Pkt::Data(_) => false,
        }
    }

    pub fn as_bytes(self) -> Bytes {
        let mut buf = BytesMut::new();
        match self {
            Self::Flush => buf.put("0000".as_bytes()),
            Self::Data(pkt) => {
                buf.put(format!("{:04x}", pkt.len() + 4).as_bytes());
                buf.put(pkt);
            }
        }
        buf.freeze()
    }

    pub fn read_line(buf: &mut impl Buf) -> Result<Self> {
        let mut size = [0; 4];
        buf.copy_to_slice(&mut size);

        let pkt = if &size == b"0000" {
            Pkt::Flush
        } else {
            let size = hex::decode(size)?;
            let size = u16::from_be_bytes(size[0..2].try_into()?);
            let content = buf.copy_to_bytes(size as usize - 4);
            Pkt::data(content)
        };

        Ok(pkt)
    }
}

impl Display for Pkt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Pkt::Flush => writeln!(f, "0000"),
            Pkt::Data(pkt) => {
                write!(f, "{:04x}{}", pkt.len() + 4, String::from_utf8_lossy(pkt))
            }
        }
    }
}
