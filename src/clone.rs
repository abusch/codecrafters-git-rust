use std::{collections::HashSet, fmt::Display, ops::Deref};

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use reqwest::{blocking::Client, header::CONTENT_TYPE, Url};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sha(String);

impl Sha {
    pub fn new(sha: String) -> Self {
        assert_eq!(sha.len(), 40);
        Self(sha)
    }

    pub fn from_bytes(bytes: [u8; 20]) -> Self {
        Self(hex::encode(bytes))
    }
}

impl Deref for Sha {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

impl Display for Sha {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ref {
    pub sha: Sha,
    pub name: String,
}

impl Ref {
    pub fn is_tag(&self) -> bool {
        self.name.starts_with("refs/tags")
    }

    pub fn is_peeled_tag(&self) -> bool {
        self.name.ends_with("^{}")
    }
}

pub struct GitClient {
    client: Client,
    repo_url: Url,
}

impl GitClient {
    pub fn new(url: Url) -> Self {
        Self {
            client: Client::new(),
            repo_url: url,
        }
    }

    pub fn discover_refs(&self) -> Result<(HashSet<String>, Vec<Ref>)> {
        let url = format!("{}/info/refs", self.repo_url);

        let mut res = self
            .client
            .get(url)
            .query(&[("service", "git-upload-pack")])
            .send()?
            .error_for_status()?
            .bytes()?;

        let Pkt::Data(first) = read_pkt_line(&mut res)? else {
            bail!("Invalid response")
        };
        if !first.starts_with(b"# service=git-upload-pack") {
            bail!("Invalid response")
        }
        if !read_pkt_line(&mut res)?.is_flush() {
            bail!("Expected flush packet");
        }

        let mut capabilities_set = HashSet::new();
        let mut advertised = Vec::new();
        loop {
            match read_pkt_line(&mut res)? {
                Pkt::Flush => break,
                Pkt::Data(pkt) => {
                    // println!("Got ref: {}", String::from_utf8_lossy(&pkt));
                    // first 40 chars are the sha1
                    let sha = pkt.slice(0..40);
                    // after that and a space, is the ref name
                    let mut ref_name = pkt.slice(41..);
                    if let Some(idx) = ref_name.iter().position(|b| *b == 0) {
                        // The first ref contains the list of capabilities
                        let mut capabilities = ref_name.split_off(idx);
                        // Remove the null byte
                        capabilities.get_u8();
                        capabilities_set = capabilities
                            .split(|b| *b == b' ')
                            .map(|s| String::from_utf8_lossy(s).trim().to_string())
                            .collect();
                    }
                    advertised.push(Ref {
                        sha: Sha(String::from_utf8_lossy(&sha).to_string()),
                        name: String::from_utf8_lossy(&ref_name).trim().to_string(),
                    });
                }
            }
        }
        println!("capabilities = {capabilities_set:?}");
        println!("advertised refs = {advertised:?}");

        Ok((capabilities_set, advertised))
    }

    pub fn request_pack(&self, sha: &str) -> Result<Bytes> {
        // TODO: implement protocol v2
        let msg = vec![
            // capabilities: include 'side-band-64k' to get progress info, but don't include
            // 'ofs_delta' to simplify things.
            // TODO: support `ofs_delta`
            Pkt::data(format!("want {sha} multi_ack side-band-64k\n")),
            Pkt::Flush,
            Pkt::data("done\n"),
        ];

        let mut buf = BytesMut::new();
        for pkt in msg {
            buf.put(pkt.as_bytes());
        }
        let buf = buf.freeze();
        // println!("Sending request:\n{}", String::from_utf8_lossy(&buf));

        // TODO: don't read the whole packfile into memory: switch to reqwest's async client and
        // stream to a temp file on disk
        let mut bytes = self
            .client
            .post(format!("{}/git-upload-pack", self.repo_url))
            .body(buf)
            .header(CONTENT_TYPE, "application/x-git-upload-pack-request")
            .send()?
            .error_for_status()?
            .bytes()?;

        let mut pack_content = BytesMut::new();
        loop {
            let pkt = read_pkt_line(&mut bytes)?;
            match pkt {
                Pkt::Flush => break,
                Pkt::Data(mut bytes) => {
                    if bytes.starts_with(b"NAK") {
                        println!("Got NAK");
                        continue;
                    }
                    let first = bytes.get_u8();
                    // demux
                    match first {
                        // stream 1 is the pack data
                        1 => pack_content.put(bytes),
                        // stream 2 is progress information to be displayed on stderr
                        2 => eprint!("remote: {}", String::from_utf8_lossy(&bytes)),
                        // TODO: handle stream 3 (=error)
                        _ => bail!("Invalid stream number: {first}"),
                    }
                }
            }
        }

        Ok(pack_content.freeze())
    }
}

pub fn read_pkt_line(buf: &mut impl Buf) -> Result<Pkt> {
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
