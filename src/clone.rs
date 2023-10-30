use std::{collections::HashSet, fmt::Display, fs::create_dir, ops::Deref, path::Path};

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use reqwest::{blocking::Client, header::CONTENT_TYPE, Url};

use crate::{git::Object, git_init, pack};

pub fn clone<P: AsRef<Path>>(url: Url, dir: P) -> Result<()> {
    println!("Cloning {url} into {}", dir.as_ref().display());
    let client = GitClient::new(url);

    // Discover refs
    let (_capabilities, advertised) = client.discover_refs()?;
    // For now only ask for the first ref, which should be HEAD
    // TODO: ask for all the refs
    let sha = advertised
        .iter()
        .next()
        .expect("At least 1 ref to be advertised");

    // Fetch packfile
    let mut pack_data = client.request_pack(sha)?;
    let pack_file = pack::parse_pack(&mut pack_data)?;
    println!("Got packfile: {:?}", pack_file.header);

    // create the requested directory and run `git init`
    let dir = dir.as_ref();
    create_dir(dir)?;
    git_init(dir)?;

    // explode packfile into loose objects
    // TODO: implement support for packfiles directly, i.e:
    // - store the packfile in `.git/objects/packs/`
    // - generate a `.idx` file alongside it
    // - implement lookup of objects directly from the packfile
    let mut deltas = Vec::new();
    for entry in pack_file.objects {
        let obj = match entry.object_type {
            pack::PackObjectType::ObjCommit => Object::commit(entry.data.into()),
            pack::PackObjectType::ObjTree => Object::tree(entry.data.into()),
            pack::PackObjectType::ObjBlob => Object::blob(entry.data.into()),
            pack::PackObjectType::ObjTag => {
                println!("Tag objects not implemented!");
                continue;
            }
            pack::PackObjectType::ObjOfsDelta(_) => {
                deltas.push(entry);
                continue;
            }
            pack::PackObjectType::ObjRefDelta(_) => {
                deltas.push(entry);
                continue;
            }
        };
        obj.write_to_file(dir)?;
    }

    Ok(())
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

    pub fn discover_refs(&self) -> Result<(HashSet<String>, HashSet<String>)> {
        // let url = self.repo_url.join("info/refs").unwrap();
        let url = format!("{}/info/refs", self.repo_url);

        println!("Making request to discover refs to {url}...");
        let mut res = self
            .client
            .get(url)
            .query(&[("service", "git-upload-pack")])
            .send()?
            .error_for_status()?
            .bytes()?;

        println!("done");
        let PktLine::Pkt(first) = read_pkt_line(&mut res)? else {
            bail!("Invalid response")
        };
        if !first.starts_with(b"# service=git-upload-pack") {
            bail!("Invalid response")
        }
        if !read_pkt_line(&mut res)?.is_flush() {
            bail!("Expected flush packet");
        }

        let mut capabilities_set = HashSet::new();
        let mut advertised = HashSet::new();
        loop {
            match read_pkt_line(&mut res)? {
                PktLine::Flush => break,
                PktLine::Pkt(pkt) => {
                    println!("Got ref: {}", String::from_utf8_lossy(&pkt));
                    // first 40 chars are the sha1
                    let sha = pkt.slice(0..40);
                    advertised.insert(String::from_utf8_lossy(&sha).to_string());
                    // after that and a space, is the ref name
                    let mut ref_name = pkt.slice(41..);
                    if let Some(idx) = ref_name.iter().position(|b| *b == 0) {
                        // The first ref contains the list of capabilities
                        let mut capabilities = ref_name.split_off(idx);
                        // Remove the null byte
                        capabilities.get_u8();
                        capabilities_set = capabilities
                            .split(|b| *b == b' ')
                            .map(|s| String::from_utf8_lossy(s).to_string())
                            .collect();
                    }
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
            PktLine::pkt(format!("want {sha} multi_ack side-band-64k\n")),
            PktLine::Flush,
            PktLine::pkt("done\n"),
        ];

        let mut buf = BytesMut::new();
        for pkt in msg {
            buf.put(pkt.as_bytes());
        }
        let buf = buf.freeze();
        println!("Sending request:\n{}", String::from_utf8_lossy(&buf));

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
                PktLine::Flush => break,
                PktLine::Pkt(Pkt(mut bytes)) => {
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

pub fn read_pkt_line(buf: &mut impl Buf) -> Result<PktLine> {
    let mut size = [0; 4];
    buf.copy_to_slice(&mut size);

    let pkt = if &size == b"0000" {
        PktLine::Flush
    } else {
        let size = hex::decode(size)?;
        let size = u16::from_be_bytes(size[0..2].try_into()?);
        let content = buf.copy_to_bytes(size as usize - 4);
        PktLine::pkt(content)
    };

    Ok(pkt)
}

pub enum PktLine {
    Flush,
    Pkt(Pkt),
}

impl PktLine {
    pub fn pkt(data: impl Into<Bytes>) -> Self {
        Self::Pkt(Pkt(data.into()))
    }

    pub fn flush() -> Self {
        Self::Flush
    }

    pub fn is_flush(&self) -> bool {
        match self {
            PktLine::Flush => true,
            PktLine::Pkt(_) => false,
        }
    }

    pub fn as_bytes(self) -> Bytes {
        let mut buf = BytesMut::new();
        match self {
            Self::Flush => buf.put("0000".as_bytes()),
            Self::Pkt(pkt) => {
                buf.put(format!("{:04x}", pkt.0.len() + 4).as_bytes());
                buf.put(pkt.0);
            }
        }
        buf.freeze()
    }
}

impl Display for PktLine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PktLine::Flush => writeln!(f, "0000"),
            PktLine::Pkt(pkt) => {
                write!(
                    f,
                    "{:04x}{}",
                    pkt.0.len() + 4,
                    String::from_utf8_lossy(&pkt.0)
                )
            }
        }
    }
}

pub struct Pkt(Bytes);

impl Deref for Pkt {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
