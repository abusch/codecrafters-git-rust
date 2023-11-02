use std::collections::HashSet;

use anyhow::{bail, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use reqwest::{blocking::Client, header::CONTENT_TYPE, Url};

use crate::{pkt::Pkt, ObjectId};

use super::Ref;

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

        let Pkt::Data(first) = Pkt::read_line(&mut res)? else {
            bail!("Invalid response")
        };
        if !first.starts_with(b"# service=git-upload-pack") {
            bail!("Invalid response")
        }
        if !Pkt::read_line(&mut res)?.is_flush() {
            bail!("Expected flush packet");
        }

        let mut capabilities_set = HashSet::new();
        let mut advertised = Vec::new();
        loop {
            match Pkt::read_line(&mut res)? {
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
                        oid: String::from_utf8_lossy(&sha).parse()?,
                        name: String::from_utf8_lossy(&ref_name).trim().to_string(),
                    });
                }
            }
        }
        println!("capabilities = {capabilities_set:?}");
        println!("advertised refs = {advertised:?}");

        Ok((capabilities_set, advertised))
    }

    pub fn request_pack(&self, oid: ObjectId) -> Result<Bytes> {
        // TODO: implement protocol v2
        let msg = vec![
            // capabilities: include 'side-band-64k' to get progress info, but don't include
            // 'ofs_delta' to simplify things.
            // TODO: support `ofs_delta`
            Pkt::data(format!("want {oid} multi_ack side-band-64k\n")),
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
            let pkt = Pkt::read_line(&mut bytes)?;
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
