//! Server-side encryption (`s3-sse`): AES256 envelope scheme.
//!
//! Per-object random DEK (AES-256-GCM, 64 KiB chunks), wrapped by the gateway
//! master key; frames are self-describing blobs to the engine — the store
//! layer never sees plaintext or key material. Red line: corruption is loud
//! (`InternalError`), never silently substituted.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use sha2::{Digest, Sha256};

use crate::error::S3Error;

/// Frame magic; versioned so future layouts can diverge.
pub const MAGIC: &[u8; 8] = b"ANTFSSE1";
/// Frame-documented chunk size (bytes of plaintext per GCM chunk).
pub const CHUNK_SIZE: usize = 64 * 1024;
/// Master key length (AES-256).
pub const KEY_LEN: usize = 32;
/// GCM nonce/tag sizes.
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
/// key_id = 8 bytes of sha256(master) as 16 hex chars.
pub const KEY_ID_LEN: usize = 16;

/// Loaded master key + derived id.
pub struct MasterKey {
    key: [u8; KEY_LEN],
    key_id: String,
}

impl MasterKey {
    /// Load from a file containing 64 hex chars. Fails loud on shape errors.
    pub fn load(path: &str) -> Result<MasterKey, S3Error> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| S3Error::InternalError(format!("sse master key file `{path}`: {e}")))?;
        let hex_strtrim = raw.trim();
        let bytes = hex::decode(hex_strtrim).map_err(|_| {
            S3Error::InternalError("sse master key file must be 64 hex characters".into())
        })?;
        if bytes.len() != KEY_LEN {
            return Err(S3Error::InternalError(format!(
                "sse master key must decode to {KEY_LEN} bytes, got {}",
                bytes.len()
            )));
        }
        let mut key = [0u8; KEY_LEN];
        key.copy_from_slice(&bytes);
        let mut id = [0u8; 8];
        let d = Sha256::digest(key);
        id.copy_from_slice(&d[..8]);
        Ok(MasterKey {
            key,
            key_id: hex::encode(id),
        })
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    fn cipher(&self) -> Aes256Gcm {
        Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.key))
    }
}

impl MasterKey {
    #[cfg(test)]
    fn test_new() -> MasterKey {
        let key = [0x42u8; KEY_LEN];
        let mut id = [0u8; 8];
        let d = Sha256::digest(key);
        id.copy_from_slice(&d[..8]);
        MasterKey {
            key,
            key_id: hex::encode(id),
        }
    }
}

fn fill_random(buf: &mut [u8]) {
    getrandom::getrandom(buf).expect("OS entropy is available on all supported platforms (Unix)");
}

/// Row descriptor persisted in `named_objects.sse` (NULL = plaintext).
pub fn descriptor(key_id: &str) -> String {
    format!("{{\"alg\":\"AES256\",\"key_id\":\"{key_id}\"}}")
}

fn internal(detail: impl AsRef<str>) -> S3Error {
    S3Error::InternalError(format!("sse integrity failure: {}", detail.as_ref()))
}

/// Encrypt `plaintext` into a self-describing frame under `master`.
pub fn frame_object(master: &MasterKey, plaintext: &[u8]) -> Result<Vec<u8>, S3Error> {
    let mut dek = [0u8; KEY_LEN];
    fill_random(&mut dek);
    let mut wrap_nonce = [0u8; NONCE_LEN];
    fill_random(&mut wrap_nonce);
    let mut base = [0u8; 8];
    fill_random(&mut base);

    let cipher = master.cipher();
    let key_id = master.key_id.as_bytes();
    let wrapped = cipher
        .encrypt(
            Nonce::from_slice(&wrap_nonce),
            Payload {
                msg: &dek,
                aad: MAGIC,
            },
        )
        .map_err(|_| S3Error::InternalError("sse DEK wrap failed".into()))?;

    let object_cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&dek));
    let _ = key_id;

    let mut out = Vec::with_capacity(plaintext.len() + 88 + TAG_LEN);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(master.key_id.as_bytes());
    out.extend_from_slice(&wrap_nonce);
    out.extend_from_slice(&wrapped);
    out.extend_from_slice(&base);
    out.extend_from_slice(&(CHUNK_SIZE as u32).to_be_bytes());

    let total_chunks = plaintext.len().div_ceil(CHUNK_SIZE);
    for idx in 0..total_chunks {
        let chunk = &plaintext[idx * CHUNK_SIZE..((idx + 1) * CHUNK_SIZE).min(plaintext.len())];
        let mut nonce = [0u8; NONCE_LEN];
        nonce[..8].copy_from_slice(&base);
        nonce[8..].copy_from_slice(&((idx as u32) + 1).to_be_bytes());
        let ct = object_cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: chunk,
                    aad: &[],
                },
            )
            .map_err(|_| S3Error::InternalError("sse chunk encrypt failed".into()))?;
        out.extend_from_slice(&ct);
    }
    Ok(out)
}

/// Decrypt a frame back to plaintext; every corruption path is loud.
pub fn read_object_frame(
    master: &MasterKey,
    frame: &[u8],
    key_id_hint: &str,
) -> Result<Vec<u8>, S3Error> {
    let header_len = 8 + KEY_ID_LEN + NONCE_LEN + (KEY_LEN + TAG_LEN) + 8 + 4;
    if frame.len() < header_len {
        return Err(internal("frame shorter than header"));
    }
    if &frame[..8] != MAGIC {
        return Err(internal("bad magic"));
    }
    let key_id =
        std::str::from_utf8(&frame[8..8 + KEY_ID_LEN]).map_err(|_| internal("key_id not utf8"))?;
    if master.key_id != key_id {
        return Err(internal(format!(
            "frame key_id {key_id} does not match configured master key"
        )));
    }
    if key_id != key_id_hint {
        return Err(internal("descriptor key_id disagrees with frame"));
    }
    let wrap_nonce = &frame[8 + KEY_ID_LEN..8 + KEY_ID_LEN + NONCE_LEN];
    let wrap_ct = &frame[8 + KEY_ID_LEN + NONCE_LEN..header_len - 12];
    let base = &frame[header_len - 12..header_len - 4];
    let chunk_sz = u32::from_be_bytes(
        frame[header_len - 4..header_len]
            .try_into()
            .map_err(|_| internal("chunk size slice"))?,
    ) as usize;
    if chunk_sz != CHUNK_SIZE {
        return Err(internal("unexpected chunk size"));
    }

    let dek = master
        .cipher()
        .decrypt(
            Nonce::from_slice(wrap_nonce),
            Payload {
                msg: wrap_ct,
                aad: MAGIC,
            },
        )
        .map_err(|_| internal("DEK unwrap failed"))?;
    if dek.len() != KEY_LEN {
        return Err(internal("unwrapped DEK length"));
    }
    let object_cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&dek));

    let body = &frame[header_len..];
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let total_chunks = body.len().div_ceil(CHUNK_SIZE + TAG_LEN);
    let mut out = Vec::with_capacity(total_chunks.saturating_sub(1) * CHUNK_SIZE);
    for idx in 0..total_chunks {
        let start = idx * (CHUNK_SIZE + TAG_LEN);
        let end = ((idx + 1) * (CHUNK_SIZE + TAG_LEN)).min(body.len());
        let ct = &body[start..end];
        if ct.len() > CHUNK_SIZE + TAG_LEN {
            return Err(internal("overlong chunk"));
        }
        let mut nonce = [0u8; NONCE_LEN];
        nonce[..8].copy_from_slice(base);
        nonce[8..].copy_from_slice(&((idx as u32) + 1).to_be_bytes());
        let plain = object_cipher
            .decrypt(Nonce::from_slice(&nonce), Payload { msg: ct, aad: &[] })
            .map_err(|_| internal(format!("chunk {} tag mismatch", idx + 1)))?;
        out.extend_from_slice(&plain);
    }
    if out.len() > (total_chunks - 1) * CHUNK_SIZE + CHUNK_SIZE {
        return Err(internal("assembled size overflow"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEADER_LEN: usize = 8 + KEY_ID_LEN + NONCE_LEN + (KEY_LEN + TAG_LEN) + 8 + 4;

    fn master() -> MasterKey {
        MasterKey::test_new()
    }

    fn pseudo(len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        while out.len() < len {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            out.extend_from_slice(&x.to_le_bytes());
        }
        out.truncate(len);
        out
    }

    #[test]
    fn roundtrips_zero_one_boundary_and_multi_chunk() {
        let m = master();
        for len in [0, 1, CHUNK_SIZE, CHUNK_SIZE + 1, CHUNK_SIZE * 2 + 123] {
            let plain = pseudo(len);
            let framed = frame_object(&m, &plain).unwrap();
            let back = read_object_frame(&m, &framed, m.key_id()).unwrap();
            assert_eq!(back, plain, "len {len}");
        }
    }

    #[test]
    fn ciphertext_is_not_plaintext_and_carries_magic() {
        let m = master();
        let plain = pseudo(2048);
        let framed = frame_object(&m, &plain).unwrap();
        assert_ne!(framed, plain);
        assert_eq!(&framed[..8], MAGIC);
    }
    #[test]
    fn tamper_and_key_mismatch_are_loud() {
        let m = master();
        let plain = pseudo(CHUNK_SIZE + 10);
        let framed = frame_object(&m, &plain).unwrap();
        for (at, name) in [
            (0usize, "magic"),
            (30, "key_id"),
            (9 + KEY_ID_LEN, "wrap nonce"),
            (50, "wrapped dek"),
            (HEADER_LEN - 6, "base nonce"),
            (HEADER_LEN, "first chunk"),
            (framed.len() - 1, "tail tag"),
        ] {
            let mut f = framed.clone();
            f[at] ^= 0x01;
            assert!(
                read_object_frame(&m, &f, m.key_id()).is_err(),
                "tampering {name} must fail loudly"
            );
        }
        let other = MasterKey {
            key: [0x77u8; KEY_LEN],
            key_id: {
                let mut id = [0u8; 8];
                id.copy_from_slice(&Sha256::digest([0x77u8; KEY_LEN])[..8]);
                hex::encode(id)
            },
        };
        assert!(read_object_frame(&other, &framed, other.key_id()).is_err());
        let short = [0u8; 16];
        assert!(read_object_frame(&m, &short, m.key_id()).is_err());
    }
}

/// Row descriptor payload (persisted column value).
#[derive(serde::Deserialize)]
pub struct SseDescriptor {
    pub alg: String,
    pub key_id: String,
}

pub fn parse_descriptor(s: &str) -> Option<SseDescriptor> {
    serde_json::from_str(s).ok()
}
