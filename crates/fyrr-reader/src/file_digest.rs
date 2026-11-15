//! Streaming SHA-256 for immutable local files; callers retain their own error policy.
use sha2::{Digest, Sha256};
use std::io::{self, Read};

pub(crate) fn sha256(file: &mut impl Read) -> io::Result<String> {
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hex::encode(hash.finalize()))
}
