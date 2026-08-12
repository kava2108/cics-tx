//! Shared checksummed page (de)serialization used by both the meta page and
//! B-tree node pages: `[u32 crc32][bincode body][zero padding]`.

use serde::{de::DeserializeOwned, Serialize};


use super::pager::PAGE_SIZE;

pub fn encode_page<T: Serialize>(value: &T) -> [u8; PAGE_SIZE] {
    let mut buf = [0u8; PAGE_SIZE];
    let body = bincode::serialize(value).expect("page serialize");
    assert!(
        body.len() + 4 <= PAGE_SIZE,
        "page payload of {} bytes exceeds page capacity; overflow pages are not implemented",
        body.len()
    );
    let checksum = crc32fast::hash(&body);
    buf[0..4].copy_from_slice(&checksum.to_le_bytes());
    buf[4..4 + body.len()].copy_from_slice(&body);
    buf
}

pub fn decode_page<T: DeserializeOwned + Serialize>(buf: &[u8; PAGE_SIZE]) -> Option<T> {
    let checksum = u32::from_le_bytes(buf[0..4].try_into().ok()?);
    // Must use the same (legacy, fixint) config as `bincode::serialize`
    // above -- `bincode::options()` defaults to varint encoding instead
    // and would silently misparse the bytes `encode_page` wrote.
    let value: T = bincode::deserialize(&buf[4..]).ok()?;
    let body = bincode::serialize(&value).ok()?;
    if body.len() > buf.len() - 4 || crc32fast::hash(&body) != checksum {
        return None;
    }
    Some(value)
}

/// Returns the exact encoded size a value would take, for split-threshold
/// checks before committing to writing it into a page.
pub fn encoded_len<T: Serialize>(value: &T) -> usize {
    bincode::serialized_size(value).expect("size") as usize + 4
}
