//! Overflow pages: storage for values too large to sit inline in a leaf.
//! Without this, a single oversized value would make `encode_page` panic
//! (it can't fit in one page) and splitting couldn't help, since a leaf
//! with just that one entry would still be too big no matter how it's
//! divided. Large values are instead chunked across a singly-linked
//! chain of dedicated pages; the leaf holds only a small
//! `StoredValue::Overflow { head, len }` reference (see `btree.rs`).
//!
//! Unlike node pages, overflow pages are written to disk immediately when
//! created rather than staged in the write transaction's overlay until
//! commit -- see `btree::Ctx::write_overflow` for why that's still safe:
//! nothing commits a path *to* them (the referencing leaf) until the
//! transaction itself commits, so an uncommitted overflow chain is just
//! as invisible to other readers as an uncommitted node would be. On
//! rollback the chain's page numbers get silently reused by whatever
//! transaction runs next, since `next_page` itself rewinds to the last
//! committed value.

use serde::{Deserialize, Serialize};

use super::codec::decode_page;
use super::pager::{PageId, Pager};

/// Comfortably under the ~4092-byte page budget once the `next` field and
/// the `Vec<u8>` length prefix are accounted for.
pub const CHUNK: usize = 4000;

#[derive(Serialize, Deserialize)]
pub struct OverflowPage {
    pub next: Option<PageId>,
    pub data: Vec<u8>,
}

/// Reassembles the full value by walking the chain from `head`.
pub fn read_chain(pager: &Pager, head: PageId, len: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len as usize);
    let mut cur = Some(head);
    while let Some(id) = cur {
        let buf = pager.read_page(id).expect("read overflow page");
        let page: OverflowPage = decode_page(&buf).expect("decode overflow page");
        out.extend_from_slice(&page.data);
        cur = page.next;
    }
    out
}
