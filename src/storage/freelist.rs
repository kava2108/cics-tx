//! The free-page list: the piece that turns "delete" into actual space you
//! get back, the way VSAM reclaims a control interval's space once its
//! records are gone. Every page a write transaction makes unreachable
//! (via COW replacement or a leaf/internal merge) is recorded here as
//! `(freed_at_txn_id, page_id)`. A future writer may hand that page id
//! back out once no active reader's snapshot predates `freed_at_txn_id`
//! -- see `txn.rs` for the reader-watermark bookkeeping that makes that
//! check safe.
//!
//! Persisted as a simple singly-linked chain of pages (not a B-tree: this
//! store's own free list doesn't need to be searched by key, just walked
//! and rebuilt each commit), each holding a batch of entries. Free-list
//! pages themselves are always bump-allocated, never drawn from the free
//! list they describe -- avoiding that bootstrapping problem costs a
//! small, bounded (not data-proportional) number of leaked pages per
//! commit; see the README for the tradeoff.

use serde::{Deserialize, Serialize};

use super::codec::{decode_page, encode_page};
use super::pager::{PageId, Pager};

/// Entries per free-list page: comfortably under the page size (each
/// entry is two u64s, ~16 bytes once encoded) with room to spare.
const CHUNK: usize = 200;

#[derive(Serialize, Deserialize)]
struct FreeListPage {
    next: Option<PageId>,
    entries: Vec<(u64, PageId)>,
}

/// Loads the entire persisted free list into memory. Called once per
/// write transaction (`Environment::begin_write`); the working copy is
/// then mutated purely in memory and rewritten wholesale at commit.
pub fn load(pager: &Pager, head: Option<PageId>) -> Vec<(u64, PageId)> {
    let mut out = Vec::new();
    let mut cur = head;
    while let Some(id) = cur {
        let buf = pager.read_page(id).expect("read free-list page");
        let page: FreeListPage = decode_page(&buf).expect("decode free-list page");
        out.extend(page.entries);
        cur = page.next;
    }
    out
}

/// Writes `entries` out as a fresh chain of free-list pages, returning the
/// new chain's head (`None` if the free list is now empty).
pub fn store(
    pager: &Pager,
    mut alloc: impl FnMut() -> PageId,
    entries: &[(u64, PageId)],
) -> std::io::Result<Option<PageId>> {
    let mut next: Option<PageId> = None;
    for chunk in entries.chunks(CHUNK) {
        let id = alloc();
        let page = FreeListPage { next, entries: chunk.to_vec() };
        pager.write_page(id, &encode_page(&page))?;
        next = Some(id);
    }
    Ok(next)
}
