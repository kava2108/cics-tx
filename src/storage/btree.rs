//! A copy-on-write B-tree. Every node that changes gets a *new* page id;
//! nodes reachable from an already-committed root are never mutated in
//! place. That single rule is what makes MVCC snapshots trivial (a reader
//! just remembers a root id) and what makes commit atomic (swap one meta
//! pointer). This is the same structural idea Berkeley DB's descendant
//! LMDB uses instead of BDB's classic WAL + lock manager.
//!
//! Space is reclaimed in two cooperating ways, mirroring VSAM's control
//! interval/control area reuse:
//! - **Merge on delete**: once a leaf or internal node's serialized size
//!   drops below [`MERGE_THRESHOLD`], it's combined with a sibling (if the
//!   result still fits in one page), shrinking the tree back down instead
//!   of leaving pages sparse forever.
//! - **Free-list reuse**: any page a COW replacement or a merge makes
//!   unreachable is hallmarked "obsolete" and, once no active reader's
//!   snapshot can still see it (`storage::freelist` / `txn.rs`), handed
//!   back out to future allocations instead of growing the file.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::codec::{decode_page, encode_page, encoded_len};
use super::pager::{PageId, Pager};

const SPLIT_THRESHOLD: usize = 3072;
/// Below this size a node is considered underfull and becomes a merge
/// candidate. Deliberately well under `SPLIT_THRESHOLD` (with room for a
/// merged node to still fit in one page) so split and merge can't thrash
/// against each other.
const MERGE_THRESHOLD: usize = SPLIT_THRESHOLD / 4;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Node {
    Leaf {
        keys: Vec<Vec<u8>>,
        values: Vec<Vec<u8>>,
    },
    Internal {
        keys: Vec<Vec<u8>>,
        children: Vec<PageId>,
    },
}

/// Dirty pages a write transaction has produced but not yet flushed to
/// disk, keyed by the (possibly not-yet-durable) page id they'll occupy.
pub type Overlay = HashMap<PageId, Node>;

fn read_node(pager: &Pager, overlay: &Overlay, id: PageId) -> Node {
    if let Some(n) = overlay.get(&id) {
        return n.clone();
    }
    let buf = pager.read_page(id).expect("read page");
    decode_page(&buf).expect("decode node page")
}

fn child_index(keys: &[Vec<u8>], key: &[u8]) -> usize {
    keys.partition_point(|k| k.as_slice() <= key)
}

pub fn get(pager: &Pager, overlay: &Overlay, root: Option<PageId>, key: &[u8]) -> Option<Vec<u8>> {
    let mut cur = root?;
    loop {
        match read_node(pager, overlay, cur) {
            Node::Leaf { keys, values } => {
                return keys
                    .binary_search_by(|k| k.as_slice().cmp(key))
                    .ok()
                    .map(|i| values[i].clone());
            }
            Node::Internal { keys, children } => {
                cur = children[child_index(&keys, key)];
            }
        }
    }
}

/// Collects all key/value pairs with `start <= key < end` (either bound
/// optional), in key order. Materialized eagerly: fine for demo-scale
/// datasets; a real range cursor is a natural v2 addition.
pub fn range(
    pager: &Pager,
    overlay: &Overlay,
    root: Option<PageId>,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    if let Some(root) = root {
        collect(pager, overlay, root, start, end, &mut out);
    }
    out
}

fn collect(
    pager: &Pager,
    overlay: &Overlay,
    node_id: PageId,
    start: Option<&[u8]>,
    end: Option<&[u8]>,
    out: &mut Vec<(Vec<u8>, Vec<u8>)>,
) {
    match read_node(pager, overlay, node_id) {
        Node::Leaf { keys, values } => {
            for (k, v) in keys.into_iter().zip(values.into_iter()) {
                if start.map_or(true, |s| k.as_slice() >= s) && end.map_or(true, |e| k.as_slice() < e) {
                    out.push((k, v));
                }
            }
        }
        Node::Internal { children, .. } => {
            for c in children {
                collect(pager, overlay, c, start, end, out);
            }
        }
    }
}

/// Everything a write transaction's tree-mutating operations share:
/// where to read committed pages from, the in-progress dirty overlay, the
/// bump allocator, and the free-page list (both the leftover unconsumed
/// entries loaded from disk, and the newly-obsoleted ids this transaction
/// itself produces).
pub struct Ctx<'a> {
    pager: &'a Pager,
    overlay: &'a mut Overlay,
    next_page: &'a mut PageId,
    free_list: &'a mut Vec<(u64, PageId)>,
    obsolete: &'a mut Vec<PageId>,
    /// Ids retired *within this same transaction* (a merge discarding a
    /// sibling that this transaction itself had already COW'd earlier).
    /// Such a page was never written to disk and is invisible to every
    /// reader (the transaction hasn't committed), so it's always safe to
    /// hand straight back out -- no watermark check needed, unlike
    /// `free_list`. Without this pool, a transaction that merges heavily
    /// (e.g. many sequential single-key deletes cascading merges) would
    /// keep minting brand-new page numbers for nodes that go obsolete
    /// moments later within the very same transaction, bloating the file
    /// even though nothing extra ever ends up reachable.
    scratch_pool: &'a mut Vec<PageId>,
    /// A page freed at txn `F` may be reused only when `F <= reclaim_watermark`
    /// -- see `txn.rs` for why a single watermark fixed at the start of the
    /// write transaction is sufficient for MVCC safety.
    reclaim_watermark: u64,
}

impl<'a> Ctx<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pager: &'a Pager,
        overlay: &'a mut Overlay,
        next_page: &'a mut PageId,
        free_list: &'a mut Vec<(u64, PageId)>,
        obsolete: &'a mut Vec<PageId>,
        scratch_pool: &'a mut Vec<PageId>,
        reclaim_watermark: u64,
    ) -> Self {
        Ctx { pager, overlay, next_page, free_list, obsolete, scratch_pool, reclaim_watermark }
    }

    /// Returns a page id for a brand-new (this-transaction-only) node.
    /// Prefers, in order: a page this same transaction already retired
    /// (free, no persistence/visibility concerns at all), then a
    /// committed page obsoleted by some past transaction and safe to
    /// reuse now, and only then grows the file.
    fn alloc(&mut self) -> PageId {
        if let Some(id) = self.scratch_pool.pop() {
            return id;
        }
        if let Some(pos) = self.free_list.iter().position(|&(freed_at, _)| freed_at <= self.reclaim_watermark) {
            let (_, id) = self.free_list.swap_remove(pos);
            return id;
        }
        let id = *self.next_page;
        *self.next_page += 1;
        id
    }

    /// A page produced by this same transaction (already in the overlay)
    /// may be mutated again in place; anything else is a committed page
    /// that must be copied instead of touched.
    fn is_scratch(&self, id: PageId) -> bool {
        self.overlay.contains_key(&id)
    }

    /// Call when `id` stops being reachable from the tree this
    /// transaction is building. If it was a committed page (not already
    /// this transaction's own scratch page), it becomes eligible for
    /// future reuse once this transaction commits.
    fn mark_obsolete(&mut self, id: PageId) {
        if !self.is_scratch(id) {
            self.obsolete.push(id);
        }
    }

    /// Call when `id` is being fully discarded and *not* reused in place
    /// for its replacement (unlike the ordinary COW case, which reuses a
    /// scratch id directly) -- the only place this currently happens is a
    /// sibling merge collapsing two children into one freshly allocated
    /// node, and root collapse discarding the old root. If `id` was this
    /// transaction's own scratch page, it's removed from the overlay (or
    /// it would still get flushed to disk at commit despite being
    /// unreachable) and handed to `scratch_pool` for immediate reuse. If
    /// `id` was a committed page, it's handled the same as any other
    /// obsoleted page: added to `obsolete`, for *future* transactions to
    /// reuse once this one commits.
    fn retire(&mut self, id: PageId) {
        if self.overlay.remove(&id).is_some() {
            self.scratch_pool.push(id);
        } else {
            self.obsolete.push(id);
        }
    }

    fn put_node(&mut self, id: PageId, node: Node) {
        self.overlay.insert(id, node);
    }

    fn read(&self, id: PageId) -> Node {
        read_node(self.pager, self.overlay, id)
    }
}

/// Result of descending into a child: its (possibly new) id, and, if the
/// child had to split, the separator key promoted to the parent plus the
/// new right-sibling id.
type DescendResult = (PageId, Option<(Vec<u8>, PageId)>);

fn insert_rec(ctx: &mut Ctx, node_id: PageId, key: &[u8], value: &[u8]) -> DescendResult {
    let is_scratch = ctx.is_scratch(node_id);
    match ctx.read(node_id) {
        Node::Leaf { mut keys, mut values } => {
            match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                Ok(idx) => values[idx] = value.to_vec(),
                Err(idx) => {
                    keys.insert(idx, key.to_vec());
                    values.insert(idx, value.to_vec());
                }
            }
            let node = Node::Leaf { keys, values };
            if encoded_len(&node) > SPLIT_THRESHOLD {
                let (keys, values) = match node {
                    Node::Leaf { keys, values } => (keys, values),
                    _ => unreachable!(),
                };
                let mid = keys.len() / 2;
                let right_keys = keys[mid..].to_vec();
                let right_values = values[mid..].to_vec();
                let left_keys = keys[..mid].to_vec();
                let left_values = values[..mid].to_vec();
                let sep = right_keys[0].clone();
                if !is_scratch {
                    ctx.mark_obsolete(node_id);
                }
                let left_id = if is_scratch { node_id } else { ctx.alloc() };
                let right_id = ctx.alloc();
                ctx.put_node(left_id, Node::Leaf { keys: left_keys, values: left_values });
                ctx.put_node(right_id, Node::Leaf { keys: right_keys, values: right_values });
                (left_id, Some((sep, right_id)))
            } else {
                if !is_scratch {
                    ctx.mark_obsolete(node_id);
                }
                let new_id = if is_scratch { node_id } else { ctx.alloc() };
                ctx.put_node(new_id, node);
                (new_id, None)
            }
        }
        Node::Internal { mut keys, mut children } => {
            let idx = child_index(&keys, key);
            let (new_child_id, split) = insert_rec(ctx, children[idx], key, value);
            children[idx] = new_child_id;
            if let Some((sep, sibling_id)) = split {
                keys.insert(idx, sep);
                children.insert(idx + 1, sibling_id);
            }
            let node = Node::Internal { keys, children };
            if encoded_len(&node) > SPLIT_THRESHOLD {
                let (keys, children) = match node {
                    Node::Internal { keys, children } => (keys, children),
                    _ => unreachable!(),
                };
                let mid = keys.len() / 2;
                let promoted = keys[mid].clone();
                let left_keys = keys[..mid].to_vec();
                let right_keys = keys[mid + 1..].to_vec();
                let left_children = children[..=mid].to_vec();
                let right_children = children[mid + 1..].to_vec();
                if !is_scratch {
                    ctx.mark_obsolete(node_id);
                }
                let left_id = if is_scratch { node_id } else { ctx.alloc() };
                let right_id = ctx.alloc();
                ctx.put_node(left_id, Node::Internal { keys: left_keys, children: left_children });
                ctx.put_node(right_id, Node::Internal { keys: right_keys, children: right_children });
                (left_id, Some((promoted, right_id)))
            } else {
                if !is_scratch {
                    ctx.mark_obsolete(node_id);
                }
                let new_id = if is_scratch { node_id } else { ctx.alloc() };
                ctx.put_node(new_id, node);
                (new_id, None)
            }
        }
    }
}

pub fn put(ctx: &mut Ctx, root: Option<PageId>, key: &[u8], value: &[u8]) -> PageId {
    match root {
        None => {
            let id = ctx.alloc();
            ctx.put_node(id, Node::Leaf { keys: vec![key.to_vec()], values: vec![value.to_vec()] });
            id
        }
        Some(root_id) => {
            let (new_root, split) = insert_rec(ctx, root_id, key, value);
            match split {
                None => new_root,
                Some((sep, sibling)) => {
                    let id = ctx.alloc();
                    ctx.put_node(id, Node::Internal { keys: vec![sep], children: vec![new_root, sibling] });
                    id
                }
            }
        }
    }
}

fn encoded_size(node: &Node) -> usize {
    encoded_len(node)
}

/// Tries to combine `left` and `right` (siblings, in key order) into one
/// node. `separator` is the parent's key between them, needed to fold
/// back into an `Internal` merge (internal nodes don't otherwise store
/// it). Returns `None` if they're not mergeable into a single page.
fn try_merge(left: Node, right: Node, separator: Option<Vec<u8>>) -> Option<Node> {
    let merged = match (left, right) {
        (Node::Leaf { keys: mut lk, values: mut lv }, Node::Leaf { keys: rk, values: rv }) => {
            lk.extend(rk);
            lv.extend(rv);
            Node::Leaf { keys: lk, values: lv }
        }
        (Node::Internal { keys: mut lk, children: mut lc }, Node::Internal { keys: rk, children: rc }) => {
            lk.push(separator.expect("merging internal nodes requires the parent's separator key"));
            lk.extend(rk);
            lc.extend(rc);
            Node::Internal { keys: lk, children: lc }
        }
        _ => return None, // a well-formed tree never mixes variants at one level
    };
    if encoded_size(&merged) <= SPLIT_THRESHOLD {
        Some(merged)
    } else {
        None
    }
}

fn is_underfull(node: &Node) -> bool {
    encoded_size(node) < MERGE_THRESHOLD
}

fn delete_rec(ctx: &mut Ctx, node_id: PageId, key: &[u8]) -> (PageId, bool) {
    let is_scratch = ctx.is_scratch(node_id);
    match ctx.read(node_id) {
        Node::Leaf { mut keys, mut values } => {
            let removed = match keys.binary_search_by(|k| k.as_slice().cmp(key)) {
                Ok(idx) => {
                    keys.remove(idx);
                    values.remove(idx);
                    true
                }
                Err(_) => false,
            };
            if !removed {
                // Nothing changed: no COW needed, and nothing to free.
                return (node_id, false);
            }
            if !is_scratch {
                ctx.mark_obsolete(node_id);
            }
            let new_id = if is_scratch { node_id } else { ctx.alloc() };
            ctx.put_node(new_id, Node::Leaf { keys, values });
            (new_id, true)
        }
        Node::Internal { mut keys, mut children } => {
            let idx = child_index(&keys, key);
            let (new_child_id, removed) = delete_rec(ctx, children[idx], key);
            if !removed {
                // The whole subtree under `node_id` is unchanged.
                return (node_id, false);
            }
            children[idx] = new_child_id;

            // The child just shrank; see if it's now worth merging with a
            // sibling instead of leaving it (and its page) underfull. Try
            // the right sibling first, then fall back to the left one --
            // either no right sibling exists, or the combined node
            // wouldn't fit in one page.
            if is_underfull(&ctx.read(children[idx])) {
                let mut merged_right = false;
                if idx + 1 < children.len() {
                    let right_id = children[idx + 1];
                    let sep = keys[idx].clone();
                    if let Some(merged) = try_merge(ctx.read(children[idx]), ctx.read(right_id), Some(sep)) {
                        ctx.retire(children[idx]);
                        ctx.retire(right_id);
                        let merged_id = ctx.alloc();
                        ctx.put_node(merged_id, merged);
                        children[idx] = merged_id;
                        children.remove(idx + 1);
                        keys.remove(idx);
                        merged_right = true;
                    }
                }
                if !merged_right && idx > 0 {
                    let left_id = children[idx - 1];
                    let sep = keys[idx - 1].clone();
                    if let Some(merged) = try_merge(ctx.read(left_id), ctx.read(children[idx]), Some(sep)) {
                        ctx.retire(left_id);
                        ctx.retire(children[idx]);
                        let merged_id = ctx.alloc();
                        ctx.put_node(merged_id, merged);
                        children[idx - 1] = merged_id;
                        children.remove(idx);
                        keys.remove(idx - 1);
                    }
                }
            }

            if !is_scratch {
                ctx.mark_obsolete(node_id);
            }
            let new_id = if is_scratch { node_id } else { ctx.alloc() };
            ctx.put_node(new_id, Node::Internal { keys, children });
            (new_id, true)
        }
    }
}

/// After a delete (possibly with merges), the root may have collapsed to
/// an internal node with a single child, or an empty leaf; unwind that
/// down to the real new root (or `None` for an empty tree).
fn finalize_root(ctx: &mut Ctx, mut root_id: PageId) -> Option<PageId> {
    loop {
        match ctx.read(root_id) {
            Node::Leaf { keys, .. } if keys.is_empty() => {
                ctx.retire(root_id);
                return None;
            }
            Node::Internal { keys, children } if keys.is_empty() && children.len() == 1 => {
                ctx.retire(root_id);
                root_id = children[0];
            }
            _ => return Some(root_id),
        }
    }
}

/// Returns `(new_root, existed)`. `new_root` is `None` if the tree is now
/// empty.
pub fn delete(ctx: &mut Ctx, root: Option<PageId>, key: &[u8]) -> (Option<PageId>, bool) {
    let Some(root_id) = root else { return (None, false) };
    let (new_root_id, removed) = delete_rec(ctx, root_id, key);
    (finalize_root(ctx, new_root_id), removed)
}

pub fn encode_node(node: &Node) -> [u8; super::pager::PAGE_SIZE] {
    encode_page(node)
}
