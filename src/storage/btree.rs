//! A copy-on-write B-tree. Every node that changes gets a *new* page id;
//! nodes reachable from an already-committed root are never mutated in
//! place. That single rule is what makes MVCC snapshots trivial (a reader
//! just remembers a root id) and what makes commit atomic (swap one meta
//! pointer). This is the same structural idea Berkeley DB's descendant
//! LMDB uses instead of BDB's classic WAL + lock manager.
//!
//! Limitation (documented, not accidental): deletes do not rebalance or
//! merge underfull nodes, and pages freed by COW are not yet reclaimed by
//! a free list. Both are natural v2 additions; see `README.md`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::codec::{decode_page, encode_page, encoded_len};
use super::pager::{PageId, Pager};

const SPLIT_THRESHOLD: usize = 3072;

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

struct Ctx<'a> {
    pager: &'a Pager,
    overlay: &'a mut Overlay,
    next_page: &'a mut PageId,
    /// Page ids >= this were allocated during the current transaction, so
    /// they're not reachable from any committed root yet and may be
    /// mutated in place instead of copied again.
    start_next_page: PageId,
}

impl<'a> Ctx<'a> {
    fn alloc(&mut self) -> PageId {
        let id = *self.next_page;
        *self.next_page += 1;
        id
    }

    fn is_scratch(&self, id: PageId) -> bool {
        id >= self.start_next_page
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
                let left_id = if is_scratch { node_id } else { ctx.alloc() };
                let right_id = ctx.alloc();
                ctx.put_node(left_id, Node::Leaf { keys: left_keys, values: left_values });
                ctx.put_node(right_id, Node::Leaf { keys: right_keys, values: right_values });
                (left_id, Some((sep, right_id)))
            } else {
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
                let left_id = if is_scratch { node_id } else { ctx.alloc() };
                let right_id = ctx.alloc();
                ctx.put_node(left_id, Node::Internal { keys: left_keys, children: left_children });
                ctx.put_node(right_id, Node::Internal { keys: right_keys, children: right_children });
                (left_id, Some((promoted, right_id)))
            } else {
                let new_id = if is_scratch { node_id } else { ctx.alloc() };
                ctx.put_node(new_id, node);
                (new_id, None)
            }
        }
    }
}

pub fn put(
    pager: &Pager,
    overlay: &mut Overlay,
    next_page: &mut PageId,
    root: Option<PageId>,
    key: &[u8],
    value: &[u8],
) -> PageId {
    let start_next_page = *next_page;
    let mut ctx = Ctx { pager, overlay, next_page, start_next_page };
    match root {
        None => {
            let id = ctx.alloc();
            ctx.put_node(id, Node::Leaf { keys: vec![key.to_vec()], values: vec![value.to_vec()] });
            id
        }
        Some(root_id) => {
            let (new_root, split) = insert_rec(&mut ctx, root_id, key, value);
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
            let new_id = if is_scratch { node_id } else { ctx.alloc() };
            ctx.put_node(new_id, Node::Leaf { keys, values });
            (new_id, removed)
        }
        Node::Internal { keys, mut children } => {
            let idx = child_index(&keys, key);
            let (new_child_id, removed) = delete_rec(ctx, children[idx], key);
            children[idx] = new_child_id;
            let new_id = if is_scratch { node_id } else { ctx.alloc() };
            ctx.put_node(new_id, Node::Internal { keys, children });
            (new_id, removed)
        }
    }
}

/// Returns `(new_root, existed)`. `new_root` is `None` if the tree is now
/// empty.
pub fn delete(
    pager: &Pager,
    overlay: &mut Overlay,
    next_page: &mut PageId,
    root: Option<PageId>,
    key: &[u8],
) -> (Option<PageId>, bool) {
    let Some(root_id) = root else { return (None, false) };
    let start_next_page = *next_page;
    let mut ctx = Ctx { pager, overlay, next_page, start_next_page };
    let (new_root_id, removed) = delete_rec(&mut ctx, root_id, key);
    match ctx.read(new_root_id) {
        Node::Leaf { keys, .. } if keys.is_empty() => (None, removed),
        _ => (Some(new_root_id), removed),
    }
}

pub fn encode_node(node: &Node) -> [u8; super::pager::PAGE_SIZE] {
    encode_page(node)
}
