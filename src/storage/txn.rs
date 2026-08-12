//! Transaction handles over an `Environment`. `ReadTxn`s are lock-free
//! snapshots (they just remember a root page id — pages are never mutated
//! once committed, so the snapshot stays consistent no matter what a
//! concurrent writer does). Only one `WriteTxn` may exist at a time: it
//! borrows the environment's mutex guard for its whole lifetime, so Rust's
//! ordinary borrow checker (not manual lock bookkeeping) guarantees the
//! lock is released exactly once, on every exit path including `?` and
//! panics — that single-writer rule is what lets us skip a full lock
//! manager and still get serializable isolation.
//!
//! Every `ReadTxn` also registers the committed txn id it snapshotted in
//! `Environment::readers`, and deregisters on drop. That's what lets a
//! `WriteTxn` compute a *reclaim watermark* at `begin_write` time: pages
//! obsoleted at or before that watermark cannot be visible to any
//! currently-active reader, and are safe to recycle. See
//! `storage::freelist` and `storage::btree::Ctx`.

use std::path::Path;

use parking_lot::{Mutex, MutexGuard, RwLock};

use super::btree::{self, Ctx, Overlay};
use super::freelist;
use super::pager::{Meta, PageId, Pager};
use crate::error::Result;

pub struct Environment {
    pager: Pager,
    committed: RwLock<Meta>,
    writer_lock: Mutex<()>,
    /// Snapshot txn ids of every currently-live `ReadTxn`, multiset-style
    /// (a Vec, not a Set, since several readers may share one txn id).
    readers: Mutex<Vec<u64>>,
}

impl Environment {
    pub fn open(path: &Path) -> Result<Environment> {
        let (pager, meta) = Pager::open(path)?;
        Ok(Environment { pager, committed: RwLock::new(meta), writer_lock: Mutex::new(()), readers: Mutex::new(Vec::new()) })
    }

    pub fn begin_read(&self) -> ReadTxn<'_> {
        let meta = self.committed.read().clone();
        self.readers.lock().push(meta.txn_id);
        ReadTxn { env: self, root: meta.root, txn_id: meta.txn_id }
    }

    fn oldest_reader_txn_id(&self) -> Option<u64> {
        self.readers.lock().iter().copied().min()
    }

    /// Blocks until any in-flight writer commits or aborts, matching the
    /// single-writer model. There is no deadlock risk: a writer never
    /// waits on anything but this lock.
    pub fn begin_write(&self) -> WriteTxn<'_> {
        let guard = self.writer_lock.lock();
        let meta = self.committed.read().clone();
        // Fixed for the whole transaction: any reader already registered
        // is counted, and any reader that shows up later necessarily
        // snapshots at or after `meta.txn_id`, which is itself always >=
        // this watermark -- so a single snapshot here stays safe for the
        // transaction's whole lifetime. See the module doc.
        let reclaim_watermark = self.oldest_reader_txn_id().unwrap_or(u64::MAX);
        let free_list = freelist::load(&self.pager, meta.free_list_head);
        WriteTxn {
            env: self,
            _guard: guard,
            base_txn_id: meta.txn_id,
            root: meta.root,
            next_page: meta.next_page,
            overlay: Overlay::new(),
            free_list,
            obsolete: Vec::new(),
            scratch_pool: Vec::new(),
            reclaim_watermark,
        }
    }
}

/// Read-only snapshot transaction.
pub struct ReadTxn<'env> {
    env: &'env Environment,
    root: Option<PageId>,
    txn_id: u64,
}

impl<'env> ReadTxn<'env> {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        btree::get(&self.env.pager, &Overlay::new(), self.root, key)
    }

    pub fn range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
        btree::range(&self.env.pager, &Overlay::new(), self.root, start, end)
    }
}

impl<'env> Drop for ReadTxn<'env> {
    fn drop(&mut self) {
        let mut readers = self.env.readers.lock();
        if let Some(pos) = readers.iter().position(|&t| t == self.txn_id) {
            readers.swap_remove(pos);
        }
    }
}

/// The single, exclusive read/write transaction. Changes are only visible
/// to this transaction (via the dirty overlay) until `commit()` runs.
pub struct WriteTxn<'env> {
    env: &'env Environment,
    _guard: MutexGuard<'env, ()>,
    base_txn_id: u64,
    root: Option<PageId>,
    next_page: PageId,
    overlay: Overlay,
    free_list: Vec<(u64, PageId)>,
    obsolete: Vec<PageId>,
    /// Transaction-local only; never persisted, never carried across
    /// transactions. See `Ctx::scratch_pool`.
    scratch_pool: Vec<PageId>,
    reclaim_watermark: u64,
}

impl<'env> WriteTxn<'env> {
    fn ctx(&mut self) -> Ctx<'_> {
        Ctx::new(
            &self.env.pager,
            &mut self.overlay,
            &mut self.next_page,
            &mut self.free_list,
            &mut self.obsolete,
            &mut self.scratch_pool,
            self.reclaim_watermark,
        )
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        btree::get(&self.env.pager, &self.overlay, self.root, key)
    }

    pub fn range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
        btree::range(&self.env.pager, &self.overlay, self.root, start, end)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let root = self.root;
        let mut ctx = self.ctx();
        self.root = Some(btree::put(&mut ctx, root, key, value));
    }

    /// Returns whether the key existed.
    pub fn delete(&mut self, key: &[u8]) -> bool {
        let root = self.root;
        let mut ctx = self.ctx();
        let (new_root, existed) = btree::delete(&mut ctx, root, key);
        self.root = new_root;
        existed
    }

    /// SYNCPOINT: make every change durable and visible to future readers
    /// in one atomic step (flush dirty pages, fsync, then swap the meta
    /// pointer). Consumes `self`, releasing the writer lock on return.
    pub fn commit(mut self) -> Result<()> {
        for (id, node) in &self.overlay {
            self.env.pager.write_page(*id, &btree::encode_node(node))?;
        }

        let base = self.env.committed.read().clone();
        debug_assert_eq!(base.txn_id, self.base_txn_id, "writer lock invariant violated");
        let new_txn_id = base.txn_id + 1;

        // Anything still sitting unused in the scratch pool (retired
        // mid-transaction but never reclaimed by a later alloc() in this
        // same transaction) would otherwise just be silently dropped --
        // wasting those page numbers forever instead of handing them to
        // a *future* transaction. Fold them in as freshly obsoleted too.
        self.obsolete.append(&mut self.scratch_pool);

        // This transaction's own obsoleted pages join whatever the free
        // list had left over (entries this transaction didn't reclaim),
        // all tagged with the txn id that's committing right now.
        for pid in self.obsolete.drain(..) {
            self.free_list.push((new_txn_id, pid));
        }
        let mut next_page = self.next_page;
        let free_list_head = freelist::store(
            &self.env.pager,
            || {
                let id = next_page;
                next_page += 1;
                id
            },
            &self.free_list,
        )?;
        self.next_page = next_page;

        self.env.pager.sync_data()?;
        let new_meta = base.advance(self.root, self.next_page, free_list_head);
        self.env.pager.commit_meta(&new_meta)?;
        *self.env.committed.write() = new_meta;
        Ok(())
        // `self` (including `_guard`) drops here, releasing the writer lock.
    }

    /// SYNCPOINT ROLLBACK: discard every change made in this transaction.
    /// Because writes never touched a committed page, "undo" is just
    /// dropping the in-memory overlay — nothing on disk needs repair.
    pub fn rollback(self) {
        // Dropping `self` discards the overlay (and the in-memory free
        // list / obsolete-page bookkeeping, none of which was ever
        // persisted) and releases the writer lock; nothing else to do. An
        // early `?` return elsewhere in caller code has the same effect,
        // so aborts are always safe.
    }
}
