//! Transaction handles over an `Environment`. `ReadTxn`s are lock-free
//! snapshots (they just remember a root page id — pages are never mutated
//! once committed, so the snapshot stays consistent no matter what a
//! concurrent writer does). Only one `WriteTxn` may exist at a time: it
//! borrows the environment's mutex guard for its whole lifetime, so Rust's
//! ordinary borrow checker (not manual lock bookkeeping) guarantees the
//! lock is released exactly once, on every exit path including `?` and
//! panics — that single-writer rule is what lets us skip a full lock
//! manager and still get serializable isolation.

use std::path::Path;

use parking_lot::{Mutex, MutexGuard, RwLock};

use super::btree::{self, Overlay};
use super::pager::{Meta, PageId, Pager};
use crate::error::Result;

pub struct Environment {
    pager: Pager,
    committed: RwLock<Meta>,
    writer_lock: Mutex<()>,
}

impl Environment {
    pub fn open(path: &Path) -> Result<Environment> {
        let (pager, meta) = Pager::open(path)?;
        Ok(Environment { pager, committed: RwLock::new(meta), writer_lock: Mutex::new(()) })
    }

    pub fn begin_read(&self) -> ReadTxn<'_> {
        let meta = self.committed.read().clone();
        ReadTxn { env: self, root: meta.root }
    }

    /// Blocks until any in-flight writer commits or aborts, matching the
    /// single-writer model. There is no deadlock risk: a writer never
    /// waits on anything but this lock.
    pub fn begin_write(&self) -> WriteTxn<'_> {
        let guard = self.writer_lock.lock();
        let meta = self.committed.read().clone();
        WriteTxn {
            env: self,
            _guard: guard,
            base_txn_id: meta.txn_id,
            root: meta.root,
            next_page: meta.next_page,
            overlay: Overlay::new(),
        }
    }
}

/// Read-only snapshot transaction.
pub struct ReadTxn<'env> {
    env: &'env Environment,
    root: Option<PageId>,
}

impl<'env> ReadTxn<'env> {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        btree::get(&self.env.pager, &Overlay::new(), self.root, key)
    }

    pub fn range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
        btree::range(&self.env.pager, &Overlay::new(), self.root, start, end)
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
}

impl<'env> WriteTxn<'env> {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        btree::get(&self.env.pager, &self.overlay, self.root, key)
    }

    pub fn range(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<(Vec<u8>, Vec<u8>)> {
        btree::range(&self.env.pager, &self.overlay, self.root, start, end)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.root = Some(btree::put(&self.env.pager, &mut self.overlay, &mut self.next_page, self.root, key, value));
    }

    /// Returns whether the key existed.
    pub fn delete(&mut self, key: &[u8]) -> bool {
        let (new_root, existed) = btree::delete(&self.env.pager, &mut self.overlay, &mut self.next_page, self.root, key);
        self.root = new_root;
        existed
    }

    /// SYNCPOINT: make every change durable and visible to future readers
    /// in one atomic step (flush dirty pages, fsync, then swap the meta
    /// pointer). Consumes `self`, releasing the writer lock on return.
    pub fn commit(self) -> Result<()> {
        for (id, node) in &self.overlay {
            self.env.pager.write_page(*id, &btree::encode_node(node))?;
        }
        self.env.pager.sync_data()?;
        let base = self.env.committed.read().clone();
        debug_assert_eq!(base.txn_id, self.base_txn_id, "writer lock invariant violated");
        let new_meta = base.advance(self.root, self.next_page);
        self.env.pager.commit_meta(&new_meta)?;
        *self.env.committed.write() = new_meta;
        Ok(())
        // `self` (including `_guard`) drops here, releasing the writer lock.
    }

    /// SYNCPOINT ROLLBACK: discard every change made in this transaction.
    /// Because writes never touched a committed page, "undo" is just
    /// dropping the in-memory overlay — nothing on disk needs repair.
    pub fn rollback(self) {
        // Dropping `self` discards the overlay and releases the writer
        // lock; nothing else to do. An early `?` return elsewhere in
        // caller code has the same effect, so aborts are always safe.
    }
}
