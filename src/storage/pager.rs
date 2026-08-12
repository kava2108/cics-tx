//! Page-oriented file access, modeled on the Berkeley DB / LMDB family of
//! embedded stores: fixed-size pages, a double-buffered meta page for
//! crash-safe atomic commits, and a simple bump allocator for new pages.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

pub const PAGE_SIZE: usize = 4096;
const MAGIC: u32 = 0xC1C5_7B0B; // "CICS TX"
const META_PAGE_COUNT: u64 = 2; // pages 0 and 1, alternating like LMDB

pub type PageId = u64;

/// The meta page: the root of atomicity. A commit is "durable" the instant
/// a new meta page has been fsync'd, because the old meta page (and the old
/// tree it points to) is still fully intact until then. This is the same
/// trick LMDB uses and that Berkeley DB's shadow-paging predecessor used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Meta {
    pub magic: u32,
    pub txn_id: u64,
    pub root: Option<PageId>,
    pub next_page: PageId,
    /// Head of the free-page list: pages obsoleted by past commits that
    /// are no longer reachable from `root` and are safe to hand back out
    /// once no active reader's snapshot still needs them. See
    /// `storage::freelist`.
    pub free_list_head: Option<PageId>,
}

impl Meta {
    pub fn advance(&self, root: Option<PageId>, next_page: PageId, free_list_head: Option<PageId>) -> Meta {
        Meta {
            magic: self.magic,
            txn_id: self.txn_id + 1,
            root,
            next_page,
            free_list_head,
        }
    }

    fn encode(&self) -> [u8; PAGE_SIZE] {
        super::codec::encode_page(self)
    }

    fn decode(buf: &[u8; PAGE_SIZE]) -> Option<Meta> {
        let meta: Meta = super::codec::decode_page(buf)?;
        if meta.magic != MAGIC {
            return None;
        }
        Some(meta)
    }
}

pub struct Pager {
    file: File,
    /// Index (0 or 1) of the meta page slot that currently holds the
    /// highest committed txn_id. `File`'s positioned I/O (`*_at`) is
    /// thread-safe without external locking, so readers never block each
    /// other or the single writer; only this counter needs atomicity.
    active_meta_slot: AtomicU64,
}

impl Pager {
    pub fn open(path: &Path) -> io::Result<(Pager, Meta)> {
        let is_new = !path.exists();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        if is_new {
            file.set_len((META_PAGE_COUNT * PAGE_SIZE as u64).max(PAGE_SIZE as u64 * 2))?;
            let pager = Pager {
                file,
                active_meta_slot: AtomicU64::new(0),
            };
            let meta = Meta {
                magic: MAGIC,
                txn_id: 0,
                root: None,
                next_page: META_PAGE_COUNT,
                free_list_head: None,
            };
            pager.write_meta_slot(0, &meta)?;
            pager.write_meta_slot(1, &meta)?;
            return Ok((pager, meta));
        }

        let pager = Pager {
            file,
            active_meta_slot: AtomicU64::new(0),
        };
        let m0 = pager.read_meta_slot(0);
        let m1 = pager.read_meta_slot(1);
        let (slot, meta) = match (m0, m1) {
            (Some(a), Some(b)) => {
                if a.txn_id >= b.txn_id {
                    (0, a)
                } else {
                    (1, b)
                }
            }
            (Some(a), None) => (0, a),
            (None, Some(b)) => (1, b),
            (None, None) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "no valid meta page found; store file is corrupt",
                ))
            }
        };
        pager.active_meta_slot.store(slot, Ordering::SeqCst);
        Ok((pager, meta))
    }

    fn meta_offset(slot: u64) -> u64 {
        slot * PAGE_SIZE as u64
    }

    fn read_meta_slot(&self, slot: u64) -> Option<Meta> {
        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact_at(&mut buf, Self::meta_offset(slot)).ok()?;
        Meta::decode(&buf)
    }

    fn write_meta_slot(&self, slot: u64, meta: &Meta) -> io::Result<()> {
        let buf = meta.encode();
        self.file.write_all_at(&buf, Self::meta_offset(slot))?;
        self.file.sync_data()?;
        Ok(())
    }

    /// Atomically publish a new meta page. Only after this fsync returns is
    /// the transaction considered durable/committed.
    pub fn commit_meta(&self, meta: &Meta) -> io::Result<()> {
        let next_slot = 1 - self.active_meta_slot.load(Ordering::SeqCst);
        self.write_meta_slot(next_slot, meta)?;
        self.active_meta_slot.store(next_slot, Ordering::SeqCst);
        Ok(())
    }

    pub fn read_page(&self, id: PageId) -> io::Result<[u8; PAGE_SIZE]> {
        let mut buf = [0u8; PAGE_SIZE];
        self.file.read_exact_at(&mut buf, id * PAGE_SIZE as u64)?;
        Ok(buf)
    }

    pub fn write_page(&self, id: PageId, buf: &[u8; PAGE_SIZE]) -> io::Result<()> {
        self.file.write_all_at(buf, id * PAGE_SIZE as u64)
    }

    /// Ensure all data pages written so far are durable. Called before the
    /// meta page swap so we never point a committed meta at not-yet-durable
    /// data (WAL-free crash safety, same ordering rule LMDB uses).
    pub fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }
}
