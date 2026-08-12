//! A minimal Berkeley-DB-family embedded transactional key/value store:
//! copy-on-write B-tree + atomic meta-page commit, giving full ACID
//! without a write-ahead log or a lock manager. See `btree.rs` and
//! `txn.rs` for the design rationale.

pub mod btree;
pub mod codec;
pub mod pager;
pub mod txn;

pub use pager::PageId;
pub use txn::{Environment, ReadTxn, WriteTxn};
