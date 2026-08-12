# cics-tx

A from-scratch, Berkeley-DB-family embedded transactional store, wrapped in
a small CICS-style transaction-processing runtime: Task Control, Program
Control (LINK / XCTL / RETURN), File Control, and SYNCPOINT.

## Why "Berkeley-DB-family" and not literally Berkeley DB's design

Berkeley DB's classic engine gets ACID from a write-ahead log plus a lock
manager. This project uses the design BDB's own alumnus LMDB popularized
instead: a **copy-on-write B-tree with an atomic meta-page swap**.

- **Atomicity & Durability**: a transaction never mutates a page reachable
  from the last committed root. It writes new pages, fsyncs them, and only
  then swaps one small meta page (double-buffered, checksummed) to publish
  the new root. A crash before that swap leaves the old, fully intact tree
  in place — there is nothing to redo or undo, so there's no WAL.
- **Isolation**: exactly one writer at a time (`storage::txn::Environment`
  enforces this with a mutex), and readers snapshot a root id at
  `begin_read()` — since committed pages are immutable, that snapshot stays
  consistent no matter what the writer does concurrently. This gets you
  serializable isolation without a lock manager.
- **Consistency**: enforced by the B-tree invariants plus whatever the
  application layer (File Control) checks.

See `src/storage/btree.rs` and `src/storage/txn.rs` for the code-level
rationale.

### Space reclamation: merge-on-delete + a reader-aware free list

Deleting a key doesn't just shrink a leaf and leave it sparse. Once a
leaf or internal node's serialized size drops below a low-water mark, it's
merged with a sibling (falling back from the right sibling to the left one
if the right merge wouldn't fit in a page); root collapses when it's left
with a single child. Every page a merge or an ordinary COW replacement
makes unreachable is recorded as `(freed_at_txn_id, page_id)` in a
persisted free list (`storage/freelist.rs`) instead of being abandoned.

The catch is MVCC safety: a page freed at txn `F` might still be exactly
what an already-open `ReadTxn` is reading, if that reader's snapshot
predates `F`. So every `ReadTxn` registers its snapshot's txn id with the
`Environment` on `begin_read()` and deregisters on drop; a `WriteTxn`
computes a *reclaim watermark* (the oldest still-registered reader, or "no
limit" if there are none) once at `begin_write()`, and only ever reuses a
page whose `freed_at_txn_id <= watermark` — i.e., no live reader could
possibly still need it. (Pages a transaction retires *within its own,
not-yet-committed* work — e.g. a merge discarding a sibling that same
transaction had already copy-on-written moments earlier — skip the
watermark check entirely via an in-memory `scratch_pool`: nothing durable
or reader-visible ever pointed at them, so they're safe to recycle
immediately, which is what keeps a single delete-heavy transaction from
burning through page numbers via its own merge cascades.)

This mirrors VSAM's control-interval/control-area reuse: as records are
deleted, the space they occupied comes back for reuse instead of only
ever growing the dataset — see `tests/storage_smoke.rs` for tests
covering both the reclaim-bounds-file-growth property and the
reader-isolation-during-reclaim property.

### Overflow pages for oversized values

A value over `INLINE_VALUE_LIMIT` (512 bytes) doesn't get stored inline in
its leaf; it's chunked across a singly-linked chain of dedicated overflow
pages (`storage/overflow.rs`), and the leaf holds only a small
`StoredValue::Overflow { head, len }` reference. Without this, a single
large record would make a leaf permanently too big to fit in one page —
splitting can't help, since a leaf holding just that one oversized entry
would still be too big no matter how it's divided.

Overflow pages are written to disk immediately when created, unlike node
pages (which stay staged in the write transaction's in-memory overlay
until commit). That's safe because nothing commits a *path* to an
overflow chain — the referencing leaf — until the transaction itself
commits, so an uncommitted chain is exactly as invisible to other readers
as an uncommitted node would be; and on rollback, the page numbers it used
get silently reused by whatever transaction runs next, since `next_page`
itself rewinds to the last committed value. Overwriting or deleting a key
whose value was stored via overflow retires the whole old chain through
the same free-list mechanism as any other obsoleted page.

Keys always stay inline — CICS/VSAM keys (RIDFLDs) are conventionally
small, fixed-length fields, so this is a deliberate scope boundary, not
an oversight.

## Layout

```
src/
  storage/          the embedded KV engine
    pager.rs         fixed-size page I/O, double-buffered meta page
    codec.rs         checksummed page (de)serialization
    btree.rs         copy-on-write B-tree (get/put/delete/range, merge-on-delete)
    freelist.rs      persisted free-page chain (load/store)
    overflow.rs      chunked storage for oversized values
    txn.rs           Environment / ReadTxn / WriteTxn, reader-watermark tracking
  runtime/           the CICS-style layer, built on storage
    task.rs           Task Control: task ids, EIB
    program.rs         Program Control: registry + LINK/XCTL/RETURN
    file_control.rs    File Control: FCT, READ/WRITE/REWRITE/DELETE/browse
    region.rs           Region + ExecCtx (the "EXEC CICS" surface, SYNCPOINT)
examples/bank_demo.rs   an end-to-end bank-transfer walkthrough
tests/
  storage_smoke.rs      ACID tests on the raw storage engine
  cics_semantics.rs     LINK/XCTL/START/SYNCPOINT/ABEND semantics
```

## Mapping to real CICS concepts

| CICS command / concept        | Here                                              |
|--------------------------------|----------------------------------------------------|
| PPT (Processing Program Table) | `runtime::program::ProgramManager`                 |
| FCT (File Control Table)       | `runtime::file_control::FileControlTable`          |
| EIB                             | `runtime::task::Eib`                               |
| `EXEC CICS LINK`                | `ExecCtx::link` (an ordinary nested Rust call)     |
| `EXEC CICS XCTL`                | a program returning `ProgramOutcome::Xctl`         |
| `EXEC CICS RETURN`              | a program returning `ProgramOutcome::Return`       |
| `EXEC CICS START`               | `ExecCtx::start` (queued, run after the caller)    |
| `EXEC CICS READ/WRITE/REWRITE/DELETE` | `ExecCtx::{read,write,rewrite,delete}`       |
| `EXEC CICS STARTBR/READNEXT/ENDBR` | `ExecCtx::browse` (materialized, not a live cursor) |
| `EXEC CICS SYNCPOINT`           | `ExecCtx::syncpoint`                                |
| `EXEC CICS SYNCPOINT ROLLBACK`  | `ExecCtx::syncpoint_rollback`                       |
| Task ABEND / dynamic transaction backout | a program returning `Err(_)` → the Region rolls back the task's whole unit of work |
| Implicit syncpoint at task end  | `Region::start_task` commits on `Ok`, rolls back on `Err` |

## Try it

```bash
cargo test              # storage ACID tests + CICS semantics tests
cargo run --example bank_demo
```

The demo opens accounts, LINKs DEBIT/CREDIT inside a TRANSFER program,
XCTLs to a STATEMENT program for the final report, START's an async
AUDIT_LOG task, then deliberately overdraws an account to show the ABEND
rolling back every change from that task — and finally closes and reopens
the store to prove committed data survives a restart.

## Known limitations (v1, by design — not oversights)

- **Merge is best-effort, not a full rebalance**: a node only merges with
  one sibling (right, falling back to left) and only when the combined
  size still fits one page; there's no borrow-from-sibling redistribution
  step, so a node can still end up modestly underfull with no eligible
  merge partner. Correctness is unaffected either way.
- **Free-list pages themselves are never recycled** (only the *data* pages
  they describe are): each commit writes its updated free list out as a
  fresh chain and abandons the previous one, to avoid the bootstrapping
  problem of a free list needing to allocate from itself. Concretely this
  leaks one (occasionally two) bookkeeping pages *per commit* — bounded
  per-commit, but linear in the number of commits over the life of a
  store, not in how much data changed. `tests/storage_smoke.rs`'s reuse
  tests account for this known overhead explicitly rather than asserting
  zero growth. See `storage/freelist.rs` module docs.
- **Free-list lookup is a linear scan** (`Vec` + `.position()`), fine at
  this scale/test-suite size; a real engine would index reclaimable pages
  by txn id for O(log n) lookup instead.
- **Keys must stay small** (well under one page): only *values* get
  overflow-page support; a single oversized key still hits the page-size
  assertion. See "Overflow pages for oversized values" above.
- **Single global writer**: matches LMDB's model and keeps the design
  simple/correct, but it means no write concurrency — writers serialize
  even across unrelated files/tasks.
- **`browse()` materializes results** rather than exposing a stateful
  cursor, so it's a snapshot read, not incremental like real
  STARTBR/READNEXT.
- **Tasks dispatch synchronously** on the calling thread, including tasks
  queued via `START`; there's no real concurrent task scheduler.
