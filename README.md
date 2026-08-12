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

## Layout

```
src/
  storage/          the embedded KV engine
    pager.rs         fixed-size page I/O, double-buffered meta page
    codec.rs         checksummed page (de)serialization
    btree.rs         copy-on-write B-tree (get/put/delete/range)
    txn.rs           Environment / ReadTxn / WriteTxn
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

- **No free-list / page reuse**: deleted/superseded pages are never
  reclaimed, so the file only grows. A free-list keyed by the txn id after
  which a page became unreachable (so it's safe to reuse once no reader
  can still see it) is the natural v2 addition.
- **No node merge/rebalance on delete**: leaves can become underfull;
  correctness is unaffected, only space efficiency.
- **No overflow pages**: a single key+value must fit well within one 4 KB
  page (~3 KB budget); there's no chaining for large records yet.
- **Single global writer**: matches LMDB's model and keeps the design
  simple/correct, but it means no write concurrency — writers serialize
  even across unrelated files/tasks.
- **`browse()` materializes results** rather than exposing a stateful
  cursor, so it's a snapshot read, not incremental like real
  STARTBR/READNEXT.
- **Tasks dispatch synchronously** on the calling thread, including tasks
  queued via `START`; there's no real concurrent task scheduler.
