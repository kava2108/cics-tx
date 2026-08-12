//! Task Control: the CICS concept of a *task* is one execution instance of
//! a *transaction* (a 4-char-ish TRANID in real CICS; here just a short
//! string). Each task gets a unique task number and an EIB-like block of
//! "environment" facts a running program can inspect — mirroring how a
//! CICS application reads `EIBTASKN`, `EIBTRNID`, etc.

use std::sync::atomic::{AtomicU64, Ordering};

pub type TaskId = u64;

/// Execute Interface Block analog: read-only facts about the current task,
/// available to every program in the LINK/XCTL chain.
#[derive(Debug, Clone)]
pub struct Eib {
    pub task_id: TaskId,
    pub tranid: String,
    /// How many programs deep the current LINK chain is (1 = the task's
    /// initial program).
    pub link_level: u32,
}

#[derive(Default)]
pub struct TaskIdGenerator {
    next: AtomicU64,
}

impl TaskIdGenerator {
    pub fn new() -> Self {
        Self { next: AtomicU64::new(1) }
    }

    pub fn next(&self) -> TaskId {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}
