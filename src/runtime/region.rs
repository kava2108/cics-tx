//! The Region ties Task Control, Program Control and File Control to the
//! storage engine, and is where CICS's implicit-syncpoint rule lives:
//! a task that RETURNs normally commits its unit of work; a task that
//! ABENDs (returns `Err`) gets a dynamic transaction backout (rollback).
//! That's the whole ACID story from the application's point of view.

use std::collections::VecDeque;
use std::path::Path;

use crate::error::Result;
use crate::runtime::file_control::{self, FileControlTable, FileId};
use crate::runtime::program::{CicsProgram, ProgramManager, ProgramOutcome};
use crate::runtime::task::{Eib, TaskId, TaskIdGenerator};
use crate::storage::{Environment, WriteTxn};

/// A task queued by `EXEC CICS START`, run after the task that issued it
/// finishes (real CICS would schedule it independently; this single
/// writer, single-threaded region just defers it to the same dispatch
/// loop, which keeps ordering deterministic for the demo/tests).
struct PendingStart {
    tranid: String,
    program: String,
    commarea: Option<Vec<u8>>,
}

const MAX_LINK_DEPTH: u32 = 64;

pub struct Region {
    env: Environment,
    programs: ProgramManager,
    fct: FileControlTable,
    task_ids: TaskIdGenerator,
}

impl Region {
    pub fn open(path: &Path) -> Result<Region> {
        Ok(Region {
            env: Environment::open(path)?,
            programs: ProgramManager::new(),
            fct: FileControlTable::new(),
            task_ids: TaskIdGenerator::new(),
        })
    }

    pub fn define_program(&mut self, name: impl Into<String>, program: impl CicsProgram + 'static) {
        self.programs.define(name, program);
    }

    pub fn define_file(&mut self, name: impl Into<String>) -> FileId {
        self.fct.define(name)
    }

    /// EXEC CICS START TRANID(tranid) PROGRAM(program) — runs the task to
    /// completion (including any further tasks it itself started) and
    /// returns the COMMAREA the originating task's top-level RETURN
    /// carried back.
    pub fn start_task(&self, tranid: &str, program: &str, commarea: Option<&[u8]>) -> Result<Option<Vec<u8>>> {
        let mut queue: VecDeque<PendingStart> = VecDeque::new();
        queue.push_back(PendingStart {
            tranid: tranid.to_string(),
            program: program.to_string(),
            commarea: commarea.map(|c| c.to_vec()),
        });

        let mut originating_result = None;
        let mut is_first = true;

        while let Some(req) = queue.pop_front() {
            let task_id = self.task_ids.next();
            let eib = Eib { task_id, tranid: req.tranid, link_level: 1 };
            let mut txn: Option<WriteTxn> = Some(self.env.begin_write());

            let outcome = run_program_loop(
                &self.env,
                &self.programs,
                &self.fct,
                &mut txn,
                &mut queue,
                &self.task_ids,
                eib,
                req.program,
                req.commarea,
            );

            match &outcome {
                Ok(_) => {
                    // Implicit SYNCPOINT: a task that ends normally commits.
                    if let Some(wtx) = txn.take() {
                        wtx.commit()?;
                    }
                }
                Err(_) => {
                    // Dynamic transaction backout: an ABENDing task's
                    // uncommitted changes never become visible.
                    if let Some(wtx) = txn.take() {
                        wtx.rollback();
                    }
                }
            }

            if is_first {
                originating_result = Some(outcome);
                is_first = false;
            }
        }

        originating_result.expect("the originating request is always dispatched first")
    }

    pub fn task_count_hint(&self) -> TaskId {
        // Not a real CICS statistic, just handy for demos/tests to show
        // task ids were actually allocated.
        self.task_ids.next()
    }
}

#[allow(clippy::too_many_arguments)]
fn run_program_loop<'env>(
    env: &'env Environment,
    programs: &ProgramManager,
    fct: &FileControlTable,
    txn: &mut Option<WriteTxn<'env>>,
    pending_starts: &mut VecDeque<PendingStart>,
    task_ids: &TaskIdGenerator,
    eib: Eib,
    mut program_name: String,
    mut commarea: Option<Vec<u8>>,
) -> Result<Option<Vec<u8>>> {
    loop {
        let program = programs.resolve(&program_name)?;
        let mut ctx = ExecCtx {
            env,
            programs,
            fct,
            txn,
            pending_starts,
            task_ids,
            eib: eib.clone(),
            commarea,
        };
        match program.execute(&mut ctx)? {
            ProgramOutcome::Return { commarea } => return Ok(commarea),
            ProgramOutcome::Xctl { program, commarea: c } => {
                program_name = program;
                commarea = c;
            }
        }
    }
}

/// EXEC CICS interface handed to a running program: EIB access, LINK,
/// START, File Control, and SYNCPOINT / SYNCPOINT ROLLBACK.
pub struct ExecCtx<'a, 'env> {
    env: &'env Environment,
    programs: &'a ProgramManager,
    fct: &'a FileControlTable,
    txn: &'a mut Option<WriteTxn<'env>>,
    pending_starts: &'a mut VecDeque<PendingStart>,
    task_ids: &'a TaskIdGenerator,
    eib: Eib,
    commarea: Option<Vec<u8>>,
}

impl<'a, 'env> ExecCtx<'a, 'env> {
    pub fn eib(&self) -> &Eib {
        &self.eib
    }

    /// The COMMAREA this program was invoked with (via START, LINK, or XCTL).
    pub fn commarea(&self) -> Option<&[u8]> {
        self.commarea.as_deref()
    }

    /// EXEC CICS LINK: calls `program` and waits for it to RETURN,
    /// returning the COMMAREA it RETURNed with. An ordinary nested call —
    /// the current program's state is untouched and resumes right after.
    pub fn link(&mut self, program: &str, commarea: Option<&[u8]>) -> Result<Option<Vec<u8>>> {
        let mut new_eib = self.eib.clone();
        new_eib.link_level += 1;
        if new_eib.link_level > MAX_LINK_DEPTH {
            return Err(crate::error::CicsError::LinkStackOverflow(program.to_string()));
        }
        run_program_loop(
            self.env,
            self.programs,
            self.fct,
            self.txn,
            self.pending_starts,
            self.task_ids,
            new_eib,
            program.to_string(),
            commarea.map(|c| c.to_vec()),
        )
    }

    /// EXEC CICS START: schedules `program` as a new task, run after the
    /// current task's program chain finishes returning.
    pub fn start(&mut self, tranid: &str, program: &str, commarea: Option<&[u8]>) {
        self.pending_starts.push_back(PendingStart {
            tranid: tranid.to_string(),
            program: program.to_string(),
            commarea: commarea.map(|c| c.to_vec()),
        });
    }

    /// EXEC CICS READ FILE(file) RIDFLD(key)
    pub fn read(&self, file: &str, key: &[u8]) -> Result<Vec<u8>> {
        let wtx = self.txn.as_ref().expect("write transaction is always present during task execution");
        file_control::exec_read(wtx, self.fct, file, key)
    }

    /// EXEC CICS WRITE FILE(file) RIDFLD(key) FROM(data)
    pub fn write(&mut self, file: &str, key: &[u8], data: &[u8]) -> Result<()> {
        let wtx = self.txn.as_mut().expect("write transaction is always present during task execution");
        file_control::exec_write(wtx, self.fct, file, key, data)
    }

    /// EXEC CICS REWRITE FILE(file) FROM(data)
    pub fn rewrite(&mut self, file: &str, key: &[u8], data: &[u8]) -> Result<()> {
        let wtx = self.txn.as_mut().expect("write transaction is always present during task execution");
        file_control::exec_rewrite(wtx, self.fct, file, key, data)
    }

    /// EXEC CICS DELETE FILE(file) RIDFLD(key)
    pub fn delete(&mut self, file: &str, key: &[u8]) -> Result<()> {
        let wtx = self.txn.as_mut().expect("write transaction is always present during task execution");
        file_control::exec_delete(wtx, self.fct, file, key)
    }

    /// EXEC CICS STARTBR + repeated READNEXT + ENDBR, collapsed into one
    /// snapshot browse (see `file_control::exec_browse`).
    pub fn browse(&self, file: &str, start_key: Option<&[u8]>, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let wtx = self.txn.as_ref().expect("write transaction is always present during task execution");
        file_control::exec_browse(wtx, self.fct, file, start_key, limit)
    }

    /// EXEC CICS SYNCPOINT: commits everything done so far in this task
    /// (and any LINKed programs) and starts a fresh unit of work.
    pub fn syncpoint(&mut self) -> Result<()> {
        let wtx = self.txn.take().expect("write transaction is always present during task execution");
        wtx.commit()?;
        *self.txn = Some(self.env.begin_write());
        Ok(())
    }

    /// EXEC CICS SYNCPOINT ROLLBACK: discards everything done so far in
    /// this unit of work and starts a fresh one; the task keeps running.
    pub fn syncpoint_rollback(&mut self) {
        let wtx = self.txn.take().expect("write transaction is always present during task execution");
        wtx.rollback();
        *self.txn = Some(self.env.begin_write());
    }
}
