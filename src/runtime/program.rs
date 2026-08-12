//! Program Control: the registry of loadable "programs" and the outcome
//! type that stands in for CICS's LINK / XCTL / RETURN family of commands.
//!
//! Real CICS dynamically loads a program's load module by name out of the
//! PPT (Processing Program Table) and runs it on the task's thread; here a
//! "program" is any Rust value implementing [`CicsProgram`], registered by
//! name up front. The LINK/XCTL semantics are then just plain control
//! flow: LINK is an ordinary (recursive) function call, and XCTL is
//! modeled by a program returning [`ProgramOutcome::Xctl`] instead of
//! [`ProgramOutcome::Return`] — see `region.rs` for the dispatch loop that
//! interprets it.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{CicsError, Result};
use crate::runtime::region::ExecCtx;

/// What a program did when it finished running.
pub enum ProgramOutcome {
    /// EXEC CICS RETURN: control goes back to whoever LINKed this program
    /// (or ends the task, if this was the task's top-level program).
    Return { commarea: Option<Vec<u8>> },
    /// EXEC CICS XCTL: control transfers to `program` *without* returning
    /// to this program's caller. From the caller's point of view this
    /// program simply hasn't returned yet — the eventual RETURN from
    /// `program` (or from whatever it XCTLs to next) is what goes back.
    Xctl { program: String, commarea: Option<Vec<u8>> },
}

pub trait CicsProgram: Send + Sync {
    fn execute(&self, ctx: &mut ExecCtx) -> Result<ProgramOutcome>;
}

/// Blanket impl so a plain closure can be registered as a program, handy
/// for small demo/test programs.
impl<F> CicsProgram for F
where
    F: Fn(&mut ExecCtx) -> Result<ProgramOutcome> + Send + Sync,
{
    fn execute(&self, ctx: &mut ExecCtx) -> Result<ProgramOutcome> {
        self(ctx)
    }
}

/// Stands in for CICS's PPT (Processing Program Table).
#[derive(Default, Clone)]
pub struct ProgramManager {
    programs: HashMap<String, Arc<dyn CicsProgram>>,
}

impl ProgramManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn define(&mut self, name: impl Into<String>, program: impl CicsProgram + 'static) {
        self.programs.insert(name.into(), Arc::new(program));
    }

    pub fn resolve(&self, name: &str) -> Result<Arc<dyn CicsProgram>> {
        self.programs
            .get(name)
            .cloned()
            .ok_or_else(|| CicsError::ProgramNotFound(name.to_string()))
    }
}
