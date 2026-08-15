//! The CICS-style transaction-processing runtime: Task Control, Program
//! Control (LINK/XCTL/RETURN), File Control, and SYNCPOINT, all built on
//! top of the `storage` module's ACID key/value engine.

pub mod file_control;
pub mod program;
pub mod region;
pub mod task;

pub use program::{CicsProgram, ProgramManager, ProgramOutcome};
pub use region::{link_external, ExecCtx, Region};
pub use task::{Eib, TaskId, TaskIdGenerator};
