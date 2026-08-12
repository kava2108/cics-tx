//! Integration tests for the CICS-style runtime layered on the storage
//! engine: Program Control (LINK/XCTL), Task Control, File Control, and
//! SYNCPOINT / SYNCPOINT ROLLBACK semantics.

use cics_tx::runtime::ExecCtx;
use cics_tx::{CicsError, ProgramOutcome, Region, Result};

fn open_region() -> (tempfile::TempDir, Region) {
    let dir = tempfile::tempdir().unwrap();
    let region = Region::open(&dir.path().join("region.db")).unwrap();
    (dir, region)
}

#[test]
fn link_returns_control_to_caller() {
    let (_dir, mut region) = open_region();
    region.define_program("CHILD", |_ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        Ok(ProgramOutcome::Return { commarea: Some(b"from child".to_vec()) })
    });
    region.define_program("PARENT", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        let reply = ctx.link("CHILD", None)?;
        assert_eq!(reply, Some(b"from child".to_vec()));
        Ok(ProgramOutcome::Return { commarea: Some(b"from parent, after child returned".to_vec()) })
    });

    let result = region.start_task("T001", "PARENT", None).unwrap();
    assert_eq!(result, Some(b"from parent, after child returned".to_vec()));
}

#[test]
fn xctl_does_not_return_to_the_transferring_program() {
    let (_dir, mut region) = open_region();
    region.define_program("FINAL", |_ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        Ok(ProgramOutcome::Return { commarea: Some(b"final answer".to_vec()) })
    });
    region.define_program("FIRST", |_ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        Ok(ProgramOutcome::Xctl { program: "FINAL".to_string(), commarea: None })
    });

    // The task's result is FINAL's RETURN, not anything FIRST could have
    // produced -- there is no way back into FIRST after the XCTL.
    let result = region.start_task("T002", "FIRST", None).unwrap();
    assert_eq!(result, Some(b"final answer".to_vec()));
}

#[test]
fn runaway_link_recursion_errors_instead_of_overflowing_the_stack() {
    let (_dir, mut region) = open_region();
    region.define_program("RECURSE", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        ctx.link("RECURSE", None)?;
        Ok(ProgramOutcome::Return { commarea: None })
    });

    let err = region.start_task("T003", "RECURSE", None).unwrap_err();
    assert!(matches!(err, CicsError::LinkStackOverflow(_)), "expected LinkStackOverflow, got {err:?}");
}

#[test]
fn abend_rolls_back_every_file_control_change_in_the_task_including_link_levels() {
    let (_dir, mut region) = open_region();
    region.define_file("LEDGER");

    region.define_program("STEP_B", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        ctx.write("LEDGER", b"b", b"written-by-b")?;
        Err(CicsError::InvalidRequest("simulated ABEND".to_string()))
    });
    region.define_program("STEP_A", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        ctx.write("LEDGER", b"a", b"written-by-a")?;
        ctx.link("STEP_B", None)?; // propagates the Err, ABENDing the task
        Ok(ProgramOutcome::Return { commarea: None })
    });

    let err = region.start_task("T004", "STEP_A", None).unwrap_err();
    assert!(matches!(err, CicsError::InvalidRequest(_)));

    // Neither write should be visible: dynamic transaction backout undoes
    // the whole unit of work, not just the LINK level that failed.
    region.define_program("CHECK", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        assert!(ctx.read("LEDGER", b"a").is_err());
        assert!(ctx.read("LEDGER", b"b").is_err());
        Ok(ProgramOutcome::Return { commarea: None })
    });
    region.start_task("T005", "CHECK", None).unwrap();
}

#[test]
fn syncpoint_makes_prior_writes_durable_even_if_the_task_later_abends() {
    let (_dir, mut region) = open_region();
    region.define_file("LEDGER");

    region.define_program("PARTIAL_COMMIT", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        ctx.write("LEDGER", b"committed", b"before syncpoint")?;
        ctx.syncpoint()?; // durability boundary
        ctx.write("LEDGER", b"lost", b"after syncpoint")?;
        Err(CicsError::InvalidRequest("simulated ABEND after syncpoint".to_string()))
    });
    let err = region.start_task("T006", "PARTIAL_COMMIT", None).unwrap_err();
    assert!(matches!(err, CicsError::InvalidRequest(_)));

    region.define_program("CHECK", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        assert_eq!(ctx.read("LEDGER", b"committed").unwrap(), b"before syncpoint");
        assert!(ctx.read("LEDGER", b"lost").is_err(), "write after the last syncpoint must not survive the ABEND");
        Ok(ProgramOutcome::Return { commarea: None })
    });
    region.start_task("T007", "CHECK", None).unwrap();
}

#[test]
fn syncpoint_rollback_discards_only_the_current_unit_of_work() {
    let (_dir, mut region) = open_region();
    region.define_file("LEDGER");

    region.define_program("ROLLBACK_THEN_CONTINUE", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        ctx.write("LEDGER", b"doomed", b"x")?;
        ctx.syncpoint_rollback();
        ctx.write("LEDGER", b"survivor", b"y")?;
        Ok(ProgramOutcome::Return { commarea: None })
    });
    region.start_task("T008", "ROLLBACK_THEN_CONTINUE", None).unwrap();

    region.define_program("CHECK", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        assert!(ctx.read("LEDGER", b"doomed").is_err());
        assert_eq!(ctx.read("LEDGER", b"survivor").unwrap(), b"y");
        Ok(ProgramOutcome::Return { commarea: None })
    });
    region.start_task("T009", "CHECK", None).unwrap();
}

#[test]
fn start_schedules_a_separate_task_with_its_own_unit_of_work() {
    let (_dir, mut region) = open_region();
    region.define_file("LEDGER");

    region.define_program("DOOMED_CHILD", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        ctx.write("LEDGER", b"child", b"partial")?;
        Err(CicsError::InvalidRequest("child ABEND".to_string()))
    });
    region.define_program("PARENT", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        ctx.write("LEDGER", b"parent", b"ok")?;
        ctx.start("CHLD", "DOOMED_CHILD", None);
        Ok(ProgramOutcome::Return { commarea: None })
    });

    // The originating task's own result reflects only itself; the
    // started child's later ABEND must not roll back the parent, which
    // already committed its own unit of work.
    region.start_task("T010", "PARENT", None).unwrap();

    region.define_program("CHECK", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        assert_eq!(ctx.read("LEDGER", b"parent").unwrap(), b"ok");
        assert!(ctx.read("LEDGER", b"child").is_err(), "the child task's own ABEND must still roll back its own writes");
        Ok(ProgramOutcome::Return { commarea: None })
    });
    region.start_task("T011", "CHECK", None).unwrap();
}

#[test]
fn write_is_insert_only_and_rewrite_is_update_only() {
    let (_dir, mut region) = open_region();
    region.define_file("F");
    region.define_program("OPS", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        ctx.write("F", b"k", b"v1")?;
        assert!(matches!(ctx.write("F", b"k", b"v2"), Err(CicsError::DuplicateKey(_, _))));
        assert!(matches!(ctx.rewrite("F", b"missing", b"v"), Err(CicsError::RecordNotFound(_, _))));
        ctx.rewrite("F", b"k", b"v2")?;
        assert_eq!(ctx.read("F", b"k").unwrap(), b"v2");
        ctx.delete("F", b"k")?;
        assert!(matches!(ctx.delete("F", b"k"), Err(CicsError::RecordNotFound(_, _))));
        Ok(ProgramOutcome::Return { commarea: None })
    });
    region.start_task("T012", "OPS", None).unwrap();
}

#[test]
fn undefined_program_and_file_are_reported_by_name() {
    let (_dir, mut region) = open_region();
    let err = region.start_task("T013", "NO_SUCH_PROGRAM", None).unwrap_err();
    assert!(matches!(err, CicsError::ProgramNotFound(name) if name == "NO_SUCH_PROGRAM"));

    region.define_program("USES_MISSING_FILE", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        ctx.read("NO_SUCH_FILE", b"k")?;
        Ok(ProgramOutcome::Return { commarea: None })
    });
    let err = region.start_task("T014", "USES_MISSING_FILE", None).unwrap_err();
    assert!(matches!(err, CicsError::FileNotFound(name) if name == "NO_SUCH_FILE"));
}
