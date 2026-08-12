//! A small bank-transfer demo exercising every corner of the runtime:
//!
//! - Program Control: TRANSFER LINKs DEBIT and CREDIT (nested calls that
//!   return), then XCTLs to STATEMENT (control transfer, no return to
//!   TRANSFER).
//! - Task Control: TRANSFER also START's an AUDIT_LOG task, which the
//!   region dispatches after the initiating task finishes.
//! - File Control: everything above is READ/WRITE/REWRITE against an
//!   ACCOUNTS file and an AUDIT file.
//! - ACID: an overdrawn transfer ABENDs (returns Err) and is completely
//!   rolled back (dynamic transaction backout); a successful one survives
//!   closing and reopening the region (durability).

use cics_tx::runtime::ExecCtx;
use cics_tx::{CicsError, ProgramOutcome, Region, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct TransferRequest {
    from: String,
    to: String,
    amount: i64,
}

fn get_balance(ctx: &ExecCtx, acct: &str) -> Result<i64> {
    let bytes = ctx.read("ACCOUNTS", acct.as_bytes())?;
    Ok(i64::from_le_bytes(bytes.try_into().expect("balance record is 8 bytes")))
}

fn register_programs(region: &mut Region) {
    region.define_program("OPEN_ACCOUNTS", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        ctx.write("ACCOUNTS", b"ALICE", &1000i64.to_le_bytes())?;
        ctx.write("ACCOUNTS", b"BOB", &500i64.to_le_bytes())?;
        Ok(ProgramOutcome::Return { commarea: None })
    });

    region.define_program("DEBIT", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        let req: TransferRequest = bincode::deserialize(ctx.commarea().expect("commarea required")).unwrap();
        let balance = get_balance(ctx, &req.from)?;
        if balance < req.amount {
            return Err(CicsError::InvalidRequest(format!(
                "insufficient funds in {}: have {}, need {}",
                req.from, balance, req.amount
            )));
        }
        ctx.rewrite("ACCOUNTS", req.from.as_bytes(), &(balance - req.amount).to_le_bytes())?;
        Ok(ProgramOutcome::Return { commarea: None })
    });

    region.define_program("CREDIT", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        let req: TransferRequest = bincode::deserialize(ctx.commarea().expect("commarea required")).unwrap();
        let balance = get_balance(ctx, &req.to)?;
        ctx.rewrite("ACCOUNTS", req.to.as_bytes(), &(balance + req.amount).to_le_bytes())?;
        Ok(ProgramOutcome::Return { commarea: None })
    });

    region.define_program("AUDIT_LOG", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        let req: TransferRequest = bincode::deserialize(ctx.commarea().expect("commarea required")).unwrap();
        let key = format!("{:020}", ctx.eib().task_id);
        let line = format!("{} -> {} : {}", req.from, req.to, req.amount);
        println!("  [task {}] AUDIT_LOG writing: {line}", ctx.eib().task_id);
        ctx.write("AUDIT", key.as_bytes(), line.as_bytes())?;
        Ok(ProgramOutcome::Return { commarea: None })
    });

    region.define_program("STATEMENT", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        let req: TransferRequest = bincode::deserialize(ctx.commarea().expect("commarea required")).unwrap();
        let from_balance = get_balance(ctx, &req.from)?;
        let to_balance = get_balance(ctx, &req.to)?;
        println!(
            "  [task {}] STATEMENT (via XCTL): {} = {}, {} = {}",
            ctx.eib().task_id,
            req.from,
            from_balance,
            req.to,
            to_balance
        );
        Ok(ProgramOutcome::Return { commarea: Some(bincode::serialize(&(from_balance, to_balance)).unwrap()) })
    });

    // TRANSFER is the orchestrator: LINK (call-and-return) to DEBIT and
    // CREDIT, START an async audit task, then XCTL (transfer-and-forget)
    // to STATEMENT for the final report.
    region.define_program("TRANSFER", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        let commarea = ctx.commarea().expect("commarea required").to_vec();
        let req: TransferRequest = bincode::deserialize(&commarea).unwrap();
        println!("  [task {}] TRANSFER: LINK DEBIT({})", ctx.eib().task_id, req.from);
        ctx.link("DEBIT", Some(&commarea))?;
        println!("  [task {}] TRANSFER: LINK CREDIT({})", ctx.eib().task_id, req.to);
        ctx.link("CREDIT", Some(&commarea))?;
        ctx.start("AUD1", "AUDIT_LOG", Some(&commarea));
        println!("  [task {}] TRANSFER: XCTL STATEMENT", ctx.eib().task_id);
        Ok(ProgramOutcome::Xctl { program: "STATEMENT".to_string(), commarea: Some(commarea) })
    });

    region.define_program("LIST_AUDIT", |ctx: &mut ExecCtx| -> Result<ProgramOutcome> {
        for (key, value) in ctx.browse("AUDIT", None, 100)? {
            println!("  AUDIT[{}] = {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
        }
        Ok(ProgramOutcome::Return { commarea: None })
    });
}

fn transfer(region: &Region, from: &str, to: &str, amount: i64) -> Result<(i64, i64)> {
    let req = TransferRequest { from: from.to_string(), to: to.to_string(), amount };
    let commarea = bincode::serialize(&req).unwrap();
    let result = region.start_task("XFER", "TRANSFER", Some(&commarea))?;
    Ok(bincode::deserialize(&result.expect("STATEMENT always returns a commarea")).unwrap())
}

fn main() -> Result<()> {
    let path = std::env::temp_dir().join("cics_tx_bank_demo.db");
    let _ = std::fs::remove_file(&path);

    let mut region = Region::open(&path)?;
    region.define_file("ACCOUNTS");
    region.define_file("AUDIT");
    register_programs(&mut region);

    println!("=== task 1: OPEN_ACCOUNTS ===");
    region.start_task("SETU", "OPEN_ACCOUNTS", None)?;

    println!("\n=== task 2: TRANSFER 200 ALICE -> BOB (should succeed) ===");
    let (alice, bob) = transfer(&region, "ALICE", "BOB", 200)?;
    println!("result: ALICE={alice} BOB={bob}");
    assert_eq!((alice, bob), (800, 700));

    println!("\n=== audit trail written by the START-ed AUDIT_LOG task ===");
    region.start_task("LIST", "LIST_AUDIT", None)?;

    println!("\n=== task 3: TRANSFER 10000 ALICE -> BOB (should ABEND and roll back) ===");
    match transfer(&region, "ALICE", "BOB", 10_000) {
        Ok(_) => panic!("overdraft should have failed"),
        Err(e) => println!("task ABENDed as expected: {e}"),
    }

    println!("\n=== verifying the failed transfer left no trace (atomicity) ===");
    let commarea = region.start_task("SHOW", "STATEMENT", Some(&bincode::serialize(&TransferRequest {
        from: "ALICE".to_string(),
        to: "BOB".to_string(),
        amount: 0,
    }).unwrap()))?;
    let (alice, bob): (i64, i64) = bincode::deserialize(&commarea.unwrap()).unwrap();
    println!("balances after failed transfer: ALICE={alice} BOB={bob}");
    assert_eq!((alice, bob), (800, 700), "ABEND must not have changed any balance");

    drop(region);
    println!("\n=== reopening the region to confirm durability ===");
    // Program/file *definitions* are region config (like CICS's PPT/FCT),
    // reloaded on startup -- it's the underlying data file that's durable
    // and gets reattached here.
    let mut region2 = Region::open(&path)?;
    region2.define_file("ACCOUNTS");
    region2.define_file("AUDIT");
    register_programs(&mut region2);
    let commarea = region2.start_task("SHOW", "STATEMENT", Some(&bincode::serialize(&TransferRequest {
        from: "ALICE".to_string(),
        to: "BOB".to_string(),
        amount: 0,
    }).unwrap()))?;
    let (alice, bob): (i64, i64) = bincode::deserialize(&commarea.unwrap()).unwrap();
    println!("balances after reopening the store: ALICE={alice} BOB={bob}");
    assert_eq!((alice, bob), (800, 700), "committed data must survive a restart");

    println!("\nall good.");
    Ok(())
}
