//! File Control: CICS's File Control Table (FCT) maps a logical file name
//! used by application programs to a physical dataset. Here every defined
//! file shares the one underlying B-tree keyspace, partitioned by a
//! per-file id prefix, so distinct files never collide and a browse over
//! one file never sees another's keys.

use std::collections::HashMap;

use crate::error::{CicsError, Result};
use crate::storage::WriteTxn;

pub type FileId = u32;

#[derive(Default)]
pub struct FileControlTable {
    files: HashMap<String, FileId>,
    next_id: FileId,
}

impl FileControlTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn define(&mut self, name: impl Into<String>) -> FileId {
        let id = self.next_id;
        self.next_id += 1;
        self.files.insert(name.into(), id);
        id
    }

    pub fn resolve(&self, name: &str) -> Result<FileId> {
        self.files.get(name).copied().ok_or_else(|| CicsError::FileNotFound(name.to_string()))
    }
}

fn storage_key(file: FileId, record_key: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + record_key.len());
    k.extend_from_slice(&file.to_be_bytes());
    k.extend_from_slice(record_key);
    k
}

/// EXEC CICS READ FILE(file) RIDFLD(key)
pub fn exec_read(wtx: &WriteTxn, fct: &FileControlTable, file: &str, key: &[u8]) -> Result<Vec<u8>> {
    let id = fct.resolve(file)?;
    wtx.get(&storage_key(id, key)).ok_or_else(|| CicsError::RecordNotFound(key.to_vec(), file.to_string()))
}

/// EXEC CICS WRITE FILE(file) RIDFLD(key) FROM(data) — insert only; fails
/// with DUPKEY if the record already exists (use REWRITE to update).
pub fn exec_write(wtx: &mut WriteTxn, fct: &FileControlTable, file: &str, key: &[u8], data: &[u8]) -> Result<()> {
    let id = fct.resolve(file)?;
    let skey = storage_key(id, key);
    if wtx.get(&skey).is_some() {
        return Err(CicsError::DuplicateKey(key.to_vec(), file.to_string()));
    }
    wtx.put(&skey, data);
    Ok(())
}

/// EXEC CICS REWRITE FILE(file) FROM(data) — update only; fails with
/// NOTFND if the record does not already exist (use WRITE to insert).
pub fn exec_rewrite(wtx: &mut WriteTxn, fct: &FileControlTable, file: &str, key: &[u8], data: &[u8]) -> Result<()> {
    let id = fct.resolve(file)?;
    let skey = storage_key(id, key);
    if wtx.get(&skey).is_none() {
        return Err(CicsError::RecordNotFound(key.to_vec(), file.to_string()));
    }
    wtx.put(&skey, data);
    Ok(())
}

/// EXEC CICS DELETE FILE(file) RIDFLD(key)
pub fn exec_delete(wtx: &mut WriteTxn, fct: &FileControlTable, file: &str, key: &[u8]) -> Result<()> {
    let id = fct.resolve(file)?;
    let skey = storage_key(id, key);
    if !wtx.delete(&skey) {
        return Err(CicsError::RecordNotFound(key.to_vec(), file.to_string()));
    }
    Ok(())
}

/// EXEC CICS STARTBR / READNEXT / ENDBR, collapsed into one call: browses
/// `file` from `start_key` (inclusive) onward and returns up to `limit`
/// records. A real CICS browse is a live cursor you step one READNEXT at a
/// time; this materializes the page instead, which is simpler and exactly
/// as correct for a snapshot read, just not incremental.
pub fn exec_browse(
    wtx: &WriteTxn,
    fct: &FileControlTable,
    file: &str,
    start_key: Option<&[u8]>,
    limit: usize,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let id = fct.resolve(file)?;
    let lo = storage_key(id, start_key.unwrap_or(&[]));
    let hi = storage_key(id + 1, &[]); // exclusive upper bound: next file's prefix
    let mut rows = wtx.range(Some(&lo), Some(&hi));
    rows.truncate(limit);
    // Strip the file-id prefix back off before handing records to the caller.
    Ok(rows.into_iter().map(|(k, v)| (k[4..].to_vec(), v)).collect())
}
