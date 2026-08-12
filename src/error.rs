use thiserror::Error;

/// Mirrors CICS's philosophy of returning a condition the caller must
/// handle (EIBRESP / RESP codes), just as a typed Rust error instead of an
/// integer.
#[derive(Debug, Error)]
pub enum CicsError {
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("FILE NOT FOUND: {0}")]
    FileNotFound(String),

    #[error("NOTFND: record with key {0:?} not found in file {1}")]
    RecordNotFound(Vec<u8>, String),

    #[error("DUPKEY: record with key {0:?} already exists in file {1}")]
    DuplicateKey(Vec<u8>, String),

    #[error("PGMIDERR: program {0} is not defined")]
    ProgramNotFound(String),

    #[error("program {0} exceeded the LINK call-stack depth (possible runaway recursion)")]
    LinkStackOverflow(String),

    #[error("INVREQ: {0}")]
    InvalidRequest(String),

    #[error("task {0} not found")]
    TaskNotFound(u64),
}

pub type Result<T> = std::result::Result<T, CicsError>;
