use derive_more::From;
use serde::Serialize;
use serde_with::{serde_as, DisplayFromStr};
pub type Result<T> = std::result::Result<T, Error>;


#[serde_as]
#[derive(Debug, Serialize, From)]
pub enum Error {
    TxnCantCommitNoOpenTxn,
    CannotBeginTxnWithTxnFalse,
    CannotCommitTxnWithtxnFalse,
    NoTxn,
    #[from]
    Sqlx(#[serde_as(as = "DisplayFromStr")] sqlx::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "{self:?}")
    }
}

impl std::error::Error for Error {}
