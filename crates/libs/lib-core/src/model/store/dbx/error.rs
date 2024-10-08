use derive_more::From;
use serde::Serialize;
use serde_with::{serde_as, DisplayFromStr};
pub type Result<T> = core::result::Result<T, Error>;


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

impl core::fmt::Display for Error {
    fn fmt(&self, fmt: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(fmt, "{self:?}")
    }
}

impl core::error::Error for Error {}
