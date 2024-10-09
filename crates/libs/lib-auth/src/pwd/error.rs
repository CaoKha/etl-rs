use derive_more::derive::From;
use serde::Serialize;

use super::scheme;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Serialize, From)]
pub enum Error {
    PwdWithSchemeFailedParse,
    FailSpawnBlockForValidate,
    FailSpawnBlockForHash,
    #[from]
    Scheme(scheme::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::result::Result<(), core::fmt::Error> {
        write!(f, "{self:?}")
    }
}

impl core::error::Error for Error {}
