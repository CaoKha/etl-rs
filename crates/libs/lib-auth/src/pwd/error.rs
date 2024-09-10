use derive_more::derive::From;
use serde::Serialize;

use super::scheme;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Serialize, From)]
pub enum Error {
	PwdWithSchemeFailedParse,
	FailSpawnBlockForValidate,
	FailSpawnBlockForHash,
	#[from]
	Scheme(scheme::Error),
}

impl std::fmt::Display for Error {
	fn fmt(
		&self,
		f: &mut std::fmt::Formatter<'_>,
	) -> std::result::Result<(), std::fmt::Error> {
		write!(f, "{self:?}")
	}
}

impl std::error::Error for Error {}
