use serde::Serialize;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Serialize)]
pub enum Error {
	HmacFailFromSlice,
	InvalidFormat,
	CannotDecodeIdent,
	CannotDecodeExpire,
	SignatureNotMatching,
	ExpireNotIso,
	Expired,
}

impl std::fmt::Display for Error {
	fn fmt(
		&self,
		fmt: &mut std::fmt::Formatter<'_>,
	) -> std::result::Result<(), std::fmt::Error> {
		write!(fmt, "{self:?}")
	}
}

impl std::error::Error for Error {}
