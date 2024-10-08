use serde::Serialize;

pub type Result<T> = core::result::Result<T, Error>;

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

impl core::fmt::Display for Error {
	fn fmt(
		&self,
		fmt: &mut core::fmt::Formatter<'_>,
	) -> core::result::Result<(), core::fmt::Error> {
		write!(fmt, "{self:?}")
	}
}

impl core::error::Error for Error {}
