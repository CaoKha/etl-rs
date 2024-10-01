use serde::Serialize;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Serialize)]
pub enum Error {
    CtxCannotNewRootCtx,
}

impl std::fmt::Display for Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "{self:?}")
    }
}

impl std::error::Error for Error {}
