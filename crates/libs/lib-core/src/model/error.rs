use lib_auth::pwd;
use derive_more::From;
use serde::Serialize;

#[derive(Debug, Serialize, From)]
pub enum Error {
    EntityNotFound {
        entity:  &'static str,
        id: i64
    },
    ListLimitOverMax {
        max: i64,
        actual: i64,
    },
    CountFail,
    UserAlreadyExists {
        username: String,
    },
    UniqueViolation {
        table: String,
        constraint: String,
    },
    CantCreateModelManagerProvider(String),
    #[from]
    Pwd(pwd::Error),
    

}
