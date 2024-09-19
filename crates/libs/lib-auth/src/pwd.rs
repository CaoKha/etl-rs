use std::str::FromStr;

use lazy_regex::regex_captures;
use scheme::{get_scheme, Scheme, SchemeStatus, DEFAULT_SCHEME};
use uuid::Uuid;

mod error;
mod scheme;
use self::error::{Error, Result};

#[cfg_attr(test, derive(Clone))]
pub struct ContentToHash {
    pub content: String,
    pub salt: Uuid,
}

struct PwdParts {
    /// The scheme only (e.g "01")
    scheme_name: String,
    /// The hashed password
    hashed: String,
}

impl FromStr for PwdParts {
    type Err = Error;
    fn from_str(pwd_with_scheme: &str) -> Result<Self> {
        regex_captures!(r#"^#(\w+)#(.*)"#, pwd_with_scheme)
            .map(|(_, scheme, hashed)| Self {
                scheme_name: scheme.to_string(),
                hashed: hashed.to_string(),
            })
            .ok_or(Error::PwdWithSchemeFailedParse)
    }
}

fn validate_for_scheme(
    scheme_name: &str,
    to_hash: ContentToHash,
    pwd_ref: String,
) -> Result<()> {
    get_scheme(scheme_name)?.validate(&to_hash, &pwd_ref)?;
    Ok(())
}

fn hash_for_scheme(scheme_name: &str, to_hash: ContentToHash) -> Result<String> {
    let pwd_hashed = get_scheme(scheme_name)?.hash(&to_hash)?;
    Ok(format!("#{scheme_name}#{pwd_hashed}"))
}

/// Validate if an ContentToHash matches
pub async fn validate_pwd(
    to_hash: ContentToHash,
    pwd_ref: String,
) -> Result<SchemeStatus> {
    let PwdParts {
        scheme_name,
        hashed,
    } = pwd_ref.parse()?;
    let scheme_status = if scheme_name == DEFAULT_SCHEME {
        SchemeStatus::Ok
    } else {
        SchemeStatus::Outdated
    };

    tokio::task::spawn_blocking(move || {
        validate_for_scheme(&scheme_name, to_hash, hashed)
    })
    .await
    .map_err(|_| Error::FailSpawnBlockForValidate)??;
    Ok(scheme_status)
}
