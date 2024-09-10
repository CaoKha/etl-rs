mod error;
use std::{fmt::Display, str::FromStr};

use crate::config::auth_config;

pub use self::error::{Error, Result};
// use crate::config::auth_config;
use hmac::{Hmac, Mac};
use lib_utils::{
	b64::{b64u_decode_to_string, b64u_encode},
	time::{now_utc, now_utc_plus_sec_str, parse_utc},
};
use sha2::Sha512;
use uuid::Uuid;

// region: --- Token Type

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub struct Token {
	pub ident: String,     // identifier (username for example)
	pub expire: String,    // Expiration date in RFC3339
	pub sign_b64u: String, // Signature, base64url encoded
}

impl FromStr for Token {
	type Err = Error;
	fn from_str(token_str: &str) -> std::result::Result<Self, Self::Err> {
		let splits: Vec<&str> = token_str.split('.').collect();
		if splits.len() != 3 {
			return Err(Error::InvalidFormat);
		}
		let (ident_b64u, expire_b64u, sign_b64u) = (splits[0], splits[1], splits[2]);
		Ok(Self {
			ident: b64u_decode_to_string(ident_b64u)
				.map_err(|_| Error::CannotDecodeIdent)?,
			expire: b64u_decode_to_string(expire_b64u)
				.map_err(|_| Error::CannotDecodeExpire)?,
			sign_b64u: sign_b64u.to_string(),
		})
	}
}

impl Display for Token {
	fn fmt(
		&self,
		f: &mut std::fmt::Formatter<'_>,
	) -> std::result::Result<(), std::fmt::Error> {
		write!(
			f,
			"{}.{}.{}",
			b64u_encode(&self.ident),
			b64u_encode(&self.expire),
			self.sign_b64u
		)
	}
}

// endregion: --- Token Type

// region: --- Web Token Gen and Validation

pub fn gen_web_token(user: &str, salt: Uuid) -> Result<Token> {
	let config = auth_config();
	_generate_token(user, config.TOKEN_DURATION_SEC, salt, &config.TOKEN_KEY)
}

pub fn validate_web_token(origin_token: &Token, salt: Uuid) -> Result<()> {
	let config = auth_config();
	_validate_token_sign_and_expire(origin_token, salt, &config.TOKEN_KEY)?;
	Ok(())
}

fn _generate_token(
	ident: &str,
	duration_sec: f64,
	salt: Uuid,
	key: &[u8],
) -> Result<Token> {
	// Compute the two first components
	let ident = ident.to_string();
	let expire = now_utc_plus_sec_str(duration_sec);
	// Sign the two first components
	let sign_b64u = _token_sign_into_b64u(&ident, &expire, salt, key)?;
	Ok(Token {
		ident,
		expire,
		sign_b64u,
	})
}

fn _validate_token_sign_and_expire(
	origin_token: &Token,
	salt: Uuid,
	key: &[u8],
) -> Result<()> {
	let new_sign_b64u =
		_token_sign_into_b64u(&origin_token.ident, &origin_token.expire, salt, key)?;
	if new_sign_b64u != origin_token.sign_b64u {
		return Err(Error::SignatureNotMatching);
	}

	// Validate expiration
	let origin_expire =
		parse_utc(&origin_token.expire).map_err(|_| Error::ExpireNotIso)?;
	let now = now_utc();
	if origin_expire < now {
		return Err(Error::Expired);
	}

	Ok(())
}

fn _token_sign_into_b64u(
	ident: &str,
	expire: &str,
	salt: Uuid,
	key: &[u8],
) -> Result<String> {
	let content = format!("{}.{}", b64u_encode(ident), b64u_encode(expire));
	// Create a HMAC-SHA-512 from key
	let mut hmac_sha512 =
		Hmac::<Sha512>::new_from_slice(key).map_err(|_| Error::HmacFailFromSlice)?;
	// Add content
	hmac_sha512.update(content.as_bytes());
	hmac_sha512.update(salt.as_bytes());
	// Finalize and b64u encode
	let hmac_result = hmac_sha512.finalize();
	let result_bytes = hmac_result.into_bytes();
	let result = b64u_encode(result_bytes);

	Ok(result)
}
