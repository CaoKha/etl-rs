use uuid::Uuid;

mod error;
mod scheme;

#[cfg_attr(test, derive(Clone))]
pub struct ContentToHash {
	pub content: String,
	pub salt: Uuid,
}
