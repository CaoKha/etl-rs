//! Model Layer
//!
//! Design:
//!
//! - The Model layer normalizes the application's data type
//!   structures and access.
//! - All application code data access must go through the Model layer.
//! - The `ModelManager` holds the internal states/resources
//!   needed by ModelControllers to access data.
//!   (e.g., db_pool, S3 client, redis client).
//! - Model Controllers (e.g., `ConvBmc`, `AgentBmc`) implement
//!   CRUD and other data access methods on a given "entity"
//!   (e.g., `Conv`, `Agent`).
//!   (`Bmc` is short for Backend Model Controller).
//! - In frameworks like Axum, Tauri, `ModelManager` are typically used as App State.
//! - ModelManager are designed to be passed as an argument
//!   to all Model Controllers functions.

use store::{dbx::Dbx, new_db_pool};

mod base;
mod error;
mod store;

pub mod agent;
pub mod modql_utils;
pub mod user;

pub use self::error::{Error, Result};

#[derive(Clone)]
pub struct ModelManager {
    dbx: Dbx,
}

impl ModelManager {
    pub async fn new() -> Result<Self> {
        let db_pool = new_db_pool()
            .await
            .map_err(|e| Error::CantCreateModelManagerProvider(e.to_string()))?;
        let dbx = Dbx::new(db_pool, false)?;
        Ok(ModelManager { dbx })
    }

    pub fn new_with_txn(&self) -> Result<ModelManager> {
        let dbx = Dbx::new(self.dbx.db().clone(), true)?;
        Ok(ModelManager { dbx })
    }

    pub fn dbx(&self) -> &Dbx {
        &self.dbx
    }
}
