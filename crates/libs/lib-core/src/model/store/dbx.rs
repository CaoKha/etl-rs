mod error;

use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

pub use error::{Error, Result};
use sqlx::{query::QueryAs, FromRow, IntoArguments, Pool, Postgres, Transaction};
use tokio::sync::Mutex;

use super::Db;

pub struct Dbx {
    db_pool: Db,
    txn_holder: Arc<Mutex<Option<TxnHolder>>>,
    with_txn: bool,
}

#[derive(Debug)]
struct TxnHolder {
    txn: Transaction<'static, Postgres>,
    counter: i32,
}

impl TxnHolder {
    fn new(txn: Transaction<'static, Postgres>) -> Self {
        TxnHolder { txn, counter: 1 }
    }

    fn inc(&mut self) {
        self.counter += 1;
    }

    fn dec(&mut self) -> i32 {
        self.counter -= 1;
        self.counter
    }
}

impl Deref for TxnHolder {
    type Target = Transaction<'static, Postgres>;
    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

impl DerefMut for TxnHolder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.txn
    }
}
impl Dbx {
    pub async fn begin_txn(&self) -> Result<()> {
        if !self.with_txn {
            return Err(Error::CannotBeginTxnWithTxnFalse);
        }
        let mut txh_g = self.txn_holder.lock().await;
        if let Some(txh) = txh_g.as_mut() {
            txh.inc();
        } else {
            let transaction = self.db_pool.begin().await?;
            let _ = txh_g.insert(TxnHolder::new(transaction));
        }
        Ok(())
    }

    pub async fn commit_txn(&self) -> Result<()> {
        if !self.with_txn {
            return Err(Error::CannotCommitTxnWithtxnFalse);
        }

        let mut txh_g= self.txn_holder.lock().await;
        if let Some(txh) = txh_g.as_mut() {
            let counter = txh.dec();
            if counter == 0 {
                if let Some(txn) = txh_g.take() {
                    txn.txn.commit().await?;
                }
            }
            Ok(())

        } else {
            Err(Error::TxnCantCommitNoOpenTxn)
        }
    }

    pub fn db(&self) -> &Pool<Postgres> {
        &self.db_pool
    }

    pub async fn fetch_one<'q, O, A>(&self, query: QueryAs<'q, Postgres, O, A>) -> Result<O>
    where O: for<'r> FromRow<'r, <Postgres as sqlx::Database>::Row> + Send + Unpin,
        A: IntoArguments<'q, Postgres> + 'q
    {
        let data  = if self.with_txn {
            let mut txh_g = self.txn_holder.lock().await;
            if let Some(txn) = txh_g.as_deref_mut() {
                query.fetch_one(txn.as_mut()).await?
            } else {
                query.fetch_one(self.db()).await?
            }
        } else {
            query.fetch_one(self.db()).await?
        };
        Ok(data)
    }
}
