use sqlx::{Pool, Postgres};

mod dbx;
pub type Db = Pool<Postgres>;
