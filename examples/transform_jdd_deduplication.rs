use artemis_rs::jdd::schema::{Jdd, JddSchema};
use dotenv::dotenv;
use log::info;
use polars::prelude::*;
use sea_query::{ColumnRef, PostgresQueryBuilder, Query};
use sea_query_binder::SqlxBinder;
use serde::Serialize;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use std::env;

fn struct_to_dataframe<T>(input: &[T]) -> DataFrame
where
    T: Serialize,
{
    // Serialize structs to a vector of maps
    let mut vec_of_maps = Vec::new();
    for e in input.iter() {
        let json_value = serde_json::to_value(e).unwrap();
        if let Value::Object(map) = json_value {
            vec_of_maps.push(map);
        }
    }

    // Initialize column vectors
    let mut columns: HashMap<String, Vec<Option<String>>> = HashMap::new();

    // Populate columns from the vector of maps
    for map in vec_of_maps.iter() {
        for (key, value) in map.iter() {
            let column = columns.entry(key.clone()).or_default();
            match value {
                Value::String(s) => column.push(Some(s.clone())),
                Value::Null => column.push(None),
                _ => unreachable!(), // assuming all fields are Option<String>
            }
        }
    }

    // Create DataFrame
    let mut df = DataFrame::default();
    for (key, values) in columns.into_iter() {
        let series = Series::new(&key, values);
        df.with_column(series).unwrap();
    }
    df
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    // Initialize PostgreSQL connection pool
    let postgres_url = env::var("POSTGRES_URL").expect("POSTGRES_URI must be set");
    info!("Database URL: {}", postgres_url);
    let pool = PgPool::connect(&postgres_url)
        .await
        .expect("Postgres connection failed");
    let (sql, values) = Query::select()
        .column(ColumnRef::Asterisk)
        .from(Jdd::Table)
        .build_sqlx(PostgresQueryBuilder);
    let rows: Vec<JddSchema> = sqlx::query_as_with(&sql, values).fetch_all(&pool).await?;
    let df = struct_to_dataframe(&rows);

    println!("{:#?}", df);
    Ok(())
}
