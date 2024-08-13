use artemis_rs::config::FILES_PATH;
use artemis_rs::jdd::config::Transform;
use artemis_rs::jdd::schema::{Jdd, JddSchema};
use artemis_rs::jdd::transforms::{
    ape::col_ape_with_polars_expr, code_naf::col_code_naf_with_polars_expr, col_with_udf_expr,
    email::col_email_with_polars_expr, libelle_naf::col_libelle_naf_with_polars_expr,
    nom::col_nom_with_polars_expr, prenom::col_prenom_with_polars_expr,
    raison_sociale::col_raison_sociale_with_polars_expr, siren::col_siren_with_polars_expr,
    siret::col_siret_with_polars_expr,
};
use dotenv::dotenv;
use log::info;
use polars::prelude::*;
use sea_query::{ColumnRef, PostgresQueryBuilder, Query};
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
    let sql = Query::select()
        .column(ColumnRef::Asterisk)
        .from(Jdd::Table)
        .to_owned()
        .to_string(PostgresQueryBuilder);
    let rows: Vec<JddSchema> = sqlx::query_as(&sql).fetch_all(&pool).await?;
    let df = struct_to_dataframe(&rows);

    let lf = df.lazy().with_columns(vec![
        col_nom_with_polars_expr(),
        col_prenom_with_polars_expr(),
        col_with_udf_expr(Jdd::Civilite, Transform::Civilite),
        col_email_with_polars_expr(),
        col_with_udf_expr(Jdd::Telephone, Transform::Telephone),
        col_raison_sociale_with_polars_expr(),
        col_code_naf_with_polars_expr(),
        col_ape_with_polars_expr(),
        col_siret_with_polars_expr(),
        col_siren_with_polars_expr(),
        col_libelle_naf_with_polars_expr(),
    ]);

    let mut df = lf.collect()?;

    let mut csv_file =
        std::fs::File::create(String::from(FILES_PATH) + "JDD_normalisation_transformed.csv")?;
    CsvWriter::new(&mut csv_file).finish(&mut df)?;

    Ok(())
}
