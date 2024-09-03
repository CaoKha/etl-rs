use artemis_rs::config::{Transform, FILES_PATH};
use artemis_rs::schemas::jdd::{Jdd, JddSchema};
use artemis_rs::schemas::SchemasEnum;
use artemis_rs::transforms::ape::col_ape_with_polars_expr;
use artemis_rs::transforms::code_naf::col_code_naf_with_polars_expr;
use artemis_rs::transforms::col_with_udf_expr;
use artemis_rs::transforms::email::col_email_with_polars_expr;
use artemis_rs::transforms::libelle_naf::col_libelle_naf_with_polars_expr;
use artemis_rs::transforms::nom::col_nom_with_polars_expr;
use artemis_rs::transforms::prenom::col_prenom_with_polars_expr;
use artemis_rs::transforms::raison_sociale::col_raison_sociale_with_polars_expr;
use artemis_rs::transforms::siren::col_siren_with_polars_expr;
use artemis_rs::transforms::siret::col_siret_with_polars_expr;
use artemis_rs::transforms::utils::struct_to_dataframe;
use log::info;
use polars::prelude::*;
use sea_query::{ColumnRef, PostgresQueryBuilder, Query};
use sqlx::PgPool;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();

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
        col_nom_with_polars_expr(SchemasEnum::Jdd),
        col_prenom_with_polars_expr(SchemasEnum::Jdd),
        col_with_udf_expr(Jdd::Civilite, Transform::Civilite),
        col_email_with_polars_expr(SchemasEnum::Jdd),
        col_with_udf_expr(Jdd::Telephone, Transform::Telephone),
        col_raison_sociale_with_polars_expr(SchemasEnum::Jdd),
        col_code_naf_with_polars_expr(),
        col_ape_with_polars_expr(),
        col_siret_with_polars_expr(SchemasEnum::Jdd),
        col_siren_with_polars_expr(),
        col_libelle_naf_with_polars_expr(),
    ]);

    let mut df = lf.collect()?;

    let mut csv_file =
        std::fs::File::create(String::from(FILES_PATH) + "JDD_normalisation_transformed.csv")?;
    CsvWriter::new(&mut csv_file).finish(&mut df)?;

    Ok(())
}
