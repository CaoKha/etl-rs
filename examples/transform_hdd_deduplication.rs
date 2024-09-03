use artemis_rs::config::{Transform, FILES_PATH};
use artemis_rs::schemas::hdd::{Hdd, HddSchema};
use artemis_rs::schemas::{AsString, SchemasEnum};
use artemis_rs::transforms::col_with_udf_expr;
use artemis_rs::transforms::email::col_email_with_polars_expr;
use artemis_rs::transforms::nom::col_nom_with_polars_expr;
use artemis_rs::transforms::pce::col_pce_with_polars_expr;
use artemis_rs::transforms::prenom::col_prenom_with_polars_expr;
use artemis_rs::transforms::raison_sociale::col_raison_sociale_with_polars_expr;
use artemis_rs::transforms::siret::col_siret_with_polars_expr;
use artemis_rs::transforms::siret_successeur::col_siret_ss_with_polars_expr;
use artemis_rs::transforms::utils::struct_to_dataframe;
use log::info;
use polars::lazy::dsl::{col, concat_list, lit};
use polars::prelude::*;
use sea_query::{ColumnRef, PostgresQueryBuilder, Query};
use sqlx::PgPool;
use std::collections::HashSet;
use std::env;

fn transform_deduplication(lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let mut original_lf = lf.clone();
    // Self join dataframe to find duplications
    let mut lf = lf
        .clone()
        .cross_join(lf, Some(String::from("_right")))
        .filter(
            col(Hdd::Id.as_str())
                .lt(col(format!("{}_{}", Hdd::Id.as_str(), "right").as_str())) // use
                // less than to remove combinations with the same Id element but with different
                // order
                .and(
                    col(Hdd::Nom.as_str())
                        .eq(col(format!("{}_{}", Hdd::Nom.as_str(), "right").as_str())),
                )
                .and(col(Hdd::Prenom.as_str()).eq(col(
                    format!("{}_{}", Hdd::Prenom.as_str(), "right").as_str(),
                )))
                .and(
                    col(Hdd::Pce.as_str())
                        .eq(col(format!("{}_{}", Hdd::Pce.as_str(), "right").as_str()))
                        .or(col(Hdd::Email.as_str()).eq(col(format!(
                            "{}_{}",
                            Hdd::Email.as_str(),
                            "right"
                        )
                        .as_str())))
                        .or(col(Hdd::Telephone.as_str()).eq(col(format!(
                            "{}_{}",
                            Hdd::Telephone.as_str(),
                            "right"
                        )
                        .as_str()))),
                ),
        )
        .select([
            col(Hdd::Nom.as_str()),
            col(Hdd::Prenom.as_str()),
            col(Hdd::Id.as_str()),
            concat_list([
                col(Hdd::Id.as_str()),
                col(format!("{}_{}", Hdd::Id.as_str(), "right").as_str()),
            ])?
            .list()
            .unique()
            .alias(Hdd::Ids.as_str()),
            concat_list([
                col(Hdd::Pce.as_str()),
                col(format!("{}_{}", Hdd::Pce.as_str(), "right").as_str()),
            ])?
            .list()
            .unique()
            .alias(Hdd::Pce.as_str()),
            concat_list([
                col(Hdd::IdSource.as_str()),
                col(format!("{}_{}", Hdd::IdSource.as_str(), "right").as_str()),
            ])?
            .list()
            .unique()
            .alias(Hdd::IdSource.as_str()),
            concat_list([
                col(Hdd::Telephone.as_str()),
                col(format!("{}_{}", Hdd::Telephone.as_str(), "right").as_str()),
            ])?
            .list()
            .unique()
            .alias(Hdd::Telephone.as_str()),
            concat_list([
                col(Hdd::Email.as_str()),
                col(format!("{}_{}", Hdd::Email.as_str(), "right").as_str()),
            ])?
            .list()
            .unique()
            .alias(Hdd::Email.as_str()),
            concat_list([
                col(Hdd::Siret.as_str()),
                col(format!("{}_{}", Hdd::Siret.as_str(), "right").as_str()),
            ])?
            .list()
            .unique()
            .alias(Hdd::Siret.as_str()),
            concat_list([
                col(Hdd::SiretSuccesseur.as_str()),
                col(format!("{}_{}", Hdd::SiretSuccesseur.as_str(), "right").as_str()),
            ])?
            .list()
            .unique()
            .alias(Hdd::SiretSuccesseur.as_str()),
            concat_list([
                col(Hdd::RaisonSociale.as_str()),
                col(format!("{}_{}", Hdd::RaisonSociale.as_str(), "right").as_str()),
            ])?
            .list()
            .unique()
            .alias(Hdd::RaisonSociale.as_str()),
        ]);

    // Grouping by Hdd::Id to find deduplicates
    lf = lf
        .group_by([Hdd::Id.as_str(), Hdd::Nom.as_str(), Hdd::Prenom.as_str()])
        .agg([
            col(Hdd::Pce.as_str()).flatten().alias(Hdd::Pce.as_str()),
            col(Hdd::Email.as_str())
                .flatten()
                .alias(Hdd::Email.as_str()),
            col(Hdd::Telephone.as_str())
                .flatten()
                .alias(Hdd::Telephone.as_str()),
            col(Hdd::Ids.as_str()).flatten().alias(Hdd::Ids.as_str()),
            col(Hdd::Siret.as_str())
                .flatten()
                .alias(Hdd::Siret.as_str()),
            col(Hdd::SiretSuccesseur.as_str())
                .flatten()
                .alias(Hdd::SiretSuccesseur.as_str()),
            col(Hdd::RaisonSociale.as_str())
                .flatten()
                .alias(Hdd::RaisonSociale.as_str()),
            col(Hdd::IdSource.as_str())
                .flatten()
                .alias(Hdd::IdSource.as_str()),
        ]);

    // Remove any rows that have the same set JDD::UIDS or is a subset of another set of JDD::UIDS
    let lf_subsets = lf
        .clone()
        .cross_join(lf.clone(), Some(String::from("_right")))
        .filter(
            col(Hdd::Ids.as_str())
                .list()
                .set_difference(col(format!("{}_{}", Hdd::Ids.as_str(), "right").as_str()))
                .len()
                .eq(0),
        )
        .select([
            col(Hdd::Id.as_str()),
            col(Hdd::Nom.as_str()),
            col(Hdd::Prenom.as_str()),
            col(Hdd::Pce.as_str()),
            col(Hdd::Email.as_str()),
            col(Hdd::Telephone.as_str()),
            col(Hdd::Ids.as_str()),
            col(Hdd::Siret.as_str()),
            col(Hdd::SiretSuccesseur.as_str()),
            col(Hdd::RaisonSociale.as_str()),
            col(Hdd::IdSource.as_str()),
        ]);

    lf = lf
        .join(
            lf_subsets,
            [col(Hdd::Id.as_str())],
            [col(Hdd::Id.as_str())],
            JoinArgs::new(JoinType::Anti),
        )
        .select([
            col(Hdd::Id.as_str()),
            col(Hdd::Nom.as_str()),
            col(Hdd::Prenom.as_str()),
            col(Hdd::Pce.as_str()),
            col(Hdd::Email.as_str()),
            col(Hdd::Telephone.as_str()),
            col(Hdd::Ids.as_str()),
            col(Hdd::Siret.as_str()),
            col(Hdd::SiretSuccesseur.as_str()),
            col(Hdd::RaisonSociale.as_str()),
            col(Hdd::IdSource.as_str()),
        ]);

    let ids_to_remove = lf
        .clone()
        .select([col(Hdd::Ids.as_str()).flatten().unique()])
        .collect()?
        .column(Hdd::Ids.as_str())?
        .str()?
        .into_iter()
        .filter_map(|opt_id| opt_id.map(|id| id.to_string()))
        .collect::<Vec<_>>();

    println!("Id to remove: {:#?}", ids_to_remove);

    let ids_to_remove_series = Series::new("ids_to_remove", ids_to_remove);

    original_lf = original_lf.filter(col(Hdd::Id.as_str()).is_in(lit(ids_to_remove_series)));

    println!("LF after removed : {:#?}", original_lf.clone().collect());

    Ok(original_lf)
}

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
        .from(Hdd::Table)
        .to_owned()
        .to_string(PostgresQueryBuilder);
    let rows: Vec<HddSchema> = sqlx::query_as(&sql).fetch_all(&pool).await?;
    let mut df = struct_to_dataframe(&rows);

    let lf = df.lazy().with_columns(vec![
        col_pce_with_polars_expr(SchemasEnum::Hdd),
        col_nom_with_polars_expr(SchemasEnum::Hdd),
        col_prenom_with_polars_expr(SchemasEnum::Hdd),
        col_email_with_polars_expr(SchemasEnum::Hdd),
        col_with_udf_expr(Hdd::Telephone, Transform::Telephone),
        col_raison_sociale_with_polars_expr(SchemasEnum::Hdd),
        col_siret_with_polars_expr(SchemasEnum::Hdd),
        col_siret_ss_with_polars_expr(SchemasEnum::Hdd),
        col(Hdd::IdSource.as_str()),
        col(Hdd::Id.as_str()),
    ]);

    df = transform_deduplication(lf)?
        .select(&[
            col(Hdd::Nom.as_str()),
            col(Hdd::Prenom.as_str()),
            col(Hdd::Id.as_str()),
            col(Hdd::Ids.as_str()).list().join(lit("/"), true),
        ])
        .collect()?;
    println!("Deduplication: {:#?}", df);

    let mut csv_file =
        std::fs::File::create(String::from(FILES_PATH) + "HDD_deduplication_transformed.csv")?;
    CsvWriter::new(&mut csv_file).finish(&mut df)?;

    Ok(())
}
