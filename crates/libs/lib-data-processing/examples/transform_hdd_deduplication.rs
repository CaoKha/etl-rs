use lib_data_processing::config::{Transform, FILES_PATH};
use lib_data_processing::schemas::hdd::{Hdd, HddSchema};
use lib_data_processing::schemas::{AsString, SchemasEnum};
use lib_data_processing::transforms::col_with_udf_expr;
use lib_data_processing::transforms::email::col_email_with_polars_expr;
use lib_data_processing::transforms::nom::col_nom_with_polars_expr;
use lib_data_processing::transforms::pce::col_pce_with_polars_expr;
use lib_data_processing::transforms::prenom::col_prenom_with_polars_expr;
use lib_data_processing::transforms::raison_sociale::col_raison_sociale_with_polars_expr;
use lib_data_processing::transforms::siret::col_siret_with_polars_expr;
use lib_data_processing::transforms::siret_successeur::col_siret_ss_with_polars_expr;
use lib_data_processing::transforms::utils::struct_to_dataframe;
use log::info;
use polars::lazy::dsl::{col, concat_list, lit, GetOutput};
use polars::prelude::*;
use polars_ops::series::RankOptions;
use sea_query::{ColumnRef, PostgresQueryBuilder, Query};
use sqlx::PgPool;
use std::collections::HashMap;
use std::env;

fn transform_deduplication(lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let lf_original = lf.clone();
    // Self join dataframe to find duplications
    let mut lf_deduplicating =
        lf.clone()
            .cross_join(lf, Some(PlSmallStr::from("_right")))
            .filter(
                col(Hdd::Siret.as_str())
                    .is_null()
                    .and(
                        col(Hdd::Id.as_str()).lt(col(format!(
                            "{}_{}",
                            Hdd::Id.as_str(),
                            "right"
                        )
                        .as_str())), // use
                                     // less than to remove combinations with the same Id element but with different
                                     // order
                    )
                    .and(col(Hdd::Nom.as_str()).eq(col(
                        format!("{}_{}", Hdd::Nom.as_str(), "right").as_str(),
                    )))
                    .and(
                        (col(Hdd::Prenom.as_str()).eq(col(format!(
                            "{}_{}",
                            Hdd::Prenom.as_str(),
                            "right"
                        )
                        .as_str())))
                        .or(col(Hdd::Prenom.as_str()).is_null())
                        .or(col(
                            format!("{}_{}", Hdd::Prenom.as_str(), "right").as_str()
                        )
                        .is_null()),
                    )
                    .and(
                        col(Hdd::Pce.as_str())
                            .eq(col(format!("{}_{}", Hdd::Pce.as_str(), "right")
                                .as_str()))
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
                    col(format!("{}_{}", Hdd::SiretSuccesseur.as_str(), "right")
                        .as_str()),
                ])?
                .list()
                .unique()
                .alias(Hdd::SiretSuccesseur.as_str()),
                concat_list([
                    col(Hdd::RaisonSociale.as_str()),
                    col(format!("{}_{}", Hdd::RaisonSociale.as_str(), "right")
                        .as_str()),
                ])?
                .list()
                .unique()
                .alias(Hdd::RaisonSociale.as_str()),
            ]);

    // println!(
    // 	"lf_deduplicating stage 1: {:#?}",
    // 	lf_deduplicating.clone().collect()
    // );

    // Grouping by Hdd::Id to find deduplicates
    lf_deduplicating = lf_deduplicating
        .group_by([Hdd::Id.as_str(), Hdd::Nom.as_str(), Hdd::Prenom.as_str()])
        .agg([
            col(Hdd::Pce.as_str()).flatten().alias(Hdd::Pce.as_str()),
            col(Hdd::Email.as_str())
                .flatten()
                .unique()
                .alias(Hdd::Email.as_str()),
            col(Hdd::Telephone.as_str())
                .flatten()
                .unique()
                .alias(Hdd::Telephone.as_str()),
            col(Hdd::Siret.as_str())
                .flatten()
                .unique()
                .alias(Hdd::Siret.as_str()),
            col(Hdd::SiretSuccesseur.as_str())
                .flatten()
                .unique()
                .alias(Hdd::SiretSuccesseur.as_str()),
            col(Hdd::RaisonSociale.as_str())
                .flatten()
                .unique()
                .alias(Hdd::RaisonSociale.as_str()),
            col(Hdd::Ids.as_str())
                .flatten()
                .unique()
                .alias(Hdd::Ids.as_str()),
            col(Hdd::IdSource.as_str())
                .flatten()
                .unique()
                .alias(Hdd::IdSource.as_str()),
        ]);

    // println!(
    // 	"lf_deduplicating stage 2: {:#?}",
    // 	lf_deduplicating.clone().collect()
    // );

    // Get any rows that have the same set JDD::UIDS or is a subset of another set of JDD::UIDS
    let lf_deduplicating_subsets = lf_deduplicating
        .clone()
        .cross_join(lf_deduplicating.clone(), Some(PlSmallStr::from("_right")))
        .filter(
            col(Hdd::Id.as_str())
                .neq(col(format!("{}_{}", Hdd::Id.as_str(), "right").as_str()))
                .and(
                    col(Hdd::Ids.as_str())
                        .list()
                        .set_difference(col(format!(
                            "{}_{}",
                            Hdd::Ids.as_str(),
                            "right"
                        )
                        .as_str()))
                        .list()
                        .len()
                        .eq(0),
                ),
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

    // println!(
    // 	"lf_deduplicating_subsets: {:#?}",
    // 	lf_deduplicating_subsets.clone().collect()
    // );

    // Remove those rows that have the same set JDD::UIDS or is a subset of another set of JDD::UIDS from lf_self_joined
    lf_deduplicating = lf_deduplicating.join(
        lf_deduplicating_subsets,
        [col(Hdd::Id.as_str())],
        [col(Hdd::Id.as_str())],
        JoinArgs::new(JoinType::Anti),
    );

    let lf_rows_to_add = lf_deduplicating.clone().select([
        col(Hdd::Id.as_str()),
        col(Hdd::Nom.as_str()),
        col(Hdd::Prenom.as_str()),
        col(Hdd::Pce.as_str()).list().join(lit("/"), false),
        col(Hdd::Email.as_str()).list().join(lit("/"), false),
        col(Hdd::Telephone.as_str()).list().join(lit("/"), false),
        col(Hdd::Siret.as_str()).list().join(lit("/"), false),
        col(Hdd::SiretSuccesseur.as_str())
            .list()
            .join(lit("/"), false),
        col(Hdd::RaisonSociale.as_str())
            .list()
            .join(lit("/"), false),
        col(Hdd::IdSource.as_str()).list().join(lit("/"), false),
        col(Hdd::Ids.as_str()).list().join(lit("/"), false),
    ]);

    let vec_ids_to_remove = lf_deduplicating
        .clone()
        .select([col(Hdd::Ids.as_str()).flatten().unique()])
        .collect()?
        .column(Hdd::Ids.as_str())?
        .str()?
        .into_iter()
        .filter_map(|opt_id| opt_id.map(|id| id.to_string()))
        .collect::<Vec<String>>();

    // println!("Ids to remove: {:#?}", vec_ids_to_remove);

    let series_ids_to_remove =
        Series::new("ids_to_remove".into(), vec_ids_to_remove);

    let lf_row_to_remove = lf_original
        .clone()
        .filter(col(Hdd::Id.as_str()).is_in(lit(series_ids_to_remove)));

    let lf_original_with_removed_rows = lf_original
        .with_column(lit(NULL).cast(DataType::String).alias(Hdd::Ids.as_str()))
        .join(
            lf_row_to_remove,
            [col(Hdd::Id.as_str())],
            [col(Hdd::Id.as_str())],
            JoinArgs::new(JoinType::Anti),
        );

    // println!(
    // 	"lf_original_with_removed_rows: {:#?}",
    // 	lf_original_with_removed_rows.clone().collect()
    // );
    //
    // println!("lf_rows_to_add: {:#?}", lf_rows_to_add.clone().collect());

    let exprs_columns_to_select = [
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
    ];
    let lf_final = concat(
        [
            lf_original_with_removed_rows.select(exprs_columns_to_select.clone()),
            lf_rows_to_add.select(exprs_columns_to_select),
        ],
        UnionArgs::default(),
    )?;

    Ok(lf_final)
}

fn transform_deduplication_optimize(lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let lf_deduplicating = lf
        .sort(
            [Hdd::Nom.as_str()],
            SortMultipleOptions::default().with_nulls_last(true),
        )
        .with_columns(vec![
            col(Hdd::Nom.as_str())
                .count()
                .over([Hdd::Nom.as_str()])
                .alias("partition_size"),
            col(Hdd::Id.as_str())
                .rank(RankOptions::default(), None)
                .over([Hdd::Nom.as_str()])
                .alias("row_index"),
        ])
        .filter(col("partition_size").gt(lit(1)));

    Ok(lf_deduplicating)
}

// WIP
fn partition_by_name(lf: LazyFrame) -> PolarsResult<HashMap<String, LazyFrame>> {
    let unique_names = lf
        .clone()
        .select([col(Hdd::Nom.as_str())])
        .unique(None, UniqueKeepStrategy::First)
        .collect()?
        .column(Hdd::Nom.as_str())?
        .str()?
        .into_iter()
        .filter_map(|opt_name| opt_name.map(|name| name.to_string()))
        .collect::<Vec<_>>();
    let mut frames_map = HashMap::new();
    let mut frames_map_df = HashMap::new();
    for name in unique_names {
        let filtered_lf = lf
            .clone()
            .filter(col(Hdd::Nom.as_str()).eq(lit(name.clone())));
        frames_map.insert(name.clone(), filtered_lf.clone());
        frames_map_df.insert(name, filtered_lf.collect()?);
    }
    println!("frames_map: {:#?}", frames_map_df);
    Ok(frames_map)
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

    let mut lf_test = transform_deduplication_optimize(lf)?;
    let hash_map = partition_by_name(lf_test.clone())?;
    println!("Deduplication optimize test: {:#?}", lf_test.clone().collect()?);

    // df = transform_deduplication(lf)?.collect()?;
    // println!("Deduplication: {:#?}", df);

    let mut csv_file = std::fs::File::create(
        String::from(FILES_PATH) + "HDD_deduplication_transformed_test.csv",
    )?;
    CsvWriter::new(&mut csv_file).finish(&mut lf_test.collect()?)?;

    Ok(())
}
