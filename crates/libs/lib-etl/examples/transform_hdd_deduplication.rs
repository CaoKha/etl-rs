use lib_etl::config::{Transform, FILES_PATH};
use lib_etl::schemas::hdd::{Hdd, HddSchema};
use lib_etl::schemas::{AsString, SchemasEnum};
use lib_etl::transforms::col_with_udf_expr;
use lib_etl::transforms::email::col_email_with_polars_expr;
use lib_etl::transforms::nom::col_nom_with_polars_expr;
use lib_etl::transforms::pce::col_pce_with_polars_expr;
use lib_etl::transforms::prenom::col_prenom_with_polars_expr;
use lib_etl::transforms::raison_sociale::col_raison_sociale_with_polars_expr;
use lib_etl::transforms::siret::col_siret_with_polars_expr;
use lib_etl::transforms::siret_successeur::col_siret_ss_with_polars_expr;
use lib_etl::transforms::utils::struct_to_dataframe;
use log::{debug, info};
use polars::lazy::dsl::{col, concat_list, lit, Expr};
use polars::prelude::*;
use rayon::prelude::*;
use sea_query::{ColumnRef, PostgresQueryBuilder, Query};
use sqlx::PgPool;
use std::collections::HashMap;
use std::env;

fn detect_duplicates(lf: LazyFrame) -> PolarsResult<(LazyFrame, Vec<String>)> {
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

    Ok((lf_rows_to_add, vec_ids_to_remove))
}

fn reconciliate_lf(
    lf_original: LazyFrame,
    lf_rows_to_add: LazyFrame,
    vec_ids_to_remove: Vec<String>,
    exprs_columns_to_select: &[Expr],
) -> PolarsResult<LazyFrame> {
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

    let lf_final = concat(
        [
            lf_original_with_removed_rows.select(exprs_columns_to_select),
            lf_rows_to_add.select(exprs_columns_to_select),
        ],
        UnionArgs::default(),
    )?;

    Ok(lf_final)
}

fn filter_duplicates(lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let lf_deduplicating = lf
        .sort(
            [Hdd::Nom.as_str()],
            SortMultipleOptions::default().with_nulls_last(true),
        )
        .with_column(
            col(Hdd::Nom.as_str())
                .count()
                .over([Hdd::Nom.as_str()])
                .alias("partition_size"),
        )
        .filter(col("partition_size").gt(lit(1)));

    Ok(lf_deduplicating)
}

// WIP
fn hash_partition(
    lf: LazyFrame,
    based_on_col: &str,
    exprs_columns_to_select: &[Expr],
) -> PolarsResult<HashMap<String, LazyFrame>> {
    let unique_names = lf
        .clone()
        .select([col(based_on_col)])
        .unique(None, UniqueKeepStrategy::First)
        .collect()?
        .column(based_on_col)?
        .str()?
        .into_iter()
        .filter_map(|opt_text| opt_text.map(|text| text.to_string()))
        .collect::<Vec<_>>();
    let mut frames_map = HashMap::new();
    let mut frames_map_df = HashMap::new();
    for name in unique_names {
        let filtered_lf = lf
            .clone()
            .filter(col(Hdd::Nom.as_str()).eq(lit(name.clone())));
        frames_map.insert(
            name.clone(),
            filtered_lf.clone().select(exprs_columns_to_select),
        );
        frames_map_df
            .insert(name, filtered_lf.select(exprs_columns_to_select).collect()?);
    }
    info!("Dataframe partitions by name: {:#?}", frames_map_df);
    Ok(frames_map)
}

fn get_hashmap_duplicates(
    data_map: HashMap<String, LazyFrame>,
) -> PolarsResult<HashMap<String, (LazyFrame, Vec<String>)>> {
    data_map
        .into_par_iter()
        .map(|(key, lf)| {
            let (lf_rows_to_add, vec_ids_to_remove) = detect_duplicates(lf)?;
            let df_rows_to_add = lf_rows_to_add.clone().collect()?;
            debug!(
                "Debug get_hashmap_duplicates: {:#?}",
                (&df_rows_to_add, &vec_ids_to_remove)
            );
            Ok((key, (lf_rows_to_add, vec_ids_to_remove)))
        })
        .collect()
}

fn get_rows_to_add_and_remove(
    hashmap_duplicates: HashMap<String, (LazyFrame, Vec<String>)>,
) -> PolarsResult<(LazyFrame, Vec<String>)> {
    let vec_all_rows_to_add: Vec<LazyFrame> = hashmap_duplicates
        .clone()
        .into_iter()
        .map(|(_, (lf_rows_to_add, _))| lf_rows_to_add)
        .collect();
    let lf_all_rows_to_add = concat(vec_all_rows_to_add, UnionArgs::default())?;
    let df_debug = lf_all_rows_to_add.clone().collect()?;

    let vec_all_ids_to_remove: Vec<String> = hashmap_duplicates
        .into_iter()
        .flat_map(|(_, (_, vec_ids_to_remove))| vec_ids_to_remove)
        .collect();
    info!("LazyFrame_all_rows_to_add: {:#?}\nids_to_remove: {:#?}", df_debug, vec_all_ids_to_remove);
    Ok((lf_all_rows_to_add, vec_all_ids_to_remove))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    dotenv::dotenv().ok();
    // Initialize PostgreSQL connection pool
    let postgres_url = env::var("DATABASE_URL").expect("POSTGRES_URI must be set");
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
    let df_original = struct_to_dataframe(&rows);

    let lf_original = df_original.lazy().with_columns(vec![
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

    let exprs_columns_to_select = [
        col(Hdd::Id.as_str()),
        col(Hdd::Nom.as_str()),
        col(Hdd::Prenom.as_str()),
        col(Hdd::Pce.as_str()),
        col(Hdd::Email.as_str()),
        col(Hdd::Telephone.as_str()),
        col(Hdd::Siret.as_str()),
        col(Hdd::SiretSuccesseur.as_str()),
        col(Hdd::RaisonSociale.as_str()),
        col(Hdd::IdSource.as_str()),
    ];

    let lf_duplicates = filter_duplicates(lf_original.clone())?;
    let hashmap_partitions = hash_partition(
        lf_duplicates.clone(),
        Hdd::Nom.as_str(),
        &exprs_columns_to_select,
    )?;
    let hashmap_duplicates = get_hashmap_duplicates(hashmap_partitions)?;
    let (lf_rows_to_add, vec_ids_to_remove) =
        get_rows_to_add_and_remove(hashmap_duplicates)?;

    let exprs_columns_to_select_with_ids = [
        col(Hdd::Id.as_str()),
        col(Hdd::Nom.as_str()),
        col(Hdd::Prenom.as_str()),
        col(Hdd::Pce.as_str()),
        col(Hdd::Email.as_str()),
        col(Hdd::Telephone.as_str()),
        col(Hdd::Siret.as_str()),
        col(Hdd::SiretSuccesseur.as_str()),
        col(Hdd::RaisonSociale.as_str()),
        col(Hdd::IdSource.as_str()),
        col(Hdd::Ids.as_str()),
    ];
    let lf_deduplicated = reconciliate_lf(
        lf_original,
        lf_rows_to_add,
        vec_ids_to_remove,
        &exprs_columns_to_select_with_ids,
    )?;

    let mut df_final = lf_deduplicated.collect()?;
    info!("Deduplication: {:#?}", df_final);

    let mut csv_file = std::fs::File::create(
        String::from(FILES_PATH) + "HDD_deduplication_transformed_test.csv",
    )?;
    CsvWriter::new(&mut csv_file).finish(&mut df_final)?;

    Ok(())
}
