use lib_etl::{
    config::{Config, FILES_PATH, IO_CONFIG_PATH},
    schemas::hdd::{Hdd, HddSchemaCSV},
};
use log::{debug, error, info};
use sea_query::{ColumnDef, PostgresQueryBuilder, Table};
use sqlx::PgPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn core::error::Error>> {
    env_logger::init();

    let config = match Config::load(IO_CONFIG_PATH) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            return Err(e);
        }
    };

    info!("Configuration loaded successfully");

    info!("CSV path: {:?}", &config.csv.hdd.file_path);

    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .comment(Some(b'#'))
        .from_path(FILES_PATH.to_string() + &config.csv.hdd.file_path)
        .expect("Failed to read CSV file");

    info!("CSV file loaded successfully");

    let postgres_url = std::env::var("DATABASE_URL")?;

    info!("Database URL: {}", postgres_url);

    let pool = PgPool::connect(&postgres_url).await?;

    create_table(&pool).await?;

    while let Some(result) = csv_reader.deserialize::<HddSchemaCSV>().next() {
        let record = result?;

        let insert_query = sea_query::Query::insert()
            .into_table(Hdd::Table)
            .columns(vec![
                Hdd::Pce,
                Hdd::RaisonSociale,
                Hdd::Siret,
                Hdd::SiretSuccesseur,
                Hdd::Nom,
                Hdd::Prenom,
                Hdd::Telephone,
                Hdd::Email,
                Hdd::IdSource,
            ])
            .values_panic([
                record.pce.into(),
                record.raison_sociale.into(),
                record.siret.into(),
                record.siret_successeur.into(),
                record.nom.into(),
                record.prenom.into(),
                record.telephone.into(),
                record.email.into(),
                record.id_source.into(),
            ])
            .to_string(PostgresQueryBuilder);

        // Insert each record into the database
        sqlx::query(&insert_query).execute(&pool).await?;
    }

    info!("CSV file imported successfully");
    Ok(())
}

async fn create_table(pool: &PgPool) -> Result<(), sqlx::Error> {
    // SQL statement to create the table
    let create_table_query = Table::create()
        .table(Hdd::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(Hdd::Id)
                .integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        )
        .col(ColumnDef::new(Hdd::Pce).double())
        .col(ColumnDef::new(Hdd::RaisonSociale).text())
        .col(ColumnDef::new(Hdd::Siret).double())
        .col(ColumnDef::new(Hdd::SiretSuccesseur).double())
        .col(ColumnDef::new(Hdd::Nom).text())
        .col(ColumnDef::new(Hdd::Prenom).text())
        .col(ColumnDef::new(Hdd::Telephone).double())
        .col(ColumnDef::new(Hdd::Email).text())
        .col(ColumnDef::new(Hdd::IdSource).integer())
        .to_owned()
        .to_string(PostgresQueryBuilder);

    // Execute the create table query
    let query_result = sqlx::query(&create_table_query).execute(pool).await?;

    info!("Query create_table executed successfully");
    debug!("Query created_table returns: {:?}", query_result);
    Ok(())
}
