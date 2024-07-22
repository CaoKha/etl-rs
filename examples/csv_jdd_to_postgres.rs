use artemis_rs::{
    config::Config,
    jdd::schema::{Jdd, JddSchema},
};
use log::{debug, error, info};
use sea_query::{ColumnDef, PostgresQueryBuilder, Table};
use sqlx::PgPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let config = match Config::load("config.json") {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            return Err(e);
        }
    };

    info!("Configuration loaded successfully");

    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .comment(Some(b'#'))
        .from_path(&config.csv.file_path)
        .expect("Failed to read CSV file");

    info!("CSV file loaded successfully");

    let postgres_url = std::env::var("DATABASE_URL")?;

    info!("Database URL: {}", postgres_url);

    let pool = PgPool::connect(&postgres_url).await?;

    create_table(&pool).await?;

    while let Some(result) = csv_reader.deserialize::<JddSchema>().next() {
        let record = result?;

        let insert_query = sea_query::Query::insert()
            .into_table(Jdd::Table)
            .columns(vec![
                Jdd::RaisonSociale,
                Jdd::Siret,
                Jdd::Siren,
                Jdd::Ape,
                Jdd::CodeNaf,
                Jdd::LibeleNaf,
                Jdd::Civilite,
                Jdd::Nom,
                Jdd::Prenom,
                Jdd::Telephone,
                Jdd::Email,
                Jdd::Address,
                Jdd::CodePostale,
                Jdd::Region,
                Jdd::Pays,
            ])
            .values_panic([
                record.raison_sociale.into(),
                record.siret.into(),
                record.siren.into(),
                record.ape.into(),
                record.code_naf.into(),
                record.libele_naf.into(),
                record.civilite.into(),
                record.nom.into(),
                record.prenom.into(),
                record.telephone.into(),
                record.email.into(),
                record.address.into(),
                record.code_postale.into(),
                record.region.into(),
                record.pays.into(),
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
        .table(Jdd::Table)
        .if_not_exists()
        .col(
            ColumnDef::new(Jdd::Id)
                .integer()
                .not_null()
                .auto_increment()
                .primary_key(),
        )
        .col(ColumnDef::new(Jdd::RaisonSociale).text())
        .col(ColumnDef::new(Jdd::Siret).text())
        .col(ColumnDef::new(Jdd::Siren).text())
        .col(ColumnDef::new(Jdd::Ape).text())
        .col(ColumnDef::new(Jdd::CodeNaf).text())
        .col(ColumnDef::new(Jdd::LibeleNaf).text())
        .col(ColumnDef::new(Jdd::Civilite).text())
        .col(ColumnDef::new(Jdd::Nom).text())
        .col(ColumnDef::new(Jdd::Prenom).text())
        .col(ColumnDef::new(Jdd::Telephone).text())
        .col(ColumnDef::new(Jdd::Email).text())
        .col(ColumnDef::new(Jdd::Address).text())
        .col(ColumnDef::new(Jdd::CodePostale).text())
        .col(ColumnDef::new(Jdd::Region).text())
        .col(ColumnDef::new(Jdd::Pays).text())
        .to_owned()
        .to_string(PostgresQueryBuilder);

    // Execute the create table query
    let query_result = sqlx::query(&create_table_query).execute(pool).await?;

    info!("Query create_table executed successfully");
    debug!("Query created_table returns: {:?}", query_result);
    Ok(())
}
