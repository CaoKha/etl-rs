use artemis_rs::config::{Config, JddSchema};
use log::{error, info};
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

    while let Some(result) = csv_reader.deserialize::<JddSchema>().next() {
        let record = result?;

        // Insert each record into the database
        sqlx::query!(
            r#"
            INSERT INTO jdd_raw (
                raison_sociale, siret, siren, ape, code_naf,
                libele_naf, civilite, nom, prenom, telephone,
                email, address, code_postale, region, pays
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            "#,
            record.raison_sociale,
            record.siret,
            record.siren,
            record.ape,
            record.code_naf,
            record.libele_naf,
            record.civilite,
            record.nom,
            record.prenom,
            record.telephone,
            record.email,
            record.address,
            record.code_postale,
            record.region,
            record.pays,
        )
        .execute(&pool)
        .await?;
    }

    info!("CSV data imported successfully");
    Ok(())
}

// async fn create_table(pool: &PgPool) -> Result<(), sqlx::Error> {
//     // SQL statement to create the table
//     let create_table_query = r#"
//         CREATE TABLE IF NOT EXISTS jdd_raw (
//             raison_sociale TEXT,
//             siret TEXT,
//             siren TEXT,
//             ape TEXT,
//             code_naf TEXT,
//             libele_naf TEXT,
//             civilite TEXT,
//             nom TEXT,
//             prenom TEXT,
//             telephone TEXT,
//             email TEXT,
//             address TEXT,
//             code_postale TEXT,
//             region TEXT,
//             pays TEXT
//         )
//     "#;
//
//     // Execute the create table query
//     sqlx::query(create_table_query).execute(pool).await?;
//
//     println!("Table created successfully");
//     Ok(())
// }
