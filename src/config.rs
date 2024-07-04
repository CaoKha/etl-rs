use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct JddSchema {
    #[serde(rename = "RAISON_SOCIALE")]
    pub raison_sociale: Option<String>,

    #[serde(rename = "SIRET")]
    pub siret: Option<String>,

    #[serde(rename = "SIREN")]
    pub siren: Option<String>,

    #[serde(rename = "APE")]
    pub ape: Option<String>,

    #[serde(rename = "CODE_NAF")]
    pub code_naf: Option<String>,

    #[serde(rename = "LIBELE_NAF")]
    pub libele_naf: Option<String>,

    #[serde(rename = "CIVILITE")]
    pub civilite: Option<String>,

    #[serde(rename = "NOM")]
    pub nom: Option<String>,

    #[serde(rename = "PRENOM")]
    pub prenom: Option<String>,

    #[serde(rename = "TELEPHONE")]
    pub telephone: Option<String>,

    #[serde(rename = "email")]
    pub email: Option<String>,

    #[serde(rename = "address")]
    pub address: Option<String>,

    #[serde(rename = "CODE POSTALE")]
    pub code_postale: Option<String>,

    #[serde(rename = "REGION")]
    pub region: Option<String>,

    #[serde(rename = "PAYS")]
    pub pays: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct KafkaMessage {
    pub key: String,
    pub value: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub topic: String,
}

#[derive(Debug, Deserialize)]
pub struct CsvConfig {
    pub file_path: String,
    pub table_name: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub kafka: KafkaConfig,
    pub csv: CsvConfig,
}

impl Config {
    pub fn load(file_path: &str) -> Result<Self, Box<dyn Error>> {
        let config_data = fs::read_to_string(file_path)
            .map_err(|e| format!("Failed to read config file: {}", e))?;
        let config: Config = serde_json::from_str(&config_data)
            .map_err(|e| format!("Failed to parse config JSON: {}", e))?;
        Ok(config)
    }
}
