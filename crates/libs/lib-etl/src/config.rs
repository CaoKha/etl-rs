use core::error::Error;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

pub const IO_CONFIG_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/configs/io-config.json");
pub const FILES_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/files/");

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
    pub number_of_rows: i64,
}

#[derive(Debug, Deserialize)]
pub struct MongoConfig {
    pub database: String,
    pub collection: String,
}

#[derive(Debug, Deserialize)]
pub struct MongoListConfig {
    pub jdd: MongoConfig,
    pub hdd: MongoConfig,
}

#[derive(Debug, Deserialize)]
pub struct CsvListConfig {
    pub jdd: CsvConfig,
    pub hdd: CsvConfig,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub kafka: KafkaConfig,
    pub csv: CsvListConfig,
    pub mongo: MongoListConfig,
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

pub const SPECIAL_CIVILITIES: [&str; 9] = [
    "DOCTEUR",
    "GÉNÉRAL",
    "COMPTE",
    "INGÉNIEUR GÉNÉRAL",
    "PRÉFET",
    "PROFESSEUR",
    "MONSEIGNEUR",
    "SŒUR",
    "COMMISSAIRE",
];

lazy_static! {
    pub static ref CIVILITE_MAP: HashMap<&'static str, &'static str> = {
        let mut map = HashMap::new();
        map.insert("MONSIEUR", "MONSIEUR");
        map.insert("M", "MONSIEUR");
        map.insert("M.", "MONSIEUR");
        map.insert("MR", "MONSIEUR");
        map.insert("MM", "MONSIEUR");
        map.insert("M(ESPACE)", "MONSIEUR");
        map.insert("MADAME", "MADAME");
        map.insert("MME", "MADAME");
        map.insert("MRS", "MADAME");
        map.insert("MS", "MADAME");
        map.insert("MLLE", "MADAME");
        map.insert("MAD", "MADAME");
        map.insert("MADEMOISELLE", "MADAME");
        map
    };
}

pub enum Transform {
    Nom,
    Prenom,
    Email,
    Civilite,
    RaisonSociale,
    Telephone,
    // Add other variants as needed
}
