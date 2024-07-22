use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs;

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
pub struct MongoConfig {
    pub database: String,
    pub collection: String,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub kafka: KafkaConfig,
    pub csv: CsvConfig,
    pub mongo: MongoConfig,
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
