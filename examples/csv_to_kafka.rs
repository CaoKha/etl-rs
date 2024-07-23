use artemis_rs::config::{Config, IO_CONFIG_PATH};
use artemis_rs::csv_processor::csv_to_json;
use artemis_rs::kafka::push_json_to_kafka;
use log::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let config = match Config::load(IO_CONFIG_PATH) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            return Err(e);
        }
    };

    info!("Configuration loaded successfully");

    let json_objects = match csv_to_json(&config.csv.file_path) {
        Ok(objects) => objects,
        Err(e) => {
            error!("Failed to convert CSV to JSON: {}", e);
            return Err(e);
        }
    };

    info!("CSV converted to JSON successfully");

    if let Err(e) = push_json_to_kafka(&json_objects, &config.kafka, &config.csv).await {
        error!("Failed to push JSON to Kafka: {}", e);
        return Err(e);
    }

    info!("JSON objects successfully pushed to Kafka");
    Ok(())
}
