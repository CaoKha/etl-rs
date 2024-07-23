use std::env;

use artemis_rs::config::{Config, KafkaMessage, MongoConfig, IO_CONFIG_PATH};
use artemis_rs::kafka::create_kafka_base_consumer;
use log::{debug, error, info, warn};
use mongodb::{bson, Client};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::Offset;
use rdkafka::{Message, TopicPartitionList};
use serde_json::Value;

enum WriteMode {
    Overwrite,
    #[allow(dead_code)]
    Append,
}

async fn read_messages_from_offset_range(
    consumer: &BaseConsumer,
    topic: &str,
    partition: i32,
    start_offset: i64,
    end_offset: i64,
) -> Vec<KafkaMessage> {
    let mut messages: Vec<KafkaMessage> = Vec::new();
    let mut topic_partition = TopicPartitionList::new();
    let _ = topic_partition.add_partition_offset(topic, partition, Offset::Offset(start_offset));

    consumer
        .assign(&topic_partition)
        .expect("Failed to get partition offset");

    info!("Reading messages from Kafka...");

    for message in consumer.iter() {
        let msg = message.expect("Failed to get message");
        let payload = match msg.payload_view::<str>() {
            Some(Ok(s)) => s.to_string(),
            _ => {
                warn!("Payload not found");
                continue;
            }
        };

        debug!("Reading message at offset {}.", msg.offset());

        let kafka_message = KafkaMessage {
            key: msg
                .key()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .unwrap_or_default(),
            value: payload,
            topic: msg.topic().to_string(),
            partition,
            offset: msg.offset(),
        };

        messages.push(kafka_message);

        if msg.offset() >= end_offset {
            break;
        }
    }

    messages
}

async fn save_kafka_messages_to_mongo(
    client: &mongodb::Client,
    mongo_cfg: &MongoConfig,
    messages: &[KafkaMessage],
    mode: WriteMode,
) -> mongodb::error::Result<()> {
    let db = client.database(&mongo_cfg.database);
    let coll = db.collection(&mongo_cfg.collection);
    let mut docs: Vec<bson::Document> = Vec::new();

    for message in messages {
        let value_json = serde_json::from_str::<Value>(&message.value)
            .expect("Cannot serialize from str to json");
        let doc = bson::to_document(&value_json)?;
        docs.push(doc);
    }

    match mode {
        WriteMode::Overwrite => {
            coll.drop().await?;
            coll.insert_many(docs).await?;
        }
        WriteMode::Append => {
            coll.insert_many(docs).await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    env_logger::init();

    let config = match Config::load(IO_CONFIG_PATH) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            return Err(e);
        }
    };

    info!("Configuration loaded successfully");

    let consumer = create_kafka_base_consumer(&config.kafka.bootstrap_servers, "group-jdd");
    let messages = read_messages_from_offset_range(&consumer, &config.kafka.topic, 0, 0, &config.csv.number_of_rows - 1).await;
    let mongo_uri = format!(
        "mongodb://{}:{}@{}:{}/",
        env::var("MONGO_ROOT_USR")?,
        env::var("MONGO_ROOT_PWD")?,
        env::var("MONGO_HOST")?,
        env::var("MONGO_PORT")?
    );

    let mongo_client = Client::with_uri_str(mongo_uri).await?;

    info!("Saving messages to MongoDB...");

    save_kafka_messages_to_mongo(
        &mongo_client,
        &config.mongo,
        &messages,
        WriteMode::Overwrite,
    )
    .await?;

    info!(
        "Messages saved successfully to MongoDB at \"{}.{}\"",
        &config.mongo.database, &config.mongo.collection
    );

    Ok(())
}
