use std::env;

use artemis_rs::config::{Config, KafkaMessage};
use artemis_rs::kafka::create_kafka_base_consumer;
use log::{error, info, warn};
use mongodb::{bson, Client};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::Offset;
use rdkafka::{Message, TopicPartitionList};
use serde_json::Value;

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

    for message in consumer.iter() {
        let msg = message.expect("Failed to get message");
        let payload = match msg.payload_view::<str>() {
            Some(Ok(s)) => s.to_string(),
            _ => {
                warn!("Payload not found");
                continue;
            }
        };

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
    db_name: &str,
    collection_name: &str,
    messages: &[KafkaMessage],
) -> mongodb::error::Result<()> {
    let db = client.database(db_name);
    let coll = db.collection(collection_name);
    let mut docs: Vec<bson::Document> = Vec::new();

    for message in messages {
        let value_json = serde_json::from_str::<Value>(&message.value)
            .expect("Cannot serialize from str to json");
        let doc = bson::to_document(&value_json)?;
        docs.push(doc);
    }

    coll.insert_many(docs).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    env_logger::init();

    let config = match Config::load("config.json") {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            return Err(e);
        }
    };

    info!("Configuration loaded successfully");

    let consumer = create_kafka_base_consumer(&config.kafka.bootstrap_servers, "test-group");
    let messages =
        read_messages_from_offset_range(&consumer, &config.kafka.topic, 0, 296, 341).await;
    info!("Messages: {:#?}", messages);
    let mongo_uri = format!(
        "mongodb://{}:{}@{}:{}/",
        env::var("MONGO_ROOT_USR")?,
        env::var("MONGO_ROOT_PWD")?,
        env::var("MONGO_HOST")?,
        env::var("MONGO_PORT")?
    );
    let mongo_client = Client::with_uri_str(mongo_uri).await?;
    save_kafka_messages_to_mongo(&mongo_client, "client_raw", "recette_brut", &messages).await?;

    Ok(())
}
