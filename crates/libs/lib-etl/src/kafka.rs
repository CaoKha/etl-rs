use crate::config::{CsvConfig, KafkaConfig};
use chrono::Utc;
use log::{error, info};
use rdkafka::{
    config::{ClientConfig, RDKafkaLogLevel},
    consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientContext, TopicPartitionList,
};
use serde_json::Value;
use core::error::Error;

// --Start-Producer--
fn create_kafka_producer(kafka_config: &KafkaConfig) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", &kafka_config.bootstrap_servers)
        .create()
        .expect("Producer creation failed")
}

pub async fn push_json_to_kafka(
    json_objects: &[Value],
    kafka_config: &KafkaConfig,
    csv_config: &CsvConfig,
) -> Result<(), Box<dyn Error>> {
    let producer = create_kafka_producer(kafka_config);

    for (index, json_obj) in json_objects.iter().enumerate() {
        let json_string = serde_json::to_string(json_obj)?;
        let key = index.to_string();

        let kafka_headers = create_kafka_headers(csv_config, &index.to_string());

        produce_to_kafka(&producer, kafka_config, &key, &json_string, kafka_headers).await?;
    }

    Ok(())
}

async fn produce_to_kafka(
    producer: &FutureProducer,
    kafka_config: &KafkaConfig,
    key: &str,
    message: &str,
    headers: OwnedHeaders,
) -> Result<(), Box<dyn Error>> {
    let record = FutureRecord::to(&kafka_config.topic)
        .payload(message)
        .key(key)
        .headers(headers);

    match producer.send(record, Timeout::Never).await {
        Ok(delivery) => {
            info!(
                "Successfully delivered message to topic {} at offset {}",
                kafka_config.topic, delivery.1
            );
        }
        Err((e, _)) => {
            error!("Failed to deliver message: {:?}", e);
            return Err(Box::new(e));
        }
    }

    Ok(())
}

fn create_kafka_headers(csv_config: &CsvConfig, row_index: &str) -> OwnedHeaders {
    OwnedHeaders::new()
        .insert(Header {
            key: "timestamp",
            value: Some(Utc::now().to_rfc3339().as_bytes()),
        })
        .insert(Header {
            key: "csv_table",
            value: Some(csv_config.table_name.as_bytes()),
        })
        .insert(Header {
            key: "csv_file_path",
            value: Some(csv_config.file_path.as_bytes()),
        })
        .insert(Header {
            key: "row_index",
            value: Some(row_index),
        })
}
// --End-Producer--

// --Start-Consumer--
pub fn create_kafka_base_consumer(brokers: &str, group_id: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation failed")
}

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
pub struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

pub fn create_kafka_stream_consumer(brokers: &str, group_id: &str) -> LoggingConsumer {
    let context = CustomContext;
    ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed")
}
// --End-Consumer--
