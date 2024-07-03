use artemis_rs::config::{Config, KafkaMessage};
use artemis_rs::kafka::create_kafka_base_consumer;
use log::{error, info};
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::Offset;
use rdkafka::{Message, TopicPartitionList};

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
            _ => continue,
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

    let consumer = create_kafka_base_consumer(&config.kafka.bootstrap_servers, "test-group");
    let messages = read_messages_from_offset_range(&consumer, &config.kafka.topic, 0, 228, 284).await;
    println!("{:?}", messages);
    Ok(())
}
