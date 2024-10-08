use lib_etl::kafka::create_kafka_stream_consumer;
use clap::{Arg, Command};
use log::{info, warn};

use rdkafka::{
    consumer::{CommitMode, Consumer},
    message::Headers,
    util::get_rdkafka_version,
    Message,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let matches = Command::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::new("brokers")
                .short('b')
                .long("brokers")
                .value_name("BROKERS")
                .help("Broker list in kafka format")
                .required(true),
        )
        .arg(
            Arg::new("group-id")
                .short('g')
                .long("group-id")
                .value_name("GROUP-ID")
                .help("Consumer group id")
                .required(true),
        )
        .arg(
            Arg::new("topics")
                .short('t')
                .long("topics")
                .value_name("TOPICS")
                .help("Topic list")
                .value_delimiter(',')
                .required(true),
        )
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches
        .get_many::<String>("topics")
        .expect("required arg 'topics'")
        .map(|s| s.as_str())
        .collect::<Vec<&str>>();
    let brokers = matches
        .get_one::<String>("brokers")
        .expect("required arg 'brokers'");
    let group_id = matches
        .get_one::<String>("group-id")
        .expect("required arg 'group-id'");
    consume_and_print(brokers, group_id, topics).await;
    Ok(())
}

async fn consume_and_print(brokers: &str, group_id: &str, topics: Vec<&str>) {
    let consumer = create_kafka_stream_consumer(brokers, group_id);

    consumer
        .subscribe(&topics)
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                info!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        info!("  Header {:#?}: {:?}", header.key, header.value);
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}
