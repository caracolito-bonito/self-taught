use anyhow::{Context, Result};
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use std::borrow::Cow;

struct DisplayKafkaMessage<'a> {
    key: Cow<'a, str>,
    payload: Cow<'a, str>,
    partition: i32,
    offset: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let topics = ["keyed-order-events"];
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "group-scaling-v1")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .create()?;
    consumer.subscribe(&topics)?;

    loop {
        match consumer.recv().await {
            Ok(message) => {
                let display = DisplayKafkaMessage {
                    key: message
                        .key()
                        .map(|k| String::from_utf8_lossy(k))
                        .unwrap_or(Cow::Borrowed("No Key")),
                    payload: message
                        .payload()
                        .map(|p| String::from_utf8_lossy(p))
                        .unwrap_or(Cow::Borrowed("No payload")),
                    partition: message.partition(),
                    offset: message.offset(),
                };

                println!(
                    "Message! key: {}\n payload: {}\n partition: {}\n offset: {}",
                    display.key, display.payload, display.partition, display.offset
                );
            }
            Err(e) => return Err(e).context("Error occured reading messages"),
        }
    }
}
