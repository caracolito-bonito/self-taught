use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::{ClientContext, Message};
use std::borrow::Cow;

struct DisplayKafkaMessage<'a> {
    key: Cow<'a, str>,
    payload: Cow<'a, str>,
    partition: i32,
    offset: i64,
}

struct RebalanceLogger;

impl ClientContext for RebalanceLogger {}

impl ConsumerContext for RebalanceLogger {
    fn pre_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        println!("PRE: {:?}", rebalance);
    }

    fn post_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        println!("POST: {:?}", rebalance);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let topic = "rebalance-input";
    let consumer: StreamConsumer<RebalanceLogger> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "rebalance-watch-v1")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.offset.store", "false")
        .set("enable.auto.commit", "false")
        .create_with_context(RebalanceLogger)?;
    consumer.subscribe(&[topic])?;

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
