use anyhow::Result;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use std::fs::OpenOptions;
use std::io::{Write, stdin};

#[tokio::main]
async fn main() -> Result<()> {
    let topics = ["order-events"];
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "crash-window-v1")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;
    consumer.subscribe(&topics)?;

    let message = consumer.recv().await?;

    let mut log = OpenOptions::new()
        .append(true)
        .create(true)
        .open("processed.log")?;

    writeln!(
        &mut log,
        "Topic: {}, partition: {}, offset: {}",
        message.topic(),
        message.partition(),
        message.offset()
    )?;

    log.sync_all()?;
    println!("Side effect saved. Press Enter to commit.");

    let mut input = String::new();

    stdin().read_line(&mut input)?;
    consumer.commit_message(&message, CommitMode::Sync)?;

    Ok(())
}
