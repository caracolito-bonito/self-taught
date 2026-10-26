use std::time::Duration;

use anyhow::Result;
use rdkafka::{
    ClientConfig, Message, TopicPartitionList,
    consumer::{Consumer, StreamConsumer},
};
use tokio::time;

#[tokio::main]
async fn main() -> Result<()> {
    let topic = "backpressure-input";

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "pause-lab-v1")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .create()?;

    consumer.subscribe(&[topic])?;

    let message = consumer.recv().await?;

    println!("Received: P{}/O{}", message.partition(), message.offset());

    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, message.partition());

    consumer.pause(&partitions)?;

    match time::timeout(Duration::from_millis(500), consumer.recv()).await {
        Ok(m) => {
            let message = m?;
            println!(
                "Received in pause: P{}/O{}",
                message.partition(),
                message.offset()
            )
        }
        Err(_) => {
            println!("Timeout has ended: there was not received event during pause");
        }
    }

    consumer.resume(&partitions)?;

    let message_after_resume = consumer.recv().await?;
    println!(
        "Received after resuming: P{}/O{}",
        message_after_resume.partition(),
        message_after_resume.offset()
    );

    Ok(())
}
