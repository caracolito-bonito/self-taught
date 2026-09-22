use std::{io::stdin, time::Duration};

use anyhow::{Context, Result};
use rdkafka::{
    ClientConfig, Message, TopicPartitionList,
    consumer::{Consumer, StreamConsumer},
    producer::{FutureProducer, FutureRecord, Producer},
};

#[tokio::main]
async fn main() -> Result<()> {
    let input_topic = "transform-input";
    let output_topic = "transform-output";

    let group_id = String::from("transactional-transform-v1");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_committed")
        .create()?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("transactional.id", "transform-producer-1")
        .set("transaction.timeout.ms", "300000")
        .create()?;

    producer.init_transactions(Duration::from_secs(30))?;

    consumer.subscribe(&[input_topic])?;

    let received = consumer.recv().await?;

    let payload = received.payload().context("Kafka record has no payload")?;

    let order = str::from_utf8(payload)?;

    println!(
        "Message received: \n Payload: {}\n Topic: {}\n P{}O{}",
        order,
        received.topic(),
        received.partition(),
        received.offset()
    );

    producer.begin_transaction()?;

    let order_to_send = format!("processed:{}", order);
    let order_record = FutureRecord::to(output_topic)
        .key(payload)
        .payload(&order_to_send);

    producer
        .send(order_record, Duration::from_secs(5))
        .await
        .map_err(|(e, _)| e)
        .context(format!(
            "Cannot send a message to a topic: {}",
            output_topic
        ))?;

    let mut partition_list = TopicPartitionList::new();

    partition_list.add_partition_offset(
        received.topic(),
        received.partition(),
        rdkafka::Offset::Offset(received.offset() + 1),
    )?;

    let group_metadata = consumer
        .group_metadata()
        .context(format!("No metadata for group: {}", group_id))?;

    producer.send_offsets_to_transaction(
        &partition_list,
        &group_metadata,
        Duration::from_secs(30),
    )?;

    println!("Output and input checkpoint staged. Press enter to commit.");

    let mut input = String::new();

    stdin().read_line(&mut input)?;

    producer.commit_transaction(Duration::from_secs(30))?;

    println!("Commited successfully");

    Ok(())
}
