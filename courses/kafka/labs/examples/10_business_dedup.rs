use anyhow::{Context, Result};
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    consumer::{Consumer, StreamConsumer},
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct OrderEvent {
    event_id: String,
    order_id: String,
    event_type: EventType,
}

#[derive(Serialize, Deserialize)]
enum EventType {
    OrderCreated,
    PaymentAuthorized,
    OrderShipped,
}

#[tokio::main]
async fn main() -> Result<()> {
    let topic = "order-events";

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "business-dedup-v1")
        .set("enable.auto.commit", "false")
        .create()?;

    let mut partition_list = TopicPartitionList::new();
    partition_list.add_partition_offset(topic, 0, Offset::Offset(7))?;
    consumer.assign(&partition_list)?;

    for i in 1..=2 {
        let message = consumer.recv().await?;

        let payload = message.payload().context("Kafka record has no payload")?;

        let order: OrderEvent = serde_json::from_slice(payload)?;

        println!(
            "Record {}: event_id: {}, order_id: {}. Read from P{}/O{}",
            i,
            order.event_id,
            order.order_id,
            message.partition(),
            message.offset()
        );
    }

    Ok(())
}
