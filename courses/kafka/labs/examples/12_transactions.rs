use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::producer::{FutureRecord, Producer};
use rdkafka::{ClientConfig, producer::FutureProducer};

#[tokio::main]
async fn main() -> Result<()> {
    let topic = "transaction-events";
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("transactional.id", "transaction-lab-producer-1")
        .create()?;

    producer.init_transactions(Duration::from_secs(30))?;

    println!("Transactional sending for producer enabled");

    producer.begin_transaction()?;

    let first = FutureRecord::to(topic)
        .key("transaction-demo")
        .payload("first-aborted-record");
    let mut deliveries = Vec::new();

    let second = FutureRecord::to(topic)
        .key("transaction-demo")
        .payload("second-aborted-record");

    let first_delivery = producer.send(first, Duration::from_secs(5)).await;

    let second_delivery = producer.send(second, Duration::from_secs(5)).await;

    deliveries.push(first_delivery);
    deliveries.push(second_delivery);

    for delivery in deliveries {
        match delivery {
            Ok(delivered) => {
                println!(
                    "Sucessfully delivered to topic {}: P{}/O{}",
                    topic, delivered.partition, delivered.offset
                )
            }
            Err((err, _)) => {
                return Err(err).context(format!("Delivery to the topic: {} FAILED", topic));
            }
        }
    }

    producer.abort_transaction(Duration::from_secs(30))?;
    println!("Transaction ABORTED");
    Ok(())
}
