use anyhow::Context;
use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()?;

    let record = FutureRecord::to("order-events")
        .key("customer-1001")
        .payload("OrderCreated:1003");
    let sent = producer.send(record, Duration::from_secs(5)).await;

    match sent {
        Ok(delivery) => {
            println!(
                "Sent! Partition: {}, offset: {}",
                delivery.partition, delivery.offset
            );
            Ok(())
        }
        Err((kafka_err, _)) => Err(kafka_err).context("Error with sending to kafka"),
    }
}
