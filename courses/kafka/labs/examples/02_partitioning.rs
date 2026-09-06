use anyhow::Context;
use anyhow::Result;
use rdkafka::Message;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<()> {
    let topic = "keyed-order-events";
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()?;

    let events = [
        ("customer-1001", "OrderCreated:2001"),
        ("customer-2002", "OrderCreated:2002"),
        ("customer-1001", "PaymentAuthorized:2001"),
        ("customer-3003", "OrderCreated:2003"),
    ];

    for event in events {
        let record = FutureRecord::to(topic).key(event.0).payload(event.1);

        let sent = producer.send(record, TIMEOUT).await;

        match sent {
            Ok(delivery) => {
                println!(
                    "Sent! Key: {}, Partition: {}, Offset: {}",
                    event.0, delivery.partition, delivery.offset
                );
            }
            Err((kafka_err, message)) => {
                let key = message.key();
                match key {
                    Some(key) => {
                        return Err(kafka_err).context(format!(
                            "Delivery of message to the topic: {} and key: {} failed",
                            message.topic(),
                            String::from_utf8_lossy(key)
                        ));
                    }
                    None => {
                        return Err(kafka_err).context(format!(
                            "Delivery of message to the topic: {} failed",
                            message.topic(),
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}
