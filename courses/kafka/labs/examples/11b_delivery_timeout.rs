use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::{
    ClientConfig, Message,
    producer::{FutureProducer, FutureRecord},
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

    let event = OrderEvent {
        event_id: String::from("evt-reliability-1"),
        order_id: String::from("order-1001"),
        event_type: EventType::OrderCreated,
    };

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "10000")
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .create()?;

    let event_serialized = serde_json::to_string(&event)?;

    let record = FutureRecord::to(topic)
        .key(&event.order_id)
        .payload(&event_serialized);

    let now = std::time::Instant::now();

    let sent = producer.send(record, Duration::from_secs(1)).await;

    println!("Passed: {:?}", now.elapsed());

    match sent {
        Ok(delivered) => println!(
            "Sent! Event_id: {} to the topic: {} P{}/O{}",
            event.event_id, topic, delivered.partition, delivered.offset
        ),
        Err((kafkar_err, message)) => {
            let key = message.key();

            match key {
                Some(key) => {
                    return Err(kafkar_err).context(format!(
                        "Delivery to the topic: {} for key: {} failed",
                        message.topic(),
                        String::from_utf8_lossy(key)
                    ));
                }
                None => {
                    return Err(kafkar_err).context(format!(
                        "Delivery to the topic: {} failed. Key is absent",
                        message.topic()
                    ));
                }
            }
        }
    }

    Ok(())
}
