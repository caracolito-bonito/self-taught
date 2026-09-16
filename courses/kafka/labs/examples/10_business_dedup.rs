use anyhow::{Context, Result};
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList,
    consumer::{Consumer, StreamConsumer},
};
use rusqlite::Connection;
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

    let mut db = Connection::open_in_memory()?;

    db.execute_batch(
        "
            BEGIN;
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT NOT NULL PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS projection_state (
                id INTEGER PRIMARY KEY,
                processed_count INTEGER NOT NULL  
            );
            INSERT INTO projection_state (id, processed_count) VALUES (
                1, 0
            ) ON CONFLICT(id) DO NOTHING;
            COMMIT;
        ",
    )?;

    for i in 1..=2 {
        let message = consumer.recv().await?;

        let payload = message.payload().context("Kafka record has no payload")?;

        let order: OrderEvent = serde_json::from_slice(payload)?;

        let tx = db.transaction()?;

        let rows_inserted = tx.execute(
            "
                INSERT INTO processed_events (event_id) VALUES (?1) ON CONFLICT(event_id) DO NOTHING;
            ",
            [&order.event_id],
        )?;

        if rows_inserted == 1 {
            tx.execute(
                "UPDATE projection_state SET processed_count = processed_count + 1 WHERE id = ?1;",
                [1],
            )?;
        }

        tx.commit()?;

        println!(
            "Record {}: event_id: {}, order_id: {}. Read from P{}/O{}. Inserted row count: {}",
            i,
            order.event_id,
            order.order_id,
            message.partition(),
            message.offset(),
            rows_inserted,
        );
    }

    let count: i64 = db.query_row(
        "SELECT processed_count FROM projection_state WHERE id = ?1",
        [1],
        |row| row.get(0),
    )?;

    println!("Processed_count is: {}", count);
    Ok(())
}
