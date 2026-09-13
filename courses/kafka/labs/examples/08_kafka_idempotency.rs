use std::fs::create_dir_all;
use std::io::stdin;

use anyhow::Result;
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rusqlite::Connection;
use rusqlite::params;

#[tokio::main]
async fn main() -> Result<()> {
    create_dir_all(".kafka-data/")?;

    let mut db = Connection::open(".kafka-data/kafka-projection.sqlite")?;

    db.execute_batch(
        "
        BEGIN;
        CREATE TABLE IF NOT EXISTS processed_records (
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            record_offset INTEGER NOT NULL,
            PRIMARY KEY (topic, partition_id, record_offset)
        );
        CREATE TABLE IF NOT EXISTS projection_state (
            id INTEGER PRIMARY KEY,
            processed_count INTEGER NOT NULL
        );
        COMMIT;
        ",
    )?;

    db.execute(
            "INSERT INTO projection_state (id, processed_count) VALUES (?1, ?2) ON CONFLICT(id) DO NOTHING;",
            [1, 0],
        )?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "idempotent-projection-v1")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()?;

    consumer.subscribe(&["order-events"])?;

    let message = consumer.recv().await?;
    let tx = db.transaction()?;

    let inserted_message = tx.execute(
        "INSERT INTO processed_records (topic, partition_id, record_offset) VALUES (?1, ?2, ?3) ON CONFLICT(topic, partition_id, record_offset) DO NOTHING;",
        params![message.topic(), message.partition(), message.offset()],
    )?;

    if inserted_message == 1 {
        tx.execute(
            "UPDATE projection_state SET processed_count = processed_count + 1 WHERE id = ?1;",
            [1],
        )?;
    }

    tx.commit()?;

    let count: i64 = db.query_row(
        "SELECT processed_count FROM projection_state WHERE id = ?1;",
        [1],
        |row| row.get(0),
    )?;

    println!(
        "Message {}:{} has offset {}, inserted count: {}, commited counter: {}",
        message.topic(),
        message.partition(),
        message.offset(),
        inserted_message,
        count
    );

    println!("Side effect saved. Press Enter to commit.");

    let mut input = String::new();

    stdin().read_line(&mut input)?;
    consumer.commit_message(&message, CommitMode::Sync)?;
    Ok(())
}
