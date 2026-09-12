use std::fs::create_dir_all;

use anyhow::Result;
use rusqlite::Connection;
use rusqlite::params;

fn main() -> Result<()> {
    create_dir_all(".kafka-data/")?;

    let mut db = Connection::open(".kafka-data/projection.sqlite")?;

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

    for attempt in 1..=3 {
        let tx = db.transaction()?;

        let inserted_rows_number = tx.execute(
            "INSERT INTO processed_records (topic, partition_id, record_offset) VALUES (?1, ?2, ?3) ON CONFLICT (topic, partition_id, record_offset) DO NOTHING;",
            params!["order-events", 0, 0],
        )?;

        println!("Inserted rows: {inserted_rows_number}");

        match attempt {
            1 => {
                tx.rollback()?;
                continue;
            }
            _ => {
                if inserted_rows_number == 1 {
                    tx.execute(
                        "UPDATE projection_state SET processed_count = processed_count + 1 WHERE id = ?1; ",
                        [1],
                    )?;
                }
            }
        }

        tx.commit()?;
    }

    let count: i64 = db.query_row(
        "SELECT processed_count FROM projection_state WHERE id = ?1;",
        [1],
        |row| row.get(0),
    )?;

    println!("Processed count is: {count}");
    Ok(())
}
