use anyhow::Result;
use rusqlite::Connection;
use rusqlite::params;

fn main() -> Result<()> {
    let mut db = Connection::open_in_memory()?;
    db.execute_batch(
        "
        BEGIN;
        CREATE TABLE processed_records (
            topic TEXT NOT NULL,
            partition_id INTEGER NOT NULL,
            record_offset INTEGER NOT NULL,
            PRIMARY KEY (topic, partition_id, record_offset)
        );
        CREATE TABLE projection_state (
            id INTEGER PRIMARY KEY,
            processed_count INTEGER NOT NULL
        );
        COMMIT;
        ",
    )?;

    db.execute(
        "INSERT INTO projection_state (id, processed_count) VALUES (?1, ?2);",
        [1, 0],
    )?;

    for _ in 0..2 {
        let tx = db.transaction()?;

        let inserted_rows_number = tx.execute(
            "INSERT INTO processed_records (topic, partition_id, record_offset) VALUES (?1, ?2, ?3) ON CONFLICT (topic, partition_id, record_offset) DO NOTHING;",
            params!["order-events", 0, 0],
        )?;

        println!("Inserted rows: {inserted_rows_number}");

        if inserted_rows_number == 1 {
            tx.execute(
                "UPDATE projection_state SET processed_count = processed_count + 1 WHERE id = ?1; ",
                [1],
            )?;
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
