use anyhow::Result;
use rusqlite::Connection;

fn main() -> Result<()> {
    let db = Connection::open_in_memory()?;
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
        "INSERT INTO projection_state (id, processed_count) VALUES (?1, ?2)",
        [1, 0],
    )?;

    let count: i64 = db.query_row(
        "SELECT processed_count FROM projection_state WHERE id = ?1",
        [1],
        |row| row.get(0),
    )?;

    println!("Processed count is: {count}");
    Ok(())
}
