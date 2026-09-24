use std::time::Duration;

use anyhow::{Context, Result};
use tokio::{task::JoinSet, time::sleep};

#[tokio::main]
async fn main() -> Result<()> {
    let mut set: JoinSet<usize> = JoinSet::new();
    let processing_delays_ms: [u64; 3] = [100, 1000, 300];
    for (offset, delay) in processing_delays_ms.into_iter().enumerate() {
        set.spawn(async move {
            sleep(Duration::from_millis(delay)).await;
            offset
        });
    }

    while let Some(res) = set.join_next().await {
        match res {
            Ok(offset) => println!("Handled offset: {}", offset),
            Err(join_err) => return Err(join_err).context("Unable to handle a task"),
        }
    }
    Ok(())
}
