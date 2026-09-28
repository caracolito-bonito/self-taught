use std::time::Duration;

use anyhow::Result;
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
    message::OwnedMessage,
};
use tokio::{task::JoinSet, time::sleep};

#[tokio::main]
async fn main() -> Result<()> {
    let topic = "parallel-input";
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "parallel-lab-v1")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .create()?;

    consumer.subscribe(&[topic])?;

    let mut recieved_events = Vec::with_capacity(3);

    let mut task_set = JoinSet::<OwnedMessage>::new();
    let delays: [u64; 3] = [100, 1000, 300];

    for _ in 0..3 {
        let m = consumer.recv().await?;
        println!("Recieved event: P{}/O{}", m.partition(), m.offset());
        recieved_events.push(m.detach());
    }

    for (delay, message) in delays.into_iter().zip(recieved_events.into_iter()) {
        task_set.spawn(async move {
            sleep(Duration::from_millis(delay)).await;
            message
        });
    }

    while let Some(res) = task_set.join_next().await {
        let message = res?;
        println!("Handled messages:");
        println!("P{}/O{}", message.partition(), message.offset());
    }

    Ok(())
}
