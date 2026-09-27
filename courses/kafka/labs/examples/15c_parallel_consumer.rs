use anyhow::Result;
use rdkafka::{
    ClientConfig, Message,
    consumer::{Consumer, StreamConsumer},
};

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

    for _ in 0..3 {
        let m = consumer.recv().await?;
        println!("Recieved event: P{}/O{}", m.partition(), m.offset());
        recieved_events.push(m.detach());
    }

    Ok(())
}
