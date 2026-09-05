use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;

#[tokio::main]
async fn main() -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create()?;
    Ok(())
}
