use std::{io::stdin, time::Duration};

use anyhow::{Context, Result};
use rdkafka::{
    ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer},
};

#[tokio::main]
async fn main() -> Result<()> {
    let topic = "lso-events";
    let ordinary_producer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .create::<FutureProducer>()?;

    let transactional_producer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("transactional.id", "lso-lab-producer-1")
        .set("transaction.timeout.ms", "300000")
        .create::<FutureProducer>()?;

    transactional_producer.init_transactions(Duration::from_secs(30))?;
    let before_transaction = FutureRecord::to(topic)
        .key("transaction-demo")
        .payload("before-transaction");

    let inside_transaction = FutureRecord::to(topic)
        .key("transaction-demo")
        .payload("inside-transaction");

    let after_open_transaction = FutureRecord::to(topic)
        .key("transaction-demo")
        .payload("after-open-transaction");

    let before_transaction_sent =
        ordinary_producer.send(before_transaction, Duration::from_secs(5));

    transactional_producer.begin_transaction()?;

    let inside_transaction_sent =
        transactional_producer.send(inside_transaction, Duration::from_secs(5));

    let after_transaction_sent =
        ordinary_producer.send(after_open_transaction, Duration::from_secs(5));

    let futures = vec![
        before_transaction_sent,
        inside_transaction_sent,
        after_transaction_sent,
    ];

    for future in futures {
        match future.await {
            Ok(delivery) => {
                println!(
                    "Sucessfully delivered to topic {}: P{}/O{}",
                    topic, delivery.partition, delivery.offset
                );
            }
            Err((kafka_err, _)) => {
                return Err(kafka_err).context(format!("Delivery to the topic: {} FAILED", topic));
            }
        }
    }

    println!("Transaction open. Press Enter to commit");

    let mut input = String::new();
    stdin().read_line(&mut input)?;

    transactional_producer.commit_transaction(Duration::from_secs(30))?;

    println!("Transaction commited succesfully!");
    Ok(())
}
