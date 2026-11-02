use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, ConsumerContext, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::{ClientContext, Message};
use std::time::Duration;
use tokio::select;
use tokio::task::JoinSet;
use tokio::time::sleep;

struct RebalanceLogger;

impl ClientContext for RebalanceLogger {}

impl ConsumerContext for RebalanceLogger {
    fn pre_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        println!("PRE: {:?}", rebalance);
    }

    fn post_rebalance(
        &self,
        _base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        println!("POST: {:?}", rebalance);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let topic = "rebalance-input";
    let consumer: StreamConsumer<RebalanceLogger> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "rebalance-tasks-v1")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.offset.store", "false")
        .set("enable.auto.commit", "false")
        .create_with_context(RebalanceLogger)?;
    consumer.subscribe(&[topic])?;

    let mut tasks = JoinSet::<(usize, OwnedMessage)>::new();

    let mut task_number = 0;

    loop {
        select! {
             received_message = consumer.recv() => {
                    let message = received_message?;
                    let current_task_number = task_number;
                    task_number += 1;
                    println!("START! number: {current_task_number}, P{}/O{}", message.partition(), message.offset());
                    let message = message.detach();
                    tasks.spawn(async move {
                        sleep(Duration::from_secs(30)).await;
                        (current_task_number, message)
                    });
             }
            Some(task) = tasks.join_next(), if !tasks.is_empty() => {
                let result = task?;
                let (number, message) = result;
                println!("DONE! number: {number}, P{}/O{}", message.partition(), message.offset());
            }

        }
    }
}
