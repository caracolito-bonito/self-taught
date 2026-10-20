use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use anyhow::Result;
use rdkafka::{
    ClientConfig, Message, TopicPartitionList,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::OwnedMessage,
};
use tokio::{task::JoinSet, time::sleep};

#[tokio::main]
async fn main() -> Result<()> {
    let topic = "backpressure-input";
    let delays: [u64; 6] = [100, 1000, 200, 100, 100, 100];

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "backpressure-lab-v1")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .create()?;

    consumer.subscribe(&[topic])?;

    let mut recieved_offsets: VecDeque<i64> = VecDeque::new();
    let mut done_offsets: HashSet<i64> = HashSet::new();
    let mut checkpoint = 0;
    let window_len: usize = 3;
    let mut received_count = 0;

    let mut task_set = JoinSet::<OwnedMessage>::new();

    loop {
        if received_count >= 6 && task_set.is_empty() {
            break;
        }

        tokio::select! {
            received_message = consumer.recv(), if received_count < 6 && recieved_offsets.len() < window_len  => {
                let message = received_message?;
                if received_count == 0 {
                    checkpoint = message.offset();
                }
                recieved_offsets.push_back(message.offset());
                println!("Recieved P{}/O{}. Current queue len is: {}", message.partition(), message.offset(), recieved_offsets.len());
                let message = message.detach();
                let delay = *delays.get(received_count).unwrap();
                task_set.spawn(async move {
                    sleep(Duration::from_millis(delay)).await;
                    message
                });
                received_count += 1;

            }
            Some(task) = task_set.join_next(), if !task_set.is_empty() => {
                todo!()
            }
        }
    }

    Ok(())
}
