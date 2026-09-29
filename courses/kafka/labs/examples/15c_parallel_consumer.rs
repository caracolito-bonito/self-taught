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
    let topic = "parallel-input";
    let delays: [u64; 3] = [100, 1000, 300];

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "parallel-lab-v1")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .create()?;

    consumer.subscribe(&[topic])?;

    let mut recieved_events = Vec::with_capacity(3);
    let mut recieved_offsets: VecDeque<i64> = VecDeque::new();
    let mut done_offsets: HashSet<i64> = HashSet::new();
    let mut checkpoint = 0;

    let mut task_set = JoinSet::<OwnedMessage>::new();

    for i in 0..3 {
        let m = consumer.recv().await?;
        println!("Recieved event: P{}/O{}", m.partition(), m.offset());
        if i == 0 {
            checkpoint = m.offset();
        }
        recieved_offsets.push_back(m.offset());
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
        done_offsets.insert(message.offset());
        let old_checkpoint = checkpoint;

        while let Some(first) = recieved_offsets.front().copied() {
            if !done_offsets.contains(&first) {
                break;
            }
            recieved_offsets.pop_front();
            done_offsets.remove(&first);
            checkpoint = first + 1;
        }

        println!("Handled messages:");
        println!("P{}/O{}", message.partition(), message.offset());

        if checkpoint != old_checkpoint {
            let mut topic_part_list = TopicPartitionList::new();

            topic_part_list.add_partition_offset(topic, 0, rdkafka::Offset::Offset(checkpoint))?;
            consumer.commit(&topic_part_list, CommitMode::Sync)?;

            println!("Saved checkpoint is: {}", checkpoint);
        }
    }

    Ok(())
}
