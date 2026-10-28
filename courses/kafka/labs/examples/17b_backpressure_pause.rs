use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use anyhow::{Context, Result};
use rdkafka::{
    ClientConfig, Message, TopicPartitionList,
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::OwnedMessage,
};
use tokio::{task::JoinSet, time::sleep};

#[derive(Debug)]
enum Pause {
    Paused,
    Resumed,
}

#[tokio::main]
async fn main() -> Result<()> {
    let topic = "backpressure-input";
    let delays: [u64; 6] = [100, 1000, 200, 100, 100, 100];

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "backpressure-pause-lab-v1")
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

    let mut partitions = TopicPartitionList::new();
    partitions.add_partition(topic, 0);

    let mut pause = Pause::Resumed;

    loop {
        if received_count >= 6 && task_set.is_empty() {
            break;
        }

        if received_count >= 6 || recieved_offsets.len() >= window_len {
            if let Pause::Resumed = pause {
                consumer.pause(&partitions)?;
                pause = Pause::Paused;
                println!("{pause:?} and queue len is: {}", recieved_offsets.len());
            }
        } else if let Pause::Paused = pause {
            consumer.resume(&partitions)?;
            pause = Pause::Resumed;
            println!("{pause:?} and queue len is: {}", recieved_offsets.len());
        }

        tokio::select! {
            received_message = consumer.recv() => {
                let message = received_message?;
                if received_count == 0 {
                    checkpoint = message.offset();
                }
                recieved_offsets.push_back(message.offset());
                println!("Recieved P{}/O{}. Current queue len is: {}", message.partition(), message.offset(), recieved_offsets.len());
                let message = message.detach();
                let delay = *delays.get(received_count).context("No more delays in delays, it shouldn't be like that")?;
                task_set.spawn(async move {
                    sleep(Duration::from_millis(delay)).await;
                    message
                });
                received_count += 1;

            }
            Some(task) = task_set.join_next(), if !task_set.is_empty() => {
                let message = task?;
                done_offsets.insert(message.offset());
                let previous_checkpoint = checkpoint;

                while let Some(first) = recieved_offsets.front().copied() {
                    if !done_offsets.contains(&first) {
                        break;
                    }
                    recieved_offsets.pop_front();
                    done_offsets.remove(&first);
                    checkpoint = first + 1;
                };


                println!("Done offset: {}, checkopoint is on: {}, current queue len is: {}", message.offset(), checkpoint, recieved_offsets.len());

                if checkpoint != previous_checkpoint {
                    let mut topic_partition_list = TopicPartitionList::new();

                    topic_partition_list.add_partition_offset(topic, 0, rdkafka::Offset::Offset(checkpoint))?;

                    consumer.commit(&topic_partition_list, CommitMode::Sync)?;

                    println!("Saved checkpoint is: {}", checkpoint);

                }

            }
        }
    }

    Ok(())
}
