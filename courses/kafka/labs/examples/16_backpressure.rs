use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use anyhow::{Context, Result};
use tokio::{task::JoinSet, time::sleep};

#[tokio::main]
async fn main() -> Result<()> {
    let pairs: [(usize, u64); 6] = [(0, 100), (1, 1000), (2, 200), (3, 100), (4, 100), (5, 100)];
    let mut recieved_offsets = VecDeque::new();
    let mut pairs_iter = pairs.into_iter();

    let mut checkpoint = 0;
    let window_len: usize = 3;
    let mut done_offsets = HashSet::<usize>::new();

    let mut task_set = JoinSet::<usize>::new();

    loop {
        while recieved_offsets.len() < window_len {
            match pairs_iter.next() {
                Some((offset, delay)) => {
                    recieved_offsets.push_back(offset);
                    task_set.spawn(async move {
                        sleep(Duration::from_millis(delay)).await;
                        offset
                    });
                    println!("Offset: {} is accepted", offset);
                }
                None => {
                    break;
                }
            }
        }

        match task_set.join_next().await {
            Some(res) => match res {
                Ok(offset) => {
                    done_offsets.insert(offset);

                    while let Some(first) = recieved_offsets.front().copied() {
                        if !done_offsets.contains(&first) {
                            break;
                        }
                        recieved_offsets.pop_front();
                        done_offsets.remove(&first);
                        checkpoint = first + 1;
                    }
                    println!("Handled offset: {}", offset);
                    println!("New safe checkpoint is: {}", checkpoint);
                    println!(
                        "Current len of recieved queue is {}",
                        recieved_offsets.len()
                    )
                }
                Err(e) => return Err(e).context("Unable to handle a task"),
            },
            None => {
                break;
            }
        }
    }
    Ok(())
}
