use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use anyhow::{Context, Result};
use tokio::{task::JoinSet, time::sleep};

#[tokio::main]
async fn main() -> Result<()> {
    let pairs: [(usize, u64); 3] = [(4, 100), (7, 1000), (9, 300)];
    let mut recieved_offsets = VecDeque::new();

    for (offset, _) in pairs {
        recieved_offsets.push_back(offset);
    }

    let mut checkpoint = 4;
    let mut done_offsets = HashSet::<usize>::new();

    let mut task_set = JoinSet::<usize>::new();

    for (offset, delay) in pairs {
        task_set.spawn(async move {
            sleep(Duration::from_millis(delay)).await;
            offset
        });
    }

    while let Some(res) = task_set.join_next().await {
        match res {
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
            }
            Err(e) => return Err(e).context("Undable to handle a task"),
        }
    }

    Ok(())
}
