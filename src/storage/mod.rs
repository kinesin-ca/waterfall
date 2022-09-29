use super::*;
use crate::executors::TaskAttempt;

/// Messages for interacting with an Executor
#[derive(Debug)]
pub enum StorageMessage {
    StoreAttempt {
        task_name: String,
        interval: Interval,
        attempt: TaskAttempt,
    },
    Stop {},
}

pub mod redis_store;
