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
    StoreState {
        state: ResourceInterval,
    },
    LoadState {
        response: oneshot::Sender<ResourceInterval>,
    },
    Stop {},
}

pub mod redis_store;
