use super::*;
use crate::executors::TaskAttempt;

/// Messages for interacting with an Executor
#[derive(Debug)]
pub enum StorageMessage {
    Clear {},
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
    /*
    GetAttempts {
        task_name: String,
        interval: Interval,
        response: oneshot::Sender<TaskAttempt>,
    },
    */
    Stop {},
}

pub mod noop;
pub mod redis;
