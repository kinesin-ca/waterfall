use super::*;
use crate::executors::TaskAttempt;
use crate::runner::ActionState;

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

pub mod memory;
pub mod noop;
pub mod redis;
