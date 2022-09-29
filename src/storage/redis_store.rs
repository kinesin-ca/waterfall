use super::*;

extern crate redis;

use futures::prelude::*;
use redis::AsyncCommands;

/// The mpsc channel can be sized to fit max parallelism
pub async fn start_redis_storage(
    mut msgs: mpsc::UnboundedReceiver<StorageMessage>,
    url: String,
    prefix: String,
) -> Result<()> {
    let client = redis::Client::open(url)?;
    let mut conn = client.get_async_connection().await?;

    while let Some(msg) = msgs.recv().await {
        use StorageMessage::{Stop, StoreAttempt};
        match msg {
            StoreAttempt {
                task_name,
                interval,
                attempt,
            } => {
                let tag = format!("{}_{}_{}", prefix, task_name, interval.end);
                redis::cmd("PUSH")
                    .arg(&[&tag, &serde_json::to_string(&attempt).unwrap()])
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(());
            }
            Stop {} => {
                break;
            }
        }
    }

    Ok(())
}

pub fn start(msgs: mpsc::UnboundedReceiver<StorageMessage>, url: String, prefix: String) {
    tokio::spawn(async move {
        start_redis_storage(msgs, url, prefix)
            .await
            .expect("Unable to start redis storage");
    });
}
