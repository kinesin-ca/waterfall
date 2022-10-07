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
        use StorageMessage::*;
        match msg {
            Clear {} => {
                let mut keys = Vec::new();
                {
                    let mut iter: redis::AsyncIter<String> =
                        conn.scan_match(format!("{}:*", prefix)).await?;
                    while let Some(key) = iter.next_item().await {
                        keys.push(key);
                    }
                }
                for key in keys {
                    conn.del(key).await?;
                }
            }
            StoreAttempt {
                task_name,
                interval,
                attempt,
            } => {
                let tag = format!("{}:{}_{}", prefix, task_name, interval.end);
                let payload = serde_json::to_string(&attempt).unwrap();
                conn.rpush(&tag, &payload).await?;
            }
            /*
            SetTaskIntervalState {
                task_name,
                interval,
                state,
            } => {
                let map = format!("{}:task_interval_states", prefix);
                let key = format!("{}_{}-{}", task_name, interval.start, interval.end);
                let value = serde_json::to_string(&state).unwrap();
                conn.hset(&map, &key, &value).await?;
            }
            */
            StoreState { state } => {
                let tag = format!("{}:state", prefix);
                let payload = serde_json::to_string(&state).unwrap();
                conn.set(&tag, &payload).await?;
            }
            LoadState { response } => {
                let tag = format!("{}:state", prefix);
                let payload: String = conn.get(&tag).await.unwrap_or("{}".to_owned());
                let is: ResourceInterval = serde_json::from_str(&payload).unwrap();
                response.send(is).unwrap();
            }
            Stop {} => {
                break;
            }
        }
    }

    Ok(())
}

pub fn start(
    msgs: mpsc::UnboundedReceiver<StorageMessage>,
    url: String,
    prefix: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        start_redis_storage(msgs, url, prefix)
            .await
            .expect("Unable to start redis storage");
    })
}
