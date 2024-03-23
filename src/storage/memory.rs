use super::*;

use futures::prelude::*;

/// The mpsc channel can be sized to fit max parallelism
pub async fn start_memory_storage(mut msgs: mpsc::UnboundedReceiver<StorageMessage>) -> Result<()> {
    let mut system_state = HashMap::<String, String>::new();
    while let Some(msg) = msgs.recv().await {
        use StorageMessage::*;
        match msg {
            Clear {} => {
                system_state.clear();
            }
            StoreAttempt {
                task_name,
                interval,
                attempt,
            } => {
                let tag = format!("{}_{}", task_name, interval.end);
                let payload = serde_json::to_string(&attempt).unwrap();
                system_state.insert(tag, payload);
            }
            StoreState { state } => {
                let payload = serde_json::to_string(&state).unwrap();
                system_state.insert("state".to_owned(), payload);
            }
            LoadState { response } => {
                let is: ResourceInterval =
                    serde_json::from_str(&system_state.get(&"state".to_owned()).unwrap()).unwrap();
                response.send(is).unwrap();
            }
            Stop {} => {
                break;
            }
        }
    }

    Ok(())
}

pub fn start(msgs: mpsc::UnboundedReceiver<StorageMessage>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        start_memory_storage(msgs)
            .await
            .expect("Unable to start memory storage");
    })
}
