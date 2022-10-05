use super::*;

/// The mpsc channel can be sized to fit max parallelism
pub async fn start_storage(mut msgs: mpsc::UnboundedReceiver<StorageMessage>) -> Result<()> {
    let mut current_state = ResourceInterval::new();
    while let Some(msg) = msgs.recv().await {
        use StorageMessage::*;
        match msg {
            StoreAttempt { .. } => {}
            StoreState { state } => {
                current_state = state;
            }
            LoadState { response } => {
                response.send(current_state.clone()).unwrap();
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
        start_storage(msgs).await.expect("Unable to start storage");
    })
}
