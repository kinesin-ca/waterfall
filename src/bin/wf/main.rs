use clap::Parser;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use waterfall::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case", deny_unknown_fields, tag = "type")]
enum StorageConfig {
    Redis { url: String, prefix: String },
}

impl StorageConfig {
    fn start(
        &self,
    ) -> (
        mpsc::UnboundedSender<StorageMessage>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        match self {
            StorageConfig::Redis { url, prefix } => {
                (tx, redis_store::start(rx, url.clone(), prefix.clone()))
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case", deny_unknown_fields, tag = "type")]
enum ExecutorConfig {
    Local { workers: usize },
}

impl ExecutorConfig {
    fn start(
        &self,
    ) -> (
        mpsc::UnboundedSender<ExecutorMessage>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        match self {
            ExecutorConfig::Local { workers } => (tx, local_executor::start(*workers, rx)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
struct Config {
    storage: StorageConfig,
    executor: ExecutorConfig,
}

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    /// Configuration File
    #[clap(short, long, default_value = "")]
    config: String,

    /// Configuration File
    #[clap(short, long, default_value = "")]
    world: String,

    /// Enable verbose logging
    #[clap(short, long)]
    verbose: bool,
}

/*
  Sample config

    {
        "storage": {
            "type": "redis",
            "url": "redis://localhost",
            "prefix": "world"
        },
        "executor": {
            "type": "local",
            "workers": 10,
        }
    }
*/

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    // Parse the config
    let world_json = std::fs::read_to_string(&args.world)
        .expect(&format!("Unable to open {} for reading", args.config));
    let world_def: WorldDefinition =
        serde_json::from_str(&world_json).expect("Unable to parse world definition");

    // Parse the config
    let config_json = std::fs::read_to_string(&args.config)
        .expect(&format!("Unable to open {} for reading", args.config));
    let config: Config =
        serde_json::from_str(&config_json).expect("Unable to parse config definition");

    // Start the config
    let (exe_tx, exe_handle) = config.executor.start();
    let (storage_tx, storage_handle) = config.storage.start();

    let tasks = world_def.taskset().unwrap();

    let mut runner = Runner::new(
        tasks,
        world_def.variables,
        exe_tx.clone(),
        storage_tx.clone(),
        world_def.output_options,
    )
    .await
    .unwrap();

    let (wtx, wrx) = oneshot::channel();
    runner.run(wrx).await;

    exe_tx.send(ExecutorMessage::Stop {}).unwrap();
    exe_handle.await.unwrap();

    storage_tx.send(StorageMessage::Stop {}).unwrap();
    storage_handle.await.unwrap();

    Ok(())
}
