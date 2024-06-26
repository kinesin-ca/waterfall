pub use serde::Deserialize;
use std::fmt::Debug;
use sysinfo::System;
use tokio::sync::mpsc;
use waterfall::prelude::*;

fn default_resources() -> TaskResources {
    let mut system = System::new_all();
    system.refresh_all();
    let cores = (system.cpus().len() as i64) - 2;
    let free_memory = (system.total_memory() - system.used_memory()) as f64;
    let memory_mb = ((free_memory * 0.8) as i64) / 1024;

    let mut resources = TaskResources::new();
    resources.insert("cores".to_owned(), cores);
    resources.insert("memory_mb".to_owned(), memory_mb);
    resources
}

fn default_ip() -> String {
    "127.0.0.1".to_owned()
}

fn default_port() -> u32 {
    2504
}

#[derive(Deserialize, Debug, Clone)]
pub struct GlobalConfigSpec {
    #[serde(default = "default_ip")]
    pub ip: String,

    #[serde(default = "default_port")]
    pub port: u32,

    #[serde(default = "default_resources")]
    pub resources: TaskResources,
}

impl Default for GlobalConfigSpec {
    fn default() -> Self {
        GlobalConfigSpec {
            ip: String::from("127.0.0.1"),
            port: default_port(),
            resources: default_resources(),
        }
    }
}

#[derive(Clone)]
pub struct GlobalConfig {
    pub ip: String,
    pub port: u32,
    pub resources: TaskResources,
    pub storage: mpsc::UnboundedSender<StorageMessage>,
    pub executor: mpsc::UnboundedSender<ExecutorMessage>,
}

impl GlobalConfig {
    pub fn new(spec: &GlobalConfigSpec) -> Self {
        let def_res = default_resources();
        let cores = def_res.get("cores").unwrap();

        let workers = spec.resources.get("cores").unwrap_or(cores);

        let (executor, exe_rx) = mpsc::unbounded_channel();
        local_executor::start(*workers as usize, exe_rx);

        // Tracker
        let (storage, trx) = mpsc::unbounded_channel();
        waterfall::storage::noop::start(trx);

        GlobalConfig {
            ip: spec.ip.clone(),
            port: spec.port,
            resources: spec.resources.clone(),
            storage,
            executor,
        }
    }

    pub fn listen_spec(&self) -> String {
        format!("{}:{}", self.ip, self.port)
    }
}
