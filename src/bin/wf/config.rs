pub use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use sysinfo::{RefreshKind, System, SystemExt};
use tokio::sync::mpsc;
use waterfall::prelude::*;

fn default_workers() -> usize {
    let system = System::new_with_specifics(RefreshKind::new().with_cpu());
    let workers = system.processors().len();
    if workers > 2 {
        workers - 2
    } else {
        workers
    }
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "executor", rename_all = "lowercase")]
pub enum PoolConfig {
    Local {
        #[serde(default = "default_workers")]
        workers: usize,
    },
}

fn default_pools() -> HashMap<String, PoolConfig> {
    HashMap::from([(
        "default".to_owned(),
        PoolConfig::Local {
            workers: default_workers(),
        },
    )])
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "storage")]
pub enum StorageConfig {
    Redis,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig::Redis
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct GlobalConfigSpec {
    #[serde(default = "default_pools")]
    pub pools: HashMap<String, PoolConfig>,

    #[serde(default)]
    pub tracker: StorageConfig,

    #[serde(default)]
    pub default_pool: String,
}

#[derive(Clone)]
pub struct GlobalConfig {
    pub pools: HashMap<String, mpsc::UnboundedSender<ExecutorMessage>>,
    pub tracker: mpsc::UnboundedSender<StorageMessage>,
    pub runner: mpsc::UnboundedSender<RunnerMessage>,
    pub default_pool: String,
    pub spec: GlobalConfigSpec,
}

impl GlobalConfig {
    pub async fn new(spec: &GlobalConfigSpec) -> Self {
        let mut pools = HashMap::new();

        use PoolConfig::*;
        for (pool, pool_spec) in spec.pools.iter() {
            let (tx, rx) = mpsc::unbounded_channel();
            match pool_spec {
                Local { workers } => {
                    local_executor::start(*workers, rx);
                }

                Ssh { targets } => {
                    ssh_executor::start(targets.clone(), rx);
                }

                Agent { targets } => {
                    agent_executor::start(targets.clone(), rx);
                }

                #[cfg(feature = "slurm")]
                Slurm { base_url } => {
                    slurm_executor::start(base_url.clone(), rx);
                }
            }
            pools.insert(pool.clone(), tx);
        }

        // Storage
        let (tracker, trx) = mpsc::unbounded_channel();
        use StorageConfig::*;
        match spec.tracker {
            Memory => memory_tracker::start(trx),
        }

        // Runner
        let (runner, rrx) = mpsc::unbounded_channel();
        let rtx = runner.clone();
        runner::start(rtx, rrx);

        let default_pool = if spec.default_pool.is_empty() {
            pools.keys().next().unwrap().clone()
        } else {
            spec.default_pool.clone()
        };

        GlobalConfig {
            server: spec.server.clone(),
            pools,
            tracker,
            runner,
            default_pool,
            spec: spec.clone(),
        }
    }

    pub fn listen_spec(&self) -> String {
        format!("{}:{}", self.server.ip, self.server.port)
    }
}

impl Debug for GlobalConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalConfig")
            .field("spec", &self.spec)
            .field("default_pool", &self.default_pool)
            .finish()
    }
}
