//! The Agent executor is essentially a wrapped version of the local executor.
//! It dispatches tasks to remote hosts

use super::*;
use futures::stream::futures_unordered::FuturesUnordered;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use futures::StreamExt;

fn default_as_true() -> bool {
    true
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AgentTarget {
    pub base_url: String,

    #[serde(default)]
    pub resources: TaskResources,

    #[serde(default)]
    pub current_resources: TaskResources,

    #[serde(default)]
    pub enabled: bool,
}

impl AgentTarget {
    fn new(base_url: String, resources: TaskResources) -> Self {
        AgentTarget {
            base_url,
            resources: resources.clone(),
            current_resources: resources,
            enabled: true,
        }
    }

    async fn refresh_resources(&mut self, client: &reqwest::Client) {
        let resource_url = format!("{}/resources", self.base_url);
        let disabled = match client.get(resource_url).send().await {
            Ok(result) => {
                if result.status() == reqwest::StatusCode::OK {
                    self.resources = result.json().await.unwrap();
                    self.current_resources = self.resources.clone();
                    false
                } else {
                    true
                }
            }
            Err(_) => true,
        };
        if self.enabled && disabled {
            warn!("Disabling {}: unable to refresh resources", self.base_url);
        }
        self.enabled = !disabled;
    }

    async fn ping(&mut self, client: &reqwest::Client) -> Result<()> {
        let resource_url = format!("{}/ready", self.base_url);
        let result = client.get(resource_url).send().await?;
        self.enabled = result.status() == reqwest::StatusCode::OK;
        Ok(())
    }
}

/// Contains specifics on how to run a local task
#[derive(Serialize, Deserialize, Clone, Debug)]
struct AgentTaskDetail {
    /// The command and all arguments to run
    command: Cmd,

    /// Environment variables to set
    #[serde(default)]
    environment: HashMap<String, String>,

    /// Timeout in seconds
    #[serde(default)]
    timeout: i64,

    /// resources required by the task
    resources: TaskResources,
}

fn extract_details(details: &TaskDetails) -> Result<AgentTaskDetail, serde_json::Error> {
    serde_json::from_value::<AgentTaskDetail>(details.clone())
}

fn validate_task(details: &TaskDetails, max_capacities: &[TaskResources]) -> Result<()> {
    let parsed = extract_details(details)?;
    if max_capacities.is_empty()
        || max_capacities.iter().all(|x| x.values().all(|x| *x == 0))
        || max_capacities
            .iter()
            .any(|x| x.can_satisfy(&parsed.resources))
    {
        Ok(())
    } else {
        Err(anyhow!("No Agent target satisfies the required resources"))
    }
}

/// Contains specifics on how to run a local task
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskSubmission {
    pub details: TaskDetails,
    pub varmap: VarMap,
    pub output_options: TaskOutputOptions,
}

async fn submit_task(
    base_url: String,
    details: TaskDetails,
    output_options: TaskOutputOptions,
    client: reqwest::Client,
    varmap: VarMap,
) -> Result<TaskAttempt> {
    let submit_url = format!("{}/run", base_url);
    let submission = TaskSubmission {
        details,
        varmap,
        output_options,
    };
    match client.post(submit_url).json(&submission).send().await {
        Ok(result) => {
            if result.status() == reqwest::StatusCode::OK {
                let mut attempt: TaskAttempt = result.json().await.unwrap();
                attempt
                    .executor
                    .push(format!("Executed on agent at {}", base_url));
                Ok(attempt)
            } else {
                Err(anyhow!(
                    "Unable to dispatch to agent at {}: {:?}",
                    base_url,
                    result.text().await.unwrap()
                ))
            }
        }
        Err(e) => Err(anyhow!(
            "Unable to dispatch to agent at {}: {:?}",
            base_url,
            e
        )),
    }
}

// async fn select_target() -> Option<usize> {}

struct RunningTask {
    resources: TaskResources,
    target_id: usize,
}

/// The mpsc channel can be sized to fit max parallelism
async fn start_agent_executor(
    mut targets: Vec<AgentTarget>,
    mut exe_msgs: mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    let client = reqwest::Client::new();

    for target in &mut targets {
        target.refresh_resources(&client).await;
    }
    let mut max_caps: Vec<TaskResources> = targets.iter().map(|x| x.resources.clone()).collect();

    // Set up the local executor
    let (le_tx, le_rx) = mpsc::unbounded_channel();
    local_executor::start(1, le_rx);

    // Tasks waiting to release resources
    let mut running = FuturesUnordered::new();

    while let Some(msg) = exe_msgs.recv().await {
        use ExecutorMessage::*;
        match msg {
            ValidateTask { details, response } => {
                let ltx = le_tx.clone();
                let caps = max_caps.clone();
                tokio::spawn(async move {
                    let result = validate_task(&details, &caps);
                    if result.is_err() {
                        response.send(result).unwrap_or(());
                    } else {
                        ltx.send(ValidateTask { details, response }).unwrap_or(());
                    }
                });
            }
            ExecuteTask {
                details,
                varmap,
                output_options,
                response,
                kill,
            } => {
                let task = extract_details(&details).unwrap();
                let resources = task.resources.clone();

                loop {
                    match targets.iter_mut().enumerate().find(|(_, x)| {
                        x.enabled && x.current_resources.can_satisfy(&task.resources)
                    }) {
                        // There is a remote agent with capacity
                        Some((tid, target)) => {
                            info!("Dispatching job to {}", target.base_url);
                            target.current_resources.sub(&resources).unwrap();
                            let base_url = target.base_url.clone();
                            let submit_client = client.clone();
                            running.push(tokio::spawn(async move {
                                let res = submit_task(
                                    base_url,
                                    details,
                                    output_options,
                                    submit_client,
                                    varmap,
                                )
                                .await;
                                let mut rc = false;
                                if let Ok(attempt) = res {
                                    response.send(attempt).unwrap();
                                    rc = true;
                                }
                                (tid, resources, rc)
                            }));
                            break;
                        }
                        // No agent has capacity
                        None => {
                            // Give the outstanding tasks a chance to complete or agents
                            // recover
                            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

                            // Refresh any disabled targets
                            for (tid, target) in targets.iter_mut().enumerate() {
                                if target.enabled {
                                    continue;
                                }
                                target.refresh_resources(&client).await;
                                if target.enabled {
                                    max_caps[tid] = target.resources.clone();
                                    info!("{} is now enabled.", target.base_url);
                                }
                            }

                            // Wait for the next item
                            if !running.is_empty() {
                                let result: Result<
                                    (usize, TaskResources, bool),
                                    tokio::task::JoinError,
                                > = running.next().await.unwrap();

                                let (tid, resources, submit_ok) = result.unwrap();
                                if !submit_ok {
                                    warn!(
                                        "Disabling agent at {} due to incomplete submission.",
                                        targets[tid].base_url
                                    );
                                    targets[tid].enabled = false;
                                }
                                targets[tid].current_resources.add(&resources);
                            }
                        }
                    }
                }
            }
            /*
            msg @ StopTask { .. } => {
                le_tx.send(msg).unwrap_or(());
            }
            */
            Stop {} => {
                break;
            }
        }
    }
}

pub fn start(
    targets: Vec<AgentTarget>,
    msgs: mpsc::UnboundedReceiver<ExecutorMessage>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        start_agent_executor(targets, msgs).await;
    })
}
