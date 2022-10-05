use super::*;
use futures::stream::futures_unordered::FuturesUnordered;
use psutil;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::process::Stdio;
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration};

use futures::StreamExt;
use tokio::io::AsyncReadExt;

type Environment = HashMap<String, Option<String>>;

/// Contains specifics on how to run a local task
#[derive(Serialize, Deserialize, Clone, Debug)]
struct LocalTaskDetail {
    /// The command and all arguments to run
    command: Cmd,

    /// Environment variables to set
    #[serde(default)]
    environment: Environment,

    /// Timeout in seconds
    #[serde(default)]
    timeout: u64,
}

fn extract_details(details: &TaskDetails) -> Result<LocalTaskDetail, serde_json::Error> {
    serde_json::from_value::<LocalTaskDetail>(details.clone())
}

fn validate_task(details: &TaskDetails) -> Result<()> {
    if let Err(err) = extract_details(details) {
        Err(anyhow!("{}", err))
    } else {
        Ok(())
    }
}

struct ChildStats {
    max_cpu: f32,
    avg_cpu: f32,
    max_rss: u64,
    avg_rss: f32,
}

// Collect performance stats for a child
async fn gather_child_stats(pid: psutil::Pid) -> Result<ChildStats> {
    let mut stats = ChildStats {
        max_cpu: 0.0,
        avg_cpu: 0.0,
        max_rss: 0,
        avg_rss: 0.0,
    };
    let mut periods: f32 = 0.0;

    let mut proc = psutil::process::Process::new(pid)?;

    while let (Ok(pct), Ok(mem)) = (proc.cpu_percent(), proc.memory_info()) {
        // update CPU
        if pct > stats.max_cpu {
            stats.max_cpu = pct;
        }
        stats.avg_cpu += pct;

        // update RSS
        let rss = mem.rss();
        if rss > stats.max_rss {
            stats.max_rss = rss;
        }
        stats.avg_rss += rss as f32;

        periods += 1.0;
        sleep(Duration::from_millis(100)).await;
    }
    if periods > 0.0 {
        stats.avg_cpu /= periods;
        stats.avg_rss /= periods;
    }
    Ok(stats)
}

async fn run_task(
    task: TaskDetails,
    mut stop_rx: oneshot::Receiver<()>,
    output_options: TaskOutputOptions,
    varmap: VarMap,
    mut env: Environment,
) -> Result<TaskAttempt> {
    let mut details = extract_details(&task).unwrap();
    let mut attempt = TaskAttempt::new();
    let cmd = details.command.generate(&varmap);
    details.command = Cmd::Split(cmd.clone());
    let (program, args) = cmd.split_first().unwrap();
    attempt.executor.push(format!("{:?}\n", details));

    let mut command = Command::new(program);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    command.args(args);

    // Build out environment. This takes the initial environment, and will
    // upsert it with the task details.
    env.extend(details.environment);
    let cmd_env: HashMap<String, String> = env
        .iter()
        .filter(|(_, v)| v.is_some())
        .map(|(k, v)| (k.clone(), varmap.apply_to(&v.clone().unwrap())))
        .collect();

    command.env_clear();
    command.envs(cmd_env);

    attempt.start_time = Utc::now();
    let mut child = command.spawn()?;

    // Start getting performance stats
    let pid = child.id().unwrap();
    let perf_monitor = tokio::spawn(async move { gather_child_stats(pid).await });

    // Read from stdout constantly to prevent pipe blocking
    let mut stdout_handle = child.stdout.take().unwrap();
    let stdout_reader: tokio::task::JoinHandle<Result<Vec<u8>>> = tokio::spawn(async move {
        let mut data = Vec::new();
        stdout_handle.read_to_end(&mut data).await?;
        Ok(data)
    });

    // Read from stderr constantly to prevent pipe blocking
    let mut stderr_handle = child.stderr.take().unwrap();
    let stderr_reader: tokio::task::JoinHandle<Result<Vec<u8>>> = tokio::spawn(async move {
        let mut data = Vec::new();
        stderr_handle.read_to_end(&mut data).await?;
        Ok(data)
    });

    // Generate a timeout message, if needed
    let (timeout_tx, mut timeout_rx) = oneshot::channel();
    if details.timeout > 0 {
        let timeout = details.timeout;
        tokio::spawn(async move {
            sleep(Duration::from_millis(1000 * timeout)).await;
            timeout_tx.send(()).unwrap_or(());
        });
    }

    tokio::select! {
        _ = child.wait() => {},
        _ = (&mut stop_rx) => {
            attempt.killed = true;
            child.kill().await.unwrap_or(());
            attempt.executor.push("Task was killed by request".to_owned());
        }
        _ = (&mut timeout_rx) => {
            child.kill().await.unwrap_or(());
            attempt.killed = true;
            attempt.executor.push("Task exceeded the timeout interval and was killed".to_owned());
        }
    }

    // Get any output
    let mut stdout = String::from_utf8_lossy(&stdout_reader.await??).to_string();
    let mut stderr = String::from_utf8_lossy(&stderr_reader.await??).to_string();

    let output = child.wait_with_output().await.unwrap();
    attempt.exit_code = output.status.code().unwrap_or(-1i32);
    attempt.succeeded = output.status.success();
    if !(attempt.succeeded && output_options.discard_successful) {
        if output_options.truncate {
            stdout = head_tail(
                &stdout,
                output_options.head_bytes,
                output_options.tail_bytes,
            );
            stderr = head_tail(
                &stdout,
                output_options.head_bytes,
                output_options.tail_bytes,
            );
        }
        attempt.output = stdout;
        attempt.error = stderr;
    }

    // Set stats
    if let Ok(stats) = perf_monitor.await? {
        attempt.max_cpu = stats.max_cpu;
        attempt.avg_cpu = stats.avg_cpu;
        attempt.max_rss = stats.max_rss;
        attempt.avg_rss = stats.avg_rss;
    }

    attempt.stop_time = Utc::now();
    Ok(attempt)
}

/// The mpsc channel can be sized to fit max parallelism
pub async fn start_local_executor(
    max_parallel: usize,
    mut exe_msgs: mpsc::UnboundedReceiver<ExecutorMessage>,
) {
    let mut running = FuturesUnordered::new();

    /*
    Inherited environment vars
    */

    let default_vars = [
        "LANG",
        "HOSTNAME",
        "LOGNAME",
        "USER",
        "PATH",
        "HOME",
        "XDG_CONFIG_HOME",
        "ALL_PROXY",
        "FTP_PROXY",
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "NO_PROXY",
    ];
    let inherited_env: Environment = default_vars
        .iter()
        .map(|envvar| (envvar.to_string(), std::env::var(envvar).ok()))
        .collect();

    while let Some(msg) = exe_msgs.recv().await {
        use ExecutorMessage::{ExecuteTask, Stop, ValidateTask};
        match msg {
            ValidateTask { details, response } => {
                tokio::spawn(async move {
                    let result = validate_task(&details);
                    response.send(result).unwrap_or(());
                });
            }
            ExecuteTask {
                details,
                varmap,
                output_options,
                response,
                kill,
            } => {
                if running.len() == max_parallel {
                    running.next().await;
                }
                let env = inherited_env.clone();
                running.push(tokio::spawn(async move {
                    let attempt = match run_task(details, kill, output_options, varmap, env).await {
                        Ok(attempt) => attempt,
                        Err(e) => TaskAttempt {
                            succeeded: false,
                            executor: vec![format!("Failed to launch command: {:?}", e)],
                            ..TaskAttempt::new()
                        },
                    };
                    response.send(attempt).unwrap();
                }));
            }
            Stop {} => {
                break;
            }
        }
    }
}

pub fn start(
    max_parallel: usize,
    msgs: mpsc::UnboundedReceiver<ExecutorMessage>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        start_local_executor(max_parallel, msgs).await;
    })
}
