use super::*;
pub mod local_executor;

/// Messages for interacting with an Executor
#[derive(Debug)]
pub enum ExecutorMessage {
    /// Validate a set of tasks.
    /// Errors
    ///    Returns the vector of task issues
    ValidateTask {
        details: serde_json::Value,
        response: oneshot::Sender<Result<()>>,
    },

    /// Execute the given task, along with enough information
    /// Errors
    ///    Will return `Err` if the tasks are invalid, according to the executor
    ExecuteTask {
        task_name: String,
        interval: Interval,
        details: serde_json::Value,
        varmap: VarMap,
        output_options: TaskOutputOptions,
        storage: mpsc::UnboundedSender<StorageMessage>,
        response: oneshot::Sender<bool>,
        kill: oneshot::Receiver<()>,
    },
    Stop {},
}

fn default_bytes() -> usize {
    20480
}

/// Options in how to handle task output. Some tasks can be quite
/// verbose, and the output may not be needed.
#[derive(Clone, Serialize, Deserialize, Copy, Debug, PartialEq, Hash, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskOutputOptions {
    /// If true, output from successful tasks is discarded entirely, in
    /// keeping with the UNIX philosophy of no news is good news
    #[serde(default)]
    pub discard_successful: bool,

    /// If true, and output is not discarded, truncate the output of
    /// each task to a maximum of the first / last `preserve` kb of
    /// data
    #[serde(default)]
    pub truncate: bool,

    /// Number of KB of output to preserve at the beginning of the ouptut
    #[serde(default = "default_bytes")]
    pub head_bytes: usize,

    /// Number of KB of output to preserve at the end of the outut
    #[serde(default = "default_bytes")]
    pub tail_bytes: usize,
}

impl Default for TaskOutputOptions {
    fn default() -> Self {
        TaskOutputOptions {
            discard_successful: true,
            truncate: true,
            head_bytes: default_bytes(),
            tail_bytes: default_bytes(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskAttempt {
    #[serde(default)]
    pub task_name: String,

    #[serde(default = "chrono::Utc::now")]
    pub scheduled_time: DateTime<Utc>,

    #[serde(default = "chrono::Utc::now")]
    pub start_time: DateTime<Utc>,

    #[serde(default = "chrono::Utc::now")]
    pub stop_time: DateTime<Utc>,

    #[serde(default)]
    pub succeeded: bool,

    #[serde(default)]
    pub killed: bool,

    #[serde(default)]
    pub infra_failure: bool,

    #[serde(default)]
    pub output: String,

    #[serde(default)]
    pub error: String,

    #[serde(default)]
    pub executor: Vec<String>,

    #[serde(default)]
    pub exit_code: i32,

    /// as a percentage
    #[serde(default)]
    pub max_cpu: f32,

    /// as a percentage
    #[serde(default)]
    pub avg_cpu: f32,

    /// In bytes
    #[serde(default)]
    pub max_rss: u64,

    /// In bytes
    #[serde(default)]
    pub avg_rss: f32,
}

impl Default for TaskAttempt {
    fn default() -> Self {
        TaskAttempt {
            task_name: String::new(),
            scheduled_time: Utc::now(),
            start_time: Utc::now(),
            stop_time: Utc::now(),
            succeeded: false,
            killed: false,
            infra_failure: false,
            output: "".to_owned(),
            error: "".to_owned(),
            executor: Vec::new(),
            exit_code: 0i32,
            max_cpu: 0.0,
            avg_cpu: 0.0,
            max_rss: 0,
            avg_rss: 0.0,
        }
    }
}

impl TaskAttempt {
    #[must_use]
    pub fn new() -> Self {
        TaskAttempt::default()
    }
}

/// Keeps the first / last bytes of a str
#[must_use]
pub fn head_tail(data: &str, head: usize, tail: usize) -> String {
    if data.len() < head + tail {
        data.to_owned()
    } else {
        let n_chars = data.chars().count();
        let charsize = (data.len() as f64 / n_chars as f64).ceil() as usize;
        let head_chars = head / charsize;
        let tail_chars = tail / charsize;
        let mut tail: String = data.chars().rev().take(tail_chars).collect();
        tail = tail.chars().rev().collect();
        format!(
            "{}\n...\n{}",
            data.chars().take(head_chars).collect::<String>(),
            tail
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_head_tail() {
        let sample = "This is a very long string".to_owned();
        assert_eq!(head_tail(&sample, 5, 5), "This \n...\ntring".to_owned());
        assert_eq!(head_tail(&sample, 50, 50), sample);
    }
}
