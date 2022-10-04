use super::*;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::StreamExt;

/*
    Runner is responsible for taking a TaskSet and a varmap and
    iteratively taking steps to converge the current state to
    be the target state.

    The runner will continue to execute until:
        - A Stop message is sent
        - current = TaskSet::coverage (the theoretical)
*/

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ActionState {
    Queued,
    Running,
    Errored,
    Completed,
}

#[derive(Debug, Clone)]
pub struct Action {
    task: String,
    interval: Interval,
    state: ActionState,
    // kill: Option<oneshot::Receiver<()>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum WorldEvent {
    Start,
    TaskFailed {
        task_name: String,
        interval: Interval,
    },
    TaskCompleted {
        task_name: String,
        interval: Interval,
    },
    Timeout,
    Stop,
}

// Takes a definition, and runs it to completion
pub struct Runner {
    tasks: TaskSet,
    vars: VarMap,
    output_options: TaskOutputOptions,

    // States
    end_state: ResourceInterval,
    target: ResourceInterval,
    current: ResourceInterval,

    queue: Vec<Action>,
    qidx: usize,

    events: FuturesUnordered<tokio::task::JoinHandle<WorldEvent>>,

    last_horizon: DateTime<Utc>,
    executor: mpsc::UnboundedSender<ExecutorMessage>,
}

fn gen_timeout(timeout: i64) -> tokio::task::JoinHandle<WorldEvent> {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::seconds(timeout).to_std().unwrap()).await;
        WorldEvent::Timeout
    })
}

async fn validate_cmd(
    executor: mpsc::UnboundedSender<ExecutorMessage>,
    cmd: serde_json::Value,
) -> Result<()> {
    let (response, rx) = oneshot::channel();
    executor
        .send(ExecutorMessage::ValidateTask {
            details: cmd,
            response,
        })
        .unwrap();
    rx.await?
}

async fn run_task(
    details: serde_json::Value,
    executor: mpsc::UnboundedSender<ExecutorMessage>,
    kill: oneshot::Receiver<()>,
    output_options: &TaskOutputOptions,
    varmap: &VarMap,
) -> bool {
    let (response, response_rx) = oneshot::channel();
    executor
        .send(ExecutorMessage::ExecuteTask {
            details,
            output_options: output_options.clone(),
            varmap: varmap.clone(),
            response,
            kill,
        })
        .unwrap();
    response_rx.await.unwrap()
}

async fn up_task(
    task_name: String,
    interval: Interval,
    kill: oneshot::Receiver<()>,
    varmap: VarMap,
    up: TaskDetails,
    check: Option<TaskDetails>,
    output_options: TaskOutputOptions,
    executor: mpsc::UnboundedSender<ExecutorMessage>,
) -> WorldEvent {
    if let Some(check_cmd) = check.clone() {
        let (subkill, subkill_rx) = oneshot::channel();
        let succeeded = run_task(
            check_cmd.clone(),
            executor.clone(),
            subkill_rx,
            &output_options,
            &varmap,
        )
        .await;

        // If check succeeded, resources are up
        if succeeded {
            return WorldEvent::TaskCompleted {
                task_name,
                interval,
            };
        }
    }

    // UP
    let (subkill, subkill_rx) = oneshot::channel();
    let succeeded = run_task(up, executor.clone(), subkill_rx, &output_options, &varmap).await;
    if !succeeded {
        return WorldEvent::TaskFailed {
            task_name,
            interval,
        };
    }

    // recheck
    if let Some(check_cmd) = check {
        let (subkill, subkill_rx) = oneshot::channel();
        let succeeded = run_task(
            check_cmd.clone(),
            executor.clone(),
            subkill_rx,
            &output_options,
            &varmap,
        )
        .await;

        // If check succeeded, resources are up
        if succeeded {
            WorldEvent::TaskCompleted {
                task_name,
                interval,
            }
        } else {
            WorldEvent::TaskFailed {
                task_name,
                interval,
            }
        }
    } else {
        WorldEvent::TaskCompleted {
            task_name,
            interval,
        }
    }
}

impl Runner {
    pub async fn new(
        tasks: TaskSet,
        vars: VarMap,
        executor: mpsc::UnboundedSender<ExecutorMessage>,
        output_options: TaskOutputOptions,
    ) -> Result<Self> {
        for tdef in tasks.values() {
            validate_cmd(executor.clone(), tdef.up.clone()).await?;
            if let Some(cmd) = &tdef.down {
                validate_cmd(executor.clone(), cmd.clone()).await?;
            }
            if let Some(cmd) = &tdef.check {
                validate_cmd(executor.clone(), cmd.clone()).await?;
            }
        }

        let end_state = tasks.coverage()?;
        let mut runner = Runner {
            tasks,
            vars,
            output_options,
            end_state,
            target: ResourceInterval::new(),
            current: ResourceInterval::new(),
            queue: Vec::new(),
            qidx: 0,
            events: FuturesUnordered::new(),
            last_horizon: DateTime::<Utc>::MIN_UTC,
            executor,
        };

        runner.tick()?;

        Ok(runner)
    }

    pub fn tick(&mut self) -> Result<()> {
        let target = self.tasks.get_state(Utc::now())?;

        // Create queue
        let required = target.difference(&self.current);
        self.queue = self.tasks.iter().fold(Vec::new(), |mut acc, (name, task)| {
            let res: Vec<Action> = task
                .generate_intervals(&required)
                .unwrap()
                .into_iter()
                .map({
                    |interval| Action {
                        task: name.clone(),
                        interval,
                        state: ActionState::Queued,
                    }
                })
                .collect();
            acc.extend(res);
            acc
        });

        // Ensure that all actions can be satisfied
        let unsatisfied = self
            .queue
            .iter()
            .filter(|act| {
                !self
                    .tasks
                    .get(&act.task)
                    .unwrap()
                    .can_be_satisfied(act.interval, &target)
            })
            .fold(HashSet::new(), |mut acc, a| {
                println!("Task cannot be satisfied: {:?}", a);
                acc.insert(a.task.clone());
                acc
            });

        // Ensure current +
        let mut result_state = self.current.clone();
        for action in &self.queue {
            for res in &self.tasks.get(&action.task).unwrap().provides {
                result_state
                    .entry(res.clone())
                    .or_insert(IntervalSet::new())
                    .insert(action.interval);
            }
        }
        if result_state != target {
            return Err(anyhow!(
                "Actions generated produce\n\t{:?}\nExpected\n\t{:?}",
                result_state,
                target
            ));
        }

        if unsatisfied.is_empty() {
            self.target = target;
            Ok(())
        } else {
            Err(anyhow!("Tasks {:?} cannot complete as the target state does not provide required resources", unsatisfied))
        }
    }

    // We'll be using channels for running
    pub async fn run(&mut self, stop: oneshot::Receiver<WorldEvent>) {
        self.events.push(tokio::spawn(async move {
            stop.await.expect("Unable to get stop");
            WorldEvent::Stop
        }));
        self.queue_actions();

        // Loop while we can make progress
        while !self.is_done() {
            match self.events.next().await {
                Some(Ok(WorldEvent::Start)) => {
                    println!("START");
                    self.queue_actions();
                }
                Some(Ok(WorldEvent::Stop)) => {
                    println!("Stop");
                    break;
                }
                Some(Ok(WorldEvent::Timeout)) => {
                    println!("Timeout");
                    self.queue_actions();
                }
                Some(Ok(WorldEvent::TaskFailed {
                    task_name,
                    interval,
                })) => {
                    println!("FAILED: {} / {}", task_name, interval);
                    println!("Well that sucks");
                }
                Some(Ok(WorldEvent::TaskCompleted {
                    task_name,
                    interval,
                })) => {
                    let action = self
                        .queue
                        .iter_mut()
                        .find(|x| x.task == task_name && x.interval == interval)
                        .unwrap();
                    let task = self.tasks.get(&task_name).unwrap();
                    action.state = ActionState::Completed;
                    for res in &task.provides {
                        self.current
                            .entry(res.clone())
                            .or_insert(IntervalSet::new())
                            .insert(action.interval);
                    }
                    self.queue_actions();
                }
                Some(Err(e)) => {
                    panic!("Something went wrong: {:?}", e)
                }
                None => {
                    // No pending actions waiting
                    // Can probably wait to the next event
                    continue;
                }
            }
            // Log stuff
        }
    }

    fn queue_actions(&mut self) {
        let now = Utc::now();

        // Collect any outstanding futures
        for action in self.queue[self.qidx..]
            .iter_mut()
            .filter(|x| x.state == ActionState::Queued && x.interval.end <= now)
        {
            let task = self.tasks.get(&action.task).unwrap();
            if !task.can_run(action.interval, &self.current) {
                continue;
            }
            let (kill_tx, kill) = oneshot::channel();
            let varmap: VarMap = VarMap::from_interval(&action.interval, task.timezone)
                .iter()
                .chain(self.vars.iter())
                .collect();
            let task_name = action.task.clone();
            let interval = action.interval;
            let up = task.up.clone();
            let check = task.check.clone();
            let output_options = self.output_options.clone();
            let exe = self.executor.clone();
            self.events.push(tokio::spawn(async move {
                up_task(
                    task_name.clone(),
                    interval,
                    kill,
                    varmap,
                    up,
                    check,
                    output_options,
                    exe,
                )
                .await
            }));
            // action.response = Some(response_rx);
            // action.kill = Some(kill_tx);
            action.state = ActionState::Running;
        }
    }

    fn is_done(&self) -> bool {
        self.end_state == self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executors::local_executor;

    #[tokio::test]
    async fn test_runner() {
        let json_runner = r#"{
            "variables": {
                "HOME": "/tmp/world_test"
            },
            "calendars": {
                "std": { "mask": [ "Mon", "Tue", "Wed", "Thu", "Fri" ] }
            },
            "tasks": {
                "task_a": {
                    "up": { "command": "/usr//bin/touch ${HOME}/task_a_${yyyymmdd}" },
                    "down": { "command": "/bin/rm ${HOME}/task_a_${yyyymmdd}" },
                    "check": { "command": "/bin/test -e ${HOME}/task_a_${yyyymmdd}" },

                    "provides": [ "task_a" ],

                    "calendar_name": "std",
                    "times": [ "09:00:00", "12:00:00"],
                    "timezone": "America/New_York",

                    "valid_from": "2022-01-01T09:00:00",
                    "valid_to": "2022-01-08T09:00:00"
                },
                "task_b": {
                    "up": { "command": "/usr//bin/touch ${HOME}/task_b_${yyyymmdd}" },
                    "down": { "command": "/bin/rm ${HOME}/task_b_${yyyymmdd}" },
                    "check": { "command": "/bin/test -e ${HOME}/task_b_${yyyymmdd}" },

                    "provides": [ "task_b" ],
                    "requires": [ { "resource": "task_a", "offset": 0 } ],

                    "calendar_name": "std",
                    "times": [ "17:00:00" ],
                    "timezone": "America/New_York",

                    "valid_from": "2022-01-04T09:00:00",
                    "valid_to": "2022-01-07T00:00:00"
                }
            }
        }"#;

        // Some Deserializer.
        let world_def: WorldDefinition = serde_json::from_str(json_runner).unwrap();

        let tasks = world_def.taskset().unwrap();

        // Executor
        let (tx, rx) = mpsc::unbounded_channel();
        local_executor::start(10, rx);

        let mut runner = Runner::new(
            tasks,
            world_def.variables,
            tx.clone(),
            world_def.output_options,
        )
        .await
        .unwrap();

        let (wtx, wrx) = oneshot::channel();
        runner.run(wrx).await;

        tx.send(ExecutorMessage::Stop {}).unwrap();

        assert_eq!(1, 1);
    }
}
