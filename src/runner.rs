use super::*;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::StreamExt;
use std::collections::VecDeque;

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
    task: usize,
    interval: Interval,
    state: ActionState,
    // kill: Option<oneshot::Receiver<()>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RunnerMessage {
    Tick,
    ActionCompleted {
        action_id: usize,
        succeeded: bool,
    },
    RetryAction {
        action_id: usize,
    },
    /// Marks all resources in the set available over the interval
    ForceUp {
        resources: HashSet<String>,
        interval: Interval,
    },
    /// Marks all resources in the set as down over _at least_ the interval.
    /// Will cause a re-check / re-gen
    ForceDown {
        resources: HashSet<String>,
        interval: Interval,
    },
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

    actions: Vec<Action>,
    qidx: usize,

    events: FuturesUnordered<tokio::task::JoinHandle<RunnerMessage>>,

    last_horizon: DateTime<Utc>,
    messages: mpsc::UnboundedReceiver<RunnerMessage>,
    executor: mpsc::UnboundedSender<ExecutorMessage>,
    storage: mpsc::UnboundedSender<StorageMessage>,
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
    task_name: String,
    interval: Interval,
    details: serde_json::Value,
    executor: mpsc::UnboundedSender<ExecutorMessage>,
    storage: mpsc::UnboundedSender<StorageMessage>,
    kill: oneshot::Receiver<()>,
    output_options: &TaskOutputOptions,
    varmap: &VarMap,
) -> bool {
    info!("Running {}/{}", task_name, interval);
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
    let attempt = response_rx.await.unwrap();
    let rc = attempt.succeeded;
    storage
        .send(StorageMessage::StoreAttempt {
            task_name,
            interval,
            attempt: attempt.clone(),
        })
        .unwrap();
    rc
}

async fn up_task(
    action_id: usize,
    task_name: String,
    interval: Interval,
    _kill: oneshot::Receiver<()>,
    varmap: VarMap,
    up: TaskDetails,
    check: Option<TaskDetails>,
    output_options: TaskOutputOptions,
    executor: mpsc::UnboundedSender<ExecutorMessage>,
    storage: mpsc::UnboundedSender<StorageMessage>,
) -> RunnerMessage {
    if let Some(check_cmd) = check.clone() {
        let (_subkill, subkill_rx) = oneshot::channel();
        let succeeded = run_task(
            task_name.clone(),
            interval,
            check_cmd.clone(),
            executor.clone(),
            storage.clone(),
            subkill_rx,
            &output_options,
            &varmap,
        )
        .await;

        // If check succeeded, resources are up
        if succeeded {
            return RunnerMessage::ActionCompleted {
                action_id,
                succeeded: true,
            };
        }
    }

    // UP
    let (_subkill, subkill_rx) = oneshot::channel();
    let succeeded = run_task(
        task_name.clone(),
        interval,
        up,
        executor.clone(),
        storage.clone(),
        subkill_rx,
        &output_options,
        &varmap,
    )
    .await;
    if !succeeded {
        return RunnerMessage::ActionCompleted {
            action_id,
            succeeded: false,
        };
    }

    // recheck
    if let Some(check_cmd) = check {
        let (_subkill, subkill_rx) = oneshot::channel();
        let succeeded = run_task(
            task_name.clone(),
            interval,
            check_cmd.clone(),
            executor.clone(),
            storage.clone(),
            subkill_rx,
            &output_options,
            &varmap,
        )
        .await;

        // If check succeeded, resources are up
        if succeeded {
            return RunnerMessage::ActionCompleted {
                action_id,
                succeeded: true,
            };
        } else {
            return RunnerMessage::ActionCompleted {
                action_id,
                succeeded: false,
            };
        }
    } else {
        return RunnerMessage::ActionCompleted {
            action_id,
            succeeded: true,
        };
    }
}

fn delayed_event(delay: Duration, event: RunnerMessage) -> tokio::task::JoinHandle<RunnerMessage> {
    tokio::spawn(async move {
        tokio::time::sleep(delay.to_std().unwrap()).await;
        event
    })
}

impl Runner {
    pub async fn new(
        tasks: TaskSet,
        vars: VarMap,
        messages: mpsc::UnboundedReceiver<RunnerMessage>,
        executor: mpsc::UnboundedSender<ExecutorMessage>,
        storage: mpsc::UnboundedSender<StorageMessage>,
        output_options: TaskOutputOptions,
        force_check: bool,
    ) -> Result<Self> {
        tasks.validate()?;

        // Validate the task commands can run on the executor
        for tdef in tasks.iter() {
            validate_cmd(executor.clone(), tdef.up.clone()).await?;
            if let Some(cmd) = &tdef.down {
                validate_cmd(executor.clone(), cmd.clone()).await?;
            }
            if let Some(cmd) = &tdef.check {
                validate_cmd(executor.clone(), cmd.clone()).await?;
            }
        }

        // Load last-known state
        let current = if force_check {
            info!("Force re-check set, starting with empty current state.");
            ResourceInterval::new()
        } else {
            info!("Pulling last state from storage");
            let (response, rx) = oneshot::channel();
            storage
                .send(StorageMessage::LoadState { response })
                .unwrap();
            let res = rx.await.unwrap();
            res
        };
        let target = current.clone();

        let end_state = tasks.coverage();
        let mut runner = Runner {
            tasks,
            vars,
            output_options,
            end_state,
            target,
            current,
            actions: Vec::new(),
            qidx: 0,
            events: FuturesUnordered::new(),
            last_horizon: DateTime::<Utc>::MIN_UTC,
            messages,
            executor,
            storage,
        };

        runner.tick();
        runner.queue_actions();

        Ok(runner)
    }

    // Generate a new target state and generate any required actions
    pub fn tick(&mut self) {
        let new_target = self.tasks.get_state(Utc::now() + Duration::days(1));
        let new_required = new_target.difference(&self.target);
        let mut new_actions =
            self.tasks
                .iter()
                .enumerate()
                .fold(Vec::new(), |mut acc, (idx, task)| {
                    let res: Vec<Action> = task
                        .generate_intervals(&new_required)
                        .unwrap()
                        .into_iter()
                        .map({
                            |interval| Action {
                                task: idx,
                                interval,
                                state: ActionState::Queued,
                            }
                        })
                        .collect();
                    acc.extend(res);
                    acc
                });
        new_actions.sort_unstable_by(|a, b| a.interval.end.partial_cmp(&b.interval.end).unwrap());

        info!("Tick: Generated {} new actions", new_actions.len());
        self.actions.extend(new_actions);
    }

    pub async fn run(&mut self) {
        self.events
            .push(delayed_event(Duration::seconds(1), RunnerMessage::Tick));

        // Need to incorporate the ability to receive messages
        //
        // Loop until the current state matches the end state
        while !self.is_done() {
            match self.events.next().await {
                Some(Ok(RunnerMessage::Tick)) => {
                    debug!("Tick");
                    // Enqueue new messages
                    while let Ok(msg) = self.messages.try_recv() {
                        self.events.push(delayed_event(Duration::seconds(0), msg));
                    }
                    match self.actions.last() {
                        Some(action) => {
                            if action.interval.end <= Utc::now() {
                                self.tick()
                            }
                        }
                        None => self.tick(),
                    }

                    // Perform maintenance
                    self.queue_actions();

                    self.events
                        .push(delayed_event(Duration::seconds(5), RunnerMessage::Tick));
                }
                Some(Ok(RunnerMessage::ForceUp {
                    resources,
                    interval,
                })) => {
                    for (tid, task) in self.tasks.iter().enumerate() {
                        if task.provides.is_subset(&resources) {
                            let aligned_is =
                                IntervalSet::from(task.schedule.align_interval(interval));
                            for resource in &task.provides {
                                self.current.get_mut(resource).unwrap().merge(&aligned_is);
                            }
                            for action in &mut self.actions {
                                if action.task == tid && aligned_is.has_subset(action.interval) {
                                    action.state = ActionState::Completed;
                                }
                            }
                        }
                    }
                    self.store_state();
                }
                Some(Ok(RunnerMessage::ForceDown {
                    resources,
                    interval,
                })) => {
                    // Use the interval to identify
                    for (tid, task) in self.tasks.iter().enumerate() {
                        if task.provides.is_subset(&resources) {
                            let aligned_is =
                                IntervalSet::from(task.schedule.align_interval(interval));
                            for resource in &task.provides {
                                self.current
                                    .get_mut(resource)
                                    .unwrap()
                                    .subtract(&aligned_is);
                            }
                            for action in &mut self.actions {
                                if action.task == tid && aligned_is.has_subset(action.interval) {
                                    action.state = ActionState::Queued;
                                }
                            }
                        }
                    }
                    self.store_state();
                }
                Some(Ok(RunnerMessage::Stop)) => {
                    info!("Stopping");
                    break;
                }
                Some(Ok(RunnerMessage::RetryAction { action_id })) => {
                    info!("Retrying action {}", action_id);
                    let action = &mut self.actions[action_id];
                    action.state = ActionState::Queued;
                }
                Some(Ok(RunnerMessage::ActionCompleted {
                    action_id,
                    succeeded,
                })) => {
                    self.complete_task(action_id, succeeded);
                }
                Some(Err(e)) => {
                    panic!("Something went wrong: {:?}", e)
                }
                None => {}
            }
            // Log stuff
        }
    }

    fn complete_task(&mut self, action_id: usize, succeeded: bool) {
        info!("Completing action {}", action_id);
        if succeeded {
            let action = &mut self.actions[action_id];
            let task = self.tasks.get(action.task).unwrap();
            action.state = ActionState::Completed;
            for res in &task.provides {
                self.current
                    .entry(res.clone())
                    .or_insert(IntervalSet::new())
                    .insert(action.interval);
            }
            self.store_state();
            self.queue_actions();
        } else {
            self.events.push(delayed_event(
                Duration::seconds(30),
                RunnerMessage::RetryAction { action_id },
            ));
        }
    }

    fn store_state(&self) {
        self.storage
            .send(StorageMessage::StoreState {
                state: self.current.clone(),
            })
            .unwrap();
    }

    fn queue_actions(&mut self) {
        let now = Utc::now();

        // Submit any elligible jobs
        for (action_id, action) in self
            .actions
            .iter_mut()
            .enumerate()
            .filter(|(_, x)| x.state == ActionState::Queued && x.interval.end <= now)
        {
            let task = self.tasks.get(action.task).unwrap();
            if !task.can_run(action.interval, &self.current) {
                continue;
            }
            let (kill_tx, kill) = oneshot::channel();
            let varmap: VarMap = VarMap::from_interval(&action.interval, task.timezone)
                .iter()
                .chain(self.vars.iter())
                .collect();
            let task_name = task.name.clone();
            let interval = action.interval;
            let up = task.up.clone();
            let check = task.check.clone();
            let output_options = self.output_options.clone();
            let exe = self.executor.clone();
            let storage = self.storage.clone();
            self.events.push(tokio::spawn(async move {
                up_task(
                    action_id,
                    task_name.clone(),
                    interval,
                    kill,
                    varmap,
                    up,
                    check,
                    output_options,
                    exe,
                    storage,
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
        let executor = local_executor::start(10, rx);

        // Storage
        let (storage_tx, storage_rx) = mpsc::unbounded_channel();
        let storage = storage::redis::start(
            storage_rx,
            "redis://localhost".to_owned(),
            "world_test".to_owned(),
        );

        let (runner_tx, runner_rx) = mpsc::unbounded_channel();
        let mut runner = Runner::new(
            tasks,
            world_def.variables,
            runner_rx,
            tx.clone(),
            storage_tx.clone(),
            world_def.output_options,
            true,
        )
        .await
        .unwrap();

        runner.run().await;

        tx.send(ExecutorMessage::Stop {}).unwrap();
        executor.await.unwrap();

        storage_tx.send(StorageMessage::Stop {}).unwrap();
        storage.await.unwrap();

        assert_eq!(1, 1);
    }
}
