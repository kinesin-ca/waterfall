use super::*;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::StreamExt;
use std::cmp::Ordering;
use std::collections::VecDeque;

/*
    Runner is responsible for taking a TaskSet and a varmap and
    iteratively taking steps to converge the current state to
    be the target state.

    The runner will continue to execute until:
        - A Stop message is sent
        - current = TaskSet::coverage (the theoretical)
*/
#[derive(Debug, Clone, Copy, PartialEq, Serialize, PartialOrd)]
pub enum ActionState {
    Queued,
    Running,
    Errored,
    Completed,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Action {
    task: usize,
    pub interval: Interval,
    pub state: ActionState,
    // kill: Option<oneshot::Receiver<()>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunnerState {
    coverage: ResourceInterval,
    current: ResourceInterval,
}

// Eventually we want to coerce the data into this format for timelines-chart
// Resource (group) -> Task (label) -> data [ { "timeRange": [date,date], "val": state } ]
pub type ResourceStateDetails = HashMap<Resource, HashMap<String, Vec<Action>>>;

#[derive(Debug)]
pub enum RunnerMessage {
    Tick,
    PollMessages,
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
    GetState {
        response: oneshot::Sender<RunnerState>,
    },
    GetResourceStateDetails {
        interval: Interval,
        response: oneshot::Sender<ResourceStateDetails>,
        max_intervals: Option<usize>,
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

// Coalesces adjascent actions
fn coalesce_actions(mut actions: Vec<Action>) -> Vec<Action> {
    if actions.is_empty() {
        return actions;
    }

    actions.sort_unstable_by(|a, b| {
        let ord = a.task.partial_cmp(&b.task).unwrap();
        if ord == Ordering::Equal {
            a.state.partial_cmp(&b.state).unwrap()
        } else {
            ord
        }
    });

    let mut res: Vec<Action> = Vec::new();
    for group in actions.chunk_by(|a, b| a.task == b.task && a.state == b.state) {
        let intervals: Vec<Interval> = group.iter().map(|x| x.interval).collect();
        let is = IntervalSet::from(intervals);
        let task = group.first().unwrap().task;
        let state = group.first().unwrap().state;

        for interval in is.iter() {
            res.push(Action {
                task,
                state,
                interval: *interval,
            })
        }
    }

    res
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
        // let target = current.clone();
        let target = ResourceInterval::new();

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

        runner.update_target();

        Ok(runner)
    }

    // Generate a new target state and generate any required actions
    pub fn update_target(&mut self) {
        let new_target = self
            .tasks
            .get_state(Utc::now() + Duration::try_days(1).unwrap());
        let new_required = new_target.difference(&self.target);
        let mut new_actions =
            self.tasks
                .iter()
                .enumerate()
                .fold(Vec::new(), |mut acc, (idx, task)| {
                    let get_state = |intv: Interval| {
                        if task.provides.iter().all(|res| {
                            self.current.contains_key(res) && self.current[res].has_subset(intv)
                        }) {
                            ActionState::Completed
                        } else {
                            ActionState::Queued
                        }
                    };
                    let res: Vec<Action> = task
                        .generate_intervals(&new_required)
                        .unwrap()
                        .into_iter()
                        .map({
                            |interval| Action {
                                task: idx,
                                interval,
                                state: get_state(interval),
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

    fn tick(&mut self) {
        debug!("Tick");
        // Enqueue new messages
        while let Ok(msg) = self.messages.try_recv() {
            self.events
                .push(delayed_event(Duration::try_seconds(0).unwrap(), msg));
        }
        /*
        match self.actions.last() {
            Some(action) => {
                if action.interval.end <= Utc::now() {
                    self.tick()
                }
            }
            None => {}
        }
        */

        // Perform maintenance
        self.queue_actions();

        self.events.push(delayed_event(
            Duration::try_milliseconds(250).unwrap(),
            RunnerMessage::Tick,
        ));
    }

    fn poll_messages(&mut self) {
        while let Ok(msg) = self.messages.try_recv() {
            self.events
                .push(delayed_event(Duration::try_seconds(0).unwrap(), msg));
        }
        self.events.push(delayed_event(
            Duration::try_milliseconds(10).unwrap(),
            RunnerMessage::PollMessages,
        ));
    }

    fn get_resource_state_details(
        &self,
        interval: Interval,
        response: oneshot::Sender<ResourceStateDetails>,
        max_intervals: Option<usize>,
    ) {
        // HashMap<Resource, HashMap<String, Vec<(DateTime<Utc>, DateTime<Utc>, ActionState)>>>;
        let mut res: ResourceStateDetails = HashMap::new();

        let all_resources: HashSet<Resource> =
            self.tasks.iter().fold(HashSet::new(), |mut acc, t| {
                acc.extend(t.provides.clone());
                acc
            });

        // Build out the hash
        for resource in all_resources {
            let mut res_ints = HashMap::new();
            for task in self.tasks.iter() {
                if task.provides.contains(&resource) {
                    res_ints.insert(task.name.clone(), Vec::new());
                }
            }
            res.insert(resource.clone(), res_ints);
        }

        let mut actions: Vec<Action> = self
            .actions
            .iter()
            .filter(|x| interval.is_contiguous(x.interval))
            .cloned()
            .collect();

        if let Some(max_intv) = max_intervals {
            if actions.len() > max_intv {
                actions = coalesce_actions(actions);
            }
        }

        info!(
            "Filtered {} actions down to {}",
            self.actions.len(),
            actions.len()
        );

        for action in actions {
            let task = &self.tasks[action.task];
            for resource in &task.provides {
                res.get_mut(resource)
                    .unwrap()
                    .get_mut(&task.name)
                    .unwrap()
                    .push(action);
            }
        }

        response.send(res).unwrap();
    }

    pub async fn run(&mut self, mut stay_up: bool) {
        self.tick();
        self.poll_messages();

        // Loop until the current state matches the end state
        while stay_up || !self.is_done() {
            match self.events.next().await {
                Some(Ok(RunnerMessage::GetState { response })) => {
                    response
                        .send(RunnerState {
                            current: self.current.clone(),
                            coverage: self.end_state.clone(),
                        })
                        .unwrap_or(());
                }
                Some(Ok(RunnerMessage::PollMessages)) => {
                    self.poll_messages();
                }
                Some(Ok(RunnerMessage::Tick)) => {
                    self.tick();
                }
                Some(Ok(RunnerMessage::GetResourceStateDetails {
                    interval,
                    response,
                    max_intervals,
                })) => {
                    self.get_resource_state_details(interval, response, max_intervals);
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
                    stay_up = false;
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
        let action = &mut self.actions[action_id];
        if succeeded {
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
            action.state = ActionState::Errored;
            self.events.push(delayed_event(
                Duration::try_seconds(30).unwrap(),
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
            let (_kill_tx, kill) = oneshot::channel();
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

        let (_runner_tx, runner_rx) = mpsc::unbounded_channel();
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

        runner.run(false).await;

        tx.send(ExecutorMessage::Stop {}).unwrap();
        executor.await.unwrap();

        storage_tx.send(StorageMessage::Stop {}).unwrap();
        storage.await.unwrap();

        assert_eq!(1, 1);
    }
}
