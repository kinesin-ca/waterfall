use super::*;
use std::ops::{Deref, DerefMut};

pub enum ActionState {
    Queued,
    Running,
    Errored,
    Completed,
}

pub struct Action {
    task: String,
    interval: Interval,
    state: ActionState,
}

pub struct TaskSet(HashMap<String, Task>);

impl TaskSet {
    pub fn new() -> Self {
        TaskSet(HashMap::new())
    }

    pub fn coverage(&self) -> Result<ResourceInterval> {
        self.get_state(MAX_TIME)
    }

    pub fn get_state<T: TimeZone>(&self, time: DateTime<T>) -> Result<ResourceInterval> {
        let mut res = ResourceInterval::new();

        let timeline = IntervalSet::from(Interval::new(MIN_TIME, time.with_timezone(&Utc)));

        // Insert all of the covered items
        for task in self.values() {
            let task_timeline = task.valid_over.intersection(&timeline);
            for resource in &task.provides {
                let ris = res.entry(resource.clone()).or_insert(IntervalSet::new());
                let already_provided = ris.intersection(&task_timeline);
                if !already_provided.is_empty() {
                    return Err(anyhow!(
                        "Task set invalid: multiple tasks provide resource {} on the intervals {:?}",
                        resource,
                        already_provided
                    ));
                }
                ris.merge(&task_timeline);
            }
        }

        Ok(res)
    }

    pub fn get_actions(&self, required: &ResourceInterval) -> Result<Vec<Action>> {
        let mut actions = Vec::new();
        for (name, task) in self.iter() {
            let new_actions: Vec<Action> = task
                .generate_intervals(required)?
                .into_iter()
                .map(|interval| Action {
                    task: name.clone(),
                    interval,
                    state: ActionState::Queued,
                })
                .collect();
            actions.extend(new_actions);
        }
        Ok(actions)
    }
}

impl Deref for TaskSet {
    type Target = HashMap<String, Task>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TaskSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
