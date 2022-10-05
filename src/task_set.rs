use super::*;
use std::convert::From;
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug)]
pub struct TaskSet(HashMap<String, Task>);

impl TaskSet {
    pub fn new() -> Self {
        TaskSet(HashMap::new())
    }

    pub fn coverage(&self) -> Result<ResourceInterval> {
        self.get_state(MAX_TIME)
    }

    pub fn validate(&self) -> Result<()> {
        self.get_state(MAX_TIME)?;
        Ok(())
    }

    pub fn get_state<T: TimeZone>(&self, time: DateTime<T>) -> Result<ResourceInterval> {
        let mut res = ResourceInterval::new();

        // Insert all of the covered items
        for task in self.values() {
            // TODO Need to align each of these intervals with a scheduled time
            let timeline = if time < MAX_TIME {
                let cur_intv = task.schedule.interval(time.clone(), 0);
                if cur_intv.end > time {
                    IntervalSet::from(Interval::new(MIN_TIME, cur_intv.start))
                } else {
                    IntervalSet::from(Interval::new(MIN_TIME, cur_intv.end))
                }
            } else {
                IntervalSet::from(Interval::new(MIN_TIME, time.with_timezone(&Utc)))
            };
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

impl From<HashMap<String, Task>> for TaskSet {
    fn from(data: HashMap<String, Task>) -> Self {
        Self(data)
    }
}
