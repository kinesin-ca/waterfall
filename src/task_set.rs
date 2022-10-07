use super::*;
use std::convert::From;
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug)]
pub struct TaskSet(Vec<Task>);

impl TaskSet {
    pub fn new() -> Self {
        TaskSet(Vec::new())
    }

    pub fn coverage(&self) -> ResourceInterval {
        self.get_state(MAX_TIME)
    }

    pub fn validate(&self) -> Result<()> {
        let state = self.coverage();

        // Ensures that all requirements are met
        for task in &self.0 {
            for resource in task.requires_resources() {
                if !state.contains_key(&resource) {
                    return Err(anyhow!(
                        "Task {} requires resource {}, which isn't produced.",
                        task.name,
                        resource
                    ));
                }
            }
        }

        // TODO Ensure that all resources will be produced over the valid_over interval

        // validate that no task generates the same resource on overlapping times
        let providers: HashMap<Resource, Vec<usize>> =
            self.0
                .iter()
                .enumerate()
                .fold(HashMap::new(), |mut acc, (idx, t)| {
                    for res in &t.provides {
                        acc.entry(res.clone()).or_insert(Vec::new()).push(idx)
                    }
                    acc
                });
        for (res, tids) in providers {
            let mut is = IntervalSet::new();
            for tid in tids {
                let already_provided = is.intersection(&self.0[tid].valid_over);
                if !already_provided.is_empty() {
                    return Err(anyhow!(
                        "Task set invalid: multiple tasks provide resource {} on the intervals {:?}",
                        res,
                        already_provided
                    ));
                }
                is.merge(&self.0[tid].valid_over);
            }
        }

        Ok(())
    }

    pub fn get_state<T: TimeZone>(&self, time: DateTime<T>) -> ResourceInterval {
        let mut res = ResourceInterval::new();

        // Insert all of the covered items
        for task in &self.0 {
            // Need to align each of these intervals with a scheduled time
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
                res.entry(resource.clone())
                    .or_insert(IntervalSet::new())
                    .merge(&task_timeline);
            }
        }

        res
    }
}

impl Deref for TaskSet {
    type Target = Vec<Task>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TaskSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<Task>> for TaskSet {
    fn from(data: Vec<Task>) -> Self {
        Self(data)
    }
}
