use super::*;
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TaskResources(HashMap<String, i64>);

impl Deref for TaskResources {
    type Target = HashMap<String, i64>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TaskResources {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TaskResources {
    #[must_use]
    pub fn new() -> Self {
        TaskResources(HashMap::new())
    }

    #[must_use]
    pub fn can_satisfy(&self, requirements: &TaskResources) -> bool {
        requirements
            .iter()
            .all(|(k, v)| self.contains_key(k) && self[k] >= *v)
    }

    /// Subtracts resources from available resources.
    /// # Errors
    /// Returns an `Err` if the requested resources cannot be fulfilled
    /// # Panics
    /// It doesn't, keys are checked for ahead-of-time
    pub fn sub(&mut self, resources: &TaskResources) -> Result<()> {
        if self.can_satisfy(resources) {
            for (k, v) in resources.iter() {
                *self.get_mut(k).unwrap() -= v;
            }
            Ok(())
        } else {
            Err(anyhow!("Cannot satisfy requested resources"))
        }
    }

    /// # Panics
    /// It doesn't, keys are checked for ahead-of-time
    pub fn add(&mut self, resources: &TaskResources) {
        for (k, v) in resources.iter() {
            if self.contains_key(k) {
                *self.get_mut(k).unwrap() += *v;
            } else {
                self.insert(k.clone(), *v);
            }
        }
    }
}

/// Defines the struct to parse for tasks
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(deny_unknown_fields)]
pub struct TaskDefinition {
    /// Command to run to generate the resources for the given interval
    pub up: TaskDetails,

    /// Command to run to remove the resource for the given interval
    /// If None, no additional action will happen when an interval goes stale
    #[serde(default)]
    pub down: Option<TaskDetails>,

    /// Command to run to verify the resources exist and are correct.
    /// Run before `up` to see if needed, and after `up` to verify output
    /// If None, no check is run to see if up needs to run, and no post-up check occurs
    /// to verify up succeeded
    #[serde(default)]
    pub check: Option<TaskDetails>,

    /// Number of seconds
    #[serde(default)]
    pub alert_delay_seconds: Option<i64>,

    #[serde(default)]
    pub provides: HashSet<String>,

    #[serde(default)]
    pub requires: Vec<Requirement>,

    pub calendar_name: String,
    pub times: Vec<NaiveTime>,
    pub timezone: Tz,

    pub valid_from: NaiveDateTime,

    #[serde(default)]
    pub valid_to: Option<NaiveDateTime>,
}

impl TaskDefinition {
    pub fn to_task(&self, name: &str, calendar: &Calendar) -> Task {
        let schedule = Schedule::new(calendar.clone(), self.times.clone(), self.timezone);
        /*
            The valid_{from,to} interval must be aligned to the actual schedule.
            They will be adjusted to include any interval who's
        */
        let start = schedule
            .interval(
                self.timezone.from_local_datetime(&self.valid_from).unwrap(),
                0,
            )
            .start;

        let provides = if self.provides.is_empty() {
            HashSet::from([name.to_owned()])
        } else {
            self.provides.clone()
        };

        let end = match self.valid_to {
            Some(nt) => self.timezone.from_local_datetime(&nt).unwrap(),
            None => MAX_TIME.with_timezone(&self.timezone),
        };

        let actual_end = schedule.interval(end, 0).start;

        Task {
            name: name.to_owned(),
            up: self.up.clone(),
            down: self.down.clone(),
            check: self.check.clone(),

            provides,
            requires: self.requires.clone(),

            schedule: schedule,
            valid_over: IntervalSet::from(Interval::new(start, actual_end)),
            timezone: self.timezone,
        }
    }
}

/*
   No need for serialize / deserialize here, since we don't
   need to transmit it anywhere. It is reconstituted by the
   definition
*/
#[derive(Clone, Serialize, Debug)]
pub struct Task {
    pub name: String,
    pub up: TaskDetails,
    pub down: Option<TaskDetails>,
    pub check: Option<TaskDetails>,

    pub provides: HashSet<Resource>,
    pub requires: Vec<Requirement>,

    pub schedule: Schedule,
    pub valid_over: IntervalSet,
    pub timezone: Tz,
}

// Really need to rethink this valid_over and scheduling times. When generating

impl Task {
    pub fn generate_intervals(&self, required: &ResourceInterval) -> Result<Vec<Interval>> {
        // Ensure that all intervals that are required are provided by this instance
        let reqs: Vec<IntervalSet> = self
            .provides
            .iter()
            .map(|res| {
                if let Some(is) = required.get(res) {
                    is.intersection(&self.valid_over)
                } else {
                    IntervalSet::new()
                }
            })
            .collect();

        let res = if reqs.is_empty() {
            Ok(Vec::new())
        } else {
            let ris = &reqs[0];
            // Ensure that all intervals are the same
            if !reqs[1..].iter().all(|is| is == ris) {
                Err(anyhow!(
                    "Task produces multiple resources, but intervals are not consistent across needs"
                ))
            } else {
                Ok(ris.iter().fold(Vec::new(), |mut acc, intv| {
                    let mut new_intervals = self.schedule.generate(Interval::new(
                        std::cmp::max(intv.start, self.valid_over.start().unwrap()),
                        std::cmp::min(intv.end, self.valid_over.end().unwrap()),
                    ));
                    acc.append(&mut new_intervals);
                    acc
                }))
            }
        };
        res
    }

    pub fn validity(&self, max_time: DateTime<Utc>) -> IntervalSet {
        if self.valid_over.is_empty() {
            IntervalSet::new()
        } else {
            let timeline =
                IntervalSet::from(vec![Interval::new(self.valid_over[0].start, max_time)]);
            self.valid_over.intersection(&timeline)
        }
    }

    /// Returns true if this task can provide any resource that isn't currently available
    /// as of the specified time
    pub fn is_needed(&self, time: &DateTime<Tz>, available: &ResourceInterval) -> bool {
        let end_dt = time.with_timezone(&Utc);
        let horizon_is = self
            .valid_over
            .difference(&IntervalSet::from(vec![Interval::new(end_dt, MAX_TIME)]));
        self.provides.iter().all(|res| {
            if let Some(is) = available.get(res) {
                !(horizon_is.difference(is)).is_empty()
            } else {
                false
            }
        })
    }

    /// Returns true if all requirements are satisfied
    pub fn can_run(&self, interval: Interval, available: &ResourceInterval) -> bool {
        self.requires
            .iter()
            .all(|req| req.is_satisfied(interval, &self.schedule, available))
    }

    pub fn can_be_satisfied(&self, interval: Interval, available: &ResourceInterval) -> bool {
        self.requires
            .iter()
            .all(|req| req.can_be_satisfied(interval, &self.schedule, available))
    }

    pub fn requires_resources(&self) -> HashSet<Resource> {
        self.requires.iter().fold(HashSet::new(), |mut acc, req| {
            acc.extend(req.resources());
            acc
        })
    }

    pub fn up(&self, interval: &Interval) -> Result<HashSet<String>> {
        if self.check(interval) {
            Ok(self.provides.clone())
        } else {
            Ok(HashSet::new())
        }
    }

    pub fn check(&self, _interval: &Interval) -> bool {
        true
    }

    pub fn down(&self, _interval: &Interval) -> Result<HashSet<String>> {
        Ok(HashSet::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono_tz::America::{Halifax, New_York};

    macro_rules! intv {
        ( $x:literal, $y:literal ) => {
            Interval::new(
                Utc.ymd(2022, 1, $x).and_hms(0, 0, 0),
                Utc.ymd(2022, 1, $y).and_hms(0, 0, 0),
            )
        };
    }

    macro_rules! ri {
        (
            $(
                (
                    $r:literal,
                    $(($x:literal, $y:literal)),*
                )
            ),*
        )
         => {
            ResourceInterval::from(HashMap::from([$((
                $r.to_owned(),
                IntervalSet::from(vec![$(intv!($x, $y)),*]),
            )),*]))
        };
    }

    #[test]
    fn check_task_can_parse() {
        // Spans a weekend
        let task_json = r#"
        {
            "up": "/usr/bin/touch /tmp/a_${yyyymmdd}_${hhmmss}",
            "down": "/usr/bin/rm /tmp/a_${yyyymmdd}_${hhmmss}",
            "check": "/usr/bin/test -e /tmp/a_${yyyymmdd}_${hhmmss}",
            "provides": [
                "resource_a",
                "resource_b"
            ],
            "requires": [
                { "resource": "alpha", "offset": 0 },
                { "resource": "beta", "offset": -1 }
            ],
            "calendar_name": "std",
            "times": [ "09:00:00", "13:00:00", "15:00:00" ],
            "timezone": "America/Halifax",
            "valid_from": "2022-01-05T12:30:00",
            "valid_to": "2022-01-11T00:00:00"
        }
        "#;

        let task_def: TaskDefinition = serde_json::from_str(task_json).unwrap();

        // Produces a std
        let cal = Calendar::new();

        let task = task_def.to_task("test", &cal);

        // Assert the valid interval is correct
        assert_eq!(
            task.valid_over,
            IntervalSet::from(vec![Interval::new(
                Halifax.ymd(2022, 1, 5).and_hms(9, 0, 0),
                Halifax.ymd(2022, 1, 10).and_hms(15, 0, 0)
            )])
        );

        // No times when out of validity
        let times = task
            .generate_intervals(&ri!(("resource_a", (13, 20)), ("resource_b", (13, 20))))
            .unwrap();
        assert!(times.is_empty());

        // Requiring within a valid time range generates times
        let times = task
            .generate_intervals(&ri!(("resource_a", (6, 8)), ("resource_b", (6, 8))))
            .unwrap();
        assert_eq!(times.len(), 6);

        // Raise error if unequal requirements
        let res = task.generate_intervals(&ri!(("resource_a", (6, 7)), ("resource_b", (6, 8))));
        assert!(res.is_err());

        // Require that all times generated be within the
        // valid_over
        let res = task.generate_intervals(&ri!(("resource_a", (1, 30)), ("resource_b", (1, 30))));
        match res {
            Ok(intervals) => {
                assert!(intervals
                    .iter()
                    .all(|interval| task.valid_over.has_subset(*interval)));
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        };

        // Require that all times generated be within the
        // valid_over
        let mut exact = ResourceInterval::new();
        exact.insert(&"resource_a".to_owned(), &task.valid_over.clone());
        exact.insert(&"resource_b".to_owned(), &task.valid_over.clone());
        let res = IntervalSet::from(task.generate_intervals(&exact).unwrap());
        assert_eq!(res, task.valid_over);

        // Ensure that the intervals generated over the valid period
        // exactly cover the valid period
        let mut theoretical = ResourceInterval::new();
        theoretical.insert(&"resource_a".to_owned(), &task.valid_over);
        theoretical.insert(&"resource_b".to_owned(), &task.valid_over);
        let generated = IntervalSet::from(task.generate_intervals(&theoretical).unwrap());
        assert_eq!(task.valid_over, generated);
    }

    #[test]
    fn check_task_valid_over() {
        let task_json = r#"
        {
            "up": "/usr/bin/touch /tmp/a_${yyyymmdd}_${hhmmss}",
            "down": "/usr/bin/rm /tmp/a_${yyyymmdd}_${hhmmss}",
            "check": "/usr/bin/test -e /tmp/a_${yyyymmdd}_${hhmmss}",
            "provides": [
                "resource_a",
                "resource_b"
            ],
            "requires": [
                { "resource": "alpha", "offset": 0 },
                { "resource": "beta", "offset": -1 }
            ],
            "calendar_name": "std",
            "times": [ "17:00:00" ],
            "timezone": "America/New_York",
            "valid_from": "2022-01-04T09:00:00",
            "valid_to": "2022-01-07T00:00:00"
        }
        "#;

        let cal = Calendar::new();
        {
            let task_def: TaskDefinition = serde_json::from_str(task_json).unwrap();
            let task = task_def.to_task("task", &cal);

            // Assert the valid interval is correct
            assert_eq!(
                task.valid_over,
                IntervalSet::from(vec![Interval::new(
                    New_York.ymd(2022, 1, 3).and_hms(17, 0, 0),
                    New_York.ymd(2022, 1, 6).and_hms(17, 0, 0)
                )])
            );
        }

        // Another test with different times
        {
            let mut task_def: TaskDefinition = serde_json::from_str(task_json).unwrap();

            task_def.times = vec![NaiveTime::from_hms(9, 0, 0), NaiveTime::from_hms(12, 0, 0)];
            task_def.valid_from = NaiveDate::from_ymd(2022, 1, 1).and_hms(9, 0, 0);
            task_def.valid_to = Some(NaiveDate::from_ymd(2022, 1, 7).and_hms(17, 0, 0));

            let task = task_def.to_task("task", &cal);

            // Assert the valid interval is correct
            assert_eq!(
                task.valid_over,
                IntervalSet::from(vec![Interval::new(
                    New_York.ymd(2021, 12, 31).and_hms(12, 0, 0),
                    New_York.ymd(2022, 1, 7).and_hms(12, 0, 0)
                )])
            );
        }
    }
}
