use super::*;

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
    pub fn to_task(&self, calendar: &Calendar) -> Task {
        let schedule = Schedule::new(calendar.clone(), self.times.clone(), self.timezone);
        /*
            The valid_{from,to} interval must be aligned to the actual schedule
        */
        let start = schedule
            .interval(
                self.timezone.from_local_datetime(&self.valid_from).unwrap(),
                0,
            )
            .start;

        let end = match self.valid_to {
            Some(nt) => self.timezone.from_local_datetime(&nt).unwrap(),
            None => DateTime::<Utc>::MAX_UTC.with_timezone(&self.timezone),
        };

        let actual_end = schedule.interval(end, 0).start;

        Task {
            up: self.up.clone(),
            down: self.down.clone(),
            check: self.check.clone(),

            provides: self.provides.clone(),
            requires: self.requires.clone(),

            schedule: schedule,
            valid_over: IntervalSet::from(vec![Interval::new(start, actual_end)]),
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
    pub fn generate_intervals(
        &self,
        required: &HashMap<Resource, IntervalSet>,
    ) -> Result<Vec<Interval>> {
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

        if reqs.is_empty() {
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
        }
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
    pub fn is_needed(&self, time: &DateTime<Tz>, available: &HashMap<String, IntervalSet>) -> bool {
        let end_dt = time.with_timezone(&Utc);
        let horizon_is = self
            .valid_over
            .difference(&IntervalSet::from(vec![Interval::new(
                end_dt,
                DateTime::<Utc>::MAX_UTC,
            )]));
        self.provides.iter().all(|res| {
            if let Some(is) = available.get(res) {
                !(horizon_is.difference(is)).is_empty()
            } else {
                false
            }
        })
    }

    /// Returns true if all requirements are satisfied
    pub fn can_run(&self, time: DateTime<Utc>, available: &HashMap<String, IntervalSet>) -> bool {
        let local_time = time.with_timezone(&self.timezone);
        self.requires
            .iter()
            .all(|req| req.is_satisfied(&local_time, &self.schedule, available))
    }

    pub fn can_be_satisfied(
        &self,
        time: DateTime<Utc>,
        available: &HashMap<String, IntervalSet>,
    ) -> bool {
        let local_time = time.with_timezone(&self.timezone);
        self.requires
            .iter()
            .all(|req| req.can_be_satisfied(&local_time, &self.schedule, available))
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
    use chrono_tz::America::Halifax;

    macro_rules! isv {
        ( $x:literal, $y:literal ) => {
            IntervalSet::from(vec![Interval::new(
                Utc.ymd(2022, 1, $x).and_hms(0, 0, 0),
                Utc.ymd(2022, 1, $y).and_hms(0, 0, 0),
            )])
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

        let task = task_def.to_task(&cal);

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
            .generate_intervals(&HashMap::from([
                ("resource_a".to_owned(), isv!(13, 20)),
                ("resource_b".to_owned(), isv!(13, 20)),
            ]))
            .unwrap();
        assert!(times.is_empty());

        // Requiring within a valid time range generates times
        let times = task
            .generate_intervals(&HashMap::from([
                ("resource_a".to_owned(), isv!(6, 8)),
                ("resource_b".to_owned(), isv!(6, 8)),
            ]))
            .unwrap();
        assert_eq!(times.len(), 6);

        // Raise error if unequal requirements
        let res = task.generate_intervals(&HashMap::from([
            ("resource_a".to_owned(), isv!(6, 7)),
            ("resource_b".to_owned(), isv!(6, 8)),
        ]));
        assert!(res.is_err());

        // Require that all times generated be within the
        // valid_over
        let res = task.generate_intervals(&HashMap::from([
            ("resource_a".to_owned(), isv!(1, 30)),
            ("resource_b".to_owned(), isv!(1, 30)),
        ]));
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
    }
}
