use super::*;
use std::collections::HashSet;

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Schedule {
    calendar: Calendar,
    times: Vec<NaiveTime>,
    timezone: Tz,
}

impl Schedule {
    pub fn new(calendar: Calendar, times: Vec<NaiveTime>, timezone: Tz) -> Self {
        let uniq: HashSet<NaiveTime> = HashSet::from_iter(times.iter().cloned());
        let mut times = Vec::from_iter(uniq.iter().cloned());
        times.sort();
        Schedule {
            calendar,
            times,
            timezone,
        }
    }

    fn is_end_time<T: TimeZone>(&self, dt: DateTime<T>) -> bool {
        // Need to get the current interval, then offset it
        let at = dt.with_timezone(&self.timezone);
        self.times.iter().any(|x| *x == at.time()) && self.calendar.includes(at.date_naive())
    }

    /// Given an interval I, return the interval J that is the smallest
    /// set of schedule intervals that completely contain I.
    /// If the given interval is bounded by MIN_TIME or MAX_TIME, then the
    /// returned interval will be likewise bounded
    pub fn align_interval(&self, interval: Interval) -> Interval {
        let st = if interval.start == MIN_TIME {
            self.next_time(interval.start).with_timezone(&Utc)
        } else {
            interval.start
        };
        let et = if interval.end == MAX_TIME {
            self.prev_time(interval.end).with_timezone(&Utc)
        } else {
            interval.end
        };

        Interval::new(self.interval(st, 0).start, self.interval(et, 0).end)
    }

    pub fn generate(&self, interval: Interval) -> Vec<Interval> {
        if self.times.is_empty() {
            return Vec::new();
        }

        let st = self.interval(interval.start, 0).start;
        let et = self.interval(interval.end, 0).end;

        //let st = interval.start.with_timezone(&self.timezone);
        //let et = interval.end.with_timezone(&self.timezone);

        let mut date = self.calendar.prev(st.date_naive());
        let end_date = self.calendar.next(et.date_naive().succ_opt().unwrap());

        let mut times = Vec::new();
        let mut prev_time = self
            .timezone
            .from_local_datetime(&date.and_time(self.times[0]))
            .unwrap()
            .with_timezone(&Utc);
        while date < end_date {
            for time in &self.times {
                let dt = self
                    .timezone
                    .from_local_datetime(&date.and_time(*time))
                    .unwrap()
                    .with_timezone(&Utc);
                if dt > interval.start && dt <= interval.end {
                    times.push(Interval::new(prev_time, dt));
                } else if interval.end < dt {
                    break;
                }
                prev_time = dt;
            }
            date = self.calendar.next(date);
        }

        times
    }

    /// Given a timestamp, return the interval that contains it
    pub fn interval<T: TimeZone>(&self, dt: DateTime<T>, offset: i32) -> Interval {
        // Need to get the current interval, then offset it
        let at = dt.with_timezone(&self.timezone);

        // If the time is at an edge
        let rt = if self.is_end_time(at) {
            at
        } else {
            self.next_time(at)
        };

        let end = self.offset(rt, offset);
        Interval::new(
            self.prev_time(end).with_timezone(&Utc),
            end.with_timezone(&Utc),
        )
    }

    pub fn next_time<T: TimeZone>(&self, dt: DateTime<T>) -> DateTime<Tz> {
        let st = dt.with_timezone(&self.timezone);

        let mut date = st.date_naive();
        let mut time = st.time();

        // Handle case where we're not on a valid date
        if !self.calendar.includes(date) {
            date = self.calendar.next(date);
            time = self.times[0] - Duration::try_milliseconds(1).unwrap();
        }

        // Figure out the time slot
        let time = match self.times.iter().find(|x| **x > time) {
            Some(t) => date.and_time(*t),
            None => self
                .calendar
                .next(date)
                .and_time(*self.times.first().unwrap()),
        };

        // Cast into a timezone
        self.timezone.from_local_datetime(&time).unwrap()
    }

    /// Given a time, generate the preceding interval according to the schedule
    pub fn prev_time<T: TimeZone>(&self, dt: DateTime<T>) -> DateTime<Tz> {
        let st = dt.with_timezone(&self.timezone);

        let mut date = st.date_naive();
        let mut time = st.time();

        // Handle case where we're not on a valid date
        if !self.calendar.includes(date) {
            date = self.calendar.prev(date);
            time = *self.times.last().unwrap() + Duration::try_milliseconds(1).unwrap();
        }

        // Figure out the time slot
        let time = match self.times.iter().rev().find(|x| **x < time) {
            Some(t) => date.and_time(*t),
            None => self
                .calendar
                .prev(date)
                .and_time(*self.times.last().unwrap()),
        };

        // Cast into a timezone
        self.timezone.from_local_datetime(&time).unwrap()
    }

    // Given a timestamp, return the scheduled time `offset`
    // A bit dangerous, providing an offset of 0
    fn offset(&self, mut dt: DateTime<Tz>, offset: i32) -> DateTime<Tz> {
        if offset > 0 {
            for _ in 0..offset {
                dt = self.next_time(dt);
            }
        } else {
            for _ in offset..0 {
                dt = self.prev_time(dt);
            }
        }
        dt
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_simple_generation() {
        let timezone = chrono_tz::America::Halifax;
        let sched = Schedule {
            calendar: Calendar::new(),
            times: vec![
                NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(11, 30, 0).unwrap(),
            ],
            timezone,
        };

        // Simple generation
        let times = sched.generate(Interval::new(
            timezone
                .with_ymd_and_hms(2022, 1, 3, 11, 0, 0)
                .unwrap()
                .with_timezone(&Utc),
            timezone
                .with_ymd_and_hms(2022, 1, 3, 12, 0, 0)
                .unwrap()
                .with_timezone(&Utc),
        ));

        assert_eq!(times.len(), 1);
        assert_eq!(
            times,
            vec![Interval::new(
                timezone
                    .with_ymd_and_hms(2022, 1, 3, 10, 30, 0)
                    .unwrap()
                    .with_timezone(&Utc),
                timezone
                    .with_ymd_and_hms(2022, 1, 3, 11, 30, 0)
                    .unwrap()
                    .with_timezone(&Utc),
            )]
        );

        // Generating scheduled times over a timerange
        assert_eq!(
            sched.generate(Interval::new(
                timezone
                    .with_ymd_and_hms(2021, 12, 31, 0, 0, 0)
                    .unwrap()
                    .with_timezone(&Utc),
                timezone
                    .with_ymd_and_hms(2022, 1, 5, 0, 0, 0)
                    .unwrap()
                    .with_timezone(&Utc),
            )),
            vec![
                Interval::new(
                    timezone
                        .with_ymd_and_hms(2021, 12, 30, 11, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                    timezone
                        .with_ymd_and_hms(2021, 12, 31, 10, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                ),
                Interval::new(
                    timezone
                        .with_ymd_and_hms(2021, 12, 31, 10, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                    timezone
                        .with_ymd_and_hms(2021, 12, 31, 11, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                ),
                Interval::new(
                    timezone
                        .with_ymd_and_hms(2021, 12, 31, 11, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                    timezone
                        .with_ymd_and_hms(2022, 1, 3, 10, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                ),
                Interval::new(
                    timezone
                        .with_ymd_and_hms(2022, 1, 3, 10, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                    timezone
                        .with_ymd_and_hms(2022, 1, 3, 11, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                ),
                Interval::new(
                    timezone
                        .with_ymd_and_hms(2022, 1, 3, 11, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                    timezone
                        .with_ymd_and_hms(2022, 1, 4, 10, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                ),
                Interval::new(
                    timezone
                        .with_ymd_and_hms(2022, 1, 4, 10, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                    timezone
                        .with_ymd_and_hms(2022, 1, 4, 11, 30, 0)
                        .unwrap()
                        .with_timezone(&Utc),
                )
            ]
        );
    }

    #[test]
    fn check_prev() {
        let timezone = chrono_tz::America::Halifax;
        let sched = Schedule {
            calendar: Calendar::new(),
            times: vec![
                NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(11, 30, 0).unwrap(),
            ],
            timezone,
        };

        assert_eq!(
            sched.prev_time(timezone.with_ymd_and_hms(2022, 1, 3, 11, 0, 0).unwrap()),
            timezone.with_ymd_and_hms(2022, 1, 3, 10, 30, 0).unwrap()
        );
        assert_eq!(
            sched.prev_time(timezone.with_ymd_and_hms(2022, 1, 3, 11, 30, 0).unwrap()),
            timezone.with_ymd_and_hms(2022, 1, 3, 10, 30, 0).unwrap()
        );
    }

    #[test]
    fn check_offset() {
        let timezone = chrono_tz::America::Halifax;
        let sched = Schedule {
            calendar: Calendar::new(),
            times: vec![
                NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(11, 30, 0).unwrap(),
            ],
            timezone,
        };

        // Asking for no offset should yield the same time
        assert_eq!(
            sched.offset(timezone.with_ymd_and_hms(2022, 1, 3, 11, 0, 0).unwrap(), 0),
            timezone.with_ymd_and_hms(2022, 1, 3, 11, 0, 0).unwrap()
        );

        //  -1 is equivalent to prev
        let test_time = timezone.with_ymd_and_hms(2022, 1, 3, 11, 0, 0).unwrap();
        assert_eq!(sched.offset(test_time, -1), sched.prev_time(test_time));
        assert_eq!(sched.offset(test_time, 1), sched.next_time(test_time));
    }

    #[test]
    fn check_next() {
        let timezone = chrono_tz::America::Halifax;
        let sched = Schedule {
            calendar: Calendar::new(),
            times: vec![
                NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(11, 30, 0).unwrap(),
            ],
            timezone,
        };

        assert_eq!(
            sched.next_time(timezone.with_ymd_and_hms(2022, 1, 3, 11, 0, 0).unwrap()),
            timezone.with_ymd_and_hms(2022, 1, 3, 11, 30, 0).unwrap()
        );
        assert_eq!(
            sched.next_time(timezone.with_ymd_and_hms(2022, 1, 3, 11, 30, 0).unwrap()),
            timezone.with_ymd_and_hms(2022, 1, 4, 10, 30, 0).unwrap()
        );
    }

    #[test]
    fn check_transivity() {
        let timezone = chrono_tz::America::Halifax;
        let sched = Schedule {
            calendar: Calendar::new(),
            times: vec![
                NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(11, 30, 0).unwrap(),
            ],
            timezone,
        };

        // prev and next are reversible
        let dt = sched.prev_time(timezone.with_ymd_and_hms(2022, 1, 3, 11, 0, 0).unwrap()); // 10:30 -> 11:30
        assert_eq!(dt, sched.prev_time(sched.next_time(dt)));
    }

    #[test]
    fn check_interval() {
        let timezone = chrono_tz::America::Halifax;
        let sched = Schedule {
            calendar: Calendar::new(),
            times: vec![
                NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
                NaiveTime::from_hms_opt(11, 30, 0).unwrap(),
            ],
            timezone,
        };

        // Weekends are correct
        assert_eq!(
            sched.interval(timezone.with_ymd_and_hms(2022, 1, 1, 9, 0, 0).unwrap(), 0),
            Interval::new(
                timezone
                    .with_ymd_and_hms(2021, 12, 31, 11, 30, 0)
                    .unwrap()
                    .with_timezone(&Utc),
                timezone
                    .with_ymd_and_hms(2022, 1, 3, 10, 30, 0)
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );

        // prev and next are reversible
        let dt = timezone.with_ymd_and_hms(2022, 1, 3, 11, 0, 0).unwrap();
        assert_eq!(
            sched.interval(dt, 0),
            Interval::new(
                timezone
                    .with_ymd_and_hms(2022, 1, 3, 10, 30, 0)
                    .unwrap()
                    .with_timezone(&Utc),
                timezone
                    .with_ymd_and_hms(2022, 1, 3, 11, 30, 0)
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );

        // Previous
        assert_eq!(
            sched.interval(dt, -1),
            Interval::new(
                timezone
                    .with_ymd_and_hms(2021, 12, 31, 11, 30, 0)
                    .unwrap()
                    .with_timezone(&Utc),
                timezone
                    .with_ymd_and_hms(2022, 1, 3, 10, 30, 0)
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );

        // Next
        assert_eq!(
            sched.interval(dt, 1),
            Interval::new(
                timezone
                    .with_ymd_and_hms(2022, 1, 3, 11, 30, 0)
                    .unwrap()
                    .with_timezone(&Utc),
                timezone
                    .with_ymd_and_hms(2022, 1, 4, 10, 30, 0)
                    .unwrap()
                    .with_timezone(&Utc)
            )
        );
    }
}
