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

    pub fn generate(&self, interval: Interval) -> Vec<Interval> {
        if self.times.is_empty() {
            return Vec::new();
        }

        let st = interval.start.with_timezone(&self.timezone);
        let et = interval.end.with_timezone(&self.timezone);

        let mut date = self.calendar.prev(st.date().naive_local());
        let end_date = self.calendar.next(et.date().succ().naive_local());

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

    pub fn interval_utc(&self, dt: DateTime<Utc>, offset: i32) -> Interval {
        // Need to get the current interval, then offset it
        let at = dt.with_timezone(&self.timezone);
        let rt = if self.times.iter().any(|x| *x == at.time()) {
            at
        } else {
            self.prev_time(at)
        };

        let start = self.offset(rt, offset);
        Interval::new(
            start.with_timezone(&Utc),
            self.next_time(start).with_timezone(&Utc),
        )
    }

    pub fn interval(&self, dt: DateTime<Tz>, offset: i32) -> Interval {
        // Need to get the current interval, then offset it
        let at = dt.with_timezone(&self.timezone);
        let rt = if self.times.iter().any(|x| *x == at.time()) {
            at
        } else {
            self.prev_time(at)
        };

        let start = self.offset(rt, offset);
        Interval::new(
            start.with_timezone(&Utc),
            self.next_time(start).with_timezone(&Utc),
        )
    }

    pub fn next_time(&self, dt: DateTime<Tz>) -> DateTime<Tz> {
        let st = dt.with_timezone(&self.timezone);

        let mut date = st.date().naive_local();
        let mut time = st.time();

        // Handle case where we're not on a valid date
        if !self.calendar.includes(date) {
            date = self.calendar.next(date);
            time = self.times[0] - Duration::milliseconds(1);
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
    pub fn prev_time(&self, dt: DateTime<Tz>) -> DateTime<Tz> {
        let st = dt.with_timezone(&self.timezone);

        let mut date = st.date().naive_local();
        let mut time = st.time();

        // Handle case where we're not on a valid date
        if !self.calendar.includes(date) {
            date = self.calendar.prev(date);
            time = *self.times.last().unwrap() + Duration::milliseconds(1);
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

    /// Given a timestamp, return the scheduled time `offset`
    pub fn offset(&self, mut dt: DateTime<Tz>, offset: i32) -> DateTime<Tz> {
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
                NaiveTime::from_hms(10, 30, 0),
                NaiveTime::from_hms(11, 30, 0),
            ],
            timezone,
        };

        // Simple generation
        let times = sched.generate(Interval::new(
            timezone
                .ymd(2022, 1, 3)
                .and_hms(11, 0, 0)
                .with_timezone(&Utc),
            timezone
                .ymd(2022, 1, 3)
                .and_hms(12, 0, 0)
                .with_timezone(&Utc),
        ));

        assert_eq!(times.len(), 1);
        assert_eq!(
            times,
            vec![Interval::new(
                timezone
                    .ymd(2022, 1, 3)
                    .and_hms(10, 30, 0)
                    .with_timezone(&Utc),
                timezone
                    .ymd(2022, 1, 3)
                    .and_hms(11, 30, 0)
                    .with_timezone(&Utc),
            )]
        );

        // Generating scheduled times over a timerange
        assert_eq!(
            sched.generate(Interval::new(
                timezone
                    .ymd(2021, 12, 31)
                    .and_hms(0, 0, 0)
                    .with_timezone(&Utc),
                timezone
                    .ymd(2022, 1, 5)
                    .and_hms(0, 0, 0)
                    .with_timezone(&Utc),
            )),
            vec![
                Interval::new(
                    timezone
                        .ymd(2021, 12, 30)
                        .and_hms(11, 30, 0)
                        .with_timezone(&Utc),
                    timezone
                        .ymd(2021, 12, 31)
                        .and_hms(10, 30, 0)
                        .with_timezone(&Utc),
                ),
                Interval::new(
                    timezone
                        .ymd(2021, 12, 31)
                        .and_hms(10, 30, 0)
                        .with_timezone(&Utc),
                    timezone
                        .ymd(2021, 12, 31)
                        .and_hms(11, 30, 0)
                        .with_timezone(&Utc),
                ),
                Interval::new(
                    timezone
                        .ymd(2021, 12, 31)
                        .and_hms(11, 30, 0)
                        .with_timezone(&Utc),
                    timezone
                        .ymd(2022, 1, 3)
                        .and_hms(10, 30, 0)
                        .with_timezone(&Utc),
                ),
                Interval::new(
                    timezone
                        .ymd(2022, 1, 3)
                        .and_hms(10, 30, 0)
                        .with_timezone(&Utc),
                    timezone
                        .ymd(2022, 1, 3)
                        .and_hms(11, 30, 0)
                        .with_timezone(&Utc),
                ),
                Interval::new(
                    timezone
                        .ymd(2022, 1, 3)
                        .and_hms(11, 30, 0)
                        .with_timezone(&Utc),
                    timezone
                        .ymd(2022, 1, 4)
                        .and_hms(10, 30, 0)
                        .with_timezone(&Utc),
                ),
                Interval::new(
                    timezone
                        .ymd(2022, 1, 4)
                        .and_hms(10, 30, 0)
                        .with_timezone(&Utc),
                    timezone
                        .ymd(2022, 1, 4)
                        .and_hms(11, 30, 0)
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
                NaiveTime::from_hms(10, 30, 0),
                NaiveTime::from_hms(11, 30, 0),
            ],
            timezone,
        };

        assert_eq!(
            sched.prev_time(timezone.ymd(2022, 1, 3).and_hms(11, 0, 0)),
            timezone.ymd(2022, 1, 3).and_hms(10, 30, 0)
        );
        assert_eq!(
            sched.prev_time(timezone.ymd(2022, 1, 3).and_hms(11, 30, 0)),
            timezone.ymd(2022, 1, 3).and_hms(10, 30, 0)
        );
    }

    #[test]
    fn check_offset() {
        let timezone = chrono_tz::America::Halifax;
        let sched = Schedule {
            calendar: Calendar::new(),
            times: vec![
                NaiveTime::from_hms(10, 30, 0),
                NaiveTime::from_hms(11, 30, 0),
            ],
            timezone,
        };

        // Asking for no offset should yield the same time
        assert_eq!(
            sched.offset(timezone.ymd(2022, 1, 3).and_hms(11, 0, 0), 0),
            timezone.ymd(2022, 1, 3).and_hms(11, 0, 0)
        );

        //  -1 is equivalent to prev
        let test_time = timezone.ymd(2022, 1, 3).and_hms(11, 0, 0);
        assert_eq!(sched.offset(test_time, -1), sched.prev_time(test_time));
        assert_eq!(sched.offset(test_time, 1), sched.next_time(test_time));
    }

    #[test]
    fn check_next() {
        let timezone = chrono_tz::America::Halifax;
        let sched = Schedule {
            calendar: Calendar::new(),
            times: vec![
                NaiveTime::from_hms(10, 30, 0),
                NaiveTime::from_hms(11, 30, 0),
            ],
            timezone,
        };

        assert_eq!(
            sched.next_time(timezone.ymd(2022, 1, 3).and_hms(11, 0, 0)),
            timezone.ymd(2022, 1, 3).and_hms(11, 30, 0)
        );
        assert_eq!(
            sched.next_time(timezone.ymd(2022, 1, 3).and_hms(11, 30, 0)),
            timezone.ymd(2022, 1, 4).and_hms(10, 30, 0)
        );
    }

    #[test]
    fn check_transivity() {
        let timezone = chrono_tz::America::Halifax;
        let sched = Schedule {
            calendar: Calendar::new(),
            times: vec![
                NaiveTime::from_hms(10, 30, 0),
                NaiveTime::from_hms(11, 30, 0),
            ],
            timezone,
        };

        // prev and next are reversible
        let dt = sched.prev_time(timezone.ymd(2022, 1, 3).and_hms(11, 0, 0)); // 10:30 -> 11:30
        assert_eq!(dt, sched.prev_time(sched.next_time(dt)));
    }

    #[test]
    fn check_interval() {
        let timezone = chrono_tz::America::Halifax;
        let sched = Schedule {
            calendar: Calendar::new(),
            times: vec![
                NaiveTime::from_hms(10, 30, 0),
                NaiveTime::from_hms(11, 30, 0),
            ],
            timezone,
        };

        // prev and next are reversible
        let dt = timezone.ymd(2022, 1, 3).and_hms(11, 0, 0);
        assert_eq!(
            sched.interval(dt, 0),
            Interval::new(
                timezone
                    .ymd(2022, 1, 3)
                    .and_hms(10, 30, 0)
                    .with_timezone(&Utc),
                timezone
                    .ymd(2022, 1, 3)
                    .and_hms(11, 30, 0)
                    .with_timezone(&Utc)
            )
        );

        // Previous
        assert_eq!(
            sched.interval(dt, -1),
            Interval::new(
                timezone
                    .ymd(2021, 12, 31)
                    .and_hms(11, 30, 0)
                    .with_timezone(&Utc),
                timezone
                    .ymd(2022, 1, 3)
                    .and_hms(10, 30, 0)
                    .with_timezone(&Utc)
            )
        );

        // Next
        assert_eq!(
            sched.interval(dt, 1),
            Interval::new(
                timezone
                    .ymd(2022, 1, 3)
                    .and_hms(11, 30, 0)
                    .with_timezone(&Utc),
                timezone
                    .ymd(2022, 1, 4)
                    .and_hms(10, 30, 0)
                    .with_timezone(&Utc)
            )
        );
    }
}
