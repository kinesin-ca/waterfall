use super::*;
use std::collections::HashSet;

pub fn default_dow_set() -> HashSet<Weekday> {
    use Weekday::*;
    HashSet::from([Mon, Tue, Wed, Thu, Fri])
}

// TODO
//   - Make sure include and exclude are disjoint

/// Maintains a list of days that are considered active
#[derive(Clone, Serialize, Deserialize, Default, Debug, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Calendar {
    /// Day of Week Mask
    #[serde(default = "default_dow_set")]
    pub mask: HashSet<Weekday>,

    /// Dates to explicitly include
    #[serde(default)]
    pub exclude: HashSet<NaiveDate>,

    /// Dates to explicitly include
    #[serde(default)]
    pub include: HashSet<NaiveDate>,
}

impl Calendar {
    pub fn new() -> Self {
        Calendar {
            mask: default_dow_set(),
            ..Calendar::default()
        }
    }

    pub fn includes(&self, date: NaiveDate) -> bool {
        if self.exclude.contains(&date) {
            false
        } else if self.include.contains(&date) {
            true
        } else {
            self.mask.contains(&date.weekday())
        }
    }

    pub fn next(&self, date: NaiveDate) -> NaiveDate {
        self.offset(date, 1)
    }

    pub fn prev(&self, date: NaiveDate) -> NaiveDate {
        self.offset(date, -1)
    }

    pub fn offset(&self, mut date: NaiveDate, mut offset: i64) -> NaiveDate {
        let incr = if offset < 0 { 1 } else { -1 };
        while offset != 0 {
            date = date + Duration::days(-1 * incr);
            while !self.includes(date) {
                date = date + Duration::days(-1 * incr);
            }
            offset += incr;
        }
        date
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_next() {
        let cal = Calendar::new();
        assert_eq!(
            cal.next(NaiveDate::from_ymd(2022, 1, 1)),
            NaiveDate::from_ymd(2022, 1, 3)
        );
    }
}
