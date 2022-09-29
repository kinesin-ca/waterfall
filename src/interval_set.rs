use super::*;
use std::convert::From;
use std::ops::{Add, BitAnd, BitOr, Deref, DerefMut, Not, Sub};

/// A coalescing set of intervals
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd)]
pub struct IntervalSet(Vec<Interval>);

impl IntervalSet {
    pub fn new() -> Self {
        IntervalSet(Vec::new())
    }

    pub fn start(&self) -> Option<DateTime<Utc>> {
        if let Some(intv) = self.first() {
            Some(intv.start)
        } else {
            None
        }
    }

    pub fn end(&self) -> Option<DateTime<Utc>> {
        if let Some(intv) = self.last() {
            Some(intv.end)
        } else {
            None
        }
    }

    /// Returns true if interval is a subset
    pub fn has_subset(&self, interval: Interval) -> bool {
        self.0.iter().any(|x| x.has_subset(interval))
    }

    pub fn contains<T: TimeZone>(&self, dt: DateTime<T>) -> bool {
        self.0.iter().any(|x| x.contains(dt.with_timezone(&Utc)))
    }

    // Naive O(n^2) implementation
    pub fn is_disjoint(&self, other: &IntervalSet) -> bool {
        self.0
            .iter()
            .all(|x| other.iter().all(|y| x.is_disjoint(*y)))
    }

    pub fn intersection(&self, other: &IntervalSet) -> Self {
        let mut res = IntervalSet(self.0.iter().fold(Vec::<Interval>::new(), |mut acc, x| {
            let new_intervals: Vec<Interval> = other
                .iter()
                .map(|y| x.intersection(*y))
                .filter(|x| !x.is_empty())
                .collect();
            acc.extend(new_intervals);
            acc
        }));
        res.coalesce();
        res
    }

    pub fn complement(&self) -> Self {
        if self.is_empty() {
            IntervalSet(vec![Interval::new(
                DateTime::<Utc>::MIN_UTC,
                DateTime::<Utc>::MAX_UTC,
            )])
        } else {
            // Need to build the start of the range
            let mut acc = Vec::new();
            let mut last_end = DateTime::<Utc>::MIN_UTC;
            for intv in &self.0 {
                if intv.start == DateTime::<Utc>::MIN_UTC {
                    last_end = intv.end;
                } else {
                    acc.push(Interval::new(last_end, intv.start));
                    last_end = intv.end;
                }
            }
            if last_end != DateTime::<Utc>::MAX_UTC {
                acc.push(Interval::new(last_end, DateTime::<Utc>::MAX_UTC));
            }
            IntervalSet(acc)
        }
    }

    pub fn insert(&mut self, interval: Interval) {
        let should_coalesce = self.0.iter().any(|intv| intv.is_contiguous(interval));
        self.0.push(interval);
        if should_coalesce {
            self.coalesce();
        }
    }

    pub fn merge(&mut self, other: &IntervalSet) {
        self.0.extend(other.0.iter().cloned());
        self.coalesce();
    }

    pub fn coalesce(&mut self) {
        self.0.sort_unstable();
        self.0 = self
            .0
            .iter()
            .filter(|x| !x.is_empty())
            .fold(Vec::new(), |mut acc, int| {
                if let Some(lst) = acc.last_mut() {
                    if !lst.is_contiguous(*int) {
                        acc.push(*int)
                    } else {
                        lst.end = int.end
                    }
                } else {
                    acc.push(*int);
                }

                acc
            });
    }

    pub fn union(&self, other: &IntervalSet) -> Self {
        let mut is = IntervalSet(self.0.iter().chain(other.0.iter()).copied().collect());
        is.coalesce();
        is
    }

    /// Subtract all intervals in `other` from self
    /// both sides must be sorted
    pub fn difference(&self, other: &Self) -> Self {
        self.intersection(&other.complement())
    }
}
impl Deref for IntervalSet {
    type Target = Vec<Interval>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for IntervalSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
impl From<Interval> for IntervalSet {
    fn from(interval: Interval) -> Self {
        IntervalSet(vec![interval])
    }
}
impl From<Vec<Interval>> for IntervalSet {
    fn from(intervals: Vec<Interval>) -> Self {
        let mut is = IntervalSet(intervals);
        is.coalesce();
        is
    }
}
impl From<&[Interval]> for IntervalSet {
    fn from(intervals: &[Interval]) -> Self {
        let mut is = IntervalSet(intervals.to_vec());
        is.coalesce();
        is
    }
}

impl Not for &IntervalSet {
    type Output = IntervalSet;
    fn not(self) -> Self::Output {
        self.complement()
    }
}
impl Add for &IntervalSet {
    type Output = IntervalSet;
    fn add(self, other: &IntervalSet) -> Self::Output {
        self.union(other)
    }
}
impl Sub for &IntervalSet {
    type Output = IntervalSet;
    fn sub(self, other: &IntervalSet) -> Self::Output {
        self.difference(other)
    }
}
impl BitOr for &IntervalSet {
    type Output = IntervalSet;
    fn bitor(self, other: &IntervalSet) -> Self::Output {
        self.union(other)
    }
}
impl BitAnd for &IntervalSet {
    type Output = IntervalSet;
    fn bitand(self, other: &IntervalSet) -> Self::Output {
        self.intersection(other)
    }
}
impl Not for IntervalSet {
    type Output = IntervalSet;
    fn not(self) -> Self::Output {
        self.complement()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! intv {
        ( $x:literal, $y:literal ) => {
            Interval::new(
                Utc.ymd(2022, 1, 1).and_hms($x, 0, 0),
                Utc.ymd(2022, 1, 1).and_hms($y, 0, 0),
            )
        };
    }

    /*
            Interval Set
    */

    #[test]
    fn test_intervalset_difference() {
        let isa = IntervalSet(vec![intv!(1, 3), intv!(5, 6)]);

        // Removing the entire span
        let full = IntervalSet(vec![intv!(1, 6)]);
        assert_eq!(isa.difference(&full), IntervalSet(vec![]));
        assert_eq!(
            isa.difference(&IntervalSet(vec![intv!(2, 5)])),
            IntervalSet(vec![intv!(1, 2), intv!(5, 6)])
        );

        // TODO need more tests here
    }

    #[test]
    fn test_intervalset_complement() {
        // Complement's complement is the same
        let is = IntervalSet(vec![intv!(2, 5), intv!(8, 20)]);
        assert_eq!(is.complement().complement(), is);

        // Complement with one end at min time
        let is = IntervalSet(vec![
            Interval::new(
                DateTime::<Utc>::MIN_UTC,
                Utc.ymd(2021, 12, 1).and_hms(0, 0, 0),
            ),
            intv!(8, 20),
        ]);
        assert_eq!(is.complement().complement(), is);
    }
}
