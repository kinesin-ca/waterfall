use super::*;
use std::fmt::Display;
use std::ops::{Add, BitAnd, BitOr, Sub};

/*
    These intervals are all half-open on the left, so:
        (start, end)

    This makes the end included in the interval for which it's
    in charge of
*/

#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Interval {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl Interval {
    pub fn new<T: TimeZone>(start: DateTime<T>, end: DateTime<T>) -> Self {
        let start = start.with_timezone(&Utc);
        let end = end.with_timezone(&Utc);
        if start > end {
            Interval { end, start }
        } else {
            Interval { start, end }
        }
    }

    pub fn is_empty(&self) -> bool {
        return self.start == self.end;
    }

    pub fn len(&self) -> Duration {
        self.end - self.start
    }

    pub fn contains<T: TimeZone>(&self, dt: DateTime<T>) -> bool {
        return self.start < dt && dt <= self.end;
    }

    /// True if `other` is a subset of this interval
    pub fn has_subset(&self, other: Interval) -> bool {
        return self.start <= other.start && other.end <= self.end;
    }

    /// True if `other` overlaps or is immediately adjascent to self
    pub fn is_contiguous(&self, other: Interval) -> bool {
        return (self.start <= other.start && other.start <= self.end)
            || (other.start <= self.start && self.start <= other.end);
    }

    /// True if self intersection other is an empty set
    pub fn is_disjoint(&self, other: Interval) -> bool {
        return self.end <= other.start || other.end <= self.start;
    }

    pub fn intersection(&self, other: Interval) -> Interval {
        if self.is_disjoint(other) {
            Interval::new(self.start, self.start)
        } else {
            Interval {
                start: std::cmp::max(self.start, other.start),
                end: std::cmp::min(self.end, other.end),
            }
        }
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {}]", self.start, self.end)
    }
}

impl BitAnd for Interval {
    type Output = Interval;
    fn bitand(self, other: Interval) -> Self::Output {
        self.intersection(other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! dt {
        ( $x:literal ) => {
            Utc.with_ymd_and_hms(2022, 1, 1,$x, 0, 0).unwrap()
        };
    }

    macro_rules! intv {
        ( $x:literal, $y:literal ) => {
            Interval::new(
                Utc.with_ymd_and_hms(2022, 1, 1,$x, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2022, 1, 1,$y, 0, 0).unwrap(),
            )
        };
    }

    /*
        Intervals
    */

    #[test]
    fn test_interval_contains() {
        let intv = intv!(2, 5);

        // Ensure the interval is half-open on the right
        assert!(!intv.contains(dt!(0)));
        assert!(!intv.contains(dt!(1)));
        assert!(!intv.contains(dt!(2)));
        assert!(intv.contains(dt!(3)));
        assert!(intv.contains(dt!(4)));
        assert!(intv.contains(dt!(5)));
        assert!(!intv.contains(dt!(6)));
        assert!(!intv.contains(dt!(7)));
    }

    #[test]
    fn test_interval_ordering() {
        assert!(intv!(1, 2) < intv!(2, 3));
        assert!(intv!(1, 3) < intv!(2, 4));
        assert!(intv!(1, 3) < intv!(4, 5));
        assert!(intv!(1, 3) < intv!(4, 6));
    }

    #[test]
    fn test_is_disjoint() {
        let int = intv!(2, 5);

        assert!(int.is_disjoint(intv!(1, 2)));
        assert!(!int.is_disjoint(intv!(1, 3)));
        assert!(int.is_disjoint(intv!(5, 6)));
    }

    #[test]
    fn test_is_contiguous() {
        let int = intv!(3, 4);

        assert!(!int.is_contiguous(intv!(1, 2)));
        assert!(int.is_contiguous(intv!(2, 3)));
        assert!(int.is_contiguous(intv!(1, 3)));
        assert!(int.is_contiguous(intv!(4, 6)));
        assert!(int.is_contiguous(intv!(1, 6)));
        assert!(!int.is_contiguous(intv!(5, 6)));
    }

    #[test]
    fn test_has_subset() {
        let int = intv!(2, 5);

        // Contains itself
        assert!(int.has_subset(int));

        assert!(int.has_subset(intv!(3, 4))); // Contains inner interval
        assert!(!int.has_subset(intv!(1, 2))); // Left contiguous
        assert!(!int.has_subset(intv!(1, 3))); // Left overlap
        assert!(!int.has_subset(intv!(4, 6))); // Right overlap
        assert!(!int.has_subset(intv!(5, 6))); // Right contiguous
        assert!(!int.has_subset(intv!(1, 6))); // Outer scope
    }

    #[test]
    fn test_intersection() {
        let int = intv!(2, 5);

        assert_eq!(int.intersection(int), int); // Union with itself
        assert_eq!(int.intersection(intv!(1, 6)), int); // Union with itself
        assert!(int.intersection(intv!(1, 2)).is_empty()); // Left
        assert_eq!(int.intersection(intv!(1, 3)), intv!(2, 3)); // Left Overlap
        assert_eq!(int.intersection(intv!(2, 3)), intv!(2, 3)); // Inner left
        assert_eq!(int.intersection(intv!(3, 4)), intv!(3, 4)); // Inner
        assert_eq!(int.intersection(intv!(4, 5)), intv!(4, 5)); // Right Inner
        assert_eq!(int.intersection(intv!(4, 6)), intv!(4, 5)); // Inner
        assert!(int.intersection(intv!(5, 6)).is_empty()); // Right
    }
}
