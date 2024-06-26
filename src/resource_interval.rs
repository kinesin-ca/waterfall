use super::*;
use std::ops::{Add, Deref, DerefMut, Sub};

/// Contains a map of resource and intervals. The intervals could
/// represent where a resource is available, or where it's required
/// Resources are independent, so overlaps between the
/// interval sets are possible.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct ResourceInterval(HashMap<Resource, IntervalSet>);

impl ResourceInterval {
    pub fn new() -> Self {
        ResourceInterval(HashMap::new())
    }

    pub fn insert(&mut self, resource: &Resource, intervals: &IntervalSet) {
        self.0
            .entry(resource.clone())
            .or_insert(IntervalSet::new())
            .merge(intervals);
    }

    pub fn union(&self, other: &ResourceInterval) -> Self {
        let res: HashMap<Resource, IntervalSet> =
            other.0.iter().fold(self.0.clone(), |mut acc, (res, is)| {
                acc.entry(res.clone())
                    .or_insert(IntervalSet::new())
                    .merge(is);
                acc
            });
        ResourceInterval(res)
    }

    pub fn difference(&self, other: &ResourceInterval) -> Self {
        let res: HashMap<Resource, IntervalSet> = self
            .0
            .iter()
            .map(|(res, is)| {
                (
                    res.clone(),
                    is.difference(other.get(res).unwrap_or(&IntervalSet::new())),
                )
            })
            .collect();
        ResourceInterval(res)
    }
}

impl Deref for ResourceInterval {
    type Target = HashMap<Resource, IntervalSet>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ResourceInterval {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<HashMap<Resource, IntervalSet>> for ResourceInterval {
    fn from(hm: HashMap<Resource, IntervalSet>) -> Self {
        ResourceInterval(hm)
    }
}

impl From<&HashMap<Resource, IntervalSet>> for ResourceInterval {
    fn from(hm: &HashMap<Resource, IntervalSet>) -> Self {
        ResourceInterval(hm.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! intv {
        ( $x:literal, $y:literal ) => {
            Interval::new(
                Utc.with_ymd_and_hms(2022, 1, 1,$x, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2022, 1, 1,$y, 0, 0).unwrap(),
            )
        };
    }

    macro_rules! ri {
        ( $r:literal, $(($x:literal, $y:literal)),* ) => {
            ResourceInterval::from(HashMap::from([(
                $r.to_owned(),
                IntervalSet::from(vec![$(intv!($x, $y)),*]),
            )]))
        };
    }

    #[test]
    fn test_conversion() {
        let ri = ResourceInterval::from(HashMap::from([("alpha".to_owned(), IntervalSet::new())]));
        assert_eq!(ri.len(), 1);
    }

    #[test]
    fn test_addition() {
        let a = ri!("alpha", (13, 15));

        assert_eq!(a.union(&ri!("alpha", (15, 18))), ri!("alpha", (13, 18)));
    }

    #[test]
    fn test_subtraction() {
        assert_eq!(
            ri!("alpha", (13, 18)).difference(&ri!("alpha", (15, 16))),
            ri!("alpha", (13, 15), (16, 18))
        );
        assert_eq!(
            ri!("alpha", (13, 18)).difference(&ResourceInterval::new()),
            ri!("alpha", (13, 18))
        );
    }
}
