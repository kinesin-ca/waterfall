use super::*;
use std::ops::{Add, Deref, DerefMut, Sub};

/// Contains a map of resource and intervals. The intervals could
/// represent where a resource is available, or where it's required
/// Resources are independent, so overlaps between the
/// interval sets are possible.
pub struct ResourceInterval(HashMap<Resource, IntervalSet>);

impl ResourceInterval {
    fn new() -> Self {
        ResourceInterval(HashMap::new())
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

impl Add for &ResourceInterval {
    type Output = ResourceInterval;
    fn add(self, other: &ResourceInterval) -> Self::Output {
        let res: HashMap<Resource, IntervalSet> =
            other.0.iter().fold(self.0.clone(), |mut acc, (res, is)| {
                acc.entry(res.clone())
                    .or_insert(IntervalSet::new())
                    .merge(is);
                acc
            });
        ResourceInterval(res)
    }
}

impl Sub for &ResourceInterval {
    type Output = ResourceInterval;
    fn sub(self, other: &ResourceInterval) -> Self::Output {
        let res: HashMap<Resource, IntervalSet> =
            other.0.iter().fold(self.0.clone(), |mut acc, (res, is)| {
                acc.entry(res.clone())
                    .or_insert(IntervalSet::new())
                    .difference(is);
                acc
            });
        ResourceInterval(res)
    }
}
