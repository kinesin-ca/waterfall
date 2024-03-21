use super::*;
use std::path::Path;

pub trait Satisfiable {
    /// Returns true if the requirement is satisfied now
    fn is_satisfied(
        &self,
        interval: Interval,
        schedule: &Schedule,
        available: &HashMap<String, IntervalSet>,
    ) -> bool;

    /// Returns true if the requirement could be satisfied at some point
    /// in time
    fn can_be_satisfied(
        &self,
        interval: Interval,
        schedule: &Schedule,
        available: &HashMap<String, IntervalSet>,
    ) -> bool;

    fn resources(&self) -> HashSet<Resource>;
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AggregateRequirement {
    All(Vec<Box<Requirement>>),
    Any(Vec<Box<Requirement>>),
    None(Vec<Box<Requirement>>),
}

impl Satisfiable for AggregateRequirement {
    fn resources(&self) -> HashSet<Resource> {
        match self {
            AggregateRequirement::All(reqs) => reqs.iter().fold(HashSet::new(), |mut acc, req| {
                acc.extend(req.resources());
                acc
            }),
            AggregateRequirement::Any(reqs) => reqs.iter().fold(HashSet::new(), |mut acc, req| {
                acc.extend(req.resources());
                acc
            }),
            AggregateRequirement::None(reqs) => reqs.iter().fold(HashSet::new(), |mut acc, req| {
                acc.extend(req.resources());
                acc
            }),
        }
    }

    fn is_satisfied(
        &self,
        interval: Interval,
        schedule: &Schedule,
        available: &HashMap<Resource, IntervalSet>,
    ) -> bool {
        match self {
            AggregateRequirement::All(reqs) => reqs
                .iter()
                .all(|x| x.is_satisfied(interval, schedule, available)),
            AggregateRequirement::Any(reqs) => reqs
                .iter()
                .any(|x| x.is_satisfied(interval, schedule, available)),
            AggregateRequirement::None(reqs) => !reqs
                .iter()
                .any(|x| x.is_satisfied(interval, schedule, available)),
        }
    }

    fn can_be_satisfied(
        &self,
        interval: Interval,
        schedule: &Schedule,
        available: &HashMap<Resource, IntervalSet>,
    ) -> bool {
        match self {
            AggregateRequirement::All(reqs) => reqs
                .iter()
                .all(|x| x.can_be_satisfied(interval, schedule, available)),
            AggregateRequirement::Any(reqs) => reqs
                .iter()
                .any(|x| x.can_be_satisfied(interval, schedule, available)),
            AggregateRequirement::None(reqs) => !reqs
                .iter()
                .any(|x| x.can_be_satisfied(interval, schedule, available)),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all = "snake_case", untagged)]
pub enum SingleRequirement {
    Offset { resource: String, offset: i32 },
    File { path: String },
}

impl Satisfiable for SingleRequirement {
    fn resources(&self) -> HashSet<Resource> {
        match self {
            SingleRequirement::Offset { resource, .. } => HashSet::from([resource.to_owned()]),
            SingleRequirement::File { path: _ } => HashSet::new(),
        }
    }

    fn is_satisfied(
        &self,
        interval: Interval,
        schedule: &Schedule,
        available: &HashMap<Resource, IntervalSet>,
    ) -> bool {
        match self {
            //SingleRequirement::ResourceInterval { .. } => true,
            SingleRequirement::Offset { resource, offset } => {
                let intv = schedule.interval(interval.end, *offset);
                match available.get(resource) {
                    Some(is) => is.has_subset(intv),
                    None => false,
                }
            }
            SingleRequirement::File { path } => Path::new(path).exists(),
        }
    }

    fn can_be_satisfied(
        &self,
        interval: Interval,
        schedule: &Schedule,
        available: &HashMap<Resource, IntervalSet>,
    ) -> bool {
        match self {
            SingleRequirement::Offset { resource, offset } => {
                let intv = schedule.interval(interval.end, *offset);
                match available.get(resource) {
                    Some(is) => is.has_subset(intv),
                    None => false,
                }
            }
            SingleRequirement::File { .. } => true,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
#[serde(untagged)]
pub enum Requirement {
    One(SingleRequirement),
    Group(AggregateRequirement),
}

impl Satisfiable for Requirement {
    fn is_satisfied(
        &self,
        interval: Interval,
        schedule: &Schedule,
        available: &HashMap<Resource, IntervalSet>,
    ) -> bool {
        match self {
            Requirement::One(req) => req.is_satisfied(interval, schedule, available),
            Requirement::Group(req) => req.is_satisfied(interval, schedule, available),
        }
    }

    fn can_be_satisfied(
        &self,
        interval: Interval,
        schedule: &Schedule,
        available: &HashMap<Resource, IntervalSet>,
    ) -> bool {
        match self {
            Requirement::One(req) => req.can_be_satisfied(interval, schedule, available),
            Requirement::Group(req) => req.can_be_satisfied(interval, schedule, available),
        }
    }

    fn resources(&self) -> HashSet<Resource> {
        match self {
            Requirement::One(req) => req.resources(),
            Requirement::Group(req) => req.resources(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_complex_parse() {
        let json = r#"{
        "any": [
            { "all": [
                    { "resource": "resource_a", "offset": -1  },
                    { "resource": "resource_b", "offset": -1  }
                ]
            },
            { "type": "file", "path": "/mnt/test/data_${yyyy}{$mm}{$dd}" }
            ]
        }"#;
        let res: serde_json::Result<Requirement> = serde_json::from_str(json);
        assert!(res.is_ok());
    }

    #[test]
    fn check_simple_parse() {
        let json = r#"{ "type": "file", "path": "/mnt/test/data_${yyyy}{$mm}{$dd}" }"#;
        let res: serde_json::Result<Requirement> = serde_json::from_str(json);
        println!("{:?}", res);
        assert!(res.is_ok());
    }

    // TODO Add tests for satisfies
}
