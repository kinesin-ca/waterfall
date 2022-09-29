use super::*;
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct VarMap(HashMap<String, String>);

impl Deref for VarMap {
    type Target = HashMap<String, String>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VarMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl VarMap {
    pub fn new() -> Self {
        VarMap(HashMap::new())
    }

    // Derive variables from a given interval
    pub fn from_interval(int: &Interval, tz: Tz) -> Self {
        let start = int.start.with_timezone(&tz);
        let end = int.end.with_timezone(&tz);

        VarMap(HashMap::from([
            ("PERIOD_START".to_owned(), format!("{}", start)),
            ("PERIOD_END".to_owned(), format!("{}", end)),
            ("yyyy".to_owned(), format!("{}", end.year())),
            ("mm".to_owned(), format!("{}", end.month())),
            ("dd".to_owned(), format!("{}", end.day())),
            (
                "yyyymmdd".to_owned(),
                format!("{}{}{}", end.year(), end.month(), end.day()),
            ),
            (
                "hhmmss".to_owned(),
                format!("{}{}{}", end.hour(), end.minute(), end.second()),
            ),
        ]))
    }

    /// Interpolate values into a string, assuming string has variables
    /// as ${varname}
    pub fn apply_to(&self, s: &str) -> String {
        let mut expanded = s.to_string();
        for (key, value) in self.0.iter() {
            expanded = expanded.replace(&format!("${{{}}}", key), value);
        }
        expanded
    }
}

impl From<HashMap<String, String>> for VarMap {
    fn from(data: HashMap<String, String>) -> Self {
        VarMap(data)
    }
}

impl<'a> FromIterator<(&'a String, &'a String)> for VarMap {
    fn from_iter<I: IntoIterator<Item = (&'a String, &'a String)>>(iter: I) -> Self {
        let mut data = HashMap::new();
        for (k, v) in iter {
            data.insert(k.clone(), v.clone());
        }
        VarMap(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_simple_apply() {
        let s = "This is a ${test} of home and ${test} of away ${beep}";
        let vm = VarMap(HashMap::from([("test".to_owned(), "alpha".to_owned())]));

        assert_eq!(
            &vm.apply_to(s),
            "This is a alpha of home and alpha of away ${beep}"
        );
    }
}
