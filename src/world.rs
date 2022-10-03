use super::*;

// A struct used for serializing / deserializing world
#[derive(Debug, Serialize, Deserialize)]
pub struct WorldDefinition {
    pub tasks: HashMap<String, TaskDefinition>,

    pub calendars: HashMap<String, Calendar>,

    #[serde(default)]
    pub variables: VarMap,

    #[serde(default)]
    pub output_options: TaskOutputOptions,
}

impl WorldDefinition {
    pub fn taskset(&self) -> Result<TaskSet> {
        // Ensure all tasks reference a valid calendar
        for (name, def) in self.tasks.iter() {
            if !self.calendars.contains_key(&def.calendar_name) {
                return Err(anyhow!(
                    "Task {} references calendar {}, which is not defined",
                    name,
                    def.calendar_name
                ));
            }
        }
        let tasks: HashMap<String, Task> = self
            .tasks
            .iter()
            .map(|(tn, td)| {
                (
                    tn.clone(),
                    td.to_task(self.calendars.get(&td.calendar_name).unwrap()),
                )
            })
            .collect();
        let ts = TaskSet::from(tasks);

        ts.validate()?;

        Ok(ts)
    }
}
