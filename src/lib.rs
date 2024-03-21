#![allow(unused_imports)]
#![allow(dead_code)]
// #![feature(slice_group_by)]

use anyhow::{anyhow, Result};
use chrono::prelude::*;
use chrono::{Duration, TimeZone};
use chrono_tz::Tz;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};

use crate::calendar::*;
use crate::executors::*;
use crate::interval::*;
use crate::interval_set::*;
use crate::requirement::*;
use crate::resource_interval::*;
use crate::schedule::*;
use crate::storage::*;
use crate::task::*;
use crate::task_set::*;
use crate::varmap::*;
use crate::world::*;

const MAX_TIME: DateTime<Utc> = chrono::DateTime::<Utc>::MAX_UTC;
const MIN_TIME: DateTime<Utc> = chrono::DateTime::<Utc>::MIN_UTC;

pub type Resource = String;
pub type TaskDetails = serde_json::Value;

pub mod calendar;
pub mod executors;
pub mod interval;
pub mod interval_set;
pub mod prelude;
pub mod requirement;
pub mod resource_interval;
pub mod runner;
pub mod schedule;
pub mod storage;
pub mod task;
pub mod task_set;
pub mod varmap;
pub mod world;
