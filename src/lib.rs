#![allow(unused_imports)]
#![allow(dead_code)]

use anyhow::{anyhow, Result};
use chrono::prelude::*;
use chrono::{Duration, TimeZone};
use chrono_tz::Tz;
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
use crate::varmap::*;

pub type Resource = String;
pub type TaskDetails = serde_json::Value;

pub mod calendar;
pub mod executors;
pub mod interval;
pub mod interval_set;
pub mod requirement;
pub mod resource_interval;
pub mod schedule;
pub mod storage;
pub mod task;
pub mod varmap;
