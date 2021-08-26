use serde::{
    Deserialize,
    Serialize,
};
use std::{
    collections::HashMap,
    fmt::Debug,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct EventDocument {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub sequence: i64,
    pub payload: String,
    pub metadata: HashMap<String, String>,
}
