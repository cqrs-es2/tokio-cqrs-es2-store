use serde::{
    Deserialize,
    Serialize,
};
use std::fmt::Debug;

#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotDocument {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub last_sequence: i64,
    pub payload: String,
}
