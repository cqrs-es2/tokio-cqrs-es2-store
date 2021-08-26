use serde::{
    Deserialize,
    Serialize,
};
use std::fmt::Debug;

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryDocument {
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub query_type: String,
    pub version: i64,
    pub payload: String,
}
