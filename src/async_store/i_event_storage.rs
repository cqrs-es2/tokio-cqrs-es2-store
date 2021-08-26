use async_trait::async_trait;
use std::collections::HashMap;

use cqrs_es2::{
    Error,
    IAggregate,
    ICommand,
    IEvent,
};

/// Async event storage interface
#[async_trait]
pub trait IEventStorage<C: ICommand, E: IEvent, A: IAggregate<C, E>>:
    Send {
    /// insert a single event
    async fn insert_event(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        sequence: i64,
        payload: &E,
        metadata: &HashMap<String, String>,
    ) -> Result<(), Error>;

    /// select all evens with metadata
    async fn select_events(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<Vec<(usize, E, HashMap<String, String>)>, Error>;

    /// update a snapshot
    async fn update_snapshot(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        last_sequence: i64,
        payload: &A,
        current_sequence: usize,
    ) -> Result<(), Error>;

    /// get a snapshot
    async fn select_snapshot(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<Option<(usize, A)>, Error>;
}
