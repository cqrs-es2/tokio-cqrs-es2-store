use async_trait::async_trait;

use cqrs_es2::Error;

#[async_trait]
pub trait IStorage: Send {
    async fn insert_event(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        sequence: i64,
        payload: &serde_json::Value,
        metadata: &serde_json::Value,
    ) -> Result<(), Error>;

    async fn select_events_only(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<Vec<(i64, serde_json::Value)>, Error>;

    async fn select_events_with_metadata(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<
        Vec<(
            i64,
            serde_json::Value,
            serde_json::Value,
        )>,
        Error,
    >;

    async fn update_snapshot(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        last_sequence: i64,
        payload: &serde_json::Value,
        current_sequence: usize,
    ) -> Result<(), Error>;

    async fn select_snapshot(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<Vec<(i64, serde_json::Value)>, Error>;

    async fn update_query(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        query_type: &str,
        version: i64,
        payload: &serde_json::Value,
    ) -> Result<(), Error>;

    async fn select_query(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        query_type: &str,
    ) -> Result<Vec<(i64, serde_json::Value)>, Error>;
}
