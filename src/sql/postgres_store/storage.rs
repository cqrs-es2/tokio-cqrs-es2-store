use async_trait::async_trait;

use sqlx::postgres::PgPool;

use cqrs_es2::Error;

use super::super::{
    event_store::EventStore,
    i_storage::IStorage,
    postgres_constants::*,
    query_store::QueryStore,
};

/// Postgres storage
pub struct Storage {
    pool: PgPool,
}

impl Storage {
    /// constructor
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IStorage for Storage {
    async fn insert_event(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        sequence: i64,
        payload: &serde_json::Value,
        metadata: &serde_json::Value,
    ) -> Result<(), Error> {
        match sqlx::query(INSERT_EVENT)
            .bind(&agg_type)
            .bind(&agg_id)
            .bind(sequence)
            .bind(&payload)
            .bind(&metadata)
            .execute(&self.pool)
            .await
        {
            Ok(x) => {
                if x.rows_affected() != 1 {
                    return Err(Error::new(
                        format!(
                            "insert new events failed for aggregate \
                             id {}",
                            &agg_id
                        )
                        .as_str(),
                    ));
                }
            },
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "could not insert new events for aggregate \
                         id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(())
    }

    async fn select_events_only(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<Vec<(i64, serde_json::Value)>, Error> {
        let rows: Vec<(i64, serde_json::Value)> =
            match sqlx::query_as(SELECT_EVENTS)
                .bind(&agg_type)
                .bind(&agg_id)
                .fetch_all(&self.pool)
                .await
            {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "could not load events table for \
                             aggregate id {} with error: {}",
                            &agg_id, e
                        )
                        .as_str(),
                    ));
                },
            };

        Ok(rows)
    }

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
    > {
        let rows: Vec<(
            i64,
            serde_json::Value,
            serde_json::Value,
        )> = match sqlx::query_as(SELECT_EVENTS_WITH_METADATA)
            .bind(&agg_type)
            .bind(&agg_id)
            .fetch_all(&self.pool)
            .await
        {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "could not load events table for aggregate \
                         id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(rows)
    }

    async fn update_snapshot(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        last_sequence: i64,
        payload: &serde_json::Value,
        current_sequence: usize,
    ) -> Result<(), Error> {
        let sql = match current_sequence {
            0 => INSERT_SNAPSHOT,
            _ => UPDATE_SNAPSHOT,
        };

        match sqlx::query(sql)
            .bind(last_sequence)
            .bind(payload)
            .bind(&agg_type)
            .bind(&agg_id)
            .execute(&self.pool)
            .await
        {
            Ok(x) => {
                if x.rows_affected() != 1 {
                    return Err(Error::new(
                        format!(
                            "insert new snapshot failed for \
                             aggregate id {}",
                            &agg_id
                        )
                        .as_str(),
                    ));
                }
            },
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "could not insert new snapshot for \
                         aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(())
    }

    async fn select_snapshot(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<Vec<(i64, serde_json::Value)>, Error> {
        let rows: Vec<(i64, serde_json::Value)> =
            match sqlx::query_as(SELECT_SNAPSHOT)
                .bind(&agg_type)
                .bind(&agg_id)
                .fetch_all(&self.pool)
                .await
            {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "could not load events table for \
                             aggregate id {} with error: {}",
                            &agg_id, e
                        )
                        .as_str(),
                    ))
                },
            };

        Ok(rows)
    }

    async fn update_query(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        query_type: &str,
        version: i64,
        payload: &serde_json::Value,
    ) -> Result<(), Error> {
        let sql = match version {
            1 => INSERT_QUERY,
            _ => UPDATE_QUERY,
        };

        match sqlx::query(sql)
            .bind(version)
            .bind(&payload)
            .bind(&agg_type)
            .bind(&agg_id)
            .bind(&query_type)
            .execute(&self.pool)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                return Err(Error::new(e.to_string().as_str()));
            },
        }
    }

    async fn select_query(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        query_type: &str,
    ) -> Result<Vec<(i64, serde_json::Value)>, Error> {
        let rows: Vec<(i64, serde_json::Value)> =
            match sqlx::query_as(SELECT_QUERY)
                .bind(&agg_type)
                .bind(&agg_id)
                .bind(&query_type)
                .fetch_all(&self.pool)
                .await
            {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(e.to_string().as_str()));
                },
            };

        Ok(rows)
    }
}

/// convenient type alias for Postgres event store
pub type PostgresEventStore<C, E, A> = EventStore<C, E, A, Storage>;

/// convenient type alias for Postgres query store
pub type PostgresQueryStore<C, E, A, Q> =
    QueryStore<C, E, A, Q, Storage>;
