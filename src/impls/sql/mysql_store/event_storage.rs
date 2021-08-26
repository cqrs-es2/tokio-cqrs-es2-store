use async_trait::async_trait;
use std::{
    collections::HashMap,
    marker::PhantomData,
};

use sqlx::mysql::MySqlPool;

use cqrs_es2::{
    Error,
    IAggregate,
    ICommand,
    IEvent,
};

use crate::async_store::{
    EventStore,
    IEventStorage,
};

use super::super::mysql_constants::*;

/// MySql/MariaDB storage
pub struct EventStorage<C: ICommand, E: IEvent, A: IAggregate<C, E>> {
    pool: MySqlPool,
    _phantom: PhantomData<(C, E, A)>,
}

impl<C: ICommand, E: IEvent, A: IAggregate<C, E>>
    EventStorage<C, E, A>
{
    /// constructor
    pub fn new(pool: MySqlPool) -> Self {
        Self {
            pool,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<C: ICommand, E: IEvent, A: IAggregate<C, E>>
    IEventStorage<C, E, A> for EventStorage<C, E, A>
{
    async fn insert_event(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        sequence: i64,
        payload: &E,
        metadata: &HashMap<String, String>,
    ) -> Result<(), Error> {
        let payload = match serde_json::to_value(&payload) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize the event payload for \
                         aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let metadata = match serde_json::to_value(&metadata) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize the event metadata for \
                         aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

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
                            "insert new event failed for aggregate \
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
                        "unable to insert new event for aggregate \
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
    ) -> Result<Vec<(usize, E)>, Error> {
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
                            "unable to load events table for \
                             aggregate id {} with error: {}",
                            &agg_id, e
                        )
                        .as_str(),
                    ));
                },
            };

        let mut result = Vec::new();

        for row in rows {
            let payload = match serde_json::from_value(row.1) {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "bad payload found in events table for \
                             aggregate id {} with error: {}",
                            &agg_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            result.push((row.0 as usize, payload));
        }

        Ok(result)
    }

    async fn select_events_with_metadata(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<Vec<(usize, E, HashMap<String, String>)>, Error> {
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
                        "unable to load events table for aggregate \
                         id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let mut result = Vec::new();

        for row in rows {
            let payload = match serde_json::from_value(row.1) {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "bad payload found in events table for \
                             aggregate id {} with error: {}",
                            &agg_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            let metadata = match serde_json::from_value(row.2) {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "bad metadata found in events table for \
                             aggregate id {} with error: {}",
                            &agg_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            result.push((row.0 as usize, payload, metadata));
        }

        Ok(result)
    }

    async fn update_snapshot(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        last_sequence: i64,
        payload: &A,
        current_sequence: usize,
    ) -> Result<(), Error> {
        let sql = match current_sequence {
            0 => INSERT_SNAPSHOT,
            _ => UPDATE_SNAPSHOT,
        };

        let payload = match serde_json::to_value(payload) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize aggregate snapshot for \
                         aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
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
                        "unable to insert new snapshot for \
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
    ) -> Result<Option<(usize, A)>, Error> {
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
                            "unable to load snapshots table for \
                             aggregate id {} with error: {}",
                            &agg_id, e
                        )
                        .as_str(),
                    ));
                },
            };

        if rows.len() == 0 {
            return Ok(None);
        };

        let row = rows[0].clone();

        let aggregate = match serde_json::from_value(row.1) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "bad payload found in snapshots table for \
                         aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(Some((row.0 as usize, aggregate)))
    }
}

/// convenient type alias for MySql/MariaDB event store
pub type MySqlEventStore<C, E, A> =
    EventStore<C, E, A, EventStorage<C, E, A>>;
