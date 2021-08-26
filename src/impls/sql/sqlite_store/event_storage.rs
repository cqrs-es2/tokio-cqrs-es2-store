use log::info;

use async_trait::async_trait;
use std::{
    collections::HashMap,
    marker::PhantomData,
};

use sqlx::sqlite::SqlitePool;

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

static CREATE_EVENTS_TABLE: &str = "
CREATE TABLE IF NOT EXISTS
events
    (
        aggregate_type TEXT                         NOT NULL,
        aggregate_id   TEXT                         NOT NULL,
        sequence       bigint CHECK (sequence >= 0) NOT NULL,
        payload        TEXT                         NOT NULL,
        metadata       TEXT                         NOT NULL,
        timestamp      timestamp DEFAULT (CURRENT_TIMESTAMP),
        PRIMARY KEY (aggregate_type, aggregate_id, sequence)
    );
";

static CREATE_SNAPSHOT_TABLE: &str = "
CREATE TABLE IF NOT EXISTS
snapshots
(
    aggregate_type TEXT                              NOT NULL,
    aggregate_id   TEXT                              NOT NULL,
    last_sequence  bigint CHECK (last_sequence >= 0) NOT NULL,
    payload        TEXT                              NOT NULL,
    timestamp      timestamp DEFAULT (CURRENT_TIMESTAMP),
    PRIMARY KEY (aggregate_type, aggregate_id, last_sequence)
);
";

/// SQLite storage
pub struct EventStorage<C: ICommand, E: IEvent, A: IAggregate<C, E>> {
    pool: SqlitePool,
    _phantom: PhantomData<(C, E, A)>,
}

impl<C: ICommand, E: IEvent, A: IAggregate<C, E>>
    EventStorage<C, E, A>
{
    /// constructor
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            _phantom: PhantomData,
        }
    }

    async fn create_events_table(&mut self) -> Result<(), Error> {
        let res = match sqlx::query(CREATE_EVENTS_TABLE)
            .execute(&self.pool)
            .await
        {
            Ok(x) => x,
            Err(e) => return Err(Error::new(e.to_string().as_str())),
        };

        info!(
            "Created events table with '{}' affected rows",
            res.rows_affected()
        );

        Ok(())
    }

    async fn create_snapshot_table(&mut self) -> Result<(), Error> {
        let res = match sqlx::query(CREATE_SNAPSHOT_TABLE)
            .execute(&self.pool)
            .await
        {
            Ok(x) => x,
            Err(e) => return Err(Error::new(e.to_string().as_str())),
        };

        info!(
            "Created snapshots table with '{}' affected rows",
            res.rows_affected()
        );

        Ok(())
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
        self.create_events_table().await?;

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

    async fn select_events(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<Vec<(usize, E, HashMap<String, String>)>, Error> {
        self.create_events_table().await?;

        let rows: Vec<(
            i64,
            serde_json::Value,
            serde_json::Value,
        )> = match sqlx::query_as(SELECT_EVENTS)
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
        self.create_snapshot_table().await?;

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
        self.create_snapshot_table().await?;

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

/// convenient type alias for SQLite event store
pub type SqliteEventStore<C, E, A> =
    EventStore<C, E, A, EventStorage<C, E, A>>;
