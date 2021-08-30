use async_trait::async_trait;
use log::{
    debug,
    trace,
};
use std::marker::PhantomData;

use sqlx::sqlite::SqlitePool;

use cqrs_es2::{
    AggregateContext,
    Error,
    EventContext,
    IAggregate,
    ICommand,
    IEvent,
};

use crate::repository::IEventStore;

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
        version        bigint       CHECK (version >= 0) NOT NULL,
        payload        TEXT                              NOT NULL,
        timestamp      timestamp DEFAULT (CURRENT_TIMESTAMP),
        PRIMARY KEY (aggregate_type, aggregate_id)
    );
";

/// Async SQLite event store
pub struct EventStore<C: ICommand, E: IEvent, A: IAggregate<C, E>> {
    pool: SqlitePool,
    _phantom: PhantomData<(C, E, A)>,
}

impl<C: ICommand, E: IEvent, A: IAggregate<C, E>>
    EventStore<C, E, A>
{
    /// constructor
    pub fn new(pool: SqlitePool) -> Self {
        let x = Self {
            pool,
            _phantom: PhantomData,
        };

        trace!("Created new async SQLite event store");

        x
    }

    async fn create_events_table(&mut self) -> Result<(), Error> {
        let res = match sqlx::query(CREATE_EVENTS_TABLE)
            .execute(&self.pool)
            .await
        {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to create events table with error: \
                         {}",
                        e
                    )
                    .as_str(),
                ));
            },
        };

        debug!(
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
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to create snapshots table with \
                         error: {}",
                        e
                    )
                    .as_str(),
                ));
            },
        };

        debug!(
            "Created snapshots table with '{}' affected rows",
            res.rows_affected()
        );

        Ok(())
    }
}

#[async_trait]
impl<C: ICommand, E: IEvent, A: IAggregate<C, E>> IEventStore<C, E, A>
    for EventStore<C, E, A>
{
    /// Save new events
    async fn save_events(
        &mut self,
        contexts: &Vec<EventContext<C, E>>,
    ) -> Result<(), Error> {
        self.create_events_table().await?;

        if contexts.len() == 0 {
            trace!("Skip saving zero contexts");
            return Ok(());
        }

        let aggregate_type = A::aggregate_type();

        let aggregate_id = contexts
            .first()
            .unwrap()
            .aggregate_id
            .clone();

        debug!(
            "storing '{}' new events for aggregate id '{}'",
            contexts.len(),
            &aggregate_id
        );

        for context in contexts {
            let payload = match serde_json::to_value(&context.payload)
            {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "unable to serialize the event payload \
                             for aggregate id {} with error: {}",
                            &aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            let metadata =
                match serde_json::to_value(&context.metadata) {
                    Ok(x) => x,
                    Err(e) => {
                        return Err(Error::new(
                            format!(
                                "unable to serialize the event \
                                 metadata for aggregate id {} with \
                                 error: {}",
                                &aggregate_id, e
                            )
                            .as_str(),
                        ));
                    },
                };

            match sqlx::query(INSERT_EVENT)
                .bind(&aggregate_type)
                .bind(&aggregate_id)
                .bind(context.sequence)
                .bind(&payload)
                .bind(&metadata)
                .execute(&self.pool)
                .await
            {
                Ok(x) => {
                    if x.rows_affected() != 1 {
                        return Err(Error::new(
                            format!(
                                "insert new event failed for \
                                 aggregate id {}",
                                &aggregate_id
                            )
                            .as_str(),
                        ));
                    }
                },
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "unable to insert new event for \
                             aggregate id {} with error: {}",
                            &aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            }
        }

        Ok(())
    }

    /// Load all events for a particular `aggregate_id`
    async fn load_events(
        &mut self,
        aggregate_id: &str,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        self.create_events_table().await?;

        let aggregate_type = A::aggregate_type();

        trace!(
            "loading events for aggregate id '{}'",
            aggregate_id
        );

        let rows: Vec<(
            i64,
            serde_json::Value,
            serde_json::Value,
        )> = match sqlx::query_as(SELECT_EVENTS)
            .bind(&aggregate_type)
            .bind(&aggregate_id)
            .fetch_all(&self.pool)
            .await
        {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to load events table for aggregate \
                         id {} with error: {}",
                        &aggregate_id, e
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
                            &aggregate_id, e
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
                            &aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            result.push(EventContext::new(
                aggregate_id.to_string(),
                row.0,
                payload,
                metadata,
            ));
        }

        Ok(result)
    }

    /// save a new aggregate snapshot
    async fn save_aggregate_snapshot(
        &mut self,
        context: AggregateContext<C, E, A>,
    ) -> Result<(), Error> {
        self.create_snapshot_table().await?;

        let aggregate_type = A::aggregate_type();

        let aggregate_id = context.aggregate_id;

        debug!(
            "storing a new snapshot for aggregate id '{}'",
            &aggregate_id
        );

        let sql = match context.version {
            1 => INSERT_SNAPSHOT,
            _ => UPDATE_SNAPSHOT,
        };

        let payload = match serde_json::to_value(context.payload) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize aggregate snapshot for \
                         aggregate id {} with error: {}",
                        &aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        match sqlx::query(sql)
            .bind(context.version)
            .bind(payload)
            .bind(&aggregate_type)
            .bind(&aggregate_id)
            .execute(&self.pool)
            .await
        {
            Ok(x) => {
                if x.rows_affected() != 1 {
                    return Err(Error::new(
                        format!(
                            "insert new snapshot failed for \
                             aggregate id {}",
                            &aggregate_id
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
                        &aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(())
    }

    /// Load aggregate at current state from snapshots
    async fn load_aggregate_from_snapshot(
        &mut self,
        aggregate_id: &str,
    ) -> Result<AggregateContext<C, E, A>, Error> {
        self.create_snapshot_table().await?;

        let aggregate_type = A::aggregate_type();

        trace!(
            "loading snapshot for aggregate id '{}'",
            aggregate_id
        );

        let rows: Vec<(i64, serde_json::Value)> =
            match sqlx::query_as(SELECT_SNAPSHOT)
                .bind(&aggregate_type)
                .bind(&aggregate_id)
                .fetch_all(&self.pool)
                .await
            {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "unable to load snapshots table for \
                             aggregate id {} with error: {}",
                            &aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            };

        if rows.len() == 0 {
            trace!(
                "returning default aggregate for aggregate id '{}'",
                aggregate_id
            );

            return Ok(AggregateContext::new(
                aggregate_id.to_string(),
                0,
                A::default(),
            ));
        };

        let row = rows[0].clone();

        let payload = match serde_json::from_value(row.1) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "bad payload found in snapshots table for \
                         aggregate id {} with error: {}",
                        &aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(AggregateContext::new(
            aggregate_id.to_string(),
            row.0,
            payload,
        ))
    }
}
