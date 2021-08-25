use log::info;

use async_trait::async_trait;

use sqlx::sqlite::SqlitePool;

use cqrs_es2::Error;

use super::super::{
    event_store::EventStore,
    i_storage::IStorage,
    mysql_constants::*,
    query_store::QueryStore,
};

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

static CREATE_QUERY_TABLE: &str = "
CREATE TABLE IF NOT EXISTS
queries
(
    aggregate_type TEXT                        NOT NULL,
    aggregate_id   TEXT                        NOT NULL,
    query_type     TEXT                        NOT NULL,
    version        bigint CHECK (version >= 0) NOT NULL,
    payload        TEXT                        NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, query_type)
);
";

/// SQLite storage
pub struct Storage {
    pool: SqlitePool,
}

impl Storage {
    /// constructor
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
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

    async fn create_query_table(&mut self) -> Result<(), Error> {
        let res = match sqlx::query(CREATE_QUERY_TABLE)
            .execute(&self.pool)
            .await
        {
            Ok(x) => x,
            Err(e) => return Err(Error::new(e.to_string().as_str())),
        };

        info!(
            "Created queries table with '{}' affected rows",
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
impl IStorage for Storage {
    async fn insert_event(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        sequence: i64,
        payload: &serde_json::Value,
        metadata: &serde_json::Value,
    ) -> Result<(), Error> {
        self.create_events_table().await?;

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
        self.create_events_table().await?;

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
        self.create_events_table().await?;

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
        self.create_snapshot_table().await?;

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
        self.create_query_table().await?;

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
        self.create_query_table().await?;

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

/// convenient type alias for SQLite event store
pub type SqliteEventStore<C, E, A> = EventStore<C, E, A, Storage>;

/// convenient type alias for SQLite query store
pub type SqliteQueryStore<C, E, A, Q> =
    QueryStore<C, E, A, Q, Storage>;
