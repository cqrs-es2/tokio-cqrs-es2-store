use async_trait::async_trait;
use serde_json::json;
use std::{
    collections::HashMap,
    marker::PhantomData,
};

use redis::{
    Commands,
    Connection,
    RedisResult,
};

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

/// Redis storage
pub struct EventStorage<C: ICommand, E: IEvent, A: IAggregate<C, E>> {
    conn: Connection,
    _phantom: PhantomData<(C, E, A)>,
}

impl<C: ICommand, E: IEvent, A: IAggregate<C, E>>
    EventStorage<C, E, A>
{
    /// constructor
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
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
        let r = json!({
            "sequence": sequence,
            "payload": payload,
            "metadata": metadata
        });

        let r = match serde_json::to_string(&r) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize the event entry for
                          aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let res: RedisResult<()> = self.conn.rpush(
            format!("events;{};{}", agg_type, agg_id),
            r,
        );

        match res {
            Ok(_) => {},
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
        let key = format!("events;{};{}", agg_type, agg_id);

        let res: RedisResult<bool> = self.conn.exists(&key);

        let res = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to check events table for key {} \
                         with error: {}",
                        &key, e
                    )
                    .as_str(),
                ));
            },
        };

        match res {
            false => {
                return Ok(Vec::new());
            },
            true => {},
        }

        let res: RedisResult<Vec<String>> =
            self.conn.lrange(&key, 0, -1);

        let rows = match res {
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
            let v: serde_json::Value =
                match serde_json::from_str(row.as_str()) {
                    Ok(x) => x,
                    Err(e) => {
                        return Err(Error::new(
                            format!(
                                "unable to serialize entry from \
                                 events table for aggregate id {} \
                                 with error: {}",
                                &agg_id, e
                            )
                            .as_str(),
                        ));
                    },
                };

            let payload = match serde_json::from_value(
                v.get("payload").unwrap().clone(),
            ) {
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

            let metadata = match serde_json::from_value(
                v.get("metadata").unwrap().clone(),
            ) {
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

            let sequence: i64 = match serde_json::from_value(
                v.get("sequence").unwrap().clone(),
            ) {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "bad sequence found in events table for \
                             aggregate id {} with error: {}",
                            &agg_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            result.push((sequence as usize, payload, metadata));
        }

        Ok(result)
    }

    async fn update_snapshot(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        last_sequence: i64,
        payload: &A,
        _current_sequence: usize,
    ) -> Result<(), Error> {
        let r = json!({
            "last_sequence": last_sequence,
            "payload": payload,
        });

        let r = match serde_json::to_string(&r) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize the snapshot entry for
                          aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let res: RedisResult<()> = self.conn.set(
            format!("snapshots;{};{}", agg_type, agg_id),
            r,
        );

        match res {
            Ok(_) => {},
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
        let key = format!("snapshots;{};{}", agg_type, agg_id);

        let res: RedisResult<bool> = self.conn.exists(&key);

        let res = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to check snapshots table for key {} \
                         with error: {}",
                        &key, e
                    )
                    .as_str(),
                ));
            },
        };

        match res {
            false => {
                return Ok(None);
            },
            true => {},
        }

        let res: RedisResult<String> = self.conn.get(&key);

        let res = match res {
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

        let v: serde_json::Value =
            match serde_json::from_str(res.as_str()) {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "unable to serialize entry from \
                             snapshots table for aggregate id {} \
                             with error: {}",
                            &agg_id, e
                        )
                        .as_str(),
                    ));
                },
            };

        let payload = match serde_json::from_value(
            v.get("payload").unwrap().clone(),
        ) {
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

        let last_sequence: i64 = match serde_json::from_value(
            v.get("last_sequence").unwrap().clone(),
        ) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "bad last_sequence found in events table \
                         for aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(Some((last_sequence as usize, payload)))
    }
}

/// convenient type alias for Redis event store
pub type RedisEventStore<C, E, A> =
    EventStore<C, E, A, EventStorage<C, E, A>>;
