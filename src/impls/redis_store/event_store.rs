use async_trait::async_trait;
use log::{
    debug,
    trace,
};
use serde_json::json;
use std::marker::PhantomData;

use redis::{
    Commands,
    Connection,
    RedisResult,
};

use cqrs_es2::{
    AggregateContext,
    Error,
    EventContext,
    IAggregate,
    ICommand,
    IEvent,
};

use crate::repository::IEventStore;

/// Async Redis event store
pub struct EventStore<C: ICommand, E: IEvent, A: IAggregate<C, E>> {
    conn: Connection,
    _phantom: PhantomData<(C, E, A)>,
}

impl<C: ICommand, E: IEvent, A: IAggregate<C, E>>
    EventStore<C, E, A>
{
    /// Constructor
    pub fn new(conn: Connection) -> Self {
        let x = Self {
            conn,
            _phantom: PhantomData,
        };

        trace!("Created new async Redis event store");

        x
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

        let key = format!(
            "events;{};{}",
            aggregate_type, &aggregate_id
        );

        for context in contexts {
            let r = json!({
                "sequence": context.sequence,
                "payload": context.payload,
                "metadata": context.metadata
            });

            let r = match serde_json::to_string(&r) {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "unable to serialize the event entry for
                              aggregate id '{}' with error: {}",
                            &aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            let res: RedisResult<()> = self.conn.rpush(&key, r);

            match res {
                Ok(_) => {},
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "unable to insert new event for \
                             aggregate id '{}' with error: {}",
                            &aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            };
        }

        Ok(())
    }

    /// Load all events for a particular `aggregate_id`
    async fn load_events(
        &mut self,
        aggregate_id: &str,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        let aggregate_type = A::aggregate_type();

        trace!(
            "loading events for aggregate id '{}'",
            aggregate_id
        );

        let key = format!(
            "events;{};{}",
            aggregate_type, aggregate_id
        );

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
                         id '{}' with error: {}",
                        aggregate_id, e
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
                                 events table for aggregate id '{}' \
                                 with error: {}",
                                aggregate_id, e
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
                             aggregate id '{}' with error: {}",
                            aggregate_id, e
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
                             aggregate id '{}' with error: {}",
                            aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            let sequence = match serde_json::from_value(
                v.get("sequence").unwrap().clone(),
            ) {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "bad sequence found in events table for \
                             aggregate id '{}' with error: {}",
                            aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            result.push(EventContext::new(
                aggregate_id.to_string(),
                sequence,
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
        let aggregate_type = A::aggregate_type();

        let aggregate_id = context.aggregate_id;

        debug!(
            "storing a new snapshot for aggregate id '{}'",
            &aggregate_id
        );

        let r = json!({
            "version": context.version,
            "payload": context.payload,
        });

        let r = match serde_json::to_string(&r) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize the snapshot entry for
                          aggregate id '{}' with error: {}",
                        &aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let res: RedisResult<()> = self.conn.set(
            format!(
                "snapshots;{};{}",
                aggregate_type, &aggregate_id
            ),
            r,
        );

        match res {
            Ok(_) => {},
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to insert new snapshot for \
                         aggregate id '{}' with error: {}",
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
        let aggregate_type = A::aggregate_type();

        trace!(
            "loading snapshot for aggregate id '{}'",
            aggregate_id
        );

        let key = format!(
            "snapshots;{};{}",
            aggregate_type, aggregate_id
        );

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
            true => {},
            false => {
                trace!(
                    "returning default aggregate for aggregate id \
                     '{}'",
                    aggregate_id
                );

                return Ok(AggregateContext::new(
                    aggregate_id.to_string(),
                    0,
                    A::default(),
                ));
            },
        }

        let res: RedisResult<String> = self.conn.get(&key);

        let res = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to load snapshots table for key {} \
                         with error: {}",
                        &key, e
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
                             snapshots table for key {} with error: \
                             {}",
                            &key, e
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
                        "bad payload found in events table for key \
                         {} with error: {}",
                        &key, e
                    )
                    .as_str(),
                ));
            },
        };

        let version = match serde_json::from_value(
            v.get("version").unwrap().clone(),
        ) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "bad version found in events table for key \
                         {} with error: {}",
                        &key, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(AggregateContext::new(
            aggregate_id.to_string(),
            version,
            payload,
        ))
    }
}
