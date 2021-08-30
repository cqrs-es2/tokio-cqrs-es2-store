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
    Error,
    EventContext,
    IAggregate,
    ICommand,
    IEvent,
    IQuery,
    QueryContext,
};

use crate::repository::{
    IEventDispatcher,
    IQueryStore,
};

/// Async Redis query store
pub struct QueryStore<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    Q: IQuery<C, E>,
> {
    conn: Connection,
    _phantom: PhantomData<(C, E, A, Q)>,
}

impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
    > QueryStore<C, E, A, Q>
{
    /// constructor
    pub fn new(conn: Connection) -> Self {
        let x = Self {
            conn,
            _phantom: PhantomData,
        };

        trace!("Created new async Redis query store");

        x
    }
}

#[async_trait]
impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
    > IQueryStore<C, E, A, Q> for QueryStore<C, E, A, Q>
{
    /// saves the updated query
    async fn save_query(
        &mut self,
        context: QueryContext<C, E, Q>,
    ) -> Result<(), Error> {
        let aggregate_type = A::aggregate_type();
        let query_type = Q::query_type();

        let aggregate_id = context.aggregate_id;

        debug!(
            "storing a new query for aggregate id '{}'",
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
                        "unable to serialize the query entry for
                          aggregate id {} with error: {}",
                        &aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let res: RedisResult<()> = self.conn.set(
            format!(
                "queries;{};{};{}",
                aggregate_type, aggregate_id, query_type
            ),
            r,
        );

        match res {
            Ok(_) => {},
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to insert new query {} for \
                         aggregate id {} with error: {}",
                        query_type, &aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(())
    }

    /// loads the most recent query
    async fn load_query(
        &mut self,
        aggregate_id: &str,
    ) -> Result<QueryContext<C, E, Q>, Error> {
        let aggregate_type = A::aggregate_type();
        let query_type = Q::query_type();

        trace!(
            "loading query '{}' for aggregate id '{}'",
            query_type,
            aggregate_id
        );

        let key = format!(
            "queries;{};{};{}",
            aggregate_type, aggregate_id, query_type
        );

        let res: RedisResult<bool> = self.conn.exists(&key);

        let res = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to check queries table for key {} \
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
                    "returning default query '{}' for aggregate id \
                     '{}'",
                    query_type,
                    aggregate_id
                );

                return Ok(QueryContext::new(
                    aggregate_id.to_string(),
                    0,
                    Default::default(),
                ));
            },
        }

        let res: RedisResult<String> = self.conn.get(&key);

        let res = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to load queries table for key {} \
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
                            "unable to serialize entry from queries \
                             table for key {} with error: {}",
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
                        "bad payload found in queries table for key \
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
                        "bad version found in queries table for key \
                         {} with error: {}",
                        &key, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(QueryContext::new(
            aggregate_id.to_string(),
            version,
            payload,
        ))
    }
}

#[async_trait]
impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
    > IEventDispatcher<C, E> for QueryStore<C, E, A, Q>
{
    async fn dispatch(
        &mut self,
        aggregate_id: &str,
        events: &Vec<EventContext<C, E>>,
    ) -> Result<(), Error> {
        self.dispatch_events(aggregate_id, events)
            .await
    }
}
