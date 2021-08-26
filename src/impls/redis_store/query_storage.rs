use async_trait::async_trait;
use serde_json::json;
use std::marker::PhantomData;

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
    IQuery,
};

use crate::async_store::{
    IQueryStorage,
    QueryStore,
};

/// Redis storage
pub struct QueryStorage<
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
    > QueryStorage<C, E, A, Q>
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
impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
    > IQueryStorage<C, E, A, Q> for QueryStorage<C, E, A, Q>
{
    async fn update_query(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        query_type: &str,
        version: i64,
        payload: &Q,
    ) -> Result<(), Error> {
        let r = json!({
            "version": version,
            "payload": payload,
        });

        let r = match serde_json::to_string(&r) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize the query entry for
                          aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let res: RedisResult<()> = self.conn.set(
            format!(
                "queries;{};{};{}",
                agg_type, agg_id, query_type
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
                        query_type, &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(())
    }

    async fn select_query(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        query_type: &str,
    ) -> Result<Option<(i64, Q)>, Error> {
        let key = format!(
            "queries;{};{};{}",
            agg_type, agg_id, query_type
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
                        "unable to load queries table for query {} \
                         aggregate id {} with error: {}",
                        query_type, &agg_id, e
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
                             table for aggregate id {} with error: \
                             {}",
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
                        "bad payload found in queries table for \
                         aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let version: i64 = match serde_json::from_value(
            v.get("version").unwrap().clone(),
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

        Ok(Some((version, payload)))
    }
}

/// convenient type alias for Redis query store
pub type RedisQueryStore<C, E, A, Q> =
    QueryStore<C, E, A, Q, QueryStorage<C, E, A, Q>>;
