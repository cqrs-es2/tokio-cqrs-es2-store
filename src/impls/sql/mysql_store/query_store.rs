use async_trait::async_trait;
use log::{
    debug,
    trace,
};
use std::marker::PhantomData;

use sqlx::mysql::MySqlPool;

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

use super::super::mysql_constants::*;

/// Async MySql/MariaDB query store
pub struct QueryStore<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    Q: IQuery<C, E>,
> {
    pool: MySqlPool,
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
    pub fn new(pool: MySqlPool) -> Self {
        let x = Self {
            pool,
            _phantom: PhantomData,
        };

        trace!("Created new async MySQL query store");

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
            "storing a new query '{}' for aggregate id '{}'",
            query_type, &aggregate_id
        );

        let sql = match context.version {
            1 => INSERT_QUERY,
            _ => UPDATE_QUERY,
        };

        let payload = match serde_json::to_value(context.payload) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize the payload of query \
                         '{}' with id: '{}', error: {}",
                        &query_type, &aggregate_id, e,
                    )
                    .as_str(),
                ));
            },
        };

        match sqlx::query(sql)
            .bind(context.version)
            .bind(&payload)
            .bind(&aggregate_type)
            .bind(&aggregate_id)
            .bind(&query_type)
            .execute(&self.pool)
            .await
        {
            Ok(x) => {
                if x.rows_affected() != 1 {
                    return Err(Error::new(
                        format!(
                            "insert/update query failed for \
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
                        "unable to insert/update query for \
                         aggregate id {} with error: {}",
                        &aggregate_id, e
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

        let rows: Vec<(i64, serde_json::Value)> =
            match sqlx::query_as(SELECT_QUERY)
                .bind(&aggregate_type)
                .bind(&aggregate_id)
                .bind(&query_type)
                .fetch_all(&self.pool)
                .await
            {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "unable to load queries table for query \
                             '{}' with id: '{}', error: {}",
                            &query_type, &aggregate_id, e,
                        )
                        .as_str(),
                    ));
                },
            };

        if rows.len() == 0 {
            trace!(
                "returning default query '{}' for aggregate id '{}'",
                query_type,
                aggregate_id
            );

            return Ok(QueryContext::new(
                aggregate_id.to_string(),
                0,
                Default::default(),
            ));
        }

        let row = rows[0].clone();

        let payload = match serde_json::from_value(row.1) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "bad payload found in queries table for \
                         query '{}' with id: '{}', error: {}",
                        &query_type, &aggregate_id, e,
                    )
                    .as_str(),
                ));
            },
        };

        Ok(QueryContext::new(
            aggregate_id.to_string(),
            row.0,
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
