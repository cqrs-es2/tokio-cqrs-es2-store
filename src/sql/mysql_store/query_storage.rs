use async_trait::async_trait;
use std::marker::PhantomData;

use sqlx::mysql::MySqlPool;

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

use super::super::mysql_constants::*;

/// MySql/MariaDB storage
pub struct QueryStorage<
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
    > QueryStorage<C, E, A, Q>
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
        let sql = match version {
            1 => INSERT_QUERY,
            _ => UPDATE_QUERY,
        };

        let payload = match serde_json::to_value(&payload) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize the payload of query \
                         '{}' with id: '{}', error: {}",
                        &query_type, &agg_id, e,
                    )
                    .as_str(),
                ));
            },
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
            Ok(x) => {
                if x.rows_affected() != 1 {
                    return Err(Error::new(
                        format!(
                            "insert/update query failed for \
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
                        "unable to insert/update query for \
                         aggregate id {} with error: {}",
                        &agg_id, e
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
                    return Err(Error::new(
                        format!(
                            "unable to load queries table for query \
                             '{}' with id: '{}', error: {}",
                            &query_type, &agg_id, e,
                        )
                        .as_str(),
                    ));
                },
            };

        if rows.len() == 0 {
            return Ok(None);
        }

        let row = rows[0].clone();

        let payload = match serde_json::from_value(row.1) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "bad payload found in queries table for \
                         query '{}' with id: '{}', error: {}",
                        &query_type, &agg_id, e,
                    )
                    .as_str(),
                ));
            },
        };

        Ok(Some((row.0, payload)))
    }
}

/// convenient type alias for MySql/MariaDB query store
pub type MySqlQueryStore<C, E, A, Q> =
    QueryStore<C, E, A, Q, QueryStorage<C, E, A, Q>>;
