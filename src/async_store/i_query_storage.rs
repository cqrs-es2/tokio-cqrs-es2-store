use async_trait::async_trait;

use cqrs_es2::{
    Error,
    IAggregate,
    ICommand,
    IEvent,
    IQuery,
};

/// Async query storage interface
#[async_trait]
pub trait IQueryStorage<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    Q: IQuery<C, E>,
>: Send {
    /// update a query
    async fn update_query(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        query_type: &str,
        version: i64,
        payload: &Q,
    ) -> Result<(), Error>;

    /// get a query
    async fn select_query(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        query_type: &str,
    ) -> Result<Option<(i64, Q)>, Error>;
}
