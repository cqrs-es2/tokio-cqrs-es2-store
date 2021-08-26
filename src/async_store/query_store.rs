use async_trait::async_trait;
use std::marker::PhantomData;

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

use super::i_query_storage::IQueryStorage;

/// Async query store.
pub struct QueryStore<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    Q: IQuery<C, E>,
    S: IQueryStorage<C, E, A, Q>,
> {
    storage: S,
    _phantom: PhantomData<(C, E, A, Q)>,
}

impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
        S: IQueryStorage<C, E, A, Q>,
    > QueryStore<C, E, A, Q, S>
{
    /// constructor
    #[must_use]
    pub fn new(storage: S) -> Self {
        Self {
            storage,
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
        S: IQueryStorage<C, E, A, Q>,
    > IQueryStore<C, E, A, Q> for QueryStore<C, E, A, Q, S>
{
    async fn load(
        &mut self,
        aggregate_id: &str,
    ) -> Result<QueryContext<C, E, Q>, Error> {
        let agg_type = A::aggregate_type();
        let query_type = Q::query_type();

        let row = self
            .storage
            .select_query(agg_type, aggregate_id, query_type)
            .await?;

        match row {
            None => {
                Ok(QueryContext::new(
                    aggregate_id.to_string(),
                    0,
                    Default::default(),
                ))
            },
            Some(x) => {
                Ok(QueryContext::new(
                    aggregate_id.to_string(),
                    x.0,
                    x.1,
                ))
            },
        }
    }

    async fn commit(
        &mut self,
        context: QueryContext<C, E, Q>,
    ) -> Result<(), Error> {
        let agg_type = A::aggregate_type();
        let aggregate_id = context.aggregate_id.as_str();
        let query_type = Q::query_type();
        let version = context.version;

        self.storage
            .update_query(
                agg_type,
                aggregate_id,
                query_type,
                version,
                &context.payload,
            )
            .await?;

        Ok(())
    }
}

#[async_trait]
impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
        S: IQueryStorage<C, E, A, Q>,
    > IEventDispatcher<C, E> for QueryStore<C, E, A, Q, S>
{
    async fn dispatch(
        &mut self,
        aggregate_id: &str,
        events: &[EventContext<C, E>],
    ) -> Result<(), Error> {
        self.dispatch_events(aggregate_id, events)
            .await
    }
}
