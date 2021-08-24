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

use super::i_storage::IStorage;

/// Async query store.
pub struct QueryStore<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    Q: IQuery<C, E>,
    S: IStorage,
> {
    storage: S,
    _phantom: PhantomData<(C, E, A, Q)>,
}

impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
        S: IStorage,
    > QueryStore<C, E, A, Q, S>
{
    /// constructor.
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
        S: IStorage,
    > IQueryStore<C, E, A, Q> for QueryStore<C, E, A, Q, S>
{
    async fn load(
        &mut self,
        aggregate_id: &str,
    ) -> Result<QueryContext<C, E, Q>, Error> {
        let agg_type = A::aggregate_type();
        let query_type = Q::query_type();

        let rows = self
            .storage
            .select_query(agg_type, aggregate_id, query_type)
            .await?;

        if rows.len() == 0 {
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
                return Err(Error::new(e.to_string().as_str()));
            },
        };

        Ok(QueryContext::new(
            aggregate_id.to_string(),
            row.0,
            payload,
        ))
    }

    async fn commit(
        &mut self,
        context: QueryContext<C, E, Q>,
    ) -> Result<(), Error> {
        let agg_type = A::aggregate_type();
        let aggregate_id = context.aggregate_id.as_str();
        let query_type = Q::query_type();
        let version = context.version;

        // let query_instance_id = &self.query_instance_id;
        let payload = match serde_json::to_value(&context.payload) {
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

        self.storage
            .update_query(
                agg_type,
                aggregate_id,
                query_type,
                version,
                &payload,
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
        S: IStorage,
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
