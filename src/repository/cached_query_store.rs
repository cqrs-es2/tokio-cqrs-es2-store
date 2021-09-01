use async_trait::async_trait;
use log::{
    debug,
    trace,
};
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

use super::{
    i_event_dispatcher::IEventDispatcher,
    i_query_store::IQueryStore,
};

/// Async cached query store
pub struct CachedQueryStore<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    Q: IQuery<C, E>,
    QS: IQueryStore<C, E, A, Q>,
    QC: IQueryStore<C, E, A, Q>,
> {
    store: QS,
    cache: QC,
    _phantom: PhantomData<(C, E, A, Q)>,
}

impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
        QS: IQueryStore<C, E, A, Q>,
        QC: IQueryStore<C, E, A, Q>,
    > CachedQueryStore<C, E, A, Q, QS, QC>
{
    /// Constructor
    pub fn new(
        store: QS,
        cache: QC,
    ) -> Self {
        let x = Self {
            store,
            cache,
            _phantom: PhantomData,
        };

        trace!("Created new async cached query store");

        x
    }
}

#[async_trait]
impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
        QS: IQueryStore<C, E, A, Q>,
        QC: IQueryStore<C, E, A, Q>,
    > IQueryStore<C, E, A, Q>
    for CachedQueryStore<C, E, A, Q, QS, QC>
{
    /// saves the updated query
    async fn save_query(
        &mut self,
        context: QueryContext<C, E, Q>,
    ) -> Result<(), Error> {
        self.cache
            .save_query(context.clone())
            .await?;
        self.store.save_query(context).await?;

        Ok(())
    }

    /// loads the most recent query
    async fn load_query(
        &mut self,
        aggregate_id: &str,
    ) -> Result<QueryContext<C, E, Q>, Error> {
        let result = self
            .cache
            .load_query(aggregate_id)
            .await?;

        if result.version == 0 {
            debug!("cache miss");
            self.store
                .load_query(aggregate_id)
                .await
        }
        else {
            debug!("cache hit");
            Ok(result)
        }
    }
}

#[async_trait]
impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
        QS: IQueryStore<C, E, A, Q>,
        QC: IQueryStore<C, E, A, Q>,
    > IEventDispatcher<C, E>
    for CachedQueryStore<C, E, A, Q, QS, QC>
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
