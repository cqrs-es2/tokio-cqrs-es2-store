use async_trait::async_trait;
use log::{
    debug,
    trace,
};
use std::marker::PhantomData;

use cqrs_es2::{
    AggregateContext,
    Error,
    EventContext,
    IAggregate,
    ICommand,
    IEvent,
};

use super::i_event_store::IEventStore;

/// Async cached event store
pub struct CachedEventStore<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    ES: IEventStore<C, E, A>,
    EC: IEventStore<C, E, A>,
> {
    store: ES,
    cache: EC,
    _phantom: PhantomData<(C, E, A)>,
}

impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        ES: IEventStore<C, E, A>,
        EC: IEventStore<C, E, A>,
    > CachedEventStore<C, E, A, ES, EC>
{
    /// Constructor
    pub fn new(
        store: ES,
        cache: EC,
    ) -> Self {
        let x = Self {
            store,
            cache,
            _phantom: PhantomData,
        };

        trace!("Created new async cached event store");

        x
    }
}

#[async_trait]
impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        ES: IEventStore<C, E, A>,
        EC: IEventStore<C, E, A>,
    > IEventStore<C, E, A> for CachedEventStore<C, E, A, ES, EC>
{
    /// Save new events
    async fn save_events(
        &mut self,
        contexts: &Vec<EventContext<C, E>>,
    ) -> Result<(), Error> {
        self.store.save_events(contexts).await
    }

    /// Load all events for a particular `aggregate_id`
    async fn load_events(
        &mut self,
        aggregate_id: &str,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        self.store
            .load_events(aggregate_id)
            .await
    }

    /// save a new aggregate snapshot
    async fn save_aggregate_snapshot(
        &mut self,
        context: AggregateContext<C, E, A>,
    ) -> Result<(), Error> {
        self.cache
            .save_aggregate_snapshot(context.clone())
            .await?;
        self.store
            .save_aggregate_snapshot(context)
            .await?;

        Ok(())
    }

    /// Load aggregate at current state from snapshots
    async fn load_aggregate_from_snapshot(
        &mut self,
        aggregate_id: &str,
    ) -> Result<AggregateContext<C, E, A>, Error> {
        let result = self
            .cache
            .load_aggregate_from_snapshot(aggregate_id)
            .await?;

        if result.version == 0 {
            debug!("cache miss");
            self.store
                .load_aggregate_from_snapshot(aggregate_id)
                .await
        }
        else {
            debug!("cache hit");
            Ok(result)
        }
    }
}
