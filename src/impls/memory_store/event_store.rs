use async_trait::async_trait;
use log::{
    debug,
    trace,
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        RwLock,
    },
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

type LockedEventContextMap<C, E> =
    RwLock<HashMap<String, Vec<EventContext<C, E>>>>;

type LockedAggregateContextMap<C, E, A> =
    RwLock<HashMap<String, AggregateContext<C, E, A>>>;

///  Simple memory store only useful for testing purposes
pub struct EventStore<C: ICommand, E: IEvent, A: IAggregate<C, E>> {
    events: Arc<LockedEventContextMap<C, E>>,
    snapshots: Arc<LockedAggregateContextMap<C, E, A>>,
}

impl<C: ICommand, E: IEvent, A: IAggregate<C, E>>
    EventStore<C, E, A>
{
    /// Constructor
    pub fn new(
        events: Arc<LockedEventContextMap<C, E>>,
        snapshots: Arc<LockedAggregateContextMap<C, E, A>>,
    ) -> Self {
        let x = Self { events, snapshots };

        trace!(
            "Created new async memory event store from passed Arcs"
        );

        x
    }
}

impl<C: ICommand, E: IEvent, A: IAggregate<C, E>> Default
    for EventStore<C, E, A>
{
    fn default() -> Self {
        let x = Self {
            events: Default::default(),
            snapshots: Default::default(),
        };

        trace!("Created default async memory event store");

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

        let mut new_contexts =
            self.load_events(&aggregate_id).await?;

        contexts
            .iter()
            .for_each(|x| new_contexts.push(x.clone()));

        // uninteresting unwrap: this is not a struct for production
        // use
        let mut map = self.events.write().unwrap();
        map.insert(aggregate_id, new_contexts);

        Ok(())
    }

    /// Load all events for a particular `aggregate_id`
    async fn load_events(
        &mut self,
        aggregate_id: &str,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        trace!(
            "loading events for aggregate id '{}'",
            aggregate_id
        );

        // uninteresting unwrap: this will not be used in production,
        // for tests only

        match self
            .events
            .read()
            .unwrap()
            .get(aggregate_id)
        {
            None => Ok(Vec::new()),
            Some(x) => Ok(x.clone()),
        }
    }

    /// save a new aggregate snapshot
    async fn save_aggregate_snapshot(
        &mut self,
        context: AggregateContext<C, E, A>,
    ) -> Result<(), Error> {
        let aggregate_id = context.aggregate_id.clone();

        debug!(
            "storing a new snapshot for aggregate id '{}'",
            &aggregate_id
        );

        // uninteresting unwrap: this is not a struct for production
        // use
        let mut map = self.snapshots.write().unwrap();
        map.insert(aggregate_id, context);

        Ok(())
    }

    /// Load aggregate at current state from snapshots
    async fn load_aggregate_from_snapshot(
        &mut self,
        aggregate_id: &str,
    ) -> Result<AggregateContext<C, E, A>, Error> {
        trace!(
            "loading snapshot for aggregate id '{}'",
            aggregate_id
        );

        // uninteresting unwrap: this will not be used in production,
        // for tests only

        match self
            .snapshots
            .read()
            .unwrap()
            .get(aggregate_id)
        {
            None => {
                Ok(AggregateContext::new(
                    aggregate_id.to_string(),
                    0,
                    A::default(),
                ))
            },
            Some(x) => Ok(x.clone()),
        }
    }
}
