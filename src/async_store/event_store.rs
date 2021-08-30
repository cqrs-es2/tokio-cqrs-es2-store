use async_trait::async_trait;
use std::{
    collections::HashMap,
    marker::PhantomData,
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

use super::i_event_storage::IEventStorage;

/// Async event store.
pub struct EventStore<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    S: IEventStorage<C, E, A>,
> {
    storage: S,
    with_snapshots: bool,
    _phantom: PhantomData<(C, E, A)>,
}

impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        S: IEventStorage<C, E, A>,
    > EventStore<C, E, A, S>
{
    /// constructor
    pub fn new(
        storage: S,
        with_snapshots: bool,
    ) -> Self {
        Self {
            storage,
            with_snapshots,
            _phantom: PhantomData,
        }
    }

    async fn load_aggregate_from_snapshot(
        &mut self,
        aggregate_id: &str,
    ) -> Result<AggregateContext<C, E, A>, Error> {
        let agg_type = A::aggregate_type();
        let id = aggregate_id.to_string();

        let row = self
            .storage
            .select_snapshot(&agg_type, &id)
            .await?;

        match row {
            None => {
                Ok(AggregateContext::new(
                    id,
                    A::default(),
                    0,
                ))
            },
            Some(x) => Ok(AggregateContext::new(id, x.1, x.0)),
        }
    }

    async fn commit_with_snapshots(
        &mut self,
        events: Vec<E>,
        context: AggregateContext<C, E, A>,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        let agg_type = A::aggregate_type().to_string();
        let aggregate_id = context.aggregate_id.as_str();
        let current_sequence = context.current_sequence;

        let contexts = self.wrap_events(
            aggregate_id,
            current_sequence,
            events,
            metadata,
        );

        let mut last_sequence = current_sequence as i64;
        let mut updated_aggregate = context.aggregate.clone();

        for context in contexts.clone() {
            let sequence = context.sequence as i64;
            last_sequence = sequence;

            self.storage
                .insert_event(
                    &agg_type,
                    &aggregate_id,
                    sequence,
                    &context.payload,
                    &context.metadata,
                )
                .await?;

            updated_aggregate.apply(&context.payload);
        }

        self.storage
            .update_snapshot(
                &agg_type,
                &aggregate_id,
                last_sequence,
                &updated_aggregate,
                context.current_sequence,
            )
            .await?;

        Ok(contexts)
    }

    async fn commit_events_only(
        &mut self,
        events: Vec<E>,
        context: AggregateContext<C, E, A>,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        let agg_type = A::aggregate_type().to_string();
        let aggregate_id = context.aggregate_id.as_str();
        let current_sequence = context.current_sequence;

        let contexts = self.wrap_events(
            &aggregate_id,
            current_sequence,
            events,
            metadata,
        );

        for context in &contexts {
            let sequence = context.sequence as i64;

            self.storage
                .insert_event(
                    &agg_type,
                    &aggregate_id,
                    sequence,
                    &context.payload,
                    &context.metadata,
                )
                .await?;
        }

        Ok(contexts)
    }
}

#[async_trait]
impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        S: IEventStorage<C, E, A>,
    > IEventStore<C, E, A> for EventStore<C, E, A, S>
{
    async fn load_events(
        &mut self,
        aggregate_id: &str,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        let agg_type = A::aggregate_type();

        let rows = self
            .storage
            .select_events(agg_type, aggregate_id)
            .await?;

        Ok(rows
            .iter()
            .map(|x| {
                EventContext::new(
                    aggregate_id.to_string(),
                    x.0 as usize,
                    x.1.clone(),
                    x.2.clone(),
                )
            })
            .collect())
    }
}
