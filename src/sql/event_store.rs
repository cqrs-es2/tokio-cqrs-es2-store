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

use super::i_storage::IStorage;

/// Async event store.
pub struct EventStore<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    S: IStorage,
> {
    storage: S,
    with_snapshots: bool,
    _phantom: PhantomData<(C, E, A)>,
}

impl<C: ICommand, E: IEvent, A: IAggregate<C, E>, S: IStorage>
    EventStore<C, E, A, S>
{
    /// constructor.
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

        let rows = self
            .storage
            .select_snapshot(&agg_type, &id)
            .await?;

        if rows.len() == 0 {
            return Ok(AggregateContext::new(
                id,
                A::default(),
                0,
            ));
        };

        let row = rows[0].clone();

        let aggregate = match serde_json::from_value(row.1) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "bad payload found in events table for \
                         aggregate id {} with error: {}",
                        &id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(AggregateContext::new(
            id,
            aggregate,
            row.0 as usize,
        ))
    }

    async fn load_aggregate_from_events(
        &mut self,
        aggregate_id: &str,
    ) -> Result<AggregateContext<C, E, A>, Error> {
        let id = aggregate_id.to_string();

        let events = self.load_events(&id, false).await?;

        if events.len() == 0 {
            return Ok(AggregateContext::new(
                id,
                A::default(),
                0,
            ));
        }

        let mut aggregate = A::default();

        events
            .iter()
            .map(|x| &x.payload)
            .for_each(|x| aggregate.apply(&x));

        Ok(AggregateContext::new(
            id,
            aggregate,
            events.last().unwrap().sequence,
        ))
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

            let payload = match serde_json::to_value(&context.payload)
            {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "could not serialize payload for \
                             aggregate id {} with error: {}",
                            &aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            let metadata =
                match serde_json::to_value(&context.metadata) {
                    Ok(x) => x,
                    Err(e) => {
                        return Err(Error::new(
                            format!(
                                "could not serialize metadata for \
                                 aggregate id {} with error: {}",
                                &aggregate_id, e
                            )
                            .as_str(),
                        ));
                    },
                };

            self.storage
                .insert_event(
                    &agg_type,
                    &aggregate_id,
                    sequence,
                    &payload,
                    &metadata,
                )
                .await?;

            updated_aggregate.apply(&context.payload);
        }

        let aggregate_payload =
            match serde_json::to_value(updated_aggregate) {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "could not serialize aggregate snapshot \
                             for aggregate id {} with error: {}",
                            &aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            };

        self.storage
            .update_snapshot(
                &agg_type,
                &aggregate_id,
                last_sequence,
                &aggregate_payload,
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

            let payload = match serde_json::to_value(&context.payload)
            {
                Ok(x) => x,
                Err(err) => {
                    return Err(Error::new(
                        format!(
                            "Could not serialize the event payload \
                             for aggregate id {} with error: {}",
                            &aggregate_id, err
                        )
                        .as_str(),
                    ));
                },
            };

            let metadata =
                match serde_json::to_value(&context.metadata) {
                    Ok(x) => x,
                    Err(err) => {
                        return Err(Error::new(
                            format!(
                                "could not serialize the event \
                                 metadata for aggregate id {} with \
                                 error: {}",
                                &aggregate_id, err
                            )
                            .as_str(),
                        ));
                    },
                };

            self.storage
                .insert_event(
                    &agg_type,
                    &aggregate_id,
                    sequence,
                    &payload,
                    &metadata,
                )
                .await?;
        }

        Ok(contexts)
    }

    async fn load_events_only(
        &mut self,
        aggregate_id: &str,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        let agg_type = A::aggregate_type();

        let rows = self
            .storage
            .select_events_only(agg_type, aggregate_id)
            .await?;

        let mut result = Vec::new();

        for row in rows {
            let payload = match serde_json::from_value(row.1) {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "bad payload found in events table for \
                             aggregate id {} with error: {}",
                            &aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            result.push(EventContext::new(
                aggregate_id.to_string(),
                row.0 as usize,
                payload,
                Default::default(),
            ));
        }

        Ok(result)
    }

    async fn load_events_with_metadata(
        &mut self,
        aggregate_id: &str,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        let agg_type = A::aggregate_type();

        let rows = self
            .storage
            .select_events_with_metadata(agg_type, aggregate_id)
            .await?;

        let mut result = Vec::new();

        for row in rows {
            let payload = match serde_json::from_value(row.1) {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "bad payload found in events table for \
                             aggregate id {} with error: {}",
                            &aggregate_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            let metadata = match serde_json::from_value(row.2) {
                Ok(x) => x,
                Err(err) => {
                    return Err(Error::new(
                        format!(
                            "bad metadata found in events table for \
                             aggregate id {} with error: {}",
                            &aggregate_id, err
                        )
                        .as_str(),
                    ));
                },
            };

            result.push(EventContext::new(
                aggregate_id.to_string(),
                row.0 as usize,
                payload,
                metadata,
            ));
        }

        Ok(result)
    }
}

#[async_trait]
impl<C: ICommand, E: IEvent, A: IAggregate<C, E>, S: IStorage>
    IEventStore<C, E, A> for EventStore<C, E, A, S>
{
    async fn load_events(
        &mut self,
        aggregate_id: &str,
        with_metadata: bool,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        match with_metadata {
            true => {
                self.load_events_with_metadata(aggregate_id)
                    .await
            },
            false => {
                self.load_events_only(aggregate_id)
                    .await
            },
        }
    }

    async fn load_aggregate(
        &mut self,
        aggregate_id: &str,
    ) -> Result<AggregateContext<C, E, A>, Error> {
        match self.with_snapshots {
            true => {
                self.load_aggregate_from_snapshot(aggregate_id)
                    .await
            },
            false => {
                self.load_aggregate_from_events(aggregate_id)
                    .await
            },
        }
    }

    async fn commit(
        &mut self,
        events: Vec<E>,
        context: AggregateContext<C, E, A>,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        match self.with_snapshots {
            true => {
                self.commit_with_snapshots(events, context, metadata)
                    .await
            },
            false => {
                self.commit_events_only(events, context, metadata)
                    .await
            },
        }
    }
}
