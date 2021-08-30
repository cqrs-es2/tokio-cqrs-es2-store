use log::{
    debug,
    error,
    trace,
};
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

use super::{
    i_event_dispatcher::IEventDispatcher,
    i_event_store::IEventStore,
};

/// This is the base framework for applying commands to produce
/// events.
///
/// In [Domain Driven Design](https://en.wikipedia.org/wiki/Domain-driven_design)
/// we require that changes are made only after loading the entire
/// `Aggregate` in order to ensure that the full context is
/// understood. With event-sourcing this means:
/// 1. loading all previous events for the aggregate instance
/// 2. applying these events, in order, to a new `Aggregate`
/// 3. using the recreated `Aggregate` to handle an inbound `Command`
/// 4. persisting any generated events or rolling back on an error
///
/// To manage these tasks we use a `Repository`.
pub struct Repository<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    ES: IEventStore<C, E, A>,
> {
    store: ES,
    dispatchers: Vec<Box<dyn IEventDispatcher<C, E>>>,
    with_snapshots: bool,
    _phantom: PhantomData<A>,
}

impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        ES: IEventStore<C, E, A>,
    > Repository<C, E, A, ES>
{
    /// Creates new framework for dispatching commands using the
    /// provided elements.
    pub fn new(
        store: ES,
        dispatchers: Vec<Box<dyn IEventDispatcher<C, E>>>,
        with_snapshots: bool,
    ) -> Self {
        let x = Self {
            store,
            dispatchers,
            with_snapshots,
            _phantom: PhantomData,
        };

        trace!("Created new async Repository");

        x
    }

    /// This applies a command to an aggregate. Executing a command
    /// in this way is the only way to make any change to
    /// the state of an aggregate.
    ///
    /// An error while processing will result in no events committed
    /// and an Error being returned.
    ///
    /// If successful the events produced will be applied to the
    /// configured `QueryProcessor`s.
    ///
    /// # Error
    /// If an error is generated while processing the command this
    /// will be returned.
    pub async fn execute(
        &mut self,
        aggregate_id: &str,
        command: C,
    ) -> Result<(), Error> {
        self.execute_with_metadata(
            aggregate_id,
            command,
            HashMap::new(),
        )
        .await
    }

    /// This applies a command to an aggregate along with associated
    /// metadata. Executing a command in this way to make any
    /// change to the state of an aggregate.
    ///
    /// A `Hashmap<String,String>` is supplied with any contextual
    /// information that should be associated with this change.
    /// This metadata will be attached to any produced events and is
    /// meant to assist in debugging and auditing. Common information
    /// might include:
    /// - time of commit
    /// - user making the change
    /// - application version
    ///
    /// An error while processing will result in no events committed
    /// and an Error being returned.
    ///
    /// If successful the events produced will be applied to the
    /// configured `QueryProcessor`s.
    pub async fn execute_with_metadata(
        &mut self,
        aggregate_id: &str,
        command: C,
        metadata: HashMap<String, String>,
    ) -> Result<(), Error> {
        trace!(
            "Applying command '{:?}' to aggregate '{}' with \
             metadata '{:?}'",
            &command,
            &aggregate_id,
            &metadata
        );

        let stored_context =
            match self.load_aggregate(&aggregate_id).await {
                Ok(x) => x,
                Err(e) => {
                    error!(
                        "Loading aggregate '{}' returned error '{}'",
                        &aggregate_id,
                        e.to_string()
                    );
                    return Err(e);
                },
            };

        let events = match stored_context
            .payload
            .handle(command.clone())
        {
            Ok(x) => x,
            Err(e) => {
                error!(
                    "Handling command '{:?}' for aggregate '{}' \
                     returned error '{}'",
                    &command,
                    &aggregate_id,
                    e.to_string()
                );
                return Err(e);
            },
        };

        if events.len() == 0 {
            return Ok(());
        }

        let event_contexts = match self
            .save_events(events, stored_context, metadata)
            .await
        {
            Ok(x) => x,
            Err(e) => {
                error!(
                    "Committing events returned error '{}'",
                    e.to_string()
                );
                return Err(e);
            },
        };

        for x in &mut self.dispatchers {
            match x
                .dispatch(&aggregate_id, &event_contexts)
                .await
            {
                Ok(_) => {},
                Err(e) => {
                    error!(
                        "dispatcher returned error '{}'",
                        e.to_string()
                    );
                    return Err(e);
                },
            }
        }

        debug!(
            "Successfully applied command '{:?}' to aggregate '{}' ",
            &command, &aggregate_id
        );

        Ok(())
    }

    async fn load_aggregate(
        &mut self,
        aggregate_id: &str,
    ) -> Result<AggregateContext<C, E, A>, Error> {
        match self.with_snapshots {
            true => {
                self.store
                    .load_aggregate_from_snapshot(aggregate_id)
                    .await
            },
            false => {
                self.load_aggregate_from_events(aggregate_id)
                    .await
            },
        }
    }

    async fn save_events(
        &mut self,
        events: Vec<E>,
        stored_context: AggregateContext<C, E, A>,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        let aggregate_id = stored_context.aggregate_id;

        let contexts = self.wrap_events(
            &aggregate_id,
            stored_context.version,
            events,
            metadata,
        );

        match self.store.save_events(&contexts).await {
            Ok(_) => {},
            Err(e) => {
                error!(
                    "save events returned error '{}'",
                    e.to_string()
                );
                return Err(e);
            },
        };

        if self.with_snapshots {
            let mut aggregate = stored_context.payload;

            contexts
                .iter()
                .map(|x| &x.payload)
                .for_each(|x| aggregate.apply(&x));

            match self
                .store
                .save_aggregate_snapshot(AggregateContext::new(
                    aggregate_id,
                    contexts.last().unwrap().sequence,
                    aggregate,
                ))
                .await
            {
                Ok(_) => {},
                Err(e) => {
                    error!(
                        "save aggregate snapshot returned error '{}'",
                        e.to_string()
                    );
                    return Err(e);
                },
            };
        }

        Ok(contexts)
    }

    /// Wrap a set of events with the additional metadata
    /// needed for persistence and publishing
    fn wrap_events(
        &self,
        aggregate_id: &str,
        current_sequence: i64,
        events: Vec<E>,
        metadata: HashMap<String, String>,
    ) -> Vec<EventContext<C, E>> {
        let mut sequence = current_sequence;

        let mut result = Vec::new();

        for x in events {
            sequence += 1;

            result.push(EventContext::new(
                aggregate_id.to_string(),
                sequence,
                x,
                metadata.clone(),
            ));
        }

        result
    }

    /// Load aggregate at current state from events stream
    async fn load_aggregate_from_events(
        &mut self,
        aggregate_id: &str,
    ) -> Result<AggregateContext<C, E, A>, Error> {
        let contexts = self
            .store
            .load_events(&aggregate_id)
            .await?;

        if contexts.len() == 0 {
            return Ok(AggregateContext::new(
                aggregate_id.to_string(),
                0,
                A::default(),
            ));
        }

        let mut aggregate = A::default();

        contexts
            .iter()
            .map(|x| &x.payload)
            .for_each(|x| aggregate.apply(&x));

        Ok(AggregateContext::new(
            aggregate_id.to_string(),
            contexts.last().unwrap().sequence,
            aggregate,
        ))
    }
}
