use async_trait::async_trait;
use log::{
    debug,
    trace,
};
use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{
        Arc,
        RwLock,
    },
};

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

type LockedQueryContextMap<C, E, Q> =
    RwLock<HashMap<String, QueryContext<C, E, Q>>>;

/// Async memory query store useful for testing purposes only
pub struct QueryStore<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    Q: IQuery<C, E>,
> {
    queries: Arc<LockedQueryContextMap<C, E, Q>>,
    _phantom: PhantomData<A>,
}

impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
    > QueryStore<C, E, A, Q>
{
    /// Constructor
    pub fn new(queries: Arc<LockedQueryContextMap<C, E, Q>>) -> Self {
        let x = Self {
            queries,
            _phantom: PhantomData,
        };

        trace!(
            "Created new async memory query store from passed Arcs"
        );

        x
    }
}

impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
    > Default for QueryStore<C, E, A, Q>
{
    fn default() -> Self {
        let x = Self {
            queries: Default::default(),
            _phantom: PhantomData,
        };

        trace!("Created default async memory query store");

        x
    }
}

#[async_trait]
impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
    > IQueryStore<C, E, A, Q> for QueryStore<C, E, A, Q>
{
    /// saves the updated query
    async fn save_query(
        &mut self,
        context: QueryContext<C, E, Q>,
    ) -> Result<(), Error> {
        let query_type = Q::query_type();
        let aggregate_id = context.aggregate_id.clone();

        debug!(
            "storing a new query '{}' for aggregate id '{}'",
            query_type, &aggregate_id
        );

        // uninteresting unwrap: this is not a struct for production
        // use
        let mut map = self.queries.write().unwrap();
        map.insert(aggregate_id, context);

        Ok(())
    }

    /// loads the most recent query
    async fn load_query(
        &mut self,
        aggregate_id: &str,
    ) -> Result<QueryContext<C, E, Q>, Error> {
        let query_type = Q::query_type();

        trace!(
            "loading query `{}` for aggregate id '{}'",
            query_type,
            aggregate_id
        );

        // uninteresting unwrap: this will not be used in production,
        // for tests only

        match self
            .queries
            .read()
            .unwrap()
            .get(aggregate_id)
        {
            None => {
                Ok(QueryContext::new(
                    aggregate_id.to_string(),
                    0,
                    Default::default(),
                ))
            },
            Some(x) => Ok(x.clone()),
        }
    }
}

#[async_trait]
impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
    > IEventDispatcher<C, E> for QueryStore<C, E, A, Q>
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
