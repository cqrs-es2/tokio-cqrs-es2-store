use async_trait::async_trait;
use log::{
    debug,
    trace,
};
use std::marker::PhantomData;

use mongodb::{
    bson::{
        doc,
        Bson,
    },
    Collection,
    Database,
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

use super::query_document::QueryDocument;

/// Async MongoDB query store
pub struct QueryStore<
    C: ICommand,
    E: IEvent,
    A: IAggregate<C, E>,
    Q: IQuery<C, E>,
> {
    db: Database,
    _phantom: PhantomData<(C, E, A, Q)>,
}

impl<
        C: ICommand,
        E: IEvent,
        A: IAggregate<C, E>,
        Q: IQuery<C, E>,
    > QueryStore<C, E, A, Q>
{
    /// constructor
    pub fn new(db: Database) -> Self {
        let x = Self {
            db,
            _phantom: PhantomData,
        };

        trace!("Created new async MongoDB query store");

        x
    }

    fn get_queries_collection(&self) -> Collection<QueryDocument> {
        self.db
            .collection::<QueryDocument>("queries")
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
        let aggregate_type = A::aggregate_type();
        let query_type = Q::query_type();

        let aggregate_id = context.aggregate_id;

        debug!(
            "storing a new query `{}` for aggregate id '{}'",
            query_type, &aggregate_id
        );

        let payload = match serde_json::to_string(&context.payload) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize the payload of query \
                         '{}' with aggregate id '{}', error: {}",
                        &query_type, &aggregate_id, e,
                    )
                    .as_str(),
                ));
            },
        };

        let col = self.get_queries_collection();

        match context.version {
            1 => {
                match col
                    .insert_one(
                        QueryDocument {
                            aggregate_type: aggregate_type
                                .to_string(),
                            aggregate_id: aggregate_id.clone(),
                            query_type: query_type.to_string(),
                            version: context.version,
                            payload,
                        },
                        None,
                    )
                    .await
                {
                    Ok(x) => {
                        match x.inserted_id {
                            Bson::ObjectId(id) => {
                                if id.to_string().is_empty() {
                                    return Err(Error::new(
                                        format!(
                                            "insert query got empty \
                                             document id ",
                                        )
                                        .as_str(),
                                    ));
                                }
                            },
                            _ => {
                                return Err(Error::new(
                                    format!(
                                        "unexpected return value \
                                         from insert query {:?}",
                                        x.inserted_id
                                    )
                                    .as_str(),
                                ));
                            },
                        };
                    },
                    Err(e) => {
                        return Err(Error::new(
                            format!(
                                "unable to insert new query for \
                                 aggregate id '{}' with error: {}",
                                &aggregate_id, e
                            )
                            .as_str(),
                        ));
                    },
                };
            },
            _ => {
                match col
                    .update_one(
                        doc! {
                            "aggregate_type": aggregate_type.to_string(),
                            "aggregate_id": aggregate_id.clone(),
                            "query_type": query_type.to_string(),
                        },
                        doc! {
                            "$set": {
                                "version": context.version,
                                "payload": payload,
                            }
                        },
                        None,
                    )
                    .await
                {
                    Ok(_) => {},
                    Err(e) => {
                        return Err(Error::new(
                            format!(
                                "unable to update query {} for \
                                 aggregate id '{}' with error: {}",
                                &query_type, &aggregate_id, e
                            )
                            .as_str(),
                        ));
                    },
                };
            },
        };

        Ok(())
    }

    /// loads the most recent query
    async fn load_query(
        &mut self,
        aggregate_id: &str,
    ) -> Result<QueryContext<C, E, Q>, Error> {
        let aggregate_type = A::aggregate_type();
        let query_type = Q::query_type();

        trace!(
            "loading query '{}' for aggregate id '{}'",
            query_type,
            aggregate_id
        );

        let entry = match self
            .get_queries_collection()
            .find_one(
                doc! {
                    "aggregate_type": aggregate_type.to_string(),
                    "aggregate_id": aggregate_id.to_string(),
                    "query_type": query_type.to_string(),
                },
                None,
            )
            .await
        {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to load queries table for query \
                         '{}' and aggregate id '{}' with error: {}",
                        query_type, aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let d = match entry {
            Some(x) => x,
            None => {
                trace!(
                    "returning default query '{}' for aggregate id \
                     '{}'",
                    query_type,
                    aggregate_id
                );

                return Ok(QueryContext::new(
                    aggregate_id.to_string(),
                    0,
                    Default::default(),
                ));
            },
        };

        let payload = match serde_json::from_str(d.payload.as_str()) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "bad payload found in queries table for \
                         query '{}' for aggregate id '{}' with \
                         error: {}",
                        query_type, aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(QueryContext::new(
            aggregate_id.to_string(),
            d.version,
            payload,
        ))
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
