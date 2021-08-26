use async_trait::async_trait;
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
    IAggregate,
    ICommand,
    IEvent,
    IQuery,
};

use crate::async_store::{
    IQueryStorage,
    QueryStore,
};

use super::query_document::QueryDocument;

/// MongoDB storage
pub struct QueryStorage<
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
    > QueryStorage<C, E, A, Q>
{
    /// constructor
    pub fn new(db: Database) -> Self {
        Self {
            db,
            _phantom: PhantomData,
        }
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
    > IQueryStorage<C, E, A, Q> for QueryStorage<C, E, A, Q>
{
    async fn update_query(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        query_type: &str,
        version: i64,
        payload: &Q,
    ) -> Result<(), Error> {
        let payload = match serde_json::to_string(&payload) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize the payload of query \
                         '{}' with id: '{}', error: {}",
                        &query_type, &agg_id, e,
                    )
                    .as_str(),
                ));
            },
        };

        let col = self.get_queries_collection();

        match version {
            1 => {
                match col
                    .insert_one(
                        QueryDocument {
                            aggregate_type: agg_type.to_string(),
                            aggregate_id: agg_id.to_string(),
                            query_type: query_type.to_string(),
                            version,
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
                                 aggregate id {} with error: {}",
                                &agg_id, e
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
                            "aggregate_type": agg_type.to_string(),
                            "aggregate_id": agg_id.to_string(),
                            "query_type": query_type.to_string(),
                        },
                        doc! {
                            "$set": {
                                "version":version,
                                "payload":payload,
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
                                 aggregate id {} with error: {}",
                                &query_type, &agg_id, e
                            )
                            .as_str(),
                        ));
                    },
                };
            },
        };

        Ok(())
    }

    async fn select_query(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        query_type: &str,
    ) -> Result<Option<(i64, Q)>, Error> {
        let entry = match self
            .get_queries_collection()
            .find_one(
                doc! {
                    "aggregate_type": agg_type.to_string(),
                    "aggregate_id": agg_id.to_string(),
                    "query_type": query_type.to_string(),
                },
                None,
            )
            .await
        {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(e.to_string().as_str()));
            },
        };

        let d = match entry {
            Some(x) => x,
            None => {
                return Ok(None);
            },
        };

        let payload = match serde_json::from_str(d.payload.as_str()) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "bad payload found in events table for \
                         aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(Some((d.version, payload)))
    }
}

/// convenient type alias for MongoDB query store
pub type MongoDbQueryStore<C, E, A, Q> =
    QueryStore<C, E, A, Q, QueryStorage<C, E, A, Q>>;
