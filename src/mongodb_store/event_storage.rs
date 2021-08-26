use async_trait::async_trait;
use futures::stream::TryStreamExt;
use mongodb::{
    bson::{
        doc,
        Bson,
    },
    options::FindOptions,
    Collection,
    Database,
};
use std::{
    collections::HashMap,
    marker::PhantomData,
};

use cqrs_es2::{
    Error,
    IAggregate,
    ICommand,
    IEvent,
};

use crate::async_store::{
    EventStore,
    IEventStorage,
};

use super::{
    event_document::EventDocument,
    snapshot_document::SnapshotDocument,
};

/// MongoDB storage
pub struct EventStorage<C: ICommand, E: IEvent, A: IAggregate<C, E>> {
    db: Database,
    _phantom: PhantomData<(C, E, A)>,
}

impl<C: ICommand, E: IEvent, A: IAggregate<C, E>>
    EventStorage<C, E, A>
{
    /// constructor
    pub fn new(db: Database) -> Self {
        Self {
            db,
            _phantom: PhantomData,
        }
    }

    fn get_events_collection(&self) -> Collection<EventDocument> {
        self.db
            .collection::<EventDocument>("events")
    }

    fn get_snapshots_collection(
        &self
    ) -> Collection<SnapshotDocument> {
        self.db
            .collection::<SnapshotDocument>("snapshots")
    }
}

#[async_trait]
impl<C: ICommand, E: IEvent, A: IAggregate<C, E>>
    IEventStorage<C, E, A> for EventStorage<C, E, A>
{
    async fn insert_event(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        sequence: i64,
        payload: &E,
        metadata: &HashMap<String, String>,
    ) -> Result<(), Error> {
        let payload = match serde_json::to_string(&payload) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "Could not serialize the event payload for \
                         aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        match self
            .get_events_collection()
            .insert_one(
                EventDocument {
                    aggregate_type: agg_type.to_string(),
                    aggregate_id: agg_id.to_string(),
                    sequence,
                    payload,
                    metadata: metadata.clone(),
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
                                format!("Empty document id",)
                                    .as_str(),
                            ));
                        }
                    },
                    _ => {
                        return Err(Error::new(
                            format!(
                                "unexpected return value from, \
                                 insert event {:?}",
                                x.inserted_id
                            )
                            .as_str(),
                        ));
                    },
                }
            },
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to insert new event for aggregate \
                         id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(())
    }

    async fn select_events_only(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<Vec<(usize, E)>, Error> {
        let events = self
            .select_events_with_metadata(agg_type, agg_id)
            .await?;

        Ok(events
            .iter()
            .map(|x| (x.0, x.1.clone()))
            .collect())
    }

    async fn select_events_with_metadata(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<Vec<(usize, E, HashMap<String, String>)>, Error> {
        let find_options = FindOptions::builder()
            .sort(doc! { "sequence": 1 })
            .build();

        let mut cursor = match self
            .get_events_collection()
            .find(
                doc! {
                    "aggregate_type": agg_type,
                    "aggregate_id": agg_id,
                },
                find_options,
            )
            .await
        {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to load events table for aggregate \
                         id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let mut result = Vec::new();

        loop {
            let d = match cursor.try_next().await {
                Ok(x) => x,
                Err(e) => {
                    return Err(Error::new(
                        format!(
                            "unable to load next entry from events \
                             table for aggregate id {} with error: \
                             {}",
                            &agg_id, e
                        )
                        .as_str(),
                    ));
                },
            };

            let d: EventDocument = match d {
                None => {
                    break;
                },
                Some(x) => x,
            };

            let payload =
                match serde_json::from_str(d.payload.as_str()) {
                    Ok(x) => x,
                    Err(e) => {
                        return Err(Error::new(
                            format!(
                                "bad payload found in events table \
                                 for aggregate id {} with error: {}",
                                &agg_id, e
                            )
                            .as_str(),
                        ));
                    },
                };

            result.push((d.sequence as usize, payload, d.metadata))
        }

        Ok(result)
    }

    async fn update_snapshot(
        &mut self,
        agg_type: &str,
        agg_id: &str,
        last_sequence: i64,
        payload: &A,
        current_sequence: usize,
    ) -> Result<(), Error> {
        let payload = match serde_json::to_string(&payload) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize aggregate snapshot for \
                         aggregate id {} with error: {}",
                        &agg_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let col = self.get_snapshots_collection();

        match current_sequence {
            0 => {
                match col
                    .insert_one(
                        SnapshotDocument {
                            aggregate_type: agg_type.to_string(),
                            aggregate_id: agg_id.to_string(),
                            last_sequence,
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
                                            "insert snapshot got \
                                             empty document id",
                                        )
                                        .as_str(),
                                    ));
                                }
                            },
                            _ => {
                                return Err(Error::new(
                                    format!(
                                        "unexpected return value \
                                         from insert snapshot {:?}",
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
                                "unable to insert new snapshot for \
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
                        },
                        doc! {
                            "$set": {
                                "last_sequence":last_sequence,
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
                                "unable to update snapshot for \
                                 aggregate id {} with error: {}",
                                &agg_id, e
                            )
                            .as_str(),
                        ));
                    },
                };
            },
        };

        Ok(())
    }

    async fn select_snapshot(
        &mut self,
        agg_type: &str,
        agg_id: &str,
    ) -> Result<Option<(usize, A)>, Error> {
        let entry = match self
            .get_snapshots_collection()
            .find_one(
                doc! {
                    "aggregate_type": agg_type.to_string(),
                    "aggregate_id": agg_id.to_string(),
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

        Ok(Some((
            d.last_sequence as usize,
            payload,
        )))
    }
}

/// convenient type alias for MongoDB event store
pub type MongoDbEventStore<C, E, A> =
    EventStore<C, E, A, EventStorage<C, E, A>>;
