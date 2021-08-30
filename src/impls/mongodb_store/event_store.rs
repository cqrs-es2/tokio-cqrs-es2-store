use async_trait::async_trait;
use futures::stream::TryStreamExt;
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
    options::FindOptions,
    Collection,
    Database,
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

use super::{
    event_document::EventDocument,
    snapshot_document::SnapshotDocument,
};

/// MongoDB storage
pub struct EventStore<C: ICommand, E: IEvent, A: IAggregate<C, E>> {
    db: Database,
    _phantom: PhantomData<(C, E, A)>,
}

impl<C: ICommand, E: IEvent, A: IAggregate<C, E>>
    EventStore<C, E, A>
{
    /// constructor
    pub fn new(db: Database) -> Self {
        let x = Self {
            db,
            _phantom: PhantomData,
        };

        trace!("Created new async MongoDB event store");

        x
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

        let aggregate_type = A::aggregate_type();

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

        let mut all_docs = Vec::new();
        for context in contexts {
            let payload =
                match serde_json::to_string(&context.payload) {
                    Ok(x) => x,
                    Err(e) => {
                        return Err(Error::new(
                            format!(
                                "unable to serialize the event \
                                 payload for aggregate id {} with \
                                 error: {}",
                                &aggregate_id, e
                            )
                            .as_str(),
                        ));
                    },
                };

            all_docs.push(EventDocument {
                aggregate_type: aggregate_type.to_string(),
                aggregate_id: aggregate_id.to_string(),
                sequence: context.sequence,
                payload,
                metadata: context.metadata.clone(),
            });
        }

        match self
            .get_events_collection()
            .insert_many(all_docs, None)
            .await
        {
            Ok(x) => {
                if x.inserted_ids.len() != contexts.len() {
                    return Err(Error::new(
                        format!(
                            "documents size mismatch, expected {} \
                             only inserted {}",
                            contexts.len(),
                            x.inserted_ids.len()
                        )
                        .as_str(),
                    ));
                }
            },
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to insert new events for aggregate \
                         id {} with error: {}",
                        &aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(())
    }

    /// Load all events for a particular `aggregate_id`
    async fn load_events(
        &mut self,
        aggregate_id: &str,
    ) -> Result<Vec<EventContext<C, E>>, Error> {
        let aggregate_type = A::aggregate_type();

        trace!(
            "loading events for aggregate id '{}'",
            aggregate_id
        );

        let find_options = FindOptions::builder()
            .sort(doc! { "sequence": 1 })
            .build();

        let mut cursor = match self
            .get_events_collection()
            .find(
                doc! {
                    "aggregate_type": aggregate_type,
                    "aggregate_id": aggregate_id,
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
                        aggregate_id, e
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
                            aggregate_id, e
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
                                aggregate_id, e
                            )
                            .as_str(),
                        ));
                    },
                };

            result.push(EventContext::new(
                aggregate_id.to_string(),
                d.sequence,
                payload,
                d.metadata,
            ));
        }

        Ok(result)
    }

    /// save a new aggregate snapshot
    async fn save_aggregate_snapshot(
        &mut self,
        context: AggregateContext<C, E, A>,
    ) -> Result<(), Error> {
        let aggregate_type = A::aggregate_type();

        let aggregate_id = context.aggregate_id;

        debug!(
            "storing a new snapshot for aggregate id '{}'",
            &aggregate_id
        );

        let payload = match serde_json::to_string(&context.payload) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to serialize aggregate snapshot for \
                         aggregate id {} with error: {}",
                        &aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let col = self.get_snapshots_collection();

        match context.version {
            1 => {
                match col
                    .insert_one(
                        SnapshotDocument {
                            aggregate_type: aggregate_type
                                .to_string(),
                            aggregate_id: aggregate_id.to_string(),
                            version: 1,
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
                            "aggregate_id": aggregate_id.to_string(),
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
                                "unable to update snapshot for \
                                 aggregate id {} with error: {}",
                                &aggregate_id, e
                            )
                            .as_str(),
                        ));
                    },
                };
            },
        };

        Ok(())
    }

    /// Load aggregate at current state from snapshots
    async fn load_aggregate_from_snapshot(
        &mut self,
        aggregate_id: &str,
    ) -> Result<AggregateContext<C, E, A>, Error> {
        let aggregate_type = A::aggregate_type();

        trace!(
            "loading snapshot for aggregate id '{}'",
            aggregate_id
        );

        let entry = match self
            .get_snapshots_collection()
            .find_one(
                doc! {
                    "aggregate_type": aggregate_type.to_string(),
                    "aggregate_id": aggregate_id.to_string(),
                },
                None,
            )
            .await
        {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "unable to check snapshots table for \
                         aggregate id {} with error: {}",
                        &aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        let d = match entry {
            Some(x) => x,
            None => {
                trace!(
                    "returning default aggregate for aggregate id \
                     '{}'",
                    aggregate_id
                );

                return Ok(AggregateContext::new(
                    aggregate_id.to_string(),
                    0,
                    A::default(),
                ));
            },
        };

        let payload = match serde_json::from_str(d.payload.as_str()) {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(
                    format!(
                        "bad payload found in snapshots table for \
                         aggregate id {} with error: {}",
                        &aggregate_id, e
                    )
                    .as_str(),
                ));
            },
        };

        Ok(AggregateContext::new(
            aggregate_id.to_string(),
            d.version,
            payload,
        ))
    }
}
