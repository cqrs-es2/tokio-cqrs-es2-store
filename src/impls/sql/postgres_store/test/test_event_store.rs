use std::collections::HashMap;

use sqlx::postgres::PgPoolOptions;

use cqrs_es2::{
    example_impl::*,
    AggregateContext,
    Error,
    EventContext,
};

use crate::{
    postgres_store::{
        EventStorage,
        PostgresEventStore,
    },
    repository::IEventStore,
};

use super::common::*;

type ThisEventStore =
    PostgresEventStore<CustomerCommand, CustomerEvent, Customer>;

type ThisAggregateContext =
    AggregateContext<CustomerCommand, CustomerEvent, Customer>;

type ThisEventContext = EventContext<CustomerCommand, CustomerEvent>;

pub fn get_metadata() -> HashMap<String, String> {
    let now = "2021-03-18T12:32:45.930Z".to_string();
    let mut metadata = HashMap::new();
    metadata.insert("time".to_string(), now);
    metadata
}

async fn commit_and_load_events(
    with_snapshots: bool
) -> Result<(), Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(CONNECTION_STRING)
        .await
        .unwrap();

    let storage = EventStorage::new(pool);

    let mut store = ThisEventStore::new(storage, with_snapshots);

    let id = uuid::Uuid::new_v4().to_string();

    // loading nonexisting stream defaults to a vector with zero
    // length
    assert_eq!(
        0,
        store
            .load_events(id.as_str())
            .await
            .unwrap()
            .len()
    );

    // loading nonexisting aggregate returns default construction
    let context = store
        .load_aggregate(id.as_str())
        .await
        .unwrap();

    assert_eq!(
        context,
        ThisAggregateContext::new(id.clone(), Customer::default(), 0)
    );

    // apply a couple of events
    let events = vec![
        CustomerEvent::NameAdded(NameAdded {
            changed_name: "test_event_A".to_string(),
        }),
        CustomerEvent::EmailUpdated(EmailUpdated {
            new_email: "test A".to_string(),
        }),
        CustomerEvent::AddressUpdated(AddressUpdated {
            new_address: "test B".to_string(),
        }),
    ];

    let metadata = get_metadata();

    store
        .commit(
            vec![events[0].clone(), events[1].clone()],
            context,
            metadata.clone(),
        )
        .await
        .unwrap();

    let contexts = store
        .load_events(id.as_str())
        .await
        .unwrap();

    // check stored events are correct
    assert_eq!(
        contexts,
        vec![
            ThisEventContext::new(
                id.to_string(),
                1,
                events[0].clone(),
                metadata.clone()
            ),
            ThisEventContext::new(
                id.to_string(),
                2,
                events[1].clone(),
                metadata.clone()
            ),
        ]
    );

    let context = store
        .load_aggregate(id.as_str())
        .await
        .unwrap();

    // check stored aggregate is correct
    assert_eq!(
        context,
        ThisAggregateContext::new(
            id.clone(),
            Customer {
                customer_id: "".to_string(),
                name: "test_event_A".to_string(),
                email: "test A".to_string(),
                addresses: Default::default()
            },
            2
        )
    );

    let metadata = get_metadata();

    store
        .commit(
            vec![events[2].clone()],
            context,
            metadata.clone(),
        )
        .await
        .unwrap();

    let contexts = store
        .load_events(id.as_str())
        .await
        .unwrap();

    // check stored events are correct
    assert_eq!(
        contexts,
        vec![
            ThisEventContext::new(
                id.to_string(),
                1,
                events[0].clone(),
                metadata.clone()
            ),
            ThisEventContext::new(
                id.to_string(),
                2,
                events[1].clone(),
                metadata.clone()
            ),
            ThisEventContext::new(
                id.to_string(),
                3,
                events[2].clone(),
                metadata.clone()
            ),
        ]
    );

    let context = store
        .load_aggregate(id.as_str())
        .await
        .unwrap();

    // check stored aggregate is correct
    assert_eq!(
        context,
        ThisAggregateContext::new(
            id.clone(),
            Customer {
                customer_id: "".to_string(),
                name: "test_event_A".to_string(),
                email: "test A".to_string(),
                addresses: vec!["test B".to_string()]
            },
            3
        )
    );

    Ok(())
}

#[test]
fn test_with_snapshots() {
    tokio_test::block_on(commit_and_load_events(true)).unwrap();
}

#[test]
fn test_no_snapshots() {
    tokio_test::block_on(commit_and_load_events(false)).unwrap();
}
