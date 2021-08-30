use std::{
    collections::HashMap,
    sync::Arc,
};

use cqrs_es2::{
    example_impl::*,
    AggregateContext,
    Error,
    EventContext,
    QueryContext,
};

use crate::{
    memory_store::{
        EventStore,
        QueryStore,
    },
    Repository,
};

use super::dispatchers::CustomDispatcher;

type ThisEventStore =
    EventStore<CustomerCommand, CustomerEvent, Customer>;

type ThisQueryStore = QueryStore<
    CustomerCommand,
    CustomerEvent,
    Customer,
    CustomerContactQuery,
>;

fn get_metadata() -> HashMap<String, String> {
    let now = "2021-03-18T12:32:45.930Z".to_string();
    let mut metadata = HashMap::new();
    metadata.insert("time".to_string(), now);
    metadata
}

async fn check_execute(with_snapshots: bool) -> Result<(), Error> {
    let events = Default::default();
    let snapshots = Default::default();
    let queries = Default::default();
    let dispatched_events = Default::default();

    let event_store = ThisEventStore::new(
        Arc::clone(&events),
        Arc::clone(&snapshots),
    );
    let query_store = ThisQueryStore::new(Arc::clone(&queries));
    let custom_dispatcher =
        CustomDispatcher::new(Arc::clone(&dispatched_events));

    let mut repo = Repository::new(
        event_store,
        vec![
            Box::new(query_store),
            Box::new(custom_dispatcher),
        ],
        with_snapshots,
    );

    let id = uuid::Uuid::new_v4().to_string();
    let metadata = get_metadata();

    repo.execute_with_metadata(
        &id,
        CustomerCommand::AddAddress(AddAddress {
            new_address: "one new address".to_string(),
        }),
        metadata.clone(),
    )
    .await
    .unwrap();

    let mut events_context_0: Vec<
        EventContext<CustomerCommand, CustomerEvent>,
    > = vec![EventContext::new(
        id.clone(),
        1,
        CustomerEvent::AddressUpdated(AddressUpdated {
            new_address: "one new address".to_string(),
        }),
        metadata.clone(),
    )];

    assert_eq!(
        events
            .read()
            .unwrap()
            .get(&id)
            .unwrap()
            .clone(),
        events_context_0.clone()
    );

    if with_snapshots {
        assert_eq!(
            snapshots
                .read()
                .unwrap()
                .get(&id)
                .unwrap()
                .clone(),
            AggregateContext::new(
                id.clone(),
                1,
                Customer {
                    customer_id: Default::default(),
                    name: Default::default(),
                    email: Default::default(),
                    addresses: vec!["one new address".to_string()]
                }
            )
        );
    }

    assert_eq!(
        queries
            .read()
            .unwrap()
            .get(&id)
            .unwrap()
            .clone(),
        QueryContext::new(
            id.clone(),
            1,
            CustomerContactQuery {
                name: Default::default(),
                email: Default::default(),
                latest_address: "one new address".to_string()
            },
        )
    );

    assert_eq!(
        dispatched_events
            .read()
            .unwrap()
            .clone(),
        events_context_0.clone()
    );

    repo.execute_with_metadata(
        &id,
        CustomerCommand::AddCustomerName(AddCustomerName {
            changed_name: "some name".to_string(),
        }),
        metadata.clone(),
    )
    .await
    .unwrap();

    repo.execute_with_metadata(
        &id,
        CustomerCommand::UpdateEmail(UpdateEmail {
            new_email: "e@mail.com".to_string(),
        }),
        metadata.clone(),
    )
    .await
    .unwrap();

    let mut events_context_1: Vec<
        EventContext<CustomerCommand, CustomerEvent>,
    > = vec![
        EventContext::new(
            id.clone(),
            2,
            CustomerEvent::NameAdded(NameAdded {
                changed_name: "some name".to_string(),
            }),
            metadata.clone(),
        ),
        EventContext::new(
            id.clone(),
            3,
            CustomerEvent::EmailUpdated(EmailUpdated {
                new_email: "e@mail.com".to_string(),
            }),
            metadata.clone(),
        ),
    ];

    events_context_0.append(&mut events_context_1);

    assert_eq!(
        events
            .read()
            .unwrap()
            .get(&id)
            .unwrap()
            .clone(),
        events_context_0.clone()
    );

    if with_snapshots {
        assert_eq!(
            snapshots
                .read()
                .unwrap()
                .get(&id)
                .unwrap()
                .clone(),
            AggregateContext::new(
                id.clone(),
                3,
                Customer {
                    customer_id: Default::default(),
                    name: "some name".to_string(),
                    email: "e@mail.com".to_string(),
                    addresses: vec!["one new address".to_string()]
                }
            )
        );
    }

    assert_eq!(
        queries
            .read()
            .unwrap()
            .get(&id)
            .unwrap()
            .clone(),
        QueryContext::new(
            id.clone(),
            3,
            CustomerContactQuery {
                name: "some name".to_string(),
                email: "e@mail.com".to_string(),
                latest_address: "one new address".to_string()
            },
        )
    );

    assert_eq!(
        dispatched_events
            .read()
            .unwrap()
            .clone(),
        events_context_0.clone()
    );

    let err = repo
        .execute_with_metadata(
            &id,
            CustomerCommand::AddCustomerName(AddCustomerName {
                changed_name: "another name".to_string(),
            }),
            metadata.clone(),
        )
        .await
        .unwrap_err();

    assert_eq!(
        Error::new("a name has already been added for this customer"),
        err
    );

    assert_eq!(
        events
            .read()
            .unwrap()
            .get(&id)
            .unwrap()
            .clone(),
        events_context_0.clone()
    );

    if with_snapshots {
        assert_eq!(
            snapshots
                .read()
                .unwrap()
                .get(&id)
                .unwrap()
                .clone(),
            AggregateContext::new(
                id.clone(),
                3,
                Customer {
                    customer_id: Default::default(),
                    name: "some name".to_string(),
                    email: "e@mail.com".to_string(),
                    addresses: vec!["one new address".to_string()]
                }
            )
        );
    }

    assert_eq!(
        queries
            .read()
            .unwrap()
            .get(&id)
            .unwrap()
            .clone(),
        QueryContext::new(
            id.clone(),
            3,
            CustomerContactQuery {
                name: "some name".to_string(),
                email: "e@mail.com".to_string(),
                latest_address: "one new address".to_string()
            },
        )
    );

    assert_eq!(
        dispatched_events
            .read()
            .unwrap()
            .clone(),
        events_context_0.clone()
    );

    Ok(())
}

#[test]
fn test_execute_no_snapshots() {
    tokio_test::block_on(check_execute(false)).unwrap();
}

#[test]
fn test_execute_with_snapshots() {
    tokio_test::block_on(check_execute(true)).unwrap();
}
