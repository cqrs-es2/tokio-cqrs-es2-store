use mongodb::{
    options::ClientOptions,
    Client,
};

use cqrs_es2::{
    example_impl::*,
    Error,
    QueryContext,
};

use crate::{
    mongodb_store::{
        MongoDbQueryStore,
        QueryStorage,
    },
    repository::IQueryStore,
};

use super::common::*;

type ThisQueryStore = MongoDbQueryStore<
    CustomerCommand,
    CustomerEvent,
    Customer,
    CustomerContactQuery,
>;

type ThisQueryContext = QueryContext<
    CustomerCommand,
    CustomerEvent,
    CustomerContactQuery,
>;

async fn commit_and_load_queries() -> Result<(), Error> {
    let mut client_options =
        match ClientOptions::parse(CONNECTION_STRING).await {
            Ok(x) => x,
            Err(e) => {
                return Err(Error::new(e.to_string().as_str()));
            },
        };

    client_options.app_name = Some("UnitTesting".to_string());

    let client = match Client::with_options(client_options) {
        Ok(x) => x,
        Err(e) => {
            return Err(Error::new(e.to_string().as_str()));
        },
    };

    let db = client.database("test");

    let storage = QueryStorage::new(db);

    let mut store = ThisQueryStore::new(storage);

    let id = uuid::Uuid::new_v4().to_string();

    // loading nonexisting query returns default constructor
    assert_eq!(
        store.load(id.as_str()).await.unwrap(),
        ThisQueryContext::new(id.to_string(), 0, Default::default())
    );

    let context = ThisQueryContext::new(
        id.to_string(),
        1,
        CustomerContactQuery {
            name: "".to_string(),
            email: "test@email.com".to_string(),
            latest_address: "one address".to_string(),
        },
    );

    store.commit(context).await.unwrap();

    let stored_context = store.load(&id).await.unwrap();

    assert_eq!(
        stored_context,
        ThisQueryContext::new(
            id.to_string(),
            1,
            CustomerContactQuery {
                name: "".to_string(),
                email: "test@email.com".to_string(),
                latest_address: "one address".to_string(),
            },
        )
    );

    Ok(())
}

#[test]
fn test_commit_and_load_queries() {
    tokio_test::block_on(commit_and_load_queries()).unwrap();
}
