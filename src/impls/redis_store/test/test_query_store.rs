use redis::Client;

use cqrs_es2::{
    example_impl::*,
    Error,
    QueryContext,
};

use crate::{
    redis_store::QueryStore,
    IQueryStore,
};

use super::common::*;

type ThisQueryStore = QueryStore<
    CustomerCommand,
    CustomerEvent,
    Customer,
    CustomerContactQuery,
>;

async fn check_save_load_queries() -> Result<(), Error> {
    let client = match Client::open(CONNECTION_STRING) {
        Ok(x) => x,
        Err(e) => {
            return Err(Error::new(e.to_string().as_str()));
        },
    };

    let conn = match client.get_connection() {
        Ok(x) => x,
        Err(e) => {
            return Err(Error::new(e.to_string().as_str()));
        },
    };

    let mut store = ThisQueryStore::new(conn);

    let id = uuid::Uuid::new_v4().to_string();

    let stored_context = store.load_query(&id).await.unwrap();

    assert_eq!(
        stored_context,
        QueryContext::new(id.to_string(), 0, Default::default())
    );

    let context = QueryContext::new(
        id.to_string(),
        1,
        CustomerContactQuery {
            name: "test name".to_string(),
            email: "test@email.com".to_string(),
            latest_address: "one address".to_string(),
        },
    );

    store
        .save_query(context.clone())
        .await
        .unwrap();

    let stored_context = store.load_query(&id).await.unwrap();

    assert_eq!(stored_context, context);

    let context = QueryContext::new(
        id.to_string(),
        2,
        CustomerContactQuery {
            name: "test name2".to_string(),
            email: "test2@email.com".to_string(),
            latest_address: "second address".to_string(),
        },
    );

    store
        .save_query(context.clone())
        .await
        .unwrap();

    let stored_context = store.load_query(&id).await.unwrap();

    assert_eq!(stored_context, context);

    Ok(())
}

#[test]
fn test_save_load_queries() {
    tokio_test::block_on(check_save_load_queries()).unwrap();
}
