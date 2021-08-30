use sqlx::mysql::MySqlPoolOptions;

use cqrs_es2::{
    example_impl::*,
    Error,
    QueryContext,
};

use crate::{
    mysql_store::QueryStore,
    IQueryStore,
};

use super::common::*;

type ThisQueryStore = QueryStore<
    CustomerCommand,
    CustomerEvent,
    Customer,
    CustomerContactQuery,
>;

async fn check_save_load_queries(uri: &str) -> Result<(), Error> {
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(uri)
        .await
        .unwrap();

    let mut store = ThisQueryStore::new(pool);

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
fn test_mariadb_save_load_queries() {
    tokio_test::block_on(check_save_load_queries(
        CONNECTION_STRING_MARIADB,
    ))
    .unwrap();
}

#[test]
fn test_mysql_save_load_queries() {
    tokio_test::block_on(check_save_load_queries(
        CONNECTION_STRING_MYSQL,
    ))
    .unwrap();
}
