use sqlx::mysql::MySqlPoolOptions;

use cqrs_es2::{
    example_impl::*,
    Error,
    QueryContext,
};

use crate::{
    mysql_store::{
        MySqlQueryStore,
        Storage,
    },
    repository::IQueryStore,
};

use super::common::*;

type ThisQueryStore = MySqlQueryStore<
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

async fn commit_and_load_queries(uri: &str) -> Result<(), Error> {
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect(uri)
        .await
        .unwrap();

    let storage = Storage::new(pool);

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
fn test_mariadb_commit_and_load_queries() {
    tokio_test::block_on(commit_and_load_queries(
        CONNECTION_STRING_MARIADB,
    ))
    .unwrap();
}

#[test]
fn test_mysql_commit_and_load_queries() {
    tokio_test::block_on(commit_and_load_queries(
        CONNECTION_STRING_MYSQL,
    ))
    .unwrap();
}
