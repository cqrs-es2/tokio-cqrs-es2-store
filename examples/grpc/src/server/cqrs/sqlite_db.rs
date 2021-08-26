use sqlx::sqlite::{
    SqliteConnectOptions,
    SqlitePool,
    SqlitePoolOptions,
};

pub async fn db_connection() -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::new()
        .filename("test.db")
        .create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .unwrap();

    Ok(pool)
}
