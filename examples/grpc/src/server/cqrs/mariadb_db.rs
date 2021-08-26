use sqlx::mysql::{
    MySqlPool,
    MySqlPoolOptions,
};

pub async fn db_connection() -> Result<MySqlPool, sqlx::Error> {
    let pool = MySqlPoolOptions::new()
        .max_connections(5)
        .connect("mysql://test_user:test_pass@localhost:9081/test")
        .await
        .unwrap();

    Ok(pool)
}
