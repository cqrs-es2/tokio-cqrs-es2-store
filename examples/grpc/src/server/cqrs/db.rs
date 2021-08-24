use sqlx::postgres::{
    PgPool,
    PgPoolOptions,
};

pub async fn db_connection() -> Result<PgPool, sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(
            "postgresql://test_user:test_pass@localhost:9084/test",
        )
        .await
        .unwrap();

    Ok(pool)
}
