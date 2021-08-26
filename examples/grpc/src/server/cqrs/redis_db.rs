use redis::{
    Client,
    Connection,
    RedisError,
};

pub async fn db_connection() -> Result<Connection, RedisError> {
    let client = Client::open("redis://localhost:9086/").unwrap();

    let conn = client.get_connection().unwrap();

    Ok(conn)
}
