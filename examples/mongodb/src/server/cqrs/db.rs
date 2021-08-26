use mongodb::{
    error::Error,
    options::ClientOptions,
    Client,
    Database,
};

pub async fn db_connection() -> Result<Database, Error> {
    let mut client_options = ClientOptions::parse(
        "mongodb://admin:admin_pass@localhost:9085",
    )
    .await?;

    client_options.app_name = Some("UnitTesting".to_string());

    let client = Client::with_options(client_options)?;

    let db = client.database("test");

    Ok(db)
}
