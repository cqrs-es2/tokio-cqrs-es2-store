#[cfg(feature = "with-sqlx-postgres")]
pub use postgres_db::db_connection;

#[cfg(feature = "with-sqlx-mysql")]
pub use mysql_db::db_connection;

#[cfg(feature = "with-sqlx-mariadb")]
pub use mariadb_db::db_connection;

#[cfg(feature = "with-sqlx-sqlite")]
pub use sqlite_db::db_connection;

#[cfg(feature = "with-mongodb")]
pub use mongodb_db::db_connection;

#[cfg(feature = "with-redis")]
pub use redis_db::db_connection;

#[cfg(feature = "with-sqlx-postgres")]
mod postgres_db;

#[cfg(feature = "with-sqlx-mysql")]
mod mysql_db;

#[cfg(feature = "with-sqlx-mariadb")]
mod mariadb_db;

#[cfg(feature = "with-sqlx-sqlite")]
mod sqlite_db;

#[cfg(feature = "with-mongodb")]
mod mongodb_db;

#[cfg(feature = "with-redis")]
mod redis_db;
