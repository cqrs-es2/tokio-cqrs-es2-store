#[cfg(feature = "with-postgres")]
pub use postgres_db::db_connection;

#[cfg(feature = "with-mysql")]
pub use mysql_db::db_connection;

#[cfg(feature = "with-mariadb")]
pub use mariadb_db::db_connection;

#[cfg(feature = "with-sqlite")]
pub use sqlite_db::db_connection;

#[cfg(feature = "with-mongodb")]
pub use mongodb_db::db_connection;

#[cfg(feature = "with-redis")]
pub use redis_db::db_connection;

#[cfg(feature = "with-postgres")]
mod postgres_db;

#[cfg(feature = "with-mysql")]
mod mysql_db;

#[cfg(feature = "with-mariadb")]
mod mariadb_db;

#[cfg(feature = "with-sqlite")]
mod sqlite_db;

#[cfg(feature = "with-mongodb")]
mod mongodb_db;

#[cfg(feature = "with-redis")]
mod redis_db;
