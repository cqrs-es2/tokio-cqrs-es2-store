#[cfg(feature = "with-sqlx-postgres")]
pub use postgres_stores::*;

#[cfg(any(
    feature = "with-sqlx-mysql",
    feature = "with-sqlx-mariadb"
))]
pub use mysql_stores::*;

#[cfg(feature = "with-sqlx-sqlite")]
pub use sqlite_stores::*;

#[cfg(feature = "with-mongodb")]
pub use mongodb_stores::*;

#[cfg(feature = "with-redis")]
pub use redis_stores::*;

#[cfg(feature = "with-sqlx-postgres")]
mod postgres_stores;

#[cfg(any(
    feature = "with-sqlx-mysql",
    feature = "with-sqlx-mariadb"
))]
mod mysql_stores;

#[cfg(feature = "with-sqlx-sqlite")]
mod sqlite_stores;

#[cfg(feature = "with-mongodb")]
mod mongodb_stores;

#[cfg(feature = "with-redis")]
mod redis_stores;
