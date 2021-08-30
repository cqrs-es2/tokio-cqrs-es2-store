#[cfg(feature = "with-postgres")]
pub use postgres_stores::*;

#[cfg(any(
    feature = "with-mysql",
    feature = "with-mariadb"
))]
pub use mysql_stores::*;

#[cfg(feature = "with-sqlite")]
pub use sqlite_stores::*;

#[cfg(feature = "with-mongodb")]
pub use mongodb_stores::*;

#[cfg(feature = "with-redis")]
pub use redis_stores::*;

#[cfg(feature = "with-postgres")]
mod postgres_stores;

#[cfg(any(
    feature = "with-mysql",
    feature = "with-mariadb"
))]
mod mysql_stores;

#[cfg(feature = "with-sqlite")]
mod sqlite_stores;

#[cfg(feature = "with-mongodb")]
mod mongodb_stores;

#[cfg(feature = "with-redis")]
mod redis_stores;
