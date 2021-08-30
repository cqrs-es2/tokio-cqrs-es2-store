#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(rust_2018_idioms)]
#![warn(
    clippy::pedantic,
    //missing_debug_implementations
)]

//! Async implementation of the cqrs-es2 store.
//!
//! Provides async interfaces to different database implementations
//! for the CQRS system store.
//!
//! # Design
//!
//! The main components of this library are:
//!
//!   - `IEventDispatcher` - an interface for async events listeners
//!   - `IEventStore` - an interface for async event stores
//!   - `IQueryStore` - an interface for async query stores
//!
//! # Features
//!
//! - `with-sqlx-postgres` - async Postgres store
//! - `with-sqlx-mysql` - async MySQL store
//! - `with-sqlx-sqlite` - async SQLite store
//! - `with-all-sqlx` - all sqlx drivers
//! - `with-mongodb` - async MongoDB store
//! - `with-all-doc-db` - all doc DBs drivers
//! - `with-redis` - async Redis store
//! - `with-all-kv-db` - all key-value DBs drivers
//! - `with-all-async` - all async drivers (default)
//!
//! # Installation
//!
//! To use this library in an async application, add the following to
//! your dependency section in the project's `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! # logging
//! log = { version = "^0.4", features = [
//!   "max_level_debug",
//!   "release_max_level_warn",
//! ] }
//! fern = "^0.5"
//!
//! # serialization
//! serde = { version = "^1.0.127", features = ["derive"] }
//! serde_json = "^1.0.66"
//!
//! async-trait = "^0.1"
//!
//! # CQRS framework
//! cqrs-es2 = { version = "*"}
//!
//! # Sync postgres store implementation
//! tokio-cqrs-es2-store = { version = "*", default-features = false, features = [
//!   "with-sqlx-postgres",
//! ] }
//!
//! # sqlx
//! sqlx = { version = "0.5.6", features = [
//!   # tokio + rustls
//!   "runtime-tokio-rustls",
//!   # PostgresSQL
//!   "postgres",
//!   "uuid",
//!   "json",
//!   # misc
//!   "macros",
//!   "chrono",
//!   "tls",
//! ] }
//!
//! tokio = { version = "1", features = [
//!   "rt-multi-thread",
//!   "time",
//!   "fs",
//!   "macros",
//!   "net",
//! ] }
//! ```
//!
//! # Usage
//!
//! A full async store example application is available [here](https://github.com/brgirgis/tokio-cqrs-es2-store/tree/master/examples/grpc).

pub use impls::*;
pub use repository::*;

mod impls;
mod repository;
