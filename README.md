# tokio-cqrs-es2-store

**Async implementation of the cqrs-es2 store.**

[![Publish](https://github.com/brgirgis/tokio-cqrs-es2-store/actions/workflows/crates-io.yml/badge.svg)](https://github.com/brgirgis/tokio-cqrs-es2-store/actions/workflows/crates-io.yml)
[![Test](https://github.com/brgirgis/tokio-cqrs-es2-store/actions/workflows/rust-ci.yml/badge.svg)](https://github.com/brgirgis/tokio-cqrs-es2-store/actions/workflows/rust-ci.yml)
[![Latest version](https://img.shields.io/crates/v/tokio-cqrs-es2-store)](https://crates.io/crates/tokio-cqrs-es2-store)
[![docs](https://img.shields.io/badge/API-docs-blue.svg)](https://docs.rs/tokio-cqrs-es2-store)
![License](https://img.shields.io/crates/l/tokio-cqrs-es2-store.svg)

---

Provides async interfaces to different database implementations for the CQRS system store.

# Design

The main components of this library are:

- `IEventDispatcher` - an interface for async events listeners
- `IEventStore` - an interface for async event stores
- `IQueryStore` - an interface for async query stores

# Features

- `with-sqlx-postgres` - async Postgres store
- `with-sqlx-mysql` - async MySQL store
- `with-sqlx-mariadb` - async MariaDB store
- `with-sqlx-sqlite` - async SQLite store
- `with-all-sqlx` - all sqlx drivers
- `with-all-async` - all async drivers (default)

# Installation

To use this library in an async application, add the following to
your dependency section in the project's `Cargo.toml`:

```toml
[dependencies]
# logging
log = { version = "^0.4", features = [
  "max_level_debug",
  "release_max_level_warn",
] }
fern = "^0.5"

# serialization
serde = { version = "^1.0.127", features = ["derive"] }
serde_json = "^1.0.66"

async-trait = "^0.1"

# CQRS framework
cqrs-es2 = { version = "*"}

# Sync postgres store implementation
tokio-cqrs-es2-store = { version = "*", default-features = false, features = [
  "with-sqlx-postgres",
] }

# sqlx
sqlx = { version = "0.5.6", features = [
  # tokio + rustls
  "runtime-tokio-rustls",
  # PostgresSQL
  "postgres",
  "uuid",
  "json",
  # misc
  "macros",
  "chrono",
  "tls",
] }

tokio = { version = "1", features = [
  "rt-multi-thread",
  "time",
  "fs",
  "macros",
  "net",
] }
```

# Usage

Full async store example applications:

- [gRPC](https://github.com/brgirgis/tokio-cqrs-es2-store/tree/master/examples/grpc).

## Change Log

A complete history of the change log can be found [here](https://github.com/brgirgis/tokio-cqrs-es2-store/blob/master/ChangeLog.md)

## TODO

An up-to-date list of development aspirations can be found [here](https://github.com/brgirgis/tokio-cqrs-es2-store/blob/master/TODO.md)
