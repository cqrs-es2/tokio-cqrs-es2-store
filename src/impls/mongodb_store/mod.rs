//!
//! MongoDB store

pub use event_store::EventStore;
pub use query_store::QueryStore;

mod event_document;
mod event_store;
mod query_document;
mod query_store;
mod snapshot_document;

mod test;
