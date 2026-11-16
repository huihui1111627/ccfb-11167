#![allow(dead_code)]
//! Client library for the mqueue broker: an idempotent producer and a
//! consumer-group client with automatic partition assignment, heartbeat-driven
//! rebalancing and committed-offset resume.

mod connection;
mod consumer;
mod producer;
mod state;

pub use connection::{ClientError, Connection};
pub use consumer::{Consumer, StartPosition};
pub use producer::Producer;

pub use mqueue_proto;
pub use mqueue_storage::partition_for_key;
