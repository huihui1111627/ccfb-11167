//! Benchmark message payloads and on-disk event journals.
//!
//! The value is JSON so a human can inspect it, but the hot fields are also
//! available in the record header (producer id, per-partition sequence).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload {
    /// Producer-wide monotonically increasing message index.
    pub global: u64,
    /// Producer wall-clock send time, epoch milliseconds — the start point of
    /// the end-to-end latency measurement.
    pub send_ts: u64,
}

impl Payload {
    pub fn new(global: u64, send_ts: u64) -> Self {
        Self { global, send_ts }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("payload serialization")
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// Build a key from a bounded key space. A fixed key space means every key is
/// reused, which is what makes same-key ordering violations observable.
pub fn key_for(global: u64, key_space: u64) -> Vec<u8> {
    format!("key-{}", global % key_space).into_bytes()
}

/// One line in a consumer's journal: a record the consumer was delivered.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEvent {
    pub consumer: String,
    pub partition: u32,
    pub offset: u64,
    pub global: u64,
    pub key: String,
    pub producer_id: u64,
    pub sequence: u64,
    pub send_ts: u64,
    pub recv_ts: u64,
}

/// One line in a producer's manifest: a record the broker acknowledged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEvent {
    pub producer: String,
    pub partition: u32,
    pub offset: u64,
    pub global: u64,
    pub send_ts: u64,
    pub ack_ts: u64,
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
    .map(|d| d.as_millis() as u64)
    .unwrap_or(0)
}
