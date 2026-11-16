//! Idempotent producer.
//!
//! For every message the producer reserves a per-partition, strictly increasing
//! sequence number and fsyncs its local state file *before* sending. On any
//! failure (timeout, disconnect, crash) the same sequence is replayed; the
//! broker's dedup table returns the original offset instead of appending twice.

use std::path::PathBuf;

use anyhow::Context;
use mqueue_proto::{
    Message, ProduceRequest, ProduceResponse, Request, Response,
};
use mqueue_storage::partition_for_key;

use crate::connection::{ClientError, Connection};
use crate::state::ProducerState;

pub struct Producer {
    topic: String,
    partition_count: u32,
    producer_id: u64,
    conn: Connection,
    state: ProducerState,
}

impl Producer {
    /// Connect and load (or initialize) the durable sequence state.
    pub async fn connect(
        addr: &str,
        topic: &str,
        partition_count: u32,
        state_path: PathBuf,
    ) -> anyhow::Result<Self> {
        let conn = Connection::new(addr);
        let state = ProducerState::load(state_path)
            .await
            .context("loading producer state")?;
        Ok(Self {
            topic: topic.to_string(),
            partition_count,
            producer_id: state.producer_id().await,
            conn,
            state,
        })
    }

    pub fn producer_id(&self) -> u64 {
        self.producer_id
    }

    /// Send one message. Transparent retries reuse the same sequence, so the
    /// broker cannot observe a duplicate.
    pub async fn send(&self, key: &[u8], value: &[u8]) -> anyhow::Result<u64> {
        let partition = partition_for_key(key, self.partition_count);
        // Sequence is persisted before the wire send: a crash between this
        // line and the broker response is exactly the retry case.
        let sequence = self.state.next_sequence(partition).await?;
        let req = Request::Produce(ProduceRequest {
            topic: self.topic.clone(),
            producer_id: self.producer_id,
            messages: vec![Message {
                key: key.to_vec(),
                value: value.to_vec(),
                timestamp_ms: now_ms(),
                sequence,
            }],
        });
        let resp = self.conn.request(&req).await?;
        match resp {
            Response::Produced(ProduceResponse { offsets, .. }) => Ok(offsets[0]),
            other => Err(ClientError::Retry(format!("unexpected response {other:?}")).into()),
        }
    }

    /// Batch send. Each message gets its own per-partition sequence first
    /// (all state fsynced up front), then one request carries the whole batch.
    /// Retries replay the identical batch.
    pub async fn send_batch(&self, items: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<Vec<u64>> {
        let mut messages = Vec::with_capacity(items.len());
        for (key, value) in items {
            let partition = partition_for_key(key, self.partition_count);
            let sequence = self.state.next_sequence(partition).await?;
            messages.push(Message {
                key: key.clone(),
                value: value.clone(),
                timestamp_ms: now_ms(),
                sequence,
            });
        }
        let req = Request::Produce(ProduceRequest {
            topic: self.topic.clone(),
            producer_id: self.producer_id,
            messages,
        });
        let resp = self.conn.request(&req).await?;
        match resp {
            Response::Produced(ProduceResponse { offsets, .. }) => Ok(offsets),
            other => Err(ClientError::Retry(format!("unexpected response {other:?}")).into()),
        }
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
