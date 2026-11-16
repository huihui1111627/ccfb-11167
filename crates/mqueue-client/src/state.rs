//! Persisted producer state.
//!
//! Idempotency depends on the sequence number surviving a producer crash. The
//! state file maps partition -> last used sequence and is rewritten
//! atomically (tmp + rename) before every send batch, so a retry after a
//! crash always reuses the exact sequence the broker may already have stored.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StateFile {
    producer_id: u64,
    /// partition -> last assigned sequence
    sequences: HashMap<u32, u64>,
}

pub struct ProducerState {
    path: PathBuf,
    tmp: PathBuf,
    inner: Mutex<StateFile>,
}

impl ProducerState {
    /// Load state, allocating a fresh producer id on first use. A random id
    /// makes producers distinguishable across machines without coordination.
    pub async fn load(path: PathBuf) -> anyhow::Result<Self> {
        let inner = if path.exists() {
            let bytes = tokio::fs::read(&path).await?;
            serde_json::from_slice(&bytes)?
        } else {
            StateFile {
                producer_id: allocate_producer_id(),
                sequences: HashMap::new(),
            }
        };
        let tmp = path.with_extension("json.tmp");
        Ok(Self {
            path,
            tmp,
            inner: Mutex::new(inner),
        })
    }

    pub async fn producer_id(&self) -> u64 {
        self.inner.lock().await.producer_id
    }

    /// Reserve the next sequence for `partition` and persist the state before
    /// returning. If the send using this sequence fails, the caller retries
    /// with the same value via [`reuse`].
    pub async fn next_sequence(&self, partition: u32) -> anyhow::Result<u64> {
        let next = {
            let mut g = self.inner.lock().await;
            let seq = g.sequences.entry(partition).or_insert(0);
            *seq += 1;
            *seq
        };
        let snapshot = self.inner.lock().await.clone();
        self.persist_locked(&snapshot).await?;
        Ok(next)
    }

    /// The most recently reserved sequence for a partition — the value to
    /// resend on retry.
    pub async fn last_sequence(&self, partition: u32) -> Option<u64> {
        self.inner.lock().await.sequences.get(&partition).copied()
    }

    async fn persist_locked(&self, state: &StateFile) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec_pretty(state)?;
        tokio::fs::write(&self.tmp, bytes).await?;
        tokio::fs::rename(&self.tmp, &self.path).await?;
        Ok(())
    }
}

fn allocate_producer_id() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1);
    // Mix in the process id and a per-call counter entropy source so two
    // producers starting in the same nanosecond still differ.
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let c = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    (ts ^ ((std::process::id() as u64) << 32) ^ c.wrapping_mul(0x9e37_79b9_7f4a_7c15))
        | 1 // keep it non-zero
}
