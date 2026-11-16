//! Broker-side idempotency guard.
//!
//! The producer assigns every partition a strictly increasing sequence number
//! and persists it locally before sending. On retry the same `(producer_id,
//! partition, sequence)` tuple arrives again; instead of appending a duplicate
//! the broker returns the offset assigned to the original append.
//!
//! Cache layout is a per-producer, per-partition ring of recent sequences.
//! After a broker restart the cache is cold, but recovery scans every log
//! segment and rebuilds the most recent window from the record headers, so
//! retries across a crash stay idempotent too.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use mqueue_storage::Record;

/// How many recent sequences per (producer, partition) we remember. A retry
/// storm beyond this window would have to replay history older than the
/// producer's own persisted state, which cannot happen in practice.
const WINDOW: usize = 4096;

#[derive(Default)]
struct ProducerState {
    /// `sequence -> assigned offset`, insertion-ordered for eviction.
    seen: VecDeque<(u64, u64)>,
    index: HashMap<u64, u64>,
}

impl ProducerState {
    fn lookup(&self, seq: u64) -> Option<u64> {
        self.index.get(&seq).copied()
    }

    fn insert(&mut self, seq: u64, offset: u64) {
        if self.index.insert(seq, offset).is_none() {
            self.seen.push_back((seq, offset));
            while self.seen.len() > WINDOW {
                if let Some((old_seq, _)) = self.seen.pop_front() {
                    self.index.remove(&old_seq);
                }
            }
        }
    }
}

#[derive(Default)]
pub struct DedupInner {
    /// producer_id -> partition -> state
    table: Mutex<HashMap<u64, HashMap<u32, ProducerState>>>,
}

/// Cheaply cloneable handle; the table itself is shared.
#[derive(Default, Clone)]
pub struct DedupTable {
    inner: Arc<DedupInner>,
}

impl DedupTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the previously assigned offset when this exact sequence was
    /// already appended.
    pub fn check(&self, producer_id: u64, partition: u32, sequence: u64) -> Option<u64> {
        let g = self.inner.table.lock().unwrap();
        g.get(&producer_id)
            .and_then(|p| p.get(&partition))
            .and_then(|s| s.lookup(sequence))
    }

    /// Record a freshly appended sequence.
    pub fn remember(&self, producer_id: u64, partition: u32, sequence: u64, offset: u64) {
        let mut g = self.inner.table.lock().unwrap();
        g.entry(producer_id)
            .or_default()
            .entry(partition)
            .or_default()
            .insert(sequence, offset);
    }

    /// Rebuild the recent window from scanned log records (broker startup).
    /// Records are scanned in offset order; the bounded ring naturally keeps
    /// only the last `WINDOW` per pair.
    pub fn rebuild_from_records(&self, records: &[Record], partition: u32) {
        let mut g = self.inner.table.lock().unwrap();
        for r in records {
            g.entry(r.producer_id)
                .or_default()
                .entry(partition)
                .or_default()
                .insert(r.sequence, r.offset);
        }
    }
}
