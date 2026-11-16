//! A single topic partition: an ordered, append-only log with a notifier for
//! long-polling consumers.
//!
//! All storage calls are synchronous (the storage crate intentionally has no
//! async dependency); the broker runs them inside `tokio::task::spawn_blocking`.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::Notify;

use crate::record::Record;
use crate::segment::{AppendOutcome, RecoveryReport, SegmentSet, SegmentStat};

#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub max_segment_bytes: u64,
    /// Segments whose newest record is older than this are deletable.
    pub max_age_ms: Option<u64>,
    /// Retention floor: keep total partition size at or below this where
    /// possible (the active segment is never deleted).
    pub max_total_bytes: u64,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_segment_bytes: 64 * 1024 * 1024,
            max_age_ms: Some(7 * 24 * 60 * 60 * 1000),
            max_total_bytes: 1024 * 1024 * 1024,
        }
    }
}

/// Snapshot used by the operations console.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PartitionStat {
    pub topic: String,
    pub partition: u32,
    pub log_start_offset: u64,
    pub log_end_offset: u64,
    pub total_bytes: u64,
    pub segments: Vec<SegmentStat>,
}

pub struct Partition {
    pub topic: String,
    pub index: u32,
    pub dir: PathBuf,
    segments: RwLock<SegmentSet>,
    notify: Notify,
    pub retention: RwLock<RetentionPolicy>,
    pub recovery: RecoveryReport,
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("topic", &self.topic)
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl Partition {
    /// Open (and recover) a partition stored under `dir`.
    pub fn open(topic: &str, index: u32, dir: &Path, retention: RetentionPolicy) -> std::io::Result<Arc<Self>> {
        std::fs::create_dir_all(dir)?;
        let (segments, recovery) =
            SegmentSet::open(dir, retention.max_segment_bytes).map_err(io_err)?;
        Ok(Arc::new(Self {
            topic: topic.to_string(),
            index,
            dir: dir.to_path_buf(),
            segments,
            notify: Notify::new(),
            retention: RwLock::new(retention),
            recovery,
        }))
    }

    pub fn log_end_offset(&self) -> u64 {
        self.segments.read().unwrap().next_offset()
    }

    pub fn log_start_offset(&self) -> u64 {
        self.segments.read().unwrap().base_offset()
    }

    /// Append one record and publish the new offset to waiting fetchers.
    pub fn append(
        &self,
        producer_id: u64,
        sequence: u64,
        timestamp_ms: u64,
        key: &[u8],
        value: &[u8],
    ) -> std::io::Result<u64> {
        let outcome: AppendOutcome = {
            let mut set = self.segments.write().unwrap();
            set.append(producer_id, sequence, timestamp_ms, key, value)
                .map_err(io_err)?
        };
        // Flush outside the write lock's critical path is unnecessary — the
        // BufWriter flush is cheap — but readers must see the bytes, so flush
        // before notifying.
        {
            let mut set = self.segments.write().unwrap();
            set.flush(false).map_err(io_err)?;
        }
        self.notify.notify_waiters();
        Ok(outcome.offset)
    }

    /// fsync the active segment (called periodically by the broker).
    pub fn sync(&self) -> std::io::Result<()> {
        let mut set = self.segments.write().unwrap();
        set.flush(true).map_err(io_err)
    }

    /// Read records starting at `start_offset`. Empty result means the caller
    /// should long-poll via [`wait_for`].
    pub fn read_from(
        &self,
        start_offset: u64,
        max_bytes: usize,
    ) -> std::io::Result<(Vec<Record>, u64)> {
        let set = self.segments.read().unwrap();
        if start_offset < set.base_offset() {
            // Offset was deleted by retention: jump to the segment start rather
            // than erroring, and let the consumer detect the jump.
            let base = set.base_offset();
            return set.read_from(base, max_bytes).map_err(io_err);
        }
        set.read_from(start_offset, max_bytes).map_err(io_err)
    }

    /// Read every retained record from the log start to the current end,
    /// following the next-offset cursor across segment boundaries. Used at
    /// startup to rebuild derived state (e.g. the idempotency window) that must
    /// cover all segments, not just the first one a single `read_from` lands on.
    pub fn read_all(&self, batch_bytes: usize) -> std::io::Result<Vec<Record>> {
        let mut out = Vec::new();
        let mut cursor = self.log_start_offset();
        let end = self.log_end_offset();
        while cursor < end {
            let (batch, next) = self.read_from(cursor, batch_bytes)?;
            if next <= cursor {
                // Defensive: a stuck cursor would otherwise loop forever.
                break;
            }
            cursor = next;
            out.extend(batch);
        }
        Ok(out)
    }

    /// Resolve a wall-clock timestamp to the earliest eligible offset.
    pub fn find_timestamp(&self, timestamp_ms: u64) -> std::io::Result<u64> {
        let set = self.segments.read().unwrap();
        set.find_timestamp(timestamp_ms).map_err(io_err)
    }

    /// Block (asynchronously) until new records appear or the timeout elapses.
    pub async fn wait_for(&self, timeout: Duration) {
        let notified = self.notify.notified();
        // Re-check under the lock pattern used by callers: they first attempt a
        // read, and only call wait_for when empty, so no data published between
        // the read and registering the waiter can be lost — notify_waiters()
        // does not buffer permits, hence we register before any future append.
        if tokio::time::timeout(timeout, notified).await.is_err() {
            // Timed out; caller retries the read and gets an empty batch.
        }
    }

    pub fn enforce_retention(&self, now_ms: u64) -> std::io::Result<Vec<u64>> {
        let policy = self.retention.read().unwrap().clone();
        let mut set = self.segments.write().unwrap();
        set.enforce_retention(
            now_ms,
            policy.max_age_ms,
            policy.max_total_bytes,
        )
        .map_err(io_err)
    }

    pub fn stat(&self) -> PartitionStat {
        let set = self.segments.read().unwrap();
        let total_bytes: u64 = set
            .segment_stats()
            .iter()
            .map(|s| s.size_bytes)
            .sum();
        PartitionStat {
            topic: self.topic.clone(),
            partition: self.index,
            log_start_offset: set.base_offset(),
            log_end_offset: set.next_offset(),
            total_bytes,
            segments: set.segment_stats(),
        }
    }
}

fn io_err(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e)
}
