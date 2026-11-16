#![allow(dead_code)]
//! A partitioned, append-only log message queue storage engine.
//!
//! The crate is transport-agnostic and synchronous at the IO layer: wrap calls
//! in `tokio::task::spawn_blocking` from an async server.

mod index;
mod partition;
mod record;
mod segment;

pub use partition::{Partition, PartitionStat, RetentionPolicy};
pub use record::{Record, RecordView, HEADER_LEN, MAGIC};
pub use segment::{RecoveryReport, SegmentStat};

/// FNV-1a 64-bit hash. Used by both broker and client for key → partition
/// routing, so the mapping is stable across language/client implementations.
pub fn fnv1a64(data: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Hash a key to a partition index in `0..partition_count`.
pub fn partition_for_key(key: &[u8], partition_count: u32) -> u32 {
    if partition_count == 0 {
        return 0;
    }
    (fnv1a64(key) % partition_count as u64) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_stable() {
        assert_eq!(fnv1a64(b""), 0xcbf29ce484222325);
        assert_eq!(fnv1a64(b"a"), 0xaf63dc4c8601ec8c);
        assert_eq!(partition_for_key(b"order-42", 8), partition_for_key(b"order-42", 8));
    }

    #[test]
    fn append_and_recover() {
        let dir = std::env::temp_dir().join(format!("mqueue-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let p = Partition::open(
            "t",
            0,
            &dir,
            RetentionPolicy {
                max_segment_bytes: 4096,
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..200u64 {
            p.append(1, i, 1000 + i, b"k", format!("v{i}").as_bytes())
                .unwrap();
        }
        let end = p.log_end_offset();
        assert_eq!(end, 200);
        drop(p);

        // Simulate a torn tail: append garbage bytes to the active log.
        let mut logs: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "log").unwrap_or(false))
            .collect();
        // The active segment is the one with the largest base offset.
        logs.sort();
        let active = logs.last().unwrap().clone();
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&active).unwrap();
            f.write_all(b"\x00garbage-torn-tail").unwrap();
        }

        let p2 = Partition::open("t", 0, &dir, RetentionPolicy::default()).unwrap();
        // Recovery must truncate the torn tail; read every retained record,
        // following the next-offset cursor across segment boundaries.
        let mut recs = Vec::new();
        let mut cursor = p2.log_start_offset();
        while cursor < p2.log_end_offset() {
            let (batch, next) = p2.read_from(cursor, 1 << 16).unwrap();
            assert!(next > cursor, "read made no progress at {cursor}");
            cursor = next;
            recs.extend(batch);
        }
        assert!(recs.windows(2).all(|w| w[0].offset + 1 == w[1].offset));
        assert!(recs.iter().all(|r| r.producer_id == 1));
        // The torn tail really was detected and removed.
        let truncated = p2
            .recovery
            .bytes_truncated
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(truncated >= 18, "expected truncation, got {truncated}");
        // Every still-retained record is readable end-to-end.
        assert_eq!(recs.len() as u64, p2.log_end_offset() - p2.log_start_offset());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_segment_reopen_preserves_all_records() {
        // Roll many small segments, close, reopen (full recovery including
        // index rebuild of sealed segments), and read every record back in
        // order across every segment boundary.
        let dir = std::env::temp_dir().join(format!("mqueue-multi-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let total = 1000u64;
        let policy = RetentionPolicy {
            max_segment_bytes: 3000,
            max_age_ms: None,
            ..Default::default()
        };
        {
            let p = Partition::open("t", 0, &dir, policy.clone()).unwrap();
            for i in 0..total {
                p.append(7, i, 5000 + i, format!("key-{}", i % 5).as_bytes(),
                    vec![i as u8; 24].as_ref()).unwrap();
            }
            assert!(p.stat().segments.len() >= 3, "expected several segments");
        }
        // Reopen: every sealed segment index gets rebuilt from its log.
        let p2 = Partition::open("t", 0, &dir, policy).unwrap();
        assert_eq!(p2.log_start_offset(), 0);
        assert_eq!(p2.log_end_offset(), total);
        let mut got = Vec::new();
        let mut cursor = 0u64;
        while cursor < total {
            let (batch, next) = p2.read_from(cursor, 4096).unwrap();
            assert!(next > cursor, "no progress at {cursor}");
            cursor = next;
            got.extend(batch);
        }
        assert_eq!(got.len() as u64, total);
        for (i, r) in got.iter().enumerate() {
            assert_eq!(r.offset, i as u64, "offset mismatch at {i}");
            assert_eq!(r.sequence, i as u64);
            assert_eq!(r.producer_id, 7);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn timestamp_lookup_and_segment_roll() {
        let dir = std::env::temp_dir().join(format!("mqueue-ts-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let p = Partition::open(
            "t",
            0,
            &dir,
            RetentionPolicy {
                max_segment_bytes: 2048,
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..300u64 {
            p.append(1, i, 10_000 + i * 10, b"k", vec![0u8; 32].as_ref())
                .unwrap();
        }
        let off = p.find_timestamp(10_500).unwrap();
        let (recs, _) = p.read_from(off, 4096).unwrap();
        assert!(recs.first().unwrap().timestamp_ms >= 10_500);
        assert!(p.stat().segments.len() >= 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
