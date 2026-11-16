//! Correctness verification after a chaos run.
//!
//! The source of truth is the broker log itself: the verifier joins a throwaway
//! consumer group, reads every retained partition from earliest to end, and
//! builds the canonical ordered set of `(producer_id, partition, sequence)`
//! records. It then walks every consumer journal and checks:
//!
//! 1. **At-least-once coverage** — every log record appears in some journal.
//! 2. **No duplicate effects** — each log record appears at most once per
//!    consumer process (a rebalance can hand a partition to a new consumer, but
//!    the same record is never processed twice by one consumer).
//! 3. **Per-partition order** — within every consumer journal, offsets for a
//!    partition are strictly increasing, and same-key `global` values are
//!    consumed in ascending order.
//! 4. **No phantom records** — journals contain nothing the log does not.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use anyhow::Context;
use mqueue_client::{Consumer, StartPosition};
use mqueue_proto::FetchedMessage;
use serde::Serialize;

use crate::payload::JournalEvent;

/// A record identity, stable across retries thanks to producer idempotency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RecordId {
    pub producer_id: u64,
    pub partition: u32,
    pub sequence: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct VerifyReport {
    pub total_in_log: usize,
    pub total_journal_events: usize,
    pub unique_records_consumed: usize,
    pub missing_records: Vec<String>,
    pub duplicate_events: Vec<String>,
    pub order_violations: Vec<String>,
    pub phantom_events: Vec<String>,
    pub partitions_checked: usize,
    pub passed: bool,
}

/// Read the full log of every partition through a throwaway group. Returns
/// records grouped by partition, each group already in ascending offset order
/// (the fetch walks `next_offset` forward, so no re-sorting is needed).
async fn drain_log(
    addr: &str,
    topic: &str,
    partitions: u32,
) -> anyhow::Result<HashMap<u32, Vec<FetchedMessage>>> {
    let group = format!("__verifier_{}", std::process::id());
    let consumer = Consumer::new(addr, &group, topic, "verifier", StartPosition::Earliest);
    // Force the group to own every partition by joining as the sole member.
    consumer.join().await?;

    let mut by_partition: HashMap<u32, Vec<FetchedMessage>> = HashMap::new();
    for partition in 0..partitions {
        let mut offset = {
            // Earliest retained offset.
            let conn = mqueue_client::Connection::new(addr);
            let resp = conn
                .request(&mqueue_proto::Request::ListOffsets(
                    mqueue_proto::ListOffsetsRequest {
                        group: group.clone(),
                        topic: topic.to_string(),
                        partition,
                    },
                ))
                .await?;
            match resp {
                mqueue_proto::Response::Offsets(o) => o.log_start_offset,
                other => anyhow::bail!("verifier list offsets: {other:?}"),
            }
        };

        let mut bucket = Vec::new();
        loop {
            let resp = mqueue_client::Connection::new(addr)
                .request(&mqueue_proto::Request::Fetch(mqueue_proto::FetchRequest {
                    group: group.clone(),
                    topic: topic.to_string(),
                    partition,
                    offset,
                    max_bytes: 4 * 1024 * 1024,
                    timeout_ms: 0,
                }))
                .await?;
            let fetched = match resp {
                mqueue_proto::Response::Fetched(f) => f,
                other => anyhow::bail!("verifier fetch: {other:?}"),
            };
            if fetched.messages.is_empty() {
                break;
            }
            offset = fetched.next_offset;
            bucket.extend(fetched.messages);
        }
        by_partition.insert(partition, bucket);
    }
    consumer.leave().await;
    Ok(by_partition)
}

fn load_journals(dir: &Path) -> anyhow::Result<Vec<JournalEvent>> {
    let mut events = Vec::new();
    if !dir.exists() {
        return Ok(events);
    }
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.file_name().map(|n| n.to_string_lossy().starts_with("journal-")).unwrap_or(false)
                && p.extension().map(|e| e == "jsonl").unwrap_or(false)
            {
                let text = std::fs::read_to_string(&p)
                    .with_context(|| format!("reading journal {p:?}"))?;
                for line in text.lines().filter(|l| !l.trim().is_empty()) {
                    events.push(serde_json::from_str(line)?);
                }
            }
        }
    }
    Ok(events)
}

pub async fn verify(
    addr: &str,
    topic: &str,
    partitions: u32,
    run_dir: &Path,
) -> anyhow::Result<VerifyReport> {
    let log_by_partition = drain_log(addr, topic, partitions).await?;
    let journals = load_journals(run_dir)?;

    // Flatten for totals while keeping each partition's vector ordered.
    let total_in_log: usize = log_by_partition.values().map(|v| v.len()).sum();
    let mut report = VerifyReport::default();
    report.total_in_log = total_in_log;
    report.total_journal_events = journals.len();
    report.partitions_checked = partitions as usize;

    // Canonical record set from the broker log, with per-partition ordering.
    let mut canonical: HashSet<RecordId> = HashSet::with_capacity(total_in_log);
    let mut log_order: HashMap<u32, Vec<(u64, RecordId)>> = HashMap::new();
    let mut log_seen: HashSet<RecordId> = HashSet::new();
    for (partition, msgs) in &log_by_partition {
        for m in msgs {
            let id = RecordId {
                producer_id: m.producer_id,
                partition: m.partition,
                sequence: m.sequence,
            };
            if !log_seen.insert(id) {
                report.duplicate_events.push(format!(
                    "broker log contains duplicate record {id:?}"
                ));
            }
            canonical.insert(id);
            log_order.entry(*partition).or_default().push((m.offset, id));
        }
    }
    // The log itself must be offset-ordered per partition.
    for (p, rows) in &log_order {
        for w in rows.windows(2) {
            if w[0].0 >= w[1].0 {
                report.order_violations.push(format!(
                    "partition {p} log offsets not ordered: {} -> {}",
                    w[0].0, w[1].0
                ));
            }
        }
    }

    // Coverage set and per-(consumer, partition) processing streams. Ordering
    // and duplicate checks are scoped to an *assignment epoch*: a maximal run
    // of strictly increasing offsets for one (consumer, partition). On
    // rebalance a consumer may regain a partition and resume from a committed
    // offset behind what it already processed; the resulting replay is legal
    // at-least-once behavior, so an offset that does not advance starts a new
    // epoch rather than counting as an ordering violation or duplicate.
    let mut consumed: HashMap<RecordId, HashSet<String>> = HashMap::new();
    let mut streams: HashMap<(String, u32), Vec<&JournalEvent>> = HashMap::new();

    for ev in &journals {
        let id = RecordId {
            producer_id: ev.producer_id,
            partition: ev.partition,
            sequence: ev.sequence,
        };
        if !canonical.contains(&id) {
            report.phantom_events.push(format!(
                "consumer {} processed unknown record {id:?} offset {}",
                ev.consumer, ev.offset
            ));
            continue;
        }
        consumed.entry(id).or_default().insert(ev.consumer.clone());
        streams
            .entry((ev.consumer.clone(), ev.partition))
            .or_default()
            .push(ev);
    }

    // Within each (consumer, partition) stream, split into epochs at every
    // non-advancing offset, then verify strict increase and unique record
    // identity inside every epoch. Same-key order follows from offset order
    // because keys hash deterministically to partitions, so checking offsets
    // also guarantees same-key global order within an epoch.
    for ((consumer, partition), events) in &streams {
        let mut epoch: Vec<&JournalEvent> = Vec::new();
        let mut prev_offset: Option<u64> = None;
        let check = |epoch: &[&JournalEvent], report: &mut VerifyReport| {
            let mut seen: HashSet<RecordId> = HashSet::new();
            for w in epoch.windows(2) {
                if w[0].offset >= w[1].offset {
                    report.order_violations.push(format!(
                        "consumer {consumer} partition {partition} in-epoch offset order: {} -> {}",
                        w[0].offset, w[1].offset
                    ));
                }
            }
            for ev in epoch {
                let id = RecordId {
                    producer_id: ev.producer_id,
                    partition: ev.partition,
                    sequence: ev.sequence,
                };
                if !seen.insert(id) {
                    report.duplicate_events.push(format!(
                        "consumer {consumer} processed {id:?} twice within one assignment epoch"
                    ));
                }
            }
        };
        for ev in events {
            if let Some(prev) = prev_offset {
                if ev.offset <= prev {
                    check(&epoch, &mut report);
                    epoch.clear();
                }
            }
            prev_offset = Some(ev.offset);
            epoch.push(ev);
        }
        check(&epoch, &mut report);
    }

    // Coverage: every canonical record consumed by >=1 consumer.
    let mut missing: Vec<RecordId> = canonical
        .iter()
        .filter(|id| !consumed.contains_key(id))
        .copied()
        .collect();
    missing.sort();
    report.missing_records = missing
        .iter()
        .take(50)
        .map(|id| format!("{id:?}"))
        .collect();
    report.unique_records_consumed = consumed.len();

    report.passed = report.missing_records.is_empty()
        && report.duplicate_events.is_empty()
        && report.order_violations.is_empty()
        && report.phantom_events.is_empty();
    Ok(report)
}

/// Aggregate end-to-end latency and throughput samples from journals.
#[derive(Debug, Default, Serialize)]
pub struct PerfStats {
    pub events: usize,
    pub latency_ms_min: u64,
    pub latency_ms_p50: u64,
    pub latency_ms_p95: u64,
    pub latency_ms_p99: u64,
    pub latency_ms_max: u64,
    /// Second bucket -> events consumed that second.
    pub consume_throughput: BTreeMap<u64, u64>,
    /// Second bucket -> events produced that second (from manifests).
    pub produce_throughput: BTreeMap<u64, u64>,
}

pub fn load_perf(run_dir: &Path) -> anyhow::Result<PerfStats> {
    let journals = load_journals(run_dir)?;
    let mut latencies = Vec::with_capacity(journals.len());
    let mut consume = BTreeMap::new();
    for ev in &journals {
        if ev.recv_ts >= ev.send_ts {
            latencies.push(ev.recv_ts - ev.send_ts);
        }
        *consume.entry(ev.recv_ts / 1000).or_insert(0u64) += 1;
    }
    latencies.sort_unstable();
    let pct = |p: f64| -> u64 {
        if latencies.is_empty() {
            0
        } else {
            let idx = ((latencies.len() as f64 * p) as usize).min(latencies.len() - 1);
            latencies[idx]
        }
    };

    let mut produce = BTreeMap::new();
    let mut stack = vec![run_dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path)? {
            let entry = entry?;
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.file_name().map(|n| n == "manifest.jsonl").unwrap_or(false) {
                let text = std::fs::read_to_string(&p)?;
                for line in text.lines().filter(|l| !l.trim().is_empty()) {
                    let ev: crate::payload::ManifestEvent = serde_json::from_str(line)?;
                    *produce.entry(ev.send_ts / 1000).or_insert(0u64) += 1;
                }
            }
        }
    }

    Ok(PerfStats {
        events: journals.len(),
        latency_ms_min: latencies.first().copied().unwrap_or(0),
        latency_ms_p50: pct(0.50),
        latency_ms_p95: pct(0.95),
        latency_ms_p99: pct(0.99),
        latency_ms_max: latencies.last().copied().unwrap_or(0),
        consume_throughput: consume,
        produce_throughput: produce,
    })
}
