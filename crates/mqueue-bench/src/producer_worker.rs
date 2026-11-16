//! Producer worker: runs as its own OS process so the orchestrator can kill it
//! with SIGKILL to simulate a real crash.
//!
//! Durability of the sequence state lives in the client (`Producer`). This
//! worker additionally journals every acknowledged record and remembers the
//! highest global index it sent, so a restarted process resumes exactly where
//! the previous incarnation left off.

use std::path::PathBuf;

use anyhow::Context;
use mqueue_client::Producer;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use crate::payload::{key_for, now_ms, ManifestEvent, Payload};
use crate::signal_flag::StopFlag;

#[derive(Debug, Clone)]
pub struct ProducerArgs {
    pub addr: String,
    pub topic: String,
    pub partitions: u32,
    pub id: String,
    pub dir: PathBuf,
    pub target_total: u64,
    pub key_space: u64,
    pub batch_size: usize,
    pub rate_per_sec: f64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Progress {
    next_global: u64,
}

pub async fn run(args: ProducerArgs) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&args.dir).await?;
    let state_path = args.dir.join("producer-state.json");
    let manifest_path = args.dir.join("manifest.jsonl");
    let progress_path = args.dir.join("progress.json");

    let progress: Progress = tokio::fs::read(&progress_path)
        .await
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default();
    let mut next_global = progress.next_global;

    let producer =
        Producer::connect(&args.addr, &args.topic, args.partitions, state_path)
            .await
            .context("producer connect")?;

    let stop = StopFlag::install();
    let mut manifest = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&manifest_path)
        .await?;

    let min_interval = if args.rate_per_sec > 0.0 {
        Some(std::time::Duration::from_secs_f64(
            args.batch_size.max(1) as f64 / args.rate_per_sec,
        ))
    } else {
        None
    };

    tracing::info!(producer = %args.id, from = next_global, "producer worker up");

    while next_global < args.target_total && !stop.is_stopped() {
        let batch_start = std::time::Instant::now();
        let n = (args.batch_size as u64)
            .min(args.target_total - next_global) as usize;
        let mut items = Vec::with_capacity(n);
        let mut metas = Vec::with_capacity(n);
        for _ in 0..n {
            let global = next_global;
            let key = key_for(global, args.key_space);
            let payload = Payload::new(global, now_ms()).encode();
            items.push((key.clone(), payload));
            metas.push((global, key));
            next_global += 1;
        }

        let send_ts = now_ms();
        let offsets = producer.send_batch(&items).await.context("batch send")?;
        let ack_ts = now_ms();

        // Journal acknowledgements, then persist progress. On crash between the
        // two, the restart re-sends some already-stored sequences; the broker
        // dedups them and no duplicate ever reaches a consumer.
        for ((global, key), offset) in metas.into_iter().zip(offsets.into_iter()) {
            let partition = mqueue_storage::partition_for_key(&key, args.partitions);
            let ev = ManifestEvent {
                producer: args.id.clone(),
                partition,
                offset,
                global,
                send_ts,
                ack_ts,
            };
            let mut line = serde_json::to_vec(&ev)?;
            line.push(b'\n');
            manifest.write_all(&line).await?;
        }
        manifest.flush().await?;
        manifest.sync_all().await?;

        let progress_bytes = serde_json::to_vec(&Progress { next_global })?;
        tokio::fs::write(&progress_path, progress_bytes).await?;

        if let Some(interval) = min_interval {
            let elapsed = batch_start.elapsed();
            if elapsed < interval {
                tokio::time::sleep(interval - elapsed).await;
            }
        }
    }

    tracing::info!(producer = %args.id, sent = next_global, "producer worker done");
    Ok(())
}
