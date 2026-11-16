//! Consumer worker: joins one consumer group and long-polls its assigned
//! partitions, journaling every delivered record and committing positions.
//!
//! The group protocol in `mqueue-client` handles rebalancing automatically:
//! when the orchestrator adds or removes consumer processes, each surviving
//! member's heartbeat observes the generation bump and re-joins on the next
//! poll.

use std::path::PathBuf;

use anyhow::Context;
use mqueue_client::{Consumer, StartPosition};
use tokio::io::AsyncWriteExt;

use crate::payload::{now_ms, JournalEvent, Payload};
use crate::signal_flag::StopFlag;

#[derive(Debug, Clone)]
pub struct ConsumerArgs {
    pub addr: String,
    pub topic: String,
    pub group: String,
    pub id: String,
    pub dir: PathBuf,
    pub max_bytes: u32,
    pub poll_timeout_ms: u32,
}

pub async fn run(args: ConsumerArgs) -> anyhow::Result<()> {
    tokio::fs::create_dir_all(&args.dir).await?;
    let journal_path = args.dir.join(format!("journal-{}.jsonl", args.id));

    // Earliest + committed offset resume: a freshly created group starts at the
    // log start; a restarting consumer picks up its last committed position.
    let consumer = Consumer::new(
        &args.addr,
        &args.group,
        &args.topic,
        &args.id,
        StartPosition::Earliest,
    );
    consumer.join().await.context("initial join")?;

    let stop = StopFlag::install();
    let mut journal = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&journal_path)
        .await?;

    tracing::info!(
        consumer = %args.id,
        assignments = ?consumer.assignments().await,
        "consumer worker up"
    );

    while !stop.is_stopped() {
        // poll() re-joins internally when a heartbeat observed a rebalance.
        let batches = match consumer
            .poll(
                args.max_bytes,
                std::time::Duration::from_millis(args.poll_timeout_ms as u64),
            )
            .await
        {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(consumer = %args.id, error = %e, "poll failed; backing off");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                continue;
            }
        };

        for (partition, messages) in batches {
            if messages.is_empty() {
                continue;
            }
            let recv_ts = now_ms();
            for m in &messages {
                let payload = Payload::decode(&m.value).unwrap_or(Payload {
                    global: u64::MAX,
                    send_ts: m.timestamp_ms,
                });
                let key = String::from_utf8_lossy(&m.key).to_string();
                let ev = JournalEvent {
                    consumer: args.id.clone(),
                    partition,
                    offset: m.offset,
                    global: payload.global,
                    key,
                    producer_id: m.producer_id,
                    sequence: m.sequence,
                    send_ts: payload.send_ts,
                    recv_ts,
                };
                let mut line = serde_json::to_vec(&ev)?;
                line.push(b'\n');
                journal.write_all(&line).await?;
            }
            // Durable journal before commit: if we crash after commit but
            // before the write lands, the record is processed twice on resume
            // — at-least-once, never a silent gap.
            journal.flush().await?;
            journal.sync_all().await?;
            if let Err(e) = consumer.commit(partition).await {
                tracing::debug!(consumer = %args.id, partition, error = %e,
                    "commit fenced; will re-join");
            }
        }
    }

    consumer.leave().await;
    tracing::info!(consumer = %args.id, "consumer worker stopped");
    Ok(())
}
