//! Consumer-group client.
//!
//! Lifecycle:
//! 1. `join` registers with the coordinator and receives a generation plus a
//!    partition assignment.
//! 2. A background heartbeat task keeps the session alive; on rebalance it
//!    bumps a flag so the poll loop re-joins and refreshes assignments.
//! 3. Each assigned partition maintains a local fetch position initialized
//!    from the last committed offset (or earliest / a timestamp seek).
//! 4. Processed batches are committed; after restart the consumer resumes from
//!    the committed position — at-least-once delivery.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use mqueue_proto::{
    CommitOffsetRequest, FetchRequest, FetchResponse, FetchedMessage, HeartbeatRequest,
    JoinGroupRequest, LeaveGroupRequest, ListOffsetsRequest, ListOffsetsResponse,
    OffsetByTimeRequest, Request, Response,
};
use tokio::sync::RwLock;

use crate::connection::Connection;

/// Where to start when no committed offset exists yet.
#[derive(Debug, Clone, Copy)]
pub enum StartPosition {
    /// Oldest retained record.
    Earliest,
    /// New data only.
    Latest,
    /// First record at or after the given timestamp (ms).
    Timestamp(u64),
}

pub struct Consumer {
    /// Data-plane connection: long-poll fetches only.
    conn: Connection,
    /// Control-plane connection: join, commit, offset lookups. Separate from
    /// both fetch and heartbeat so none can head-of-line block another.
    control_conn: Connection,
    /// Dedicated connection for the background heartbeat. Keeping this
    /// separate is essential: a long-poll fetch (or its reconnect backoff)
    /// holds the data connection, and a heartbeat sharing that connection
    /// would block indefinitely, never observing a generation change after a
    /// broker restart. One TCP connection per role avoids the head-of-line
    /// stall entirely.
    heartbeat_conn: Connection,
    addr: String,
    group: String,
    topic: String,
    consumer_id: String,
    start: StartPosition,
    generation: Arc<RwLock<u64>>,
    assignments: Arc<RwLock<Vec<u32>>>,
    /// partition -> next offset to fetch
    positions: Arc<RwLock<HashMap<u32, u64>>>,
    rebalance_flag: Arc<AtomicBool>,
    heartbeat_stop: Arc<AtomicBool>,
    /// Bumped on every (re-)join; a heartbeat task exits once it observes a
    /// newer epoch, guaranteeing a single live heartbeat task.
    heartbeat_epoch: Arc<std::sync::atomic::AtomicU64>,
    joined: AtomicBool,
}

impl Consumer {
    pub fn new(
        addr: &str,
        group: &str,
        topic: &str,
        consumer_id: &str,
        start: StartPosition,
    ) -> Self {
        Self {
            conn: Connection::new(addr),
            control_conn: Connection::new(addr),
            heartbeat_conn: Connection::new(addr),
            addr: addr.to_string(),
            group: group.to_string(),
            topic: topic.to_string(),
            consumer_id: consumer_id.to_string(),
            start,
            generation: Arc::new(RwLock::new(0)),
            assignments: Arc::new(RwLock::new(Vec::new())),
            positions: Arc::new(RwLock::new(HashMap::new())),
            rebalance_flag: Arc::new(AtomicBool::new(false)),
            heartbeat_stop: Arc::new(AtomicBool::new(true)),
            heartbeat_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            joined: AtomicBool::new(false),
        }
    }

    pub fn id(&self) -> &str {
        &self.consumer_id
    }

    pub async fn assignments(&self) -> Vec<u32> {
        self.assignments.read().await.clone()
    }

    /// Join (or re-join after a rebalance) and initialize fetch positions.
    ///
    /// Group-coordination calls (join, list-offsets, offset-by-time) run on
    /// the dedicated control connection so they never queue behind a
    /// long-polling fetch on the data connection.
    pub async fn join(&self) -> anyhow::Result<Vec<u32>> {
        let resp = self
            .control_conn
            .request(&Request::JoinGroup(JoinGroupRequest {
                group: self.group.clone(),
                topic: self.topic.clone(),
                consumer_id: self.consumer_id.clone(),
            }))
            .await?;
        let joined = match resp {
            Response::Joined(j) => j,
            other => anyhow::bail!("join failed: {other:?}"),
        };
        *self.generation.write().await = joined.generation;
        let assigned = joined.assignments.clone();
        *self.assignments.write().await = assigned.clone();
        self.rebalance_flag.store(false, Ordering::SeqCst);

        // Resolve a starting position per newly assigned partition. Existing
        // positions survive a re-join so in-flight work isn't re-fetched.
        for p in &assigned {
            let already = self.positions.read().await.contains_key(p);
            if already {
                continue;
            }
            let offset = self.resolve_start(*p).await?;
            self.positions.write().await.insert(*p, offset);
        }
        // Drop partitions we no longer own.
        {
            let mut positions = self.positions.write().await;
            positions.retain(|p, _| assigned.contains(p));
        }

        self.joined.store(true, Ordering::SeqCst);
        self.spawn_heartbeat();
        Ok(self.assignments.read().await.clone())
    }

    async fn resolve_start(&self, partition: u32) -> anyhow::Result<u64> {
        let resp = self
            .control_conn
            .request(&Request::ListOffsets(ListOffsetsRequest {
                group: self.group.clone(),
                topic: self.topic.clone(),
                partition,
            }))
            .await?;
        let info: ListOffsetsResponse = match resp {
            Response::Offsets(o) => o,
            other => anyhow::bail!("list offsets failed: {other:?}"),
        };
        if let Some(committed) = info.committed {
            // Retention may have deleted the committed offset's segment.
            return Ok(committed.max(info.log_start_offset));
        }
        match self.start {
            StartPosition::Earliest => Ok(info.log_start_offset),
            StartPosition::Latest => Ok(info.log_end_offset),
            StartPosition::Timestamp(ts) => {
                let resp = self
                    .control_conn
                    .request(&Request::OffsetByTime(OffsetByTimeRequest {
                        topic: self.topic.clone(),
                        partition,
                        timestamp_ms: ts,
                    }))
                    .await?;
                match resp {
                    Response::Offset(o) => Ok(o),
                    other => anyhow::bail!("offset-by-time failed: {other:?}"),
                }
            }
        }
    }

    fn spawn_heartbeat(&self) {
        // A new epoch invalidates any previously spawned heartbeat task.
        let my_epoch = self
            .heartbeat_epoch
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        self.heartbeat_stop.store(false, Ordering::SeqCst);
        let conn = self.heartbeat_conn.clone();
        let group = self.group.clone();
        let id = self.consumer_id.clone();
        let generation = self.generation.clone();
        let flag = self.rebalance_flag.clone();
        let stop_handle = self.heartbeat_stop.clone();
        let epoch = self.heartbeat_epoch.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                // Stop on shutdown, or once a newer (re-)join spawned a
                // replacement task.
                if stop_handle.load(Ordering::SeqCst)
                    || epoch.load(Ordering::SeqCst) != my_epoch
                {
                    return;
                }
                let gen = *generation.read().await;
                let resp = conn
                    .request(&Request::Heartbeat(HeartbeatRequest {
                        group: group.clone(),
                        consumer_id: id.clone(),
                        generation: gen,
                    }))
                    .await;
                match resp {
                    Ok(Response::Heartbeat(h)) if h.rebalanced => {
                        tracing::info!(
                            consumer = %id, old = gen, new = h.generation,
                            "group rebalanced; will re-join"
                        );
                        flag.store(true, Ordering::SeqCst);
                        return;
                    }
                    Ok(Response::Heartbeat(_)) => {}
                    Ok(Response::Error(mqueue_proto::ErrorCode::UnknownConsumer, _)) => {
                        // Broker restarted and forgot our membership, or the
                        // coordinator swept us. Re-join on the next poll.
                        tracing::warn!(consumer = %id, "unknown consumer; marking re-join");
                        flag.store(true, Ordering::SeqCst);
                        return;
                    }
                    Ok(other) => {
                        tracing::debug!(consumer = %id, ?other, "unexpected heartbeat response");
                    }
                    Err(e) => {
                        // Broker application errors arrive wrapped as
                        // ClientError::Broker. A lost membership (broker
                        // restart, session expiry) or a fenced generation means
                        // we must re-join; anything else is a transport failure
                        // that the connection layer already retried.
                        if let crate::connection::ClientError::Broker(code, _) = &e {
                            use mqueue_proto::ErrorCode::*;
                            match code {
                                UnknownConsumer | IllegalGeneration | NotCoordinator => {
                                    tracing::warn!(
                                        consumer = %id, ?code,
                                        "membership invalid; marking re-join"
                                    );
                                    flag.store(true, Ordering::SeqCst);
                                    return;
                                }
                                _ => {}
                            }
                        }
                        tracing::debug!(consumer = %id, error = %e, "heartbeat request failed");
                    }
                }
            }
        });
    }

    /// True when the assignment changed and the caller should re-join.
    pub fn needs_rejoin(&self) -> bool {
        self.rebalance_flag.load(Ordering::SeqCst)
    }

    /// Fetch the next batch across assigned partitions, long-polling up to
    /// `timeout`. Returns `(partition, messages)`.
    pub async fn poll(
        &self,
        max_bytes: u32,
        timeout: Duration,
    ) -> anyhow::Result<Vec<(u32, Vec<FetchedMessage>)>> {
        if !self.joined.load(Ordering::SeqCst) {
            self.join().await?;
        }
        if self.needs_rejoin() {
            self.join().await.context("re-join after rebalance")?;
        }
        let assignments = self.assignments.read().await.clone();
        let mut out = Vec::new();
        for partition in assignments {
            let offset = {
                let positions = self.positions.read().await;
                *positions.get(&partition).unwrap_or(&0)
            };
            let resp = self
                .conn
                .request(&Request::Fetch(FetchRequest {
                    group: self.group.clone(),
                    topic: self.topic.clone(),
                    partition,
                    offset,
                    max_bytes,
                    timeout_ms: timeout.as_millis() as u32,
                }))
                .await?;
            let fetched: FetchResponse = match resp {
                Response::Fetched(f) => f,
                other => anyhow::bail!("fetch failed: {other:?}"),
            };
            if !fetched.messages.is_empty() {
                self.positions
                    .write()
                    .await
                    .insert(partition, fetched.next_offset);
                out.push((partition, fetched.messages));
            }
        }
        Ok(out)
    }

    /// Commit the current fetch position of `partition` (the next offset to
    /// consume). Fenced generations force a re-join.
    pub async fn commit(&self, partition: u32) -> anyhow::Result<()> {
        let generation = *self.generation.read().await;
        let offset = *self
            .positions
            .read()
            .await
            .get(&partition)
            .context("no position for partition")?;
        let resp = self
            .control_conn
            .request(&Request::CommitOffset(CommitOffsetRequest {
                group: self.group.clone(),
                topic: self.topic.clone(),
                partition,
                offset,
                generation,
            }))
            .await?;
        match resp {
            Response::Ok => Ok(()),
            Response::Error(mqueue_proto::ErrorCode::IllegalGeneration, _) => {
                self.rebalance_flag.store(true, Ordering::SeqCst);
                anyhow::bail!("commit fenced by generation change");
            }
            other => anyhow::bail!("commit failed: {other:?}"),
        }
    }

    /// Seek a partition to an explicit offset (e.g. for time-based replay).
    pub async fn seek(&self, partition: u32, offset: u64) {
        self.positions.write().await.insert(partition, offset);
    }

    pub async fn leave(&self) {
        self.heartbeat_stop.store(true, Ordering::SeqCst);
        let generation = *self.generation.read().await;
        let _ = self
            .control_conn
            .request(&Request::LeaveGroup(LeaveGroupRequest {
                group: self.group.clone(),
                consumer_id: self.consumer_id.clone(),
                generation,
            }))
            .await;
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.heartbeat_stop.store(true, Ordering::SeqCst);
    }
}
