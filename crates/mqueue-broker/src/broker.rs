//! Broker core: owns the partition registry, group coordinator, dedup table
//! and metrics, and dispatches every protocol request.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use mqueue_proto::{
    CommitOffsetRequest, CreateTopicRequest, ErrorCode, FetchRequest, FetchResponse,
    FetchedMessage, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupResponse,
    ListOffsetsRequest, ListOffsetsResponse, OffsetByTimeRequest, ProduceRequest,
    ProduceResponse, Request, Response, TopicInfo,
};
use mqueue_storage::{partition_for_key, Partition, RetentionPolicy};
use tokio::sync::RwLock;

use crate::dedup::DedupTable;
use crate::group::{GroupCoordinator, HeartbeatResult};
use crate::metrics::{LatencyStats, Metrics};
use crate::metadata::{MetadataStore, TopicConfig};

pub struct Broker {
    pub data_dir: PathBuf,
    defaults: RetentionPolicy,
    metadata: Arc<MetadataStore>,
    pub(crate) topics: RwLock<HashMap<String, Arc<TopicHandle>>>,
    pub groups: Arc<GroupCoordinator>,
    pub dedup: DedupTable,
    pub metrics: Metrics,
    pub fetch_latency: LatencyStats,
    started_at: Instant,
}

pub struct TopicHandle {
    pub config: TopicConfig,
    pub partitions: Vec<Arc<Partition>>,
}

impl Broker {
    pub async fn start(data_dir: PathBuf, defaults: RetentionPolicy) -> anyhow::Result<Arc<Self>> {
        tokio::fs::create_dir_all(&data_dir).await?;
        let meta_path = data_dir.join("topics.json");
        let metadata = Arc::new(MetadataStore::load(&meta_path).await?);
        let groups = Arc::new(GroupCoordinator::new(data_dir.join("offsets"))?);

        let broker = Arc::new(Self {
            data_dir: data_dir.clone(),
            defaults,
            metadata,
            topics: RwLock::new(HashMap::new()),
            groups,
            dedup: DedupTable::new(),
            metrics: Metrics::new(),
            fetch_latency: LatencyStats::new(),
            started_at: Instant::now(),
        });

        for cfg in broker.metadata.list().await {
            broker.open_topic(cfg).await?;
        }
        broker.spawn_maintenance(broker.clone());
        Ok(broker)
    }

    async fn open_topic(&self, cfg: TopicConfig) -> anyhow::Result<Arc<TopicHandle>> {
        let retention = cfg.retention(&self.defaults);
        let mut partitions = Vec::with_capacity(cfg.partitions as usize);
        for p in 0..cfg.partitions {
            let dir = self.data_dir.join("topics").join(&cfg.name).join(format!("{p}"));
            let partition = Partition::open(&cfg.name, p, &dir, retention.clone())
                .with_context(|| format!("opening partition {}-{}", cfg.name, p))?;
            // Rebuild the idempotency window from everything that survived on
            // disk. read_all walks every segment; a single-segment read would
            // miss records in rolled segments and let post-restart retries
            // bypass dedup (producing duplicate appends).
            let records = partition.read_all(16 * 1024 * 1024)?;
            self.dedup.rebuild_from_records(&records, p);
            partitions.push(partition);
        }
        let handle = Arc::new(TopicHandle {
            config: cfg.clone(),
            partitions,
        });
        self.topics
            .write()
            .await
            .insert(cfg.name.clone(), handle.clone());
        Ok(handle)
    }

    fn spawn_maintenance(self: &Arc<Self>, broker: Arc<Broker>) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            loop {
                tick.tick().await;
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let handles: Vec<Arc<TopicHandle>> =
                    broker.topics.read().await.values().cloned().collect();
                for h in &handles {
                    for p in &h.partitions {
                        let _ = p.sync();
                        let _ = p.enforce_retention(now_ms);
                    }
                }
                let _ = broker.groups.maintain().await;
            }
        });
    }

    // -- topic admin -------------------------------------------------------

    async fn create_topic(&self, req: CreateTopicRequest) -> Response {
        if req.partitions == 0 {
            return Response::Error(
                ErrorCode::InvalidRequest,
                "partition count must be > 0".into(),
            );
        }
        let cfg = TopicConfig {
            name: req.name.clone(),
            partitions: req.partitions,
            max_segment_bytes: req.max_segment_bytes,
            max_age_ms: req.max_age_ms,
            max_total_bytes: req.max_total_bytes,
        };
        match self.metadata.create(cfg.clone()).await {
            Ok(true) => {
                if let Err(e) = self.open_topic(cfg).await {
                    tracing::error!(error = %e, topic = %req.name, "failed to open topic");
                    return Response::Error(ErrorCode::BrokerUnavailable, e.to_string());
                }
                Response::TopicCreated
            }
            Ok(false) => Response::Error(
                ErrorCode::InvalidRequest,
                format!("topic {} already exists", req.name),
            ),
            Err(e) => Response::Error(ErrorCode::BrokerUnavailable, e.to_string()),
        }
    }

    async fn list_topics(&self) -> Response {
        let handles: Vec<Arc<TopicHandle>> =
            self.topics.read().await.values().cloned().collect();
        let mut infos = Vec::new();
        for h in handles {
            infos.push(TopicInfo {
                name: h.config.name.clone(),
                partitions: h.config.partitions,
                log_start_offsets: h.partitions.iter().map(|p| p.log_start_offset()).collect(),
                log_end_offsets: h.partitions.iter().map(|p| p.log_end_offset()).collect(),
            });
        }
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        Response::Topics(infos)
    }

    async fn topic(&self, name: &str) -> Option<Arc<TopicHandle>> {
        self.topics.read().await.get(name).cloned()
    }

    // -- produce -----------------------------------------------------------

    async fn produce(&self, req: ProduceRequest) -> Response {
        let Some(topic) = self.topic(&req.topic).await else {
            return Response::Error(ErrorCode::UnknownTopic, req.topic.clone());
        };
        let count = topic.config.partitions;
        let mut partitions_used = Vec::with_capacity(req.messages.len());
        let mut total_bytes = 0u64;
        let now_ms = now_ms();

        // Group messages by partition while preserving request order.
        let mut by_partition: HashMap<u32, Vec<usize>> = HashMap::new();
        for (i, msg) in req.messages.iter().enumerate() {
            let p = partition_for_key(&msg.key, count);
            by_partition.entry(p).or_default().push(i);
            partitions_used.push(p);
        }

        // One append critical section per partition; different partitions can
        // be served by different connections concurrently.
        let mut assigned = vec![0u64; req.messages.len()];
        for (partition, indexes) in by_partition {
            let partition_handle = topic.partitions[partition as usize].clone();
            let producer_id = req.producer_id;
            let messages = req.messages.clone();
            let dedup = self.dedup.clone();
            let result = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<(usize, u64, bool)>> {
                let mut out = Vec::new();
                for i in indexes {
                    let msg = &messages[i];
                    let sequence = msg.sequence;
                    if let Some(existing) = dedup.check(producer_id, partition, sequence) {
                        out.push((i, existing, true));
                        continue;
                    }
                    let ts = if msg.timestamp_ms == 0 { now_ms } else { msg.timestamp_ms };
                    let offset =
                        partition_handle.append(producer_id, sequence, ts, &msg.key, &msg.value)?;
                    dedup.remember(producer_id, partition, sequence, offset);
                    out.push((i, offset, false));
                }
                // fsync before the ack so an acknowledged record survives a
                // broker SIGKILL (at-least-once across crashes). Batch requests
                // amortize this into one sync per partition.
                partition_handle.sync()?;
                Ok(out)
            })
            .await;
            match result {
                Ok(Ok(rows)) => {
                    for (i, offset, was_dup) in rows {
                        assigned[i] = offset;
                        if was_dup {
                            self.metrics.record_dedup_hit();
                        }
                    }
                }
                Ok(Err(e)) => {
                    return Response::Error(ErrorCode::BrokerUnavailable, e.to_string())
                }
                Err(e) => return Response::Error(ErrorCode::BrokerUnavailable, e.to_string()),
            }
        }
        for msg in &req.messages {
            total_bytes += (msg.key.len() + msg.value.len()) as u64;
        }
        self.metrics
            .record_produce(req.messages.len() as u64, total_bytes);
        Response::Produced(ProduceResponse {
            offsets: assigned,
            partitions: partitions_used,
        })
    }

    // -- group protocol ----------------------------------------------------

    async fn join_group(&self, req: JoinGroupRequest) -> Response {
        let Some(topic) = self.topic(&req.topic).await else {
            return Response::Error(ErrorCode::UnknownTopic, req.topic.clone());
        };
        let (generation, assignments, members) = self
            .groups
            .join(&req.group, &req.topic, topic.config.partitions, &req.consumer_id)
            .await;
        Response::Joined(JoinGroupResponse {
            generation,
            assignments,
            members,
        })
    }

    async fn heartbeat(&self, req: HeartbeatRequest) -> Response {
        match self
            .groups
            .heartbeat(&req.group, &req.consumer_id, req.generation)
            .await
        {
            HeartbeatResult::Stable(gen) => Response::Heartbeat(HeartbeatResponse {
                generation: gen,
                rebalanced: false,
            }),
            HeartbeatResult::Rebalanced(gen) => Response::Heartbeat(HeartbeatResponse {
                generation: gen,
                rebalanced: true,
            }),
            HeartbeatResult::UnknownConsumer => Response::Error(
                ErrorCode::UnknownConsumer,
                "consumer must re-join the group".into(),
            ),
        }
    }

    async fn leave_group(&self, req: mqueue_proto::LeaveGroupRequest) -> Response {
        self.groups
            .leave(&req.group, &req.consumer_id, req.generation)
            .await;
        Response::Ok
    }

    // -- fetch / offsets ---------------------------------------------------

    async fn fetch(&self, req: FetchRequest) -> Response {
        let started = Instant::now();
        let Some(topic) = self.topic(&req.topic).await else {
            return Response::Error(ErrorCode::UnknownTopic, req.topic.clone());
        };
        let Some(partition) = topic.partitions.get(req.partition as usize).cloned() else {
            return Response::Error(ErrorCode::UnknownPartition, req.partition.to_string());
        };
        let max_bytes = req.max_bytes.max(1024) as usize;
        let deadline = Duration::from_millis(req.timeout_ms as u64);

        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let p = partition.clone();
            let offset = req.offset;
            let read = tokio::task::spawn_blocking(move || p.read_from(offset, max_bytes))
                .await
                .unwrap_or_else(|e| Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
            let (records, next_offset) = match read {
                Ok(v) => v,
                Err(e) => return Response::Error(ErrorCode::BrokerUnavailable, e.to_string()),
            };
            if !records.is_empty() {
                let bytes: u64 = records
                    .iter()
                    .map(|r| (r.key.len() + r.value.len()) as u64)
                    .sum();
                self.metrics.record_fetch(records.len() as u64, bytes);
                let messages = records
                    .into_iter()
                    .map(|r| FetchedMessage {
                        partition: req.partition,
                        offset: r.offset,
                        timestamp_ms: r.timestamp_ms,
                        key: r.key,
                        value: r.value,
                        producer_id: r.producer_id,
                        sequence: r.sequence,
                    })
                    .collect();
                self.fetch_latency.observe(started.elapsed());
                return Response::Fetched(FetchResponse {
                    next_offset,
                    messages,
                });
            }
            if attempt == 1 && req.timeout_ms == 0 {
                return Response::Fetched(FetchResponse {
                    next_offset: req.offset,
                    messages: Vec::new(),
                });
            }
            // Long poll: wait for new data, capped by the client timeout.
            let wait = deadline.min(Duration::from_millis(500));
            partition.wait_for(wait).await;
            if started.elapsed() >= deadline {
                return Response::Fetched(FetchResponse {
                    next_offset: req.offset,
                    messages: Vec::new(),
                });
            }
        }
    }

    async fn commit_offset(&self, req: CommitOffsetRequest) -> Response {
        match self
            .groups
            .commit(&req.group, req.partition, req.offset, req.generation)
            .await
        {
            Ok(()) => Response::Ok,
            Err(crate::group::CommitError::IllegalGeneration) => Response::Error(
                ErrorCode::IllegalGeneration,
                "generation fenced; re-join the group".into(),
            ),
        }
    }

    async fn list_offsets(&self, req: ListOffsetsRequest) -> Response {
        let Some(topic) = self.topic(&req.topic).await else {
            return Response::Error(ErrorCode::UnknownTopic, req.topic.clone());
        };
        let Some(partition) = topic.partitions.get(req.partition as usize).cloned() else {
            return Response::Error(ErrorCode::UnknownPartition, req.partition.to_string());
        };
        let committed = self.groups.committed(&req.group, req.partition).await;
        Response::Offsets(ListOffsetsResponse {
            committed,
            log_start_offset: partition.log_start_offset(),
            log_end_offset: partition.log_end_offset(),
        })
    }

    async fn offset_by_time(&self, req: OffsetByTimeRequest) -> Response {
        let Some(topic) = self.topic(&req.topic).await else {
            return Response::Error(ErrorCode::UnknownTopic, req.topic.clone());
        };
        let Some(partition) = topic.partitions.get(req.partition as usize).cloned() else {
            return Response::Error(ErrorCode::UnknownPartition, req.partition.to_string());
        };
        let p = partition.clone();
        let ts = req.timestamp_ms;
        let off = tokio::task::spawn_blocking(move || p.find_timestamp(ts))
            .await
            .unwrap_or_else(|e| Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
        match off {
            Ok(v) => Response::Offset(v),
            Err(e) => Response::Error(ErrorCode::BrokerUnavailable, e.to_string()),
        }
    }

    /// Single dispatch entry point used by the connection handler.
    pub async fn handle(self: &Arc<Self>, req: Request) -> Response {
        match req {
            Request::CreateTopic(c) => self.create_topic(c).await,
            Request::ListTopics => self.list_topics().await,
            Request::Produce(p) => self.produce(p).await,
            Request::JoinGroup(j) => self.join_group(j).await,
            Request::Heartbeat(h) => self.heartbeat(h).await,
            Request::LeaveGroup(l) => self.leave_group(l).await,
            Request::Fetch(f) => self.fetch(f).await,
            Request::CommitOffset(c) => self.commit_offset(c).await,
            Request::ListOffsets(l) => self.list_offsets(l).await,
            Request::OffsetByTime(o) => self.offset_by_time(o).await,
        }
    }

    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn now_ms_pub() -> u64 {
    now_ms()
}
