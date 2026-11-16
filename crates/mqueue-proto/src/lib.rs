#![allow(dead_code)]
//! Wire protocol for the mqueue broker.
//!
//! Framing: a 4-byte little-endian u32 length prefix followed by a
//! `bincode`-serialized [`Request`] or [`Response`]. Both enums share one
//! namespace per direction; the client sends requests, the broker answers each
//! request with exactly one response (streaming is emulated with long-poll
//! fetch requests).

use serde::{Deserialize, Serialize};

pub mod frame;

pub use mqueue_storage::partition_for_key;
pub use mqueue_storage::Record;

/// Error codes carried in [`Response::Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    UnknownTopic,
    UnknownPartition,
    OffsetOutOfRange,
    NotCoordinator,
    IllegalGeneration,
    UnknownConsumer,
    DuplicateSequence,
    InvalidRequest,
    BrokerUnavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// Producer wall-clock timestamp in milliseconds; the broker falls back to
    /// its own clock when zero.
    pub timestamp_ms: u64,
    /// Producer-assigned, per-partition monotonic sequence number used for
    /// broker-side idempotent dedup. The client fills this in.
    pub sequence: u64,
}

/// A fetched record plus its broker-side timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchedMessage {
    pub partition: u32,
    pub offset: u64,
    pub timestamp_ms: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub producer_id: u64,
    pub sequence: u64,
}

// ---------------------------------------------------------------------------
// Metadata / topic administration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTopicRequest {
    pub name: String,
    pub partitions: u32,
    /// Optional retention overrides; defaults apply when `None`.
    pub max_segment_bytes: Option<u64>,
    pub max_age_ms: Option<u64>,
    pub max_total_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: u32,
    pub log_start_offsets: Vec<u64>,
    pub log_end_offsets: Vec<u64>,
}

// ---------------------------------------------------------------------------
// Produce
// ---------------------------------------------------------------------------

/// Batch produce for one topic. The partition of each message is derived from
/// its key, so a request may fan out to several partitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceRequest {
    pub topic: String,
    pub producer_id: u64,
    /// Per-partition monotonic sequence numbers, parallel to `messages`.
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProduceResponse {
    /// Offset assigned to each message, in request order.
    pub offsets: Vec<u64>,
    /// Partition selected for each message, in request order.
    pub partitions: Vec<u32>,
}

// ---------------------------------------------------------------------------
// Consumer group coordination
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinGroupRequest {
    pub group: String,
    pub topic: String,
    pub consumer_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinGroupResponse {
    pub generation: u64,
    /// Partitions currently owned by this consumer.
    pub assignments: Vec<u32>,
    /// All members of the group, for observability.
    pub members: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub group: String,
    pub consumer_id: String,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// Reassigned when the group rebalanced; the consumer must re-join.
    pub generation: u64,
    pub rebalanced: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaveGroupRequest {
    pub group: String,
    pub consumer_id: String,
    pub generation: u64,
}

// ---------------------------------------------------------------------------
// Fetch / offsets
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    pub group: String,
    pub topic: String,
    pub partition: u32,
    /// Offset to read from. Use the committed offset returned by
    /// [`Request::ListOffsets`] to resume.
    pub offset: u64,
    pub max_bytes: u32,
    /// Long-poll ceiling in milliseconds.
    pub timeout_ms: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    /// Next offset to request — equals the last message offset + 1, or the
    /// request offset when the batch is empty.
    pub next_offset: u64,
    pub messages: Vec<FetchedMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitOffsetRequest {
    pub group: String,
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListOffsetsRequest {
    pub group: String,
    pub topic: String,
    pub partition: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListOffsetsResponse {
    /// Last committed offset, or `None` when the group never committed.
    pub committed: Option<u64>,
    pub log_start_offset: u64,
    pub log_end_offset: u64,
}

/// Resolve a timestamp to an offset for time-travel replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetByTimeRequest {
    pub topic: String,
    pub partition: u32,
    pub timestamp_ms: u64,
}

// ---------------------------------------------------------------------------
// Top-level enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    CreateTopic(CreateTopicRequest),
    ListTopics,
    Produce(ProduceRequest),
    JoinGroup(JoinGroupRequest),
    Heartbeat(HeartbeatRequest),
    LeaveGroup(LeaveGroupRequest),
    Fetch(FetchRequest),
    CommitOffset(CommitOffsetRequest),
    ListOffsets(ListOffsetsRequest),
    OffsetByTime(OffsetByTimeRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    Ok,
    TopicCreated,
    Topics(Vec<TopicInfo>),
    Produced(ProduceResponse),
    Joined(JoinGroupResponse),
    Heartbeat(HeartbeatResponse),
    Fetched(FetchResponse),
    Offsets(ListOffsetsResponse),
    Offset(u64),
    Error(ErrorCode, String),
}
