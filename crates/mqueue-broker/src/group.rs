//! Consumer group coordination: membership, session timeouts, range-based
//! partition assignment, generation fencing and committed offsets.
//!
//! All state lives behind one `RwLock` per group; groups themselves live in a
//! map behind the broker-wide lock. The coordinator is deliberately simple —
//! there is no separate coordinator node because the broker is single-process.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// A consumer may miss this many heartbeats before being declared dead.
pub const SESSION_TIMEOUT: Duration = Duration::from_secs(15);
/// Clients heartbeat at roughly a third of the session timeout.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
struct Member {
    generation: u64,
    last_heartbeat: Instant,
}

/// Committed offsets as persisted to `<group>.offsets.json`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OffsetTable {
    /// partition -> next offset to consume
    pub offsets: HashMap<u32, u64>,
}

pub struct Group {
    pub name: String,
    pub topic: String,
    pub partition_count: u32,
    members: HashMap<String, Member>,
    generation: u64,
    /// consumer_id -> owned partitions, valid for `generation`.
    assignments: HashMap<String, Vec<u32>>,
    committed: HashMap<u32, u64>,
    offsets_path: PathBuf,
    dirty: bool,
}

impl std::fmt::Debug for Group {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Group")
            .field("name", &self.name)
            .field("topic", &self.topic)
            .field("generation", &self.generation)
            .field("members", &self.members.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Group {
    fn new(name: String, topic: String, partition_count: u32, offsets_dir: &std::path::Path) -> Self {
        let offsets_path = offsets_dir.join(format!("{}.offsets.json", sanitize(&name)));
        let committed = std::fs::read(&offsets_path)
            .ok()
            .and_then(|b| serde_json::from_slice::<OffsetTable>(&b).ok())
            .map(|t| t.offsets)
            .unwrap_or_default();
        Self {
            name,
            topic,
            partition_count,
            members: HashMap::new(),
            generation: 0,
            assignments: HashMap::new(),
            committed,
            offsets_path,
            dirty: false,
        }
    }

    /// Add (or re-register) a member and rebalance. Returns the new generation
    /// and this member's assignment.
    fn join(&mut self, consumer_id: &str) -> (u64, Vec<u32>) {
        let known = self.members.contains_key(consumer_id);
        self.members.insert(
            consumer_id.to_string(),
            Member {
                generation: self.generation.wrapping_add(1),
                last_heartbeat: Instant::now(),
            },
        );
        if !known {
            self.rebalance();
        }
        let gen = self.generation;
        let assigned = self
            .assignments
            .get(consumer_id)
            .cloned()
            .unwrap_or_default();
        (gen, assigned)
    }

    /// Heartbeat. Expired members are swept first (which may bump the
    /// generation). Returns the current generation and whether the caller must
    /// re-join.
    fn heartbeat(&mut self, consumer_id: &str, generation: u64) -> HeartbeatOutcome {
        self.sweep_expired();
        let Some(member) = self.members.get_mut(consumer_id) else {
            return HeartbeatOutcome::UnknownConsumer;
        };
        member.last_heartbeat = Instant::now();
        if member.generation != generation || self.generation != generation {
            return HeartbeatOutcome::Rebalance(self.generation);
        }
        HeartbeatOutcome::Stable(self.generation)
    }

    fn leave(&mut self, consumer_id: &str, generation: u64) {
        if let Some(m) = self.members.get(consumer_id) {
            if m.generation == generation {
                self.members.remove(consumer_id);
                self.rebalance();
            }
        }
    }

    /// Remove members whose session has timed out, rebalancing on change.
    fn sweep_expired(&mut self) {
        let now = Instant::now();
        let before = self.members.len();
        self.members
            .retain(|_, m| now.duration_since(m.last_heartbeat) < SESSION_TIMEOUT);
        if self.members.len() != before {
            self.rebalance();
        }
    }

    /// Assign contiguous partition ranges over the sorted member ids. Range
    /// assignment keeps per-key ordering stable as long as the membership is
    /// stable, and is trivial to reason about during rebalancing.
    fn rebalance(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.assignments.clear();
        let mut ids: Vec<String> = self.members.keys().cloned().collect();
        ids.sort();
        let n = ids.len();
        if n == 0 {
            return;
        }
        let partitions = self.partition_count as usize;
        for (idx, id) in ids.iter().enumerate() {
            // Split partitions as evenly as possible; earlier members absorb
            // the remainder.
            let start = idx * partitions / n;
            let end = (idx + 1) * partitions / n;
            let owned: Vec<u32> = (start..end).map(|p| p as u32).collect();
            if let Some(m) = self.members.get_mut(id) {
                m.generation = self.generation;
            }
            self.assignments.insert(id.clone(), owned);
        }
    }

    fn assignment_of(&self, consumer_id: &str) -> Vec<u32> {
        self.assignments.get(consumer_id).cloned().unwrap_or_default()
    }

    fn commit(&mut self, partition: u32, offset: u64, generation: u64) -> Result<(), CommitError> {
        if self.generation != generation {
            return Err(CommitError::IllegalGeneration);
        }
        self.committed.insert(partition, offset);
        self.dirty = true;
        Ok(())
    }

    fn committed(&self, partition: u32) -> Option<u64> {
        self.committed.get(&partition).copied()
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let table = OffsetTable {
            offsets: self.committed.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&table)?;
        let tmp = self.offsets_path.with_extension("json.tmp");
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(tmp, &self.offsets_path)?;
        self.dirty = false;
        Ok(())
    }

    fn snapshot(&self) -> GroupSnapshot {
        let mut members: Vec<MemberSnapshot> = self
            .members
            .keys()
            .map(|id| MemberSnapshot {
                consumer_id: id.clone(),
                assignments: self.assignment_of(id),
            })
            .collect();
        members.sort_by(|a, b| a.consumer_id.cmp(&b.consumer_id));
        GroupSnapshot {
            name: self.name.clone(),
            topic: self.topic.clone(),
            generation: self.generation,
            members,
            committed: self.committed.clone(),
        }
    }
}

#[derive(Debug)]
enum HeartbeatOutcome {
    Stable(u64),
    Rebalance(u64),
    UnknownConsumer,
}

#[derive(Debug)]
pub enum CommitError {
    IllegalGeneration,
}

/// Serializable view for the operations console.
#[derive(Debug, Clone, Serialize)]
pub struct MemberSnapshot {
    pub consumer_id: String,
    pub assignments: Vec<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GroupSnapshot {
    pub name: String,
    pub topic: String,
    pub generation: u64,
    pub members: Vec<MemberSnapshot>,
    pub committed: HashMap<u32, u64>,
}

/// All groups, behind one broker-wide lock. Contention is short critical
/// sections only; storage IO happens outside the lock.
#[derive(Debug)]
pub struct GroupCoordinator {
    groups: RwLock<HashMap<String, Arc<RwLock<Group>>>>,
    offsets_dir: PathBuf,
}

impl GroupCoordinator {
    pub fn new(offsets_dir: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&offsets_dir)?;
        Ok(Self {
            groups: RwLock::new(HashMap::new()),
            offsets_dir,
        })
    }

    /// Get or create the group for `(group, topic)`. A group is pinned to one
    /// topic — mixing topics inside one group adds ambiguity without value for
    /// this single-process design.
    pub async fn group_for(&self, group: &str, topic: &str, partition_count: u32) -> Arc<RwLock<Group>> {
        let read = self.groups.read().await;
        if let Some(g) = read.get(group) {
            return g.clone();
        }
        drop(read);
        let mut write = self.groups.write().await;
        write
            .entry(group.to_string())
            .or_insert_with(|| {
                Arc::new(RwLock::new(Group::new(
                    group.to_string(),
                    topic.to_string(),
                    partition_count,
                    &self.offsets_dir,
                )))
            })
            .clone()
    }

    pub async fn join(
        &self,
        group: &str,
        topic: &str,
        partition_count: u32,
        consumer_id: &str,
    ) -> (u64, Vec<u32>, Vec<String>) {
        let g = self.group_for(group, topic, partition_count).await;
        let mut guard = g.write().await;
        let (generation, assignments) = guard.join(consumer_id);
        let members = guard.members.keys().cloned().collect();
        (generation, assignments, members)
    }

    pub async fn heartbeat(
        &self,
        group: &str,
        consumer_id: &str,
        generation: u64,
    ) -> HeartbeatResult {
        let map = self.groups.read().await;
        let g = match map.get(group) {
            Some(handle) => handle.clone(),
            None => return HeartbeatResult::UnknownConsumer,
        };
        drop(map);
        let outcome = g.write().await.heartbeat(consumer_id, generation);
        match outcome {
            HeartbeatOutcome::Stable(gen) => HeartbeatResult::Stable(gen),
            HeartbeatOutcome::Rebalance(gen) => HeartbeatResult::Rebalanced(gen),
            HeartbeatOutcome::UnknownConsumer => HeartbeatResult::UnknownConsumer,
        }
    }

    pub async fn leave(&self, group: &str, consumer_id: &str, generation: u64) {
        let map = self.groups.read().await;
        let g = match map.get(group) {
            Some(handle) => handle.clone(),
            None => return,
        };
        drop(map);
        g.write().await.leave(consumer_id, generation);
    }

    pub async fn commit(
        &self,
        group: &str,
        partition: u32,
        offset: u64,
        generation: u64,
    ) -> Result<(), CommitError> {
        let map = self.groups.read().await;
        let g = match map.get(group) {
            Some(handle) => handle.clone(),
            None => return Err(CommitError::IllegalGeneration),
        };
        drop(map);
        let result = g.write().await.commit(partition, offset, generation);
        result
    }

    pub async fn committed(&self, group: &str, partition: u32) -> Option<u64> {
        let map = self.groups.read().await;
        let g = map.get(group)?.clone();
        drop(map);
        let committed = g.read().await.committed(partition);
        committed
    }

    pub async fn snapshots(&self) -> Vec<GroupSnapshot> {
        let groups: Vec<_> = self.groups.read().await.values().cloned().collect();
        let mut out = Vec::new();
        for g in groups {
            out.push(g.read().await.snapshot());
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Periodic background work: expire dead sessions and fsync offset tables.
    pub async fn maintain(&self) -> std::io::Result<()> {
        let groups: Vec<_> = self.groups.read().await.values().cloned().collect();
        for g in groups {
            let mut guard = g.write().await;
            guard.sweep_expired();
            guard.flush()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatResult {
    Stable(u64),
    Rebalanced(u64),
    UnknownConsumer,
}

fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}
