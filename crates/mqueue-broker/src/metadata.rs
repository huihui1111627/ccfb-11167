//! Persisted topic metadata: topic name, partition count and per-partition
//! retention overrides. Stored as a single JSON file rewritten atomically.

use std::path::{Path, PathBuf};

use mqueue_storage::RetentionPolicy;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub partitions: u32,
    #[serde(default)]
    pub max_segment_bytes: Option<u64>,
    #[serde(default)]
    pub max_age_ms: Option<u64>,
    #[serde(default)]
    pub max_total_bytes: Option<u64>,
}

impl TopicConfig {
    pub fn retention(&self, defaults: &RetentionPolicy) -> RetentionPolicy {
        RetentionPolicy {
            max_segment_bytes: self.max_segment_bytes.unwrap_or(defaults.max_segment_bytes),
            max_age_ms: self.max_age_ms.or(defaults.max_age_ms),
            max_total_bytes: self.max_total_bytes.unwrap_or(defaults.max_total_bytes),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MetaFile {
    topics: Vec<TopicConfig>,
}

#[derive(Debug)]
pub struct MetadataStore {
    path: PathBuf,
    tmp_path: PathBuf,
    inner: RwLock<MetaFile>,
}

impl MetadataStore {
    pub async fn load(path: &Path) -> anyhow::Result<Self> {
        let inner = if path.exists() {
            let bytes = tokio::fs::read(path).await?;
            serde_json::from_slice(&bytes)?
        } else {
            MetaFile::default()
        };
        Ok(Self {
            path: path.to_path_buf(),
            tmp_path: path.with_extension("json.tmp"),
            inner: RwLock::new(inner),
        })
    }

    async fn persist(&self, snapshot: &MetaFile) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec_pretty(snapshot)?;
        tokio::fs::write(&self.tmp_path, bytes).await?;
        tokio::fs::rename(&self.tmp_path, &self.path).await?;
        Ok(())
    }

    /// Add a topic. Returns `false` (without persisting) when it already exists.
    pub async fn create(&self, cfg: TopicConfig) -> anyhow::Result<bool> {
        let mut guard = self.inner.write().await;
        if guard.topics.iter().any(|t| t.name == cfg.name) {
            return Ok(false);
        }
        guard.topics.push(cfg);
        self.persist(&guard).await?;
        Ok(true)
    }

    pub async fn list(&self) -> Vec<TopicConfig> {
        self.inner.read().await.topics.clone()
    }

    pub async fn get(&self, name: &str) -> Option<TopicConfig> {
        self.inner.read().await.topics.iter().find(|t| t.name == name).cloned()
    }
}
