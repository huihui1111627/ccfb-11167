//! Sparse offset/time index for a segment.
//!
//! The index mirrors the `.idx` file: a sequence of fixed 24-byte entries, one
//! per *N* appended records (not one per record — that is what makes it sparse).
//! Each entry maps a record offset to its byte position inside the `.log` file
//! and records the first timestamp seen at that point, which powers
//! timestamp-based offset lookups.
//!
//! Entries are sorted by offset, so both lookup modes are binary searches.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use thiserror::Error;

pub const INDEX_ENTRY_LEN: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub offset: u64,
    pub position: u64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Error)]
pub enum IndexError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("index entry {found} is out of order after {prev}")]
    OutOfOrder { found: u64, prev: u64 },
}

/// Append-only sparse index kept both on disk and in memory.
#[derive(Debug)]
pub struct Index {
    file: Option<File>,
    entries: Vec<IndexEntry>,
}

impl Index {
    /// Open (or create) an index file and load any entries already on disk.
    pub fn open(path: &Path, writable: bool) -> Result<Self, IndexError> {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(writable)
            .create(writable)
            .truncate(false)
            .open(path)?;
        let mut entries: Vec<IndexEntry> = Vec::new();
        file.seek(SeekFrom::Start(0))?;
        let mut raw = [0u8; INDEX_ENTRY_LEN];
        loop {
            match file.read_exact(&mut raw) {
                Ok(()) => {
                    let entry = decode_entry(&raw);
                    if let Some(prev) = entries.last() {
                        if entry.offset <= prev.offset {
                            return Err(IndexError::OutOfOrder {
                                found: entry.offset,
                                prev: prev.offset,
                            });
                        }
                    }
                    entries.push(entry);
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file: if writable { Some(file) } else { None },
            entries,
        })
    }

    /// An in-memory-only index (used for rebuilt recovery indexes before they
    /// are persisted).
    pub fn memory() -> Self {
        Self {
            file: None,
            entries: Vec::new(),
        }
    }

    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Append an entry. Callers guarantee offsets increase monotonically.
    pub fn append(&mut self, entry: IndexEntry) -> Result<(), IndexError> {
        if let Some(prev) = self.entries.last() {
            debug_assert!(entry.offset > prev.offset);
            if entry.offset <= prev.offset {
                return Err(IndexError::OutOfOrder {
                    found: entry.offset,
                    prev: prev.offset,
                });
            }
        }
        if let Some(file) = self.file.as_mut() {
            let mut raw = [0u8; INDEX_ENTRY_LEN];
            encode_entry(&entry, &mut raw);
            file.write_all(&raw)?;
        }
        self.entries.push(entry);
        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        if let Some(file) = self.file.as_mut() {
            file.flush()?;
            file.sync_data()?;
        }
        Ok(())
    }

    /// Largest indexed offset that is `<= target`, together with its byte
    /// position. Returns `None` when nothing at or below `target` is indexed —
    /// the caller then starts at byte 0 of the segment.
    pub fn lookup_offset(&self, target: u64) -> Option<IndexEntry> {
        match self.entries.binary_search_by(|e| e.offset.cmp(&target)) {
            Ok(i) => Some(self.entries[i]),
            Err(0) => None,
            Err(i) => Some(self.entries[i - 1]),
        }
    }

    /// Entry whose timestamp is the greatest indexed timestamp `<= target_ms`.
    /// The returned position is a lower bound; the caller must scan forward to
    /// the first record whose own timestamp satisfies the query.
    pub fn lookup_timestamp(&self, target_ms: u64) -> Option<IndexEntry> {
        match self
            .entries
            .binary_search_by(|e| e.timestamp_ms.cmp(&target_ms))
        {
            Ok(i) => Some(self.entries[i]),
            Err(0) => None,
            Err(i) => Some(self.entries[i - 1]),
        }
    }

    /// First (smallest-offset) entry, if any.
    pub fn first(&self) -> Option<IndexEntry> {
        self.entries.first().copied()
    }

    /// Last (largest-offset) entry, if any.
    pub fn last(&self) -> Option<IndexEntry> {
        self.entries.last().copied()
    }
}

pub fn encode_entry(entry: &IndexEntry, out: &mut [u8; INDEX_ENTRY_LEN]) {
    out[0..8].copy_from_slice(&entry.offset.to_le_bytes());
    out[8..16].copy_from_slice(&entry.position.to_le_bytes());
    out[16..24].copy_from_slice(&entry.timestamp_ms.to_le_bytes());
}

pub fn decode_entry(raw: &[u8; INDEX_ENTRY_LEN]) -> IndexEntry {
    IndexEntry {
        offset: u64::from_le_bytes(raw[0..8].try_into().unwrap()),
        position: u64::from_le_bytes(raw[8..16].try_into().unwrap()),
        timestamp_ms: u64::from_le_bytes(raw[16..24].try_into().unwrap()),
    }
}
