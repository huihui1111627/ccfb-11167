//! Log segments: one active read-write segment plus zero or more sealed
//! read-only segments per partition.
//!
//! Files on disk:
//! - `<base_offset>.log` — concatenated record frames
//! - `<base_offset>.idx` — sparse 24-byte index entries
//!
//! Sealed segments are memory-mapped, so serving a fetch never copies frame
//! bytes through a kernel read buffer (`mmap` zero-copy reads); the active
//! segment is read straight from its page-cache-backed file.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use memmap2::Mmap;
use thiserror::Error;
use tracing::{debug, warn};

use crate::index::{Index, IndexEntry};
use crate::record::{self, Record};

/// How often a sparse index entry is emitted (every N records).
pub const INDEX_INTERVAL_RECORDS: u64 = 64;

#[derive(Debug, Error)]
pub enum SegmentError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Index(#[from] crate::index::IndexError),
    #[error(transparent)]
    Record(#[from] record::ParseError),
    #[error("segment {base}: offset {offset} out of range")]
    OutOfRange { base: u64, offset: u64 },
}

/// The single writable segment of a partition.
pub struct ActiveSegment {
    pub base_offset: u64,
    pub log_path: PathBuf,
    pub index_path: PathBuf,
    writer: BufWriter<File>,
    index: Index,
    /// Byte length currently visible to readers. `BufWriter` may hold more in
    /// its buffer; we flush before publishing a new offset.
    readable_len: u64,
    file_len: u64,
    records_since_index: u64,
    next_offset: u64,
    first_timestamp_ms: Option<u64>,
}

impl std::fmt::Debug for ActiveSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveSegment")
            .field("base_offset", &self.base_offset)
            .field("file_len", &self.file_len)
            .field("next_offset", &self.next_offset)
            .finish()
    }
}

impl ActiveSegment {
    /// Create a brand new empty segment.
    pub fn create(dir: &Path, base_offset: u64) -> Result<Self, SegmentError> {
        let log_path = segment_path(dir, base_offset, "log");
        let index_path = segment_path(dir, base_offset, "idx");
        let writer = BufWriter::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&log_path)?,
        );
        let index = Index::open(&index_path, true)?;
        Ok(Self {
            base_offset,
            log_path,
            index_path,
            writer,
            index,
            readable_len: 0,
            file_len: 0,
            records_since_index: 0,
            next_offset: base_offset,
            first_timestamp_ms: None,
        })
    }

    /// Append one frame. Returns the assigned offset.
    pub fn append(
        &mut self,
        producer_id: u64,
        sequence: u64,
        timestamp_ms: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<u64, SegmentError> {
        let offset = self.next_offset;
        let frame = Record {
            offset,
            producer_id,
            sequence,
            timestamp_ms,
            key: key.to_vec(),
            value: value.to_vec(),
        }
        .encode();
        let position = self.file_len;
        if self.first_timestamp_ms.is_none() {
            self.first_timestamp_ms = Some(timestamp_ms);
        }
        self.writer.write_all(&frame)?;
        self.file_len += frame.len() as u64;
        self.next_offset += 1;
        self.records_since_index += 1;
        if self.records_since_index >= INDEX_INTERVAL_RECORDS {
            self.index.append(IndexEntry {
                offset,
                position,
                timestamp_ms,
            })?;
            self.records_since_index = 0;
        }
        Ok(offset)
    }

    /// Make all buffered frames durable enough to be read by other threads
    /// (flush to the kernel page cache) and optionally fsync.
    pub fn flush(&mut self, sync: bool) -> Result<(), SegmentError> {
        self.writer.flush()?;
        self.readable_len = self.file_len;
        self.index.flush()?;
        if sync {
            let file = self.writer.get_ref();
            file.sync_data()?;
        }
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.file_len
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn first_timestamp_ms(&self) -> Option<u64> {
        self.first_timestamp_ms
    }

    /// Read up to `max_bytes` starting at record offset `start_offset`.
    /// Returns owned records and the offset immediately after the last one.
    pub fn read_from(
        &self,
        start_offset: u64,
        max_bytes: usize,
    ) -> Result<(Vec<Record>, u64), SegmentError> {
        if start_offset < self.base_offset || start_offset > self.next_offset {
            return Err(SegmentError::OutOfRange {
                base: self.base_offset,
                offset: start_offset,
            });
        }
        if start_offset == self.next_offset {
            return Ok((Vec::new(), start_offset));
        }
        let hint = self.index.lookup_offset(start_offset);
        let start_pos = match hint {
            Some(e) if e.offset <= start_offset => e.position,
            _ => 0,
        };
        let mut file = OpenOptions::new().read(true).open(&self.log_path)?;
        file.seek(SeekFrom::Start(start_pos))?;
        read_records(
            &mut file,
            self.readable_len.saturating_sub(start_pos),
            start_offset,
            max_bytes,
        )
    }

    /// Smallest record offset whose timestamp is `>= target_ms`, scanning from
    /// the closest sparse-index hint. Returns the end offset if none matches.
    pub fn find_timestamp(&self, target_ms: u64) -> Result<u64, SegmentError> {
        let hint = self.index.lookup_timestamp(target_ms);
        let start_pos = hint.map(|e| e.position).unwrap_or(0);
        let mut file = OpenOptions::new().read(true).open(&self.log_path)?;
        file.seek(SeekFrom::Start(start_pos))?;
        let mut remaining = self.readable_len.saturating_sub(start_pos);
        while remaining >= crate::record::HEADER_LEN as u64 {
            let mut header = [0u8; crate::record::HEADER_LEN];
            if let Err(e) = file.read_exact(&mut header) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(self.next_offset);
                }
                return Err(e.into());
            }
            let info = match record::parse_header(&header) {
                Ok(i) => i,
                Err(record::ParseError::NeedMore) | Err(record::ParseError::Truncated) => {
                    return Ok(self.next_offset)
                }
                Err(e) => return Err(e.into()),
            };
            if remaining < info.total as u64 {
                return Ok(self.next_offset);
            }
            // Skip the payload; only the header timestamp is needed here.
            let mut payload = vec![0u8; info.total - crate::record::HEADER_LEN];
            file.read_exact(&mut payload)?;
            remaining -= info.total as u64;
            if info.timestamp_ms >= target_ms {
                return Ok(info.offset);
            }
        }
        Ok(self.next_offset)
    }
}

/// A sealed segment: immutable, served through an mmap.
pub struct ReadSegment {
    pub base_offset: u64,
    pub log_path: PathBuf,
    pub index_path: PathBuf,
    mmap: Mmap,
    index: Index,
    /// Exclusive upper bound of offsets in this segment.
    pub end_offset: u64,
    pub first_timestamp_ms: Option<u64>,
    pub last_timestamp_ms: u64,
}

impl std::fmt::Debug for ReadSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadSegment")
            .field("base_offset", &self.base_offset)
            .field("end_offset", &self.end_offset)
            .finish()
    }
}

impl ReadSegment {
    /// Map a sealed segment and validate its index.
    pub fn open(dir: &Path, base_offset: u64, end_offset: u64) -> Result<Self, SegmentError> {
        let log_path = segment_path(dir, base_offset, "log");
        let index_path = segment_path(dir, base_offset, "idx");
        let file = File::open(&log_path)?;
        // SAFETY: the segment is sealed; nobody writes through this mapping or
        // through another handle. mmap2 requires the file not be truncated
        // while mapped, and retention only deletes segments it has removed
        // from the routing table and dropped.
        let mmap = unsafe { Mmap::map(&file)? };
        let index = Index::open(&index_path, false)?;
        let first_timestamp_ms = index.first().map(|e| e.timestamp_ms);
        let last_timestamp_ms = last_frame_timestamp(&mmap).unwrap_or(0);
        Ok(Self {
            base_offset,
            log_path,
            index_path,
            mmap,
            index,
            end_offset,
            first_timestamp_ms,
            last_timestamp_ms,
        })
    }

    pub fn contains(&self, offset: u64) -> bool {
        offset >= self.base_offset && offset < self.end_offset
    }

    pub fn size_bytes(&self) -> u64 {
        self.mmap.len() as u64
    }

    /// Zero-copy read: records borrow straight from the mmap pages while the
    /// caller serializes them, so the payload is copied exactly once (into the
    /// socket), never through an intermediate userspace buffer.
    pub fn read_from(
        &self,
        start_offset: u64,
        max_bytes: usize,
    ) -> Result<(Vec<Record>, u64), SegmentError> {
        if !self.contains(start_offset) {
            return Err(SegmentError::OutOfRange {
                base: self.base_offset,
                offset: start_offset,
            });
        }
        let hint = self.index.lookup_offset(start_offset);
        let start_pos = match hint {
            Some(e) if e.offset <= start_offset => e.position as usize,
            _ => 0,
        };
        let buf = &self.mmap[start_pos..];
        let mut records = Vec::new();
        let mut cursor = 0usize;
        let mut bytes = 0usize;
        let mut next = start_offset;
        loop {
            if cursor >= buf.len() {
                break;
            }
            match record::parse_at(&buf[cursor..]) {
                Ok((view, frame_len)) => {
                    if view.offset < start_offset {
                        cursor += frame_len;
                        continue;
                    }
                    if bytes > 0 && bytes + frame_len > max_bytes {
                        break;
                    }
                    records.push(view.to_owned());
                    bytes += frame_len;
                    cursor += frame_len;
                    next = view.offset + 1;
                    if bytes >= max_bytes {
                        break;
                    }
                }
                Err(record::ParseError::NeedMore) | Err(record::ParseError::Truncated) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok((records, next))
    }

    pub fn find_timestamp(&self, target_ms: u64) -> Result<u64, SegmentError> {
        let hint = self.index.lookup_timestamp(target_ms);
        let start_pos = hint.map(|e| e.position as usize).unwrap_or(0);
        let buf = &self.mmap[start_pos..];
        let mut cursor = 0usize;
        loop {
            if cursor >= buf.len() {
                break;
            }
            match record::parse_at(&buf[cursor..]) {
                Ok((view, frame_len)) => {
                    if view.timestamp_ms >= target_ms {
                        return Ok(view.offset);
                    }
                    cursor += frame_len;
                }
                Err(record::ParseError::NeedMore) | Err(record::ParseError::Truncated) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(self.end_offset)
    }
}

fn last_frame_timestamp(buf: &[u8]) -> Option<u64> {
    // The sparse index stores timestamps; derive the last timestamp from the
    // final parseable frame by a forward scan. Segments are bounded in size so
    // this only runs at open time.
    let mut cursor = 0usize;
    let mut last_ts = None;
    while cursor < buf.len() {
        match record::parse_at(&buf[cursor..]) {
            Ok((view, len)) => {
                last_ts = Some(view.timestamp_ms);
                cursor += len;
            }
            Err(_) => break,
        }
    }
    last_ts
}

/// Read owned records from a regular file handle, skipping frames below
/// `start_offset` and stopping once `max_bytes` of frame data is collected.
fn read_records(
    file: &mut File,
    available: u64,
    start_offset: u64,
    max_bytes: usize,
) -> Result<(Vec<Record>, u64), SegmentError> {
    let mut records = Vec::new();
    let mut remaining = available;
    let mut bytes = 0usize;
    let mut next = start_offset;
    while remaining >= crate::record::HEADER_LEN as u64 {
        let mut header = [0u8; crate::record::HEADER_LEN];
        if let Err(e) = file.read_exact(&mut header) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(e.into());
        }
        let info = match record::parse_header(&header) {
            Ok(i) => i,
            Err(record::ParseError::NeedMore) | Err(record::ParseError::Truncated) => break,
            Err(e) => return Err(e.into()),
        };
        if remaining < info.total as u64 {
            // A full frame is not visible yet; stop without consuming it.
            break;
        }
        let mut payload = vec![0u8; info.total - crate::record::HEADER_LEN];
        if let Err(e) = file.read_exact(&mut payload) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                break;
            }
            return Err(e.into());
        }
        remaining -= info.total as u64;
        if info.offset < start_offset {
            continue;
        }
        if bytes > 0 && bytes + info.total > max_bytes {
            break;
        }
        // Reassemble the full frame and validate CRC before trusting it.
        let mut frame = Vec::with_capacity(info.total);
        frame.extend_from_slice(&header);
        frame.extend_from_slice(&payload);
        let (view, _) = record::parse_at(&frame).map_err(SegmentError::from)?;
        records.push(view.to_owned());
        bytes += info.total;
        next = view.offset + 1;
        if bytes >= max_bytes {
            break;
        }
    }
    Ok((records, next))
}

pub fn segment_path(dir: &Path, base_offset: u64, suffix: &str) -> PathBuf {
    dir.join(format!("{:020}.{}", base_offset, suffix))
}

/// All segments of one partition behind a shared lock.
pub struct SegmentSet {
    pub dir: PathBuf,
    pub sealed: Vec<Arc<ReadSegment>>,
    pub active: ActiveSegment,
    pub max_segment_bytes: u64,
}

impl std::fmt::Debug for SegmentSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentSet")
            .field("dir", &self.dir)
            .field("sealed_count", &self.sealed.len())
            .field("active", &self.active)
            .finish()
    }
}

impl SegmentSet {
    /// Recover a partition directory: validate every segment, truncate torn
    /// tail writes from the active segment, rebuild its sparse index.
    pub fn open(
        dir: &Path,
        max_segment_bytes: u64,
    ) -> Result<(RwLock<Self>, RecoveryReport), SegmentError> {
        std::fs::create_dir_all(dir)?;
        let mut bases: Vec<u64> = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(base) = name.strip_suffix(".log") {
                if let Ok(b) = base.parse::<u64>() {
                    bases.push(b);
                }
            }
        }
        bases.sort_unstable();

        let report = RecoveryReport::default();
        let mut sealed: Vec<Arc<ReadSegment>> = Vec::new();
        let active: ActiveSegment = match bases.len() {
            0 => ActiveSegment::create(dir, 0)?,
            n => {
                for (idx, &base) in bases[..n - 1].iter().enumerate() {
                    // A sealed segment's exclusive end offset is the following
                    // segment's base offset. Rebuild its index from the log so
                    // a crash mid-roll can never leave a stale .idx beside the
                    // .log; the index is a pure derivative of the log.
                    let next_base = bases[idx + 1];
                    rebuild_index_for_segment(dir, base, &report)?;
                    sealed.push(Arc::new(ReadSegment::open(dir, base, next_base)?));
                }
                recover_active(dir, *bases.last().unwrap(), &report)?
            }
        };

        Ok((
            RwLock::new(Self {
                dir: dir.to_path_buf(),
                sealed,
                active,
                max_segment_bytes,
            }),
            report,
        ))
    }

    pub fn next_offset(&self) -> u64 {
        self.active.next_offset()
    }

    pub fn base_offset(&self) -> u64 {
        // Smallest offset currently retained.
        self.sealed
            .first()
            .map(|s| s.base_offset)
            .unwrap_or_else(|| self.active.base_offset())
    }

    /// Append and return `(offset, rolled_segment)` where `rolled_segment` is
    /// present when this append triggered a segment roll. The broker must
    /// remap the new sealed segment after releasing the write lock.
    pub fn append(
        &mut self,
        producer_id: u64,
        sequence: u64,
        timestamp_ms: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<AppendOutcome, SegmentError> {
        let offset = self.active.append(
            producer_id,
            sequence,
            timestamp_ms,
            key,
            value,
        )?;
        let rolled = if self.active.size() >= self.max_segment_bytes {
            Some(self.roll()?)
        } else {
            None
        };
        Ok(AppendOutcome { offset, rolled })
    }

    /// Seal the current active segment and open a fresh one.
    fn roll(&mut self) -> Result<Arc<ReadSegment>, SegmentError> {
        self.active.flush(true)?;
        let base = self.active.base_offset();
        let end = self.active.next_offset();
        let seg = Arc::new(ReadSegment::open(&self.dir, base, end)?);
        debug!(base, end, bytes = seg.size_bytes(), "segment rolled");
        self.sealed.push(seg.clone());
        self.active = ActiveSegment::create(&self.dir, end)?;
        Ok(seg)
    }

    pub fn flush(&mut self, sync: bool) -> Result<(), SegmentError> {
        self.active.flush(sync)
    }

    /// Route an offset read to the segment whose base offset is the largest
    /// one `<= start_offset`, then read from there. Sealed segments are
    /// immutable and sorted by base offset.
    pub fn read_from(
        &self,
        start_offset: u64,
        max_bytes: usize,
    ) -> Result<(Vec<Record>, u64), SegmentError> {
        // The active segment owns offsets at or beyond its base.
        if start_offset >= self.active.base_offset() {
            return self.active.read_from(start_offset, max_bytes);
        }
        // Find the last sealed segment whose base offset is <= start_offset.
        let idx = match self
            .sealed
            .binary_search_by_key(&start_offset, |s| s.base_offset)
        {
            Ok(i) => i,
            Err(0) => 0,
            Err(i) => i - 1,
        };
        self.sealed[idx].read_from(start_offset, max_bytes)
    }

    /// Offset of the first record at or after `target_ms`.
    pub fn find_timestamp(&self, target_ms: u64) -> Result<u64, SegmentError> {
        for seg in &self.sealed {
            if seg.last_timestamp_ms >= target_ms {
                return seg.find_timestamp(target_ms);
            }
        }
        self.active.find_timestamp(target_ms)
    }

    /// Delete sealed segments that are fully older than the retention policy.
    /// The active segment is always retained. Returns removed segment bases.
    pub fn enforce_retention(
        &mut self,
        now_ms: u64,
        max_age_ms: Option<u64>,
        min_bytes: u64,
    ) -> Result<Vec<u64>, SegmentError> {
        let total_bytes: u64 =
            self.sealed.iter().map(|s| s.size_bytes()).sum::<u64>() + self.active.size();
        let mut removed = Vec::new();
        let mut running = total_bytes;
        let mut keep = Vec::new();
        for seg in self.sealed.drain(..) {
            let age = now_ms.saturating_sub(seg.last_timestamp_ms);
            let too_old = max_age_ms.map(|m| age > m).unwrap_or(false);
            let over_size = running > min_bytes;
            if (too_old || over_size) && seg.end_offset <= self.active.base_offset() {
                warn!(base = seg.base_offset, "deleting sealed segment");
                std::fs::remove_file(&seg.log_path)?;
                std::fs::remove_file(&seg.index_path)?;
                running -= seg.size_bytes();
                removed.push(seg.base_offset);
            } else {
                keep.push(seg);
            }
        }
        self.sealed = keep;
        Ok(removed)
    }

    pub fn segment_stats(&self) -> Vec<SegmentStat> {
        let mut out: Vec<SegmentStat> = self
            .sealed
            .iter()
            .map(|s| SegmentStat {
                base_offset: s.base_offset,
                end_offset: s.end_offset,
                size_bytes: s.size_bytes(),
                active: false,
                first_timestamp_ms: s.first_timestamp_ms,
                last_timestamp_ms: s.last_timestamp_ms,
            })
            .collect();
        out.push(SegmentStat {
            base_offset: self.active.base_offset(),
            end_offset: self.active.next_offset(),
            size_bytes: self.active.size(),
            active: true,
            first_timestamp_ms: self.active.first_timestamp_ms(),
            last_timestamp_ms: 0,
        });
        out
    }
}

#[derive(Debug)]
pub struct AppendOutcome {
    pub offset: u64,
    pub rolled: Option<Arc<ReadSegment>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SegmentStat {
    pub base_offset: u64,
    pub end_offset: u64,
    pub size_bytes: u64,
    pub active: bool,
    pub first_timestamp_ms: Option<u64>,
    pub last_timestamp_ms: u64,
}

#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub segments_scanned: std::sync::atomic::AtomicUsize,
    pub records_valid: std::sync::atomic::AtomicU64,
    pub bytes_truncated: std::sync::atomic::AtomicU64,
}

/// Result of scanning one segment log during recovery.
struct ScanResult {
    valid_len: u64,
    next_offset: u64,
    records_since_index: u64,
    first_timestamp_ms: Option<u64>,
    rebuilt: Index,
}

/// Frame-by-frame scan of a segment log starting at `base`. Stops at the first
/// torn/corrupt frame (recovery never skips bytes) and builds a fresh sparse
/// index whose positions are absolute byte offsets within this log file.
fn scan_segment_log(
    mmap: &[u8],
    base: u64,
    report: &RecoveryReport,
) -> Result<ScanResult, SegmentError> {
    let mut valid_len = 0u64;
    let mut cursor = 0usize;
    let mut next_offset = base;
    let mut records_since = 0u64;
    let mut first_ts = None;
    let mut rebuilt = Index::memory();

    loop {
        if cursor >= mmap.len() {
            break;
        }
        match record::parse_at(&mmap[cursor..]) {
            Ok((view, frame_len)) => {
                if view.offset != next_offset {
                    warn!(
                        base,
                        expected = next_offset,
                        got = view.offset,
                        at = cursor,
                        "offset gap during recovery; stopping before frame"
                    );
                    break;
                }
                if first_ts.is_none() {
                    first_ts = Some(view.timestamp_ms);
                }
                if records_since >= INDEX_INTERVAL_RECORDS {
                    rebuilt.append(IndexEntry {
                        offset: view.offset,
                        position: cursor as u64,
                        timestamp_ms: view.timestamp_ms,
                    })?;
                    records_since = 0;
                }
                records_since += 1;
                report
                    .records_valid
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                valid_len += frame_len as u64;
                next_offset += 1;
                cursor += frame_len;
            }
            Err(record::ParseError::NeedMore) | Err(record::ParseError::Truncated) => {
                warn!(base, at = cursor, "torn tail detected; truncating");
                break;
            }
            Err(record::ParseError::CrcMismatch)
            | Err(record::ParseError::BadMagic)
            | Err(record::ParseError::BadVersion)
            | Err(record::ParseError::LengthTooSmall) => {
                warn!(base, at = cursor, "corrupt frame; truncating tail");
                break;
            }
        }
    }
    report
        .segments_scanned
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(ScanResult {
        valid_len,
        next_offset,
        records_since_index: records_since,
        first_timestamp_ms: first_ts,
        rebuilt,
    })
}

/// Persist a rebuilt index atomically over the segment's `.idx` file.
fn write_index_atomic(index_path: &Path, rebuilt: &Index) -> Result<(), SegmentError> {
    let tmp = index_path.with_extension("idx.tmp");
    {
        let mut idx_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        for e in rebuilt.entries() {
            let mut raw = [0u8; crate::index::INDEX_ENTRY_LEN];
            crate::index::encode_entry(e, &mut raw);
            idx_file.write_all(&raw)?;
        }
        idx_file.sync_data()?;
    }
    std::fs::rename(tmp, index_path)?;
    Ok(())
}

/// Recover a sealed segment: validate its log, truncate any torn tail and
/// rebuild its index so it can never be inconsistent with the log.
fn rebuild_index_for_segment(
    dir: &Path,
    base: u64,
    report: &RecoveryReport,
) -> Result<(), SegmentError> {
    let log_path = segment_path(dir, base, "log");
    let index_path = segment_path(dir, base, "idx");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log_path)?;
    let len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(0))?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let scan = scan_segment_log(&mmap, base, report)?;
    drop(mmap);
    if scan.valid_len < len {
        file.set_len(scan.valid_len)?;
        report.bytes_truncated.fetch_add(
            len - scan.valid_len,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
    file.sync_data()?;
    write_index_atomic(&index_path, &scan.rebuilt)?;
    Ok(())
}

/// Validate the last (active) segment frame-by-frame, truncating any torn tail
/// and rebuilding the sparse index from scratch.
fn recover_active(
    dir: &Path,
    base: u64,
    report: &RecoveryReport,
) -> Result<ActiveSegment, SegmentError> {
    let log_path = segment_path(dir, base, "log");
    let index_path = segment_path(dir, base, "idx");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log_path)?;
    let len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(0))?;

    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let scan = scan_segment_log(&mmap, base, report)?;
    drop(mmap);

    if scan.valid_len < len {
        file.set_len(scan.valid_len)?;
        report.bytes_truncated.fetch_add(
            len - scan.valid_len,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
    file.sync_data()?;

    // Persist the rebuilt index atomically.
    write_index_atomic(&index_path, &scan.rebuilt)?;

    // Reopen for appending and seek to the validated end. Without this seek
    // the cursor sits at byte 0, so the next append would overwrite existing
    // records from the start — a silent data-corruption path after recovery.
    let mut reopen = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&log_path)?;
    reopen.seek(SeekFrom::Start(scan.valid_len))?;
    let writer = BufWriter::new(reopen);
    let index = Index::open(&index_path, true)?;
    Ok(ActiveSegment {
        base_offset: base,
        log_path,
        index_path,
        writer,
        index,
        readable_len: scan.valid_len,
        file_len: scan.valid_len,
        records_since_index: scan.records_since_index,
        next_offset: scan.next_offset,
        first_timestamp_ms: scan.first_timestamp_ms,
    })
}
