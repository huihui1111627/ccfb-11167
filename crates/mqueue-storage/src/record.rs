//! On-disk record framing.
//!
//! Every record is a self-describing binary frame:
//!
//! ```text
//! ┌──────────┬────────┬──────────┬─────────┬──────┬───────────┬──────────┬───────┐
//! │ magic u16│ ver u8 │ flags u8 │ total u32│ off u64 (LE) ... │ producer u64 ... │
//! └──────────┴────────┴──────────┴─────────┴──────┴───────────┴──────────┴───────┘
//! ```
//!
//! The 32-byte header is followed by the key and value payloads. A CRC32 over
//! everything except the first 8 bytes (magic/version/flags/length/crc) lets
//! recovery distinguish a complete frame from a torn tail write.

use bytes::{Buf, BufMut};

pub const MAGIC: u16 = 0x4D51; // "MQ"
pub const VERSION: u8 = 1;
/// Header length in bytes: magic(2)+ver(1)+flags(1)+len(4)+crc(4)+off(8)+
/// producer(8)+seq(8)+ts(8)+klen(4)+vlen(4)
pub const HEADER_LEN: usize = 52;

/// Bit flags stored in the frame header.
pub mod flags {
    pub const NONE: u8 = 0;
}

/// A decoded record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    pub producer_id: u64,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    /// Serialize the record into one contiguous frame.
    pub fn encode(&self) -> Vec<u8> {
        let total = HEADER_LEN + self.key.len() + self.value.len();
        let mut out = Vec::with_capacity(total);
        out.put_u16_le(MAGIC);
        out.put_u8(VERSION);
        out.put_u8(flags::NONE);
        out.put_u32_le(total as u32);
        out.put_u32_le(0); // CRC placeholder
        out.put_u64_le(self.offset);
        out.put_u64_le(self.producer_id);
        out.put_u64_le(self.sequence);
        out.put_u64_le(self.timestamp_ms);
        out.put_u32_le(self.key.len() as u32);
        out.put_u32_le(self.value.len() as u32);
        out.extend_from_slice(&self.key);
        out.extend_from_slice(&self.value);
        let crc = crc32fast::hash(&out[12..]);
        out[8..12].copy_from_slice(&crc.to_le_bytes());
        out
    }
}

/// Zero-copy view over a record inside a segment buffer (mmap or read buffer).
#[derive(Debug, Clone, Copy)]
pub struct RecordView<'a> {
    pub offset: u64,
    pub producer_id: u64,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> RecordView<'a> {
    pub fn to_owned(&self) -> Record {
        Record {
            offset: self.offset,
            producer_id: self.producer_id,
            sequence: self.sequence,
            timestamp_ms: self.timestamp_ms,
            key: self.key.to_vec(),
            value: self.value.to_vec(),
        }
    }
}

/// Why a parse attempt failed. Recovery uses this to decide whether a frame is
/// torn (truncate and stop) or corrupt (also truncate and stop — we never skip
/// bytes, which would silently lose data).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    /// Fewer bytes than a header can occupy.
    NeedMore,
    BadMagic,
    BadVersion,
    LengthTooSmall,
    /// Declared frame length exceeds the bytes available.
    Truncated,
    CrcMismatch,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for ParseError {}

/// Fields extracted from just the fixed-size header, before the payload has
/// been read. Used by file-based readers that pull the frame in two syscalls.
#[derive(Debug, Clone, Copy)]
pub struct HeaderInfo {
    pub total: usize,
    pub offset: u64,
    pub producer_id: u64,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub klen: usize,
    pub vlen: usize,
}

/// Parse only the fixed-size header. Magic/version/length are validated; the
/// CRC cannot be checked until the payload is available, so the caller must
/// run [`parse_at`] over the reassembled frame before trusting the data.
pub fn parse_header(buf: &[u8]) -> Result<HeaderInfo, ParseError> {
    if buf.len() < HEADER_LEN {
        return Err(ParseError::NeedMore);
    }
    let mut cursor = &buf[..];
    let magic = cursor.get_u16_le();
    if magic != MAGIC {
        return Err(ParseError::BadMagic);
    }
    let version = cursor.get_u8();
    if version != VERSION {
        return Err(ParseError::BadVersion);
    }
    let _flags = cursor.get_u8();
    let total = cursor.get_u32_le() as usize;
    if total < HEADER_LEN {
        return Err(ParseError::LengthTooSmall);
    }
    let _crc = cursor.get_u32_le();
    let offset = cursor.get_u64_le();
    let producer_id = cursor.get_u64_le();
    let sequence = cursor.get_u64_le();
    let timestamp_ms = cursor.get_u64_le();
    let klen = cursor.get_u32_le() as usize;
    let vlen = cursor.get_u32_le() as usize;
    if HEADER_LEN + klen + vlen != total {
        return Err(ParseError::LengthTooSmall);
    }
    Ok(HeaderInfo {
        total,
        offset,
        producer_id,
        sequence,
        timestamp_ms,
        klen,
        vlen,
    })
}

/// Parse the record that starts at `buf[0]`. On success returns the view and the
/// total frame length so the caller can advance.
pub fn parse_at(buf: &[u8]) -> Result<(RecordView<'_>, usize), ParseError> {
    if buf.len() < HEADER_LEN {
        return Err(ParseError::NeedMore);
    }
    let mut cursor = &buf[..];
    let magic = cursor.get_u16_le();
    if magic != MAGIC {
        return Err(ParseError::BadMagic);
    }
    let version = cursor.get_u8();
    if version != VERSION {
        return Err(ParseError::BadVersion);
    }
    let _flags = cursor.get_u8();
    let total = cursor.get_u32_le() as usize;
    if total < HEADER_LEN {
        return Err(ParseError::LengthTooSmall);
    }
    if buf.len() < total {
        return Err(ParseError::Truncated);
    }
    let stored_crc = cursor.get_u32_le();
    let actual_crc = crc32fast::hash(&buf[12..total]);
    if stored_crc != actual_crc {
        return Err(ParseError::CrcMismatch);
    }
    let offset = cursor.get_u64_le();
    let producer_id = cursor.get_u64_le();
    let sequence = cursor.get_u64_le();
    let timestamp_ms = cursor.get_u64_le();
    let klen = cursor.get_u32_le() as usize;
    let vlen = cursor.get_u32_le() as usize;
    if HEADER_LEN + klen + vlen != total {
        return Err(ParseError::LengthTooSmall);
    }
    let key = &buf[HEADER_LEN..HEADER_LEN + klen];
    let value = &buf[HEADER_LEN + klen..total];
    Ok((
        RecordView {
            offset,
            producer_id,
            sequence,
            timestamp_ms,
            key,
            value,
        },
        total,
    ))
}
