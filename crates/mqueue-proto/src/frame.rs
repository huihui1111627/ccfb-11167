//! Length-prefixed frame IO over any async stream.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{Request, Response};

/// Maximum frame size we are willing to allocate (16 MiB). A malicious or
/// broken peer advertising a larger length gets an error instead of OOM.
pub const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("frame length {0} exceeds {MAX_FRAME_LEN}")]
    TooLarge(u32),
    #[error("connection closed")]
    Closed,
    #[error("frame decode failed: {0}")]
    Decode(String),
}

async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, FrameError> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_FRAME_LEN {
        return Err(FrameError::TooLarge(len));
    }
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, buf: &[u8]) -> Result<(), FrameError> {
    if buf.len() > MAX_FRAME_LEN as usize {
        return Err(FrameError::TooLarge(buf.len() as u32));
    }
    writer.write_all(&(buf.len() as u32).to_le_bytes()).await?;
    writer.write_all(buf).await?;
    Ok(())
}

pub async fn read_request<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Request, FrameError> {
    let buf = read_frame(reader).await?;
    bincode::deserialize(&buf).map_err(|e| FrameError::Decode(e.to_string()))
}

/// Read one length-prefixed frame, returning the raw length prefix bytes and
/// payload on failure so the caller can log exactly what was on the wire.
pub async fn read_frame_diagnostic<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Request, FrameDiagnostic> {
    let mut len_buf = [0u8; 4];
    if let Err(e) = reader.read_exact(&mut len_buf).await {
        return Err(FrameDiagnostic {
            len_prefix: len_buf,
            got_full_prefix: false,
            source: e.to_string(),
        });
    }
    let len = u32::from_le_bytes(len_buf);
    if len > MAX_FRAME_LEN {
        return Err(FrameDiagnostic {
            len_prefix: len_buf,
            got_full_prefix: true,
            source: format!("length {len} exceeds {MAX_FRAME_LEN}"),
        });
    }
    let mut buf = vec![0u8; len as usize];
    if let Err(e) = reader.read_exact(&mut buf).await {
        return Err(FrameDiagnostic {
            len_prefix: len_buf,
            got_full_prefix: true,
            source: e.to_string(),
        });
    }
    bincode::deserialize(&buf).map_err(|e| FrameDiagnostic {
        len_prefix: len_buf,
        got_full_prefix: true,
        source: format!("decode: {e}; payload head {:02x?}", &buf[..buf.len().min(16)]),
    })
}

/// Details of a failed frame read, used to distinguish a torn stream from a
/// protocol mismatch during crash-recovery testing.
#[derive(Debug)]
pub struct FrameDiagnostic {
    pub len_prefix: [u8; 4],
    pub got_full_prefix: bool,
    pub source: String,
}

pub async fn write_request<W: AsyncWrite + Unpin>(
    writer: &mut W,
    req: &Request,
) -> Result<(), FrameError> {
    let buf = bincode::serialize(req).map_err(|e| FrameError::Decode(e.to_string()))?;
    write_frame(writer, &buf).await
}

pub async fn read_response<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Response, FrameError> {
    let buf = read_frame(reader).await?;
    bincode::deserialize(&buf).map_err(|e| FrameError::Decode(e.to_string()))
}

pub async fn write_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    resp: &Response,
) -> Result<(), FrameError> {
    let buf = bincode::serialize(resp).map_err(|e| FrameError::Decode(e.to_string()))?;
    write_frame(writer, &buf).await
}
