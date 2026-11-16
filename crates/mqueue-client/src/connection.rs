//! A reconnecting, request/response TCP connection to the broker.

use std::sync::Arc;
use std::time::Duration;

use mqueue_proto::frame::{read_response, write_request, FrameError};
use mqueue_proto::{Request, Response};
use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("broker error {0:?}: {1}")]
    Broker(mqueue_proto::ErrorCode, String),
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("max retries exhausted: {0}")]
    Retry(String),
}

/// One shared connection per client. All requests are serialized through a
/// mutex — the broker is single-node and requests are cheap, while ordering
/// guarantees benefit from a single ordered byte stream.
#[derive(Clone)]
pub struct Connection {
    addr: Arc<String>,
    inner: Arc<Mutex<Option<BufStream<TcpStream>>>>,
    timeout: Duration,
}

impl Connection {
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: Arc::new(addr.into()),
            inner: Arc::new(Mutex::new(None)),
            timeout: Duration::from_secs(10),
        }
    }

    pub fn with_timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }

    async fn ensure(&self) -> std::io::Result<()> {
        let mut g = self.inner.lock().await;
        if g.is_none() {
            let stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(self.addr.as_str()))
                .await
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timeout"))??;
            stream.set_nodelay(true)?;
            *g = Some(BufStream::new(stream));
        }
        Ok(())
    }

    async fn raw_request(&self, req: &Request) -> Result<Response, ClientError> {
        self.ensure().await?;
        let mut g = self.inner.lock().await;
        let stream = g
            .as_mut()
            .ok_or_else(|| ClientError::Retry("no connection".into()))?;
        let result: Result<Response, FrameError> = async {
            write_request(stream, req).await?;
            stream.flush().await?;
            read_response(stream).await
        }
        .await;
        match result {
            Ok(Response::Error(code, msg)) => {
                // A protocol-level error doesn't kill the socket; transport
                // errors do, and are handled below.
                Err(ClientError::Broker(code, msg))
            }
            Ok(r) => Ok(r),
            Err(e) => {
                // Drop the broken socket so the next call reconnects.
                *g = None;
                Err(ClientError::Frame(e))
            }
        }
    }

    /// Send a request, reconnecting and retrying while the broker is
    /// unavailable. Retries are *safe*: produce requests are idempotent via
    /// producer sequence numbers, and every other request type (fetch,
    /// heartbeat, commit, metadata) re-asserts the same state, so replaying it
    /// has no side effects.
    ///
    /// Only transport-level failures and `BrokerUnavailable` are retried, and
    /// they are retried indefinitely with capped exponential backoff so a
    /// broker crash/restart of a few seconds never surfaces as a client error.
    /// Application-level errors (fenced generation, unknown consumer) still
    /// return immediately for the caller to handle.
    pub async fn request(&self, req: &Request) -> Result<Response, ClientError> {
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            match tokio::time::timeout(self.timeout, self.raw_request(req)).await {
                Ok(Ok(r)) => return Ok(r),
                Ok(Err(ClientError::Broker(code, msg))) => {
                    // Fenced generations and unknown consumers must surface to
                    // the caller; transport-ish broker failures get retried.
                    use mqueue_proto::ErrorCode::*;
                    match code {
                        BrokerUnavailable => {}
                        _ => return Err(ClientError::Broker(code, msg)),
                    }
                }
                Ok(Err(e)) => {
                    // Drop the broken socket so the next call reconnects.
                    *self.inner.lock().await = None;
                    tracing::debug!(attempt, error = %e, "request failed; reconnecting");
                }
                Err(_) => {
                    // Timeout: force reconnect.
                    *self.inner.lock().await = None;
                    tracing::debug!(attempt, "request timed out; reconnecting");
                }
            }
            // Exponential backoff capped at 2s so a long broker outage is
            // handled without busy-looping or giving up.
            let backoff_ms = (50u64.saturating_mul(1u64 << attempt.min(6))).min(2000);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }
}
