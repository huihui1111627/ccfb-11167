//! TCP server: one framed task per connection.

use std::sync::Arc;

use mqueue_proto::frame::{read_frame_diagnostic, write_response, FrameError};
use mqueue_proto::Response;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, warn};

use crate::broker::Broker;

pub async fn serve(broker: Arc<Broker>, addr: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "mq protocol listener up");
    loop {
        let (stream, peer) = listener.accept().await?;
        let broker = broker.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(broker, stream).await {
                match e {
                    FrameError::Closed => debug!(?peer, "connection closed"),
                    other => warn!(?peer, error = %other, "connection error"),
                }
            }
        });
    }
}

async fn handle_connection(broker: Arc<Broker>, stream: TcpStream) -> Result<(), FrameError> {
    stream.set_nodelay(true)?;
    let (mut reader, mut writer) = stream.into_split();
    loop {
        let req = match read_frame_diagnostic(&mut reader).await {
            Ok(r) => r,
            Err(diag) => {
                // A clean EOF means the peer went away (normal on restart).
                if !diag.got_full_prefix && diag.source.contains("unexpected end of file") {
                    return Ok(());
                }
                warn!(
                    len_prefix = ?diag.len_prefix,
                    full_prefix = diag.got_full_prefix,
                    source = %diag.source,
                    "malformed frame; closing connection"
                );
                return Err(FrameError::Decode(diag.source));
            }
        };
        let resp = broker.handle(req).await;
        if let Response::Error(code, msg) = &resp {
            debug!(?code, %msg, "request failed");
        }
        write_response(&mut writer, &resp).await?;
    }
}
