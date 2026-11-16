//! Operations console: a small dependency-free HTTP/1.1 server serving a
//! single-page dashboard plus JSON APIs.
//!
//! Routes:
//! - `GET /`                 dashboard HTML
//! - `GET /api/overview`     broker totals, rates, uptime
//! - `GET /api/topics`       topics, partitions, end offsets, segment stats
//! - `GET /api/groups`       consumer groups, members, assignments, lag
//! - `GET /api/metrics`      per-second produce/fetch time series
//! - `POST /api/topics`      create a topic (JSON body)

use std::sync::Arc;

use mqueue_proto::CreateTopicRequest;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, warn};

use crate::broker::Broker;

pub async fn serve(broker: Arc<Broker>, addr: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "console listener up");
    loop {
        let (stream, peer) = listener.accept().await?;
        let broker = broker.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(broker, stream).await {
                debug!(?peer, error = %e, "console connection ended");
            }
        });
    }
}

async fn handle(broker: Arc<Broker>, mut stream: tokio::net::TcpStream) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 64 * 1024];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let head = String::from_utf8_lossy(&buf[..n]);
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("GET");
    let target = parts.next().unwrap_or("/");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    debug!(%method, %path, "console request");

    let (status, content_type, body) = match (method, path) {
        ("GET", "/") => (200, "text/html; charset=utf-8", crate::console_html::DASHBOARD.to_string()),
        ("GET", "/api/overview") => {
            json(200, serde_json::to_value(overview(&broker).await).unwrap())
        }
        ("GET", "/api/topics") => {
            let topics = topic_details(&broker).await;
            json(200, serde_json::json!({ "topics": topics }))
        }
        ("GET", "/api/groups") => {
            let groups = broker.groups.snapshots().await;
            let topics = broker.topics.read().await;
            let mut enriched = Vec::new();
            for g in groups {
                let end_offsets: Vec<u64> = topics
                    .get(&g.topic)
                    .map(|h| h.partitions.iter().map(|p| p.log_end_offset()).collect())
                    .unwrap_or_default();
                let lag: u64 = g
                    .committed
                    .iter()
                    .filter_map(|(p, committed)| {
                        end_offsets.get(*p as usize).map(|end| end.saturating_sub(*committed))
                    })
                    .sum();
                enriched.push(serde_json::json!({
                    "snapshot": g,
                    "log_end_offsets": end_offsets,
                    "total_lag": lag,
                }));
            }
            json(200, serde_json::json!({ "groups": enriched }))
        }
        ("GET", "/api/metrics") => {
            let secs: u64 = parse_query_u64(query, "seconds").unwrap_or(300);
            json(
                200,
                serde_json::json!({
                    "rates": broker.metrics.rates(),
                    "series": broker.metrics.series(secs),
                    "fetch_latency": broker.fetch_latency.snapshot(),
                }),
            )
        }
        ("POST", "/api/topics") => {
            let body = head.split("\r\n\r\n").nth(1).unwrap_or("");
            match serde_json::from_str::<CreateTopicRequest>(body) {
                Ok(req) => {
                    let resp = broker
                        .handle(mqueue_proto::Request::CreateTopic(req))
                        .await;
                    match resp {
                        mqueue_proto::Response::TopicCreated => {
                            json(201, serde_json::json!({ "created": true }))
                        }
                        mqueue_proto::Response::Error(code, msg) => json(
                            400,
                            serde_json::json!({ "error": format!("{code:?}: {msg}") }),
                        ),
                        _ => json(500, serde_json::json!({ "error": "unexpected" })),
                    }
                }
                Err(e) => json(400, serde_json::json!({ "error": e.to_string() })),
            }
        }
        _ => (404, "application/json", "{\"error\":\"not found\"}".to_string()),
    };

    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n",
        reason = if status == 200 { "OK" } else { "ERROR" },
        len = body.len(),
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

fn json(status: u16, value: serde_json::Value) -> (u16, &'static str, String) {
    (status, "application/json", value.to_string())
}

fn parse_query_u64(query: &str, key: &str) -> Option<u64> {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == key)
        .and_then(|(_, v)| v.parse().ok())
}

#[derive(Serialize)]
struct Overview {
    uptime_secs: u64,
    topics: usize,
    partitions: usize,
    total_produced: u64,
    total_fetched: u64,
    dedup_hits: u64,
    rates: serde_json::Value,
}

async fn overview(broker: &Arc<Broker>) -> Overview {
    let topics = broker.topics.read().await;
    let (total_produced, total_fetched, dedup_hits) = broker.metrics.totals();
    Overview {
        uptime_secs: broker.uptime().as_secs(),
        topics: topics.len(),
        partitions: topics.values().map(|t| t.partitions.len()).sum(),
        total_produced,
        total_fetched,
        dedup_hits,
        rates: serde_json::to_value(broker.metrics.rates()).unwrap(),
    }
}

async fn topic_details(broker: &Arc<Broker>) -> Vec<serde_json::Value> {
    let handles: Vec<Arc<crate::broker::TopicHandle>> =
        broker.topics.read().await.values().cloned().collect();
    let mut out = Vec::new();
    for h in handles {
        let partitions: Vec<_> = h
            .partitions
            .iter()
            .map(|p| serde_json::to_value(p.stat()).unwrap())
            .collect();
        out.push(serde_json::json!({
            "name": h.config.name,
            "partition_count": h.config.partitions,
            "partitions": partitions,
        }));
    }
    out.sort_by(|a, b| a["name"].as_str().unwrap_or("").cmp(b["name"].as_str().unwrap_or("")));
    out
}

// Keep warn imported even when debug assertions are off.
#[allow(dead_code)]
fn _warn_used() {
    warn!("console");
}
