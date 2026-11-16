#![allow(dead_code)]
//! Chaos orchestrator and CLI entrypoint.
//!
//! Subcommands:
//! - `run`       full benchmark + chaos + verification + report
//! - `producer`  a single producer worker process (spawned by `run`)
//! - `consumer`  a single consumer worker process (spawned by `run`)
//! - `verify`    re-run verification against an existing run directory

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand};
use mqueue_proto::frame;
use rand::Rng;
use serde::Serialize;

mod consumer_worker;
mod payload;
mod producer_worker;
mod report;
mod signal_flag;
mod verifier;

#[derive(Parser, Debug)]
#[command(name = "mqueue-bench", version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run the full chaos benchmark.
    Run(RunArgs),
    /// Producer worker (used internally).
    Producer(ProducerCli),
    /// Consumer worker (used internally).
    Consumer(ConsumerCli),
    /// Verify an existing run directory.
    Verify(VerifyCli),
}

#[derive(Parser, Debug, Clone)]
struct RunArgs {
    #[arg(long, default_value = "127.0.0.1:19100")]
    addr: String,
    #[arg(long, default_value = "127.0.0.1:19101")]
    console: String,
    #[arg(long, default_value = "bench-topic")]
    topic: String,
    #[arg(long, default_value_t = 4)]
    partitions: u32,
    #[arg(long, default_value_t = 20_000)]
    messages: u64,
    #[arg(long, default_value_t = 64)]
    key_space: u64,
    #[arg(long, default_value_t = 128)]
    batch_size: usize,
    #[arg(long, default_value_t = 2)]
    producers: usize,
    #[arg(long, default_value_t = 2)]
    consumers: usize,
    #[arg(long, default_value_t = 45)]
    duration_secs: u64,
    /// Chaos event interval range (seconds), chosen uniformly at random.
    #[arg(long, default_value_t = 4)]
    chaos_min_secs: u64,
    #[arg(long, default_value_t = 9)]
    chaos_max_secs: u64,
    /// Whether to also SIGKILL the broker (exercises crash recovery).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    kill_broker: bool,
    #[arg(long, default_value = "./bench-run")]
    run_dir: PathBuf,
    /// Broker segment size, small so segment rolling is exercised during the
    /// run.
    #[arg(long, default_value_t = 256 * 1024)]
    segment_bytes: u64,
}

#[derive(Parser, Debug)]
struct ProducerCli {
    #[arg(long)]
    addr: String,
    #[arg(long)]
    topic: String,
    #[arg(long)]
    partitions: u32,
    #[arg(long)]
    id: String,
    #[arg(long)]
    dir: PathBuf,
    #[arg(long)]
    target_total: u64,
    #[arg(long)]
    key_space: u64,
    #[arg(long)]
    batch_size: usize,
    #[arg(long, default_value_t = 0.0)]
    rate_per_sec: f64,
}

#[derive(Parser, Debug)]
struct ConsumerCli {
    #[arg(long)]
    addr: String,
    #[arg(long)]
    topic: String,
    #[arg(long)]
    group: String,
    #[arg(long)]
    id: String,
    #[arg(long)]
    dir: PathBuf,
    #[arg(long, default_value_t = 1024 * 1024)]
    max_bytes: u32,
    #[arg(long, default_value_t = 1000)]
    poll_timeout_ms: u32,
}

#[derive(Parser, Debug)]
struct VerifyCli {
    #[arg(long, default_value = "127.0.0.1:19100")]
    addr: String,
    #[arg(long, default_value = "bench-topic")]
    topic: String,
    #[arg(long, default_value_t = 4)]
    partitions: u32,
    #[arg(long)]
    run_dir: PathBuf,
}

/// A running worker process with the metadata needed to restart it.
struct Worker {
    kind: WorkerKind,
    id: String,
    child: Option<Child>,
    cli: Vec<String>,
    dir: PathBuf,
    log_path: PathBuf,
    restarts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerKind {
    Producer,
    Consumer,
}

#[derive(Serialize)]
struct ChaosEvent {
    at_secs: u64,
    action: String,
    target: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,mqueue_bench=debug".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Producer(a) => {
            producer_worker::run(producer_worker::ProducerArgs {
                addr: a.addr,
                topic: a.topic,
                partitions: a.partitions,
                id: a.id,
                dir: a.dir,
                target_total: a.target_total,
                key_space: a.key_space,
                batch_size: a.batch_size,
                rate_per_sec: a.rate_per_sec,
            })
            .await?;
        }
        Cmd::Consumer(a) => {
            consumer_worker::run(consumer_worker::ConsumerArgs {
                addr: a.addr,
                topic: a.topic,
                group: a.group,
                id: a.id,
                dir: a.dir,
                max_bytes: a.max_bytes,
                poll_timeout_ms: a.poll_timeout_ms,
            })
            .await?;
        }
        Cmd::Verify(a) => {
            let report = verifier::verify(&a.addr, &a.topic, a.partitions, &a.run_dir).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            std::process::exit(if report.passed { 0 } else { 1 });
        }
        Cmd::Run(a) => run_chaos(a).await?,
    }
    Ok(())
}

fn current_exe() -> PathBuf {
    std::env::current_exe().expect("current executable path")
}

fn broker_exe() -> PathBuf {
    // The broker binary sits next to this bench binary in target/<profile>.
    let mut p = current_exe();
    p.pop();
    p.push(if cfg!(windows) { "mqueue-broker.exe" } else { "mqueue-broker" });
    p
}

async fn wait_port(addr: &str, timeout: Duration) -> anyhow::Result<()> {
    let host_port = addr.strip_prefix("http://").unwrap_or(addr);
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(host_port).await.is_ok() {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            bail!("timed out waiting for {addr}");
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

/// Query the console for a consumer group's total backlog (sum of
/// end-offset minus committed-offset across its partitions).
async fn group_lag(console_addr: &str, group: &str) -> anyhow::Result<u64> {
    use tokio::io::AsyncReadExt;
    let host_port = console_addr.strip_prefix("http://").unwrap_or(console_addr);
    let mut stream = tokio::net::TcpStream::connect(host_port).await?;
    let request = format!(
        "GET /api/groups HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n"
    );
    use tokio::io::AsyncWriteExt;
    stream.write_all(request.as_bytes()).await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    let text = String::from_utf8_lossy(&raw);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .ok_or_else(|| anyhow::anyhow!("malformed console response"))?;
    let json: serde_json::Value = serde_json::from_str(body)?;
    let groups = json.get("groups").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    for g in groups {
        if g["snapshot"]["name"].as_str() == Some(group) {
            return Ok(g["total_lag"].as_u64().unwrap_or(0));
        }
    }
    // Group not registered yet means no committed offsets; report a large lag
    // so the drain keeps waiting rather than exiting prematurely.
    Ok(u64::MAX)
}

async fn create_topic(addr: &str, topic: &str, partitions: u32) -> anyhow::Result<()> {
    use mqueue_proto::{CreateTopicRequest, Request, Response};
    use tokio::io::AsyncWriteExt;
    let stream = tokio::net::TcpStream::connect(addr).await?;
    let (r, mut w) = stream.into_split();
    let mut reader = tokio::io::BufReader::new(r);
    frame::write_request(
        &mut w,
        &Request::CreateTopic(CreateTopicRequest {
            name: topic.to_string(),
            partitions,
            max_segment_bytes: None,
            max_age_ms: None,
            max_total_bytes: None,
        }),
    )
    .await?;
    w.flush().await?;
    match frame::read_response(&mut reader).await? {
        Response::TopicCreated | Response::Error(_, _) => Ok(()),
        other => bail!("unexpected create-topic response: {other:?}"),
    }
}

async fn run_chaos(args: RunArgs) -> anyhow::Result<()> {
    let run_dir = args.run_dir.clone();
    let broker_data = run_dir.join("broker-data");
    let workers_dir = run_dir.join("workers");
    tokio::fs::create_dir_all(&broker_data).await?;
    tokio::fs::create_dir_all(&workers_dir).await?;

    // ---- start broker -----------------------------------------------------
    let logs_dir = run_dir.join("logs");
    tokio::fs::create_dir_all(&logs_dir).await?;
    let broker = start_broker(&args, &broker_data, &logs_dir.join("broker.log"))?;
    wait_port(&args.addr, Duration::from_secs(15)).await?;
    create_topic(&args.addr, &args.topic, args.partitions).await?;

    // ---- spawn producers & consumers -------------------------------------
    let exe = current_exe();
    let mut workers: Vec<Worker> = Vec::new();
    let per_producer_target =
        (args.messages + args.producers as u64 - 1) / args.producers.max(1) as u64;

    for i in 0..args.producers {
        let id = format!("producer-{i}");
        let dir = workers_dir.join(&id);
        tokio::fs::create_dir_all(&dir).await?;
        let cli = vec![
            "producer".into(),
            "--addr".into(), args.addr.clone(),
            "--topic".into(), args.topic.clone(),
            "--partitions".into(), args.partitions.to_string(),
            "--id".into(), id.clone(),
            "--dir".into(), dir.to_string_lossy().into_owned(),
            "--target-total".into(), per_producer_target.to_string(),
            "--key-space".into(), args.key_space.to_string(),
            "--batch-size".into(), args.batch_size.to_string(),
        ];
        let log_path = logs_dir.join(format!("{id}.log"));
        let child = spawn_worker(&exe, &cli, &log_path)?;
        workers.push(Worker {
            kind: WorkerKind::Producer,
            id,
            child: Some(child),
            cli,
            dir,
            log_path,
            restarts: 0,
        });
    }

    let group = "bench-group".to_string();
    for i in 0..args.consumers {
        let id = format!("consumer-{i}");
        let dir = workers_dir.join(&id);
        tokio::fs::create_dir_all(&dir).await?;
        let cli = vec![
            "consumer".into(),
            "--addr".into(), args.addr.clone(),
            "--topic".into(), args.topic.clone(),
            "--group".into(), group.clone(),
            "--id".into(), id.clone(),
            "--dir".into(), dir.to_string_lossy().into_owned(),
        ];
        let log_path = logs_dir.join(format!("{id}.log"));
        let child = spawn_worker(&exe, &cli, &log_path)?;
        workers.push(Worker {
            kind: WorkerKind::Consumer,
            id,
            child: Some(child),
            cli,
            dir,
            log_path,
            restarts: 0,
        });
    }

    // ---- chaos loop -------------------------------------------------------
    // The loop runs for at least `duration_secs` (so chaos has time to happen)
    // and until every producer has finished its target. A producer that exits
    // 0 is considered done and is never restarted; only unexpected deaths are.
    let mut rng = rand::thread_rng();
    let start = std::time::Instant::now();
    let min_deadline = start + Duration::from_secs(args.duration_secs);
    let mut finished: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut events: Vec<ChaosEvent> = Vec::new();
    let mut broker_handle = Some(broker);
    let mut next_chaos = start.elapsed() + chaos_delay(&mut rng, &args);

    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Detect workers that exited. Producers exiting cleanly are "finished";
        // anything else (signal death during chaos is handled separately, but a
        // consumer that dies on its own) gets revived to preserve capacity.
        for w in workers.iter_mut() {
            if finished.contains(&w.id) {
                continue;
            }
            if let Some(child) = w.child.as_mut() {
                if let Ok(Some(status)) = child.try_wait() {
                    match (w.kind, status.success()) {
                        (WorkerKind::Producer, true) => {
                            finished.insert(w.id.clone());
                            tracing::info!(worker = %w.id, "producer finished its target");
                        }
                        _ => {
                            tracing::warn!(worker = %w.id, ?status, "worker died; restarting");
                            w.child = Some(spawn_worker(&exe, &w.cli, &w.log_path)?);
                            w.restarts += 1;
                        }
                    }
                }
            }
        }

        let producers_total = workers
            .iter()
            .filter(|w| w.kind == WorkerKind::Producer)
            .count();
        let producers_done = workers
            .iter()
            .filter(|w| w.kind == WorkerKind::Producer)
            .filter(|w| finished.contains(&w.id))
            .count();
        let min_time_elapsed = std::time::Instant::now() >= min_deadline;
        if min_time_elapsed && producers_done == producers_total {
            break;
        }

        if start.elapsed() >= next_chaos {
            let action = pick_chaos(&mut rng, &args, &workers, args.kill_broker);
            apply_chaos(
                &action,
                &exe,
                &mut workers,
                &mut broker_handle,
                &args,
                &broker_data,
                &mut events,
                start.elapsed().as_secs(),
            )
            .await?;
            next_chaos = start.elapsed() + chaos_delay(&mut rng, &args);
        }
    }

    // ---- graceful shutdown ------------------------------------------------
    // The run ends on producer completion (not a fixed wall clock): once every
    // producer has sent its target, we stop injecting chaos, signal the
    // producers to exit, and drain consumers until the group lag reaches zero
    // (with a bounded fallback so a stuck consumer can't hang the bench).
    tracing::info!("producers complete; draining until consumer lag is zero");
    for w in workers.iter_mut().filter(|w| w.kind == WorkerKind::Producer) {
        signal_graceful(w);
    }
    let drain_deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let producers_exited = workers
            .iter()
            .filter(|w| w.kind == WorkerKind::Producer)
            .all(|w| finished.contains(&w.id));
        let lag = group_lag(&args.console, "bench-group").await.unwrap_or(u64::MAX);
        if producers_exited && lag == 0 {
            tracing::info!("consumer group fully caught up (lag = 0)");
            break;
        }
        if std::time::Instant::now() > drain_deadline {
            tracing::warn!(lag, "drain deadline reached; stopping with residual lag");
            break;
        }
    }
    // Consumers have committed everything they processed; stop them now.
    for w in workers.iter_mut().filter(|w| w.kind == WorkerKind::Consumer) {
        signal_graceful(w);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    for w in workers.iter_mut() {
        if let Some(mut c) = w.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    // ---- verify against the still-running broker --------------------------
    let report = verifier::verify(&args.addr, &args.topic, args.partitions, &run_dir).await?;
    let perf = verifier::load_perf(&run_dir)?;

    // ---- write report ------------------------------------------------------
    let summary = build_summary_html(&args, &report, &perf, &events);
    let report_html = report::wrap_html(&summary, "mqueue 压测与混沌测试报告");
    tokio::fs::write(run_dir.join("report.html"), &report_html).await?;
    tokio::fs::write(
        run_dir.join("verify.json"),
        serde_json::to_string_pretty(&report)?,
    )
    .await?;
    tokio::fs::write(
        run_dir.join("perf.json"),
        serde_json::to_string_pretty(&perf)?,
    )
    .await?;
    tokio::fs::write(
        run_dir.join("chaos.json"),
        serde_json::to_string_pretty(&events)?,
    )
    .await?;

    // Stop the broker last.
    if let Some(mut b) = broker_handle.take() {
        let _ = b.kill();
        let _ = b.wait();
    }

    if report.passed {
        println!(
            "\n✅ 校验通过：{} 条日志记录全部至少处理一次，同分区顺序正确，无重复、无幽灵记录。",
            report.total_in_log
        );
    } else {
        println!("\n❌ 校验失败：");
        for m in &report.missing_records {
            println!("  缺失: {m}");
        }
        for m in &report.duplicate_events {
            println!("  重复: {m}");
        }
        for m in &report.order_violations {
            println!("  乱序: {m}");
        }
        for m in &report.phantom_events {
            println!("  幽灵: {m}");
        }
    }
    println!(
        "延迟 p50={}ms p95={}ms p99={}ms max={}ms；报告: {}/report.html",
        perf.latency_ms_p50,
        perf.latency_ms_p95,
        perf.latency_ms_p99,
        perf.latency_ms_max,
        run_dir.display()
    );
    anyhow::ensure!(report.passed, "verification failed");
    Ok(())
}

fn start_broker(args: &RunArgs, data: &Path, log_path: &Path) -> anyhow::Result<Child> {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    let err = log.try_clone()?;
    let child = Command::new(broker_exe())
        .arg("--data-dir")
        .arg(data)
        .arg("--bind")
        .arg(&args.addr)
        .arg("--console")
        .arg(&args.console)
        .arg("--segment-bytes")
        .arg(args.segment_bytes.to_string())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()
        .context("spawning broker")?;
    Ok(child)
}

fn spawn_worker(exe: &Path, cli: &[String], log_path: &Path) -> anyhow::Result<Child> {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    let err = log.try_clone()?;
    Ok(Command::new(exe)
        .args(cli)
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(err))
        .spawn()?)
}

fn chaos_delay(rng: &mut rand::rngs::ThreadRng, args: &RunArgs) -> Duration {
    let lo = args.chaos_min_secs;
    let hi = args.chaos_max_secs.max(lo);
    Duration::from_secs(rng.gen_range(lo..=hi))
}

enum Chaos {
    KillWorker(usize),
    AddConsumer,
    KillBroker,
}

fn pick_chaos(
    rng: &mut rand::rngs::ThreadRng,
    args: &RunArgs,
    workers: &[Worker],
    allow_broker: bool,
) -> Chaos {
    // Weighted: mostly worker kills/adds, occasionally a broker kill.
    let roll: u32 = rng.gen_range(0..10);
    if allow_broker && roll == 0 {
        return Chaos::KillBroker;
    }
    match rng.gen_range(0..3) {
        0 => {
            let idx = rng.gen_range(0..workers.len());
            Chaos::KillWorker(idx)
        }
        _ => {
            // Cap consumer count so partitions stay assignable and the test
            // machine doesn't spawn unbounded processes.
            let consumer_count = workers
                .iter()
                .filter(|w| w.kind == WorkerKind::Consumer)
                .count();
            if consumer_count < args.partitions as usize + 2 {
                Chaos::AddConsumer
            } else {
                let idx = rng.gen_range(0..workers.len());
                Chaos::KillWorker(idx)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn apply_chaos(
    action: &Chaos,
    exe: &Path,
    workers: &mut Vec<Worker>,
    broker: &mut Option<Child>,
    args: &RunArgs,
    broker_data: &Path,
    events: &mut Vec<ChaosEvent>,
    at_secs: u64,
) -> anyhow::Result<()> {
    match action {
        Chaos::KillWorker(idx) => {
            let w = workers.get_mut(*idx).expect("worker index");
            if let Some(mut child) = w.child.take() {
                // SIGKILL = hard crash; the worker's durable state is the only
                // thing that survives.
                let _ = child.kill();
                let _ = child.wait();
                events.push(ChaosEvent {
                    at_secs,
                    action: "sigkill".into(),
                    target: w.id.clone(),
                });
                tracing::info!(worker = %w.id, "chaos: SIGKILL worker");
            }
            // Immediately restart the same logical worker (same id and state
            // directory) so it resumes from durable state.
            w.child = Some(spawn_worker(exe, &w.cli, &w.log_path)?);
            w.restarts += 1;
            tracing::info!(worker = %w.id, restarts = w.restarts, "worker restarted");
        }
        Chaos::AddConsumer => {
            let n = workers
                .iter()
                .filter(|w| w.kind == WorkerKind::Consumer)
                .count();
            let id = format!("consumer-extra-{n}");
            let dir = args.run_dir.join("workers").join(&id);
            tokio::fs::create_dir_all(&dir).await?;
            let cli = vec![
                "consumer".into(),
                "--addr".into(), args.addr.clone(),
                "--topic".into(), args.topic.clone(),
                "--group".into(), "bench-group".into(),
                "--id".into(), id.clone(),
                "--dir".into(), dir.to_string_lossy().into_owned(),
            ];
            let log_path = args
                .run_dir
                .join("logs")
                .join(format!("{id}.log"));
            let child = spawn_worker(exe, &cli, &log_path)?;
            events.push(ChaosEvent {
                at_secs,
                action: "add-consumer".into(),
                target: id.clone(),
            });
            tracing::info!(consumer = %id, "chaos: add consumer (rebalance)");
            workers.push(Worker {
                kind: WorkerKind::Consumer,
                id,
                child: Some(child),
                cli,
                dir,
                log_path,
                restarts: 0,
            });
        }
        Chaos::KillBroker => {
            if let Some(mut child) = broker.take() {
                let _ = child.kill();
                let _ = child.wait();
                events.push(ChaosEvent {
                    at_secs,
                    action: "sigkill-broker".into(),
                    target: "mqueue-broker".into(),
                });
                tracing::info!("chaos: SIGKILL broker; exercising crash recovery");
                let fresh = start_broker(
                    args,
                    broker_data,
                    &args.run_dir.join("logs").join("broker.log"),
                )?;
                wait_port(&args.addr, Duration::from_secs(20)).await?;
                *broker = Some(fresh);
                tracing::info!("broker recovered and is serving again");
            }
        }
    }
    Ok(())
}

fn signal_graceful(w: &mut Worker) {
    if let Some(child) = w.child.as_mut() {
        #[cfg(unix)]
        {
            let pid = child.id();
            unsafe {
                libc_kill(pid as i32, 15 /* SIGTERM */);
            }
        }
        #[cfg(not(unix))]
        {
            let _ = child.kill();
        }
    }
}

#[cfg(unix)]
unsafe fn libc_kill(pid: i32, sig: i32) {
    // Avoid a libc dependency: SIGTERM (15) via the raw syscall is enough.
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    let _ = kill(pid, sig);
}

fn build_summary_html(
    args: &RunArgs,
    report: &verifier::VerifyReport,
    perf: &verifier::PerfStats,
    events: &[ChaosEvent],
) -> String {
    let verdict = if report.passed {
        r#"<p class="ok">✅ 校验通过：每条消息至少处理一次，同分区顺序正确，无重复。</p>"#
    } else {
        r#"<p class="bad">❌ 校验失败，详见下方明细。</p>"#
    };
    let events_rows: String = events
        .iter()
        .map(|e| {
            format!(
                "<tr><td>{}s</td><td>{}</td><td>{}</td></tr>",
                e.at_secs, e.action, e.target
            )
        })
        .collect();
    let missing: String = report
        .missing_records
        .iter()
        .chain(report.duplicate_events.iter())
        .chain(report.order_violations.iter())
        .chain(report.phantom_events.iter())
        .map(|m| format!("<li>{m}</li>"))
        .collect::<Vec<_>>()
        .join("");
    format!(
        r#"
{verdict}
<table>
<tr><th>主题</th><td>{topic}</td><th>分区</th><td>{partitions}</td></tr>
<tr><th>目标消息</th><td>{messages}</td><th>键空间</th><td>{key_space}</td></tr>
<tr><th>生产者/消费者</th><td>{producers} / {consumers}</td><th>混沌时长</th><td>{duration}s</td></tr>
<tr><th>日志记录数</th><td>{in_log}</td><th>消费事件数</th><td>{events_n}</td></tr>
<tr><th>去重后已消费</th><td>{unique}</td><th>混沌事件</th><td>{chaos_n}</td></tr>
</table>
<h2>吞吐</h2>
{throughput}
<h2>端到端延迟</h2>
{latency}
<table>
<tr><th>min</th><th>p50</th><th>p95</th><th>p99</th><th>max</th></tr>
<tr><td>{lmin} ms</td><td>{p50} ms</td><td>{p95} ms</td><td>{p99} ms</td><td>{lmax} ms</td></tr>
</table>
<h2>混沌事件时序</h2>
<table><tr><th>时刻</th><th>动作</th><th>目标</th></tr>{events_rows}</table>
<h2>校验问题明细</h2>
<ul>{missing}</ul>
"#,
        verdict = verdict,
        topic = args.topic,
        partitions = args.partitions,
        messages = args.messages,
        key_space = args.key_space,
        producers = args.producers,
        consumers = args.consumers,
        duration = args.duration_secs,
        in_log = report.total_in_log,
        events_n = report.total_journal_events,
        unique = report.unique_records_consumed,
        chaos_n = events.len(),
        throughput = report::throughput_svg(perf),
        latency = report::latency_svg(perf),
        lmin = perf.latency_ms_min,
        p50 = perf.latency_ms_p50,
        p95 = perf.latency_ms_p95,
        p99 = perf.latency_ms_p99,
        lmax = perf.latency_ms_max,
        events_rows = events_rows,
        missing = if missing.is_empty() {
            "<li>无</li>".to_string()
        } else {
            missing
        }
    )
}
