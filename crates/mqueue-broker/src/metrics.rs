//! Broker-side metrics: counters plus rolling windows used by the console to
//! render produce/fetch rates and backlog.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;

/// One window bucket. Samples arrive at high frequency; we keep one bucket per
/// second and retain `WINDOW_SECS` history.
const WINDOW_SECS: usize = 300;

#[derive(Default)]
struct Bucket {
    produced_messages: u64,
    produced_bytes: u64,
    fetched_messages: u64,
    fetched_bytes: u64,
}

struct Window {
    started: Instant,
    buckets: VecDeque<(u64, Bucket)>,
}

impl Window {
    fn new() -> Self {
        let mut buckets = VecDeque::with_capacity(WINDOW_SECS);
        buckets.push_back((0, Bucket::default()));
        Self {
            started: Instant::now(),
            buckets,
        }
    }

    fn second(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    fn roll(&mut self) {
        let now = self.second();
        while let Some((s, _)) = self.buckets.front() {
            if now.saturating_sub(*s) >= WINDOW_SECS as u64 {
                self.buckets.pop_front();
            } else {
                break;
            }
        }
        match self.buckets.back() {
            Some((s, _)) if *s == now => {}
            _ => self.buckets.push_back((now, Bucket::default())),
        }
    }

    fn add<F: FnOnce(&mut Bucket)>(&mut self, f: F) {
        self.roll();
        if let Some((_, b)) = self.buckets.back_mut() {
            f(b);
        }
    }

    /// Rate per second averaged over the last `seconds` of observed data.
    fn rate<F: Fn(&Bucket) -> u64>(&self, seconds: u64, pick: F) -> f64 {
        let now = self.second();
        let cutoff = now.saturating_sub(seconds);
        let (sum, span) = self
            .buckets
            .iter()
            .filter(|(s, _)| *s >= cutoff)
            .fold((0u64, 0u64), |(sum, span), (_, b)| (sum + pick(b), span + 1));
        if span == 0 {
            0.0
        } else {
            sum as f64 / span as f64
        }
    }

    fn series<F: Fn(&Bucket) -> u64>(&self, seconds: u64, pick: F) -> Vec<(u64, u64)> {
        let now = self.second();
        let cutoff = now.saturating_sub(seconds);
        self.buckets
            .iter()
            .filter(|(s, _)| *s >= cutoff)
            .map(|(s, b)| (*s, pick(b)))
            .collect()
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct RateSnapshot {
    /// Messages/sec, averaged over 1m / 5m.
    pub produce_mps_1m: f64,
    pub produce_mps_5m: f64,
    pub fetch_mps_1m: f64,
    pub fetch_mps_5m: f64,
    pub produce_bytes_1m: f64,
    pub fetch_bytes_1m: f64,
}

#[derive(Debug, Serialize, Clone)]
pub struct SeriesPoint {
    pub t_sec: u64,
    pub produced: u64,
    pub fetched: u64,
}

#[derive(Debug, Default)]
pub struct Metrics {
    window: Mutex<WindowStats>,
    total_produced: Mutex<u64>,
    total_fetched: Mutex<u64>,
    dedup_hits: Mutex<u64>,
}

/// Named separately because the rolling window contains an `Instant` and has
/// no meaningful debug representation.
struct WindowStats {
    inner: Window,
}

impl std::fmt::Debug for WindowStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowStats").finish_non_exhaustive()
    }
}

impl Default for WindowStats {
    fn default() -> Self {
        Self {
            inner: Window::new(),
        }
    }
}

impl std::ops::Deref for WindowStats {
    type Target = Window;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for WindowStats {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            window: Mutex::new(WindowStats::default()),
            total_produced: Mutex::new(0),
            total_fetched: Mutex::new(0),
            dedup_hits: Mutex::new(0),
        }
    }

    pub fn record_produce(&self, messages: u64, bytes: u64) {
        *self.total_produced.lock().unwrap() += messages;
        self.window.lock().unwrap().add(|b| {
            b.produced_messages += messages;
            b.produced_bytes += bytes;
        });
    }

    pub fn record_fetch(&self, messages: u64, bytes: u64) {
        *self.total_fetched.lock().unwrap() += messages;
        self.window.lock().unwrap().add(|b| {
            b.fetched_messages += messages;
            b.fetched_bytes += bytes;
        });
    }

    pub fn record_dedup_hit(&self) {
        *self.dedup_hits.lock().unwrap() += 1;
    }

    pub fn rates(&self) -> RateSnapshot {
        let w = self.window.lock().unwrap();
        RateSnapshot {
            produce_mps_1m: w.rate(60, |b| b.produced_messages),
            produce_mps_5m: w.rate(WINDOW_SECS as u64, |b| b.produced_messages),
            fetch_mps_1m: w.rate(60, |b| b.fetched_messages),
            fetch_mps_5m: w.rate(WINDOW_SECS as u64, |b| b.fetched_messages),
            produce_bytes_1m: w.rate(60, |b| b.produced_bytes),
            fetch_bytes_1m: w.rate(60, |b| b.fetched_bytes),
        }
    }

    pub fn series(&self, seconds: u64) -> Vec<SeriesPoint> {
        let w = self.window.lock().unwrap();
        let p = w.series(seconds, |b| b.produced_messages);
        let f = w.series(seconds, |b| b.fetched_messages);
        let mut map: std::collections::BTreeMap<u64, (u64, u64)> = std::collections::BTreeMap::new();
        for (t, v) in p {
            map.entry(t).or_default().0 = v;
        }
        for (t, v) in f {
            map.entry(t).or_default().1 = v;
        }
        map.into_iter()
            .map(|(t_sec, (produced, fetched))| SeriesPoint {
                t_sec,
                produced,
                fetched,
            })
            .collect()
    }

    pub fn totals(&self) -> (u64, u64, u64) {
        (
            *self.total_produced.lock().unwrap(),
            *self.total_fetched.lock().unwrap(),
            *self.dedup_hits.lock().unwrap(),
        )
    }
}

/// A minimal latency histogram (log-bucketed, microseconds) for fetch
/// long-poll response handling. End-to-end latency in the benchmark is measured
/// from message timestamps instead.
#[derive(Debug)]
pub struct LatencyStats {
    inner: Mutex<LatencyInner>,
}

#[derive(Debug, Default)]
struct LatencyInner {
    count: u64,
    sum_us: u128,
    min_us: Option<u64>,
    max_us: u64,
}

impl LatencyStats {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(LatencyInner::default()),
        }
    }

    pub fn observe(&self, d: Duration) {
        let us = d.as_micros() as u64;
        let mut g = self.inner.lock().unwrap();
        g.count += 1;
        g.sum_us += us as u128;
        g.min_us = Some(g.min_us.map_or(us, |m| m.min(us)));
        g.max_us = g.max_us.max(us);
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        let g = self.inner.lock().unwrap();
        LatencySnapshot {
            count: g.count,
            avg_us: if g.count == 0 {
                0.0
            } else {
                g.sum_us as f64 / g.count as f64
            },
            min_us: g.min_us.unwrap_or(0),
            max_us: g.max_us,
        }
    }
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct LatencySnapshot {
    pub count: u64,
    pub avg_us: f64,
    pub min_us: u64,
    pub max_us: u64,
}
