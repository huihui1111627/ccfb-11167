//! Process-wide graceful shutdown flag toggled by SIGTERM/SIGINT.
//!
//! SIGKILL cannot be caught — that is exactly the signal the orchestrator uses
//! to simulate a crash, so this flag only handles the graceful stop path.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::signal;

#[derive(Clone)]
pub struct StopFlag {
    stop: Arc<AtomicBool>,
}

impl StopFlag {
    pub fn install() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let inner = stop.clone();
        tokio::spawn(async move {
            // Either ctrl-c (SIGINT) or a plain SIGTERM flips the flag.
            tokio::select! {
                _ = signal::ctrl_c() => {}
                _ = wait_sigterm() => {}
            }
            inner.store(true, Ordering::SeqCst);
        });
        Self { stop }
    }

    pub fn is_stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }
}

#[cfg(unix)]
async fn wait_sigterm() {
    use signal::unix::{signal, SignalKind};
    let mut sig = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => std::future::pending().await,
    };
    sig.recv().await;
}

#[cfg(not(unix))]
async fn wait_sigterm() {
    std::future::pending::<()>().await;
}
