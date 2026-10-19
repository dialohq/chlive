//! Time for `now()`: the server's clock, and waiting for the next instant a row goes out or
//! comes in (see [`crate::timeline`]), plus the server's coalescing window, so rows due close
//! together change together.

use std::time::{SystemTime, UNIX_EPOCH};

/// Nanoseconds since the Unix epoch, now.
pub trait Clock: Send + Sync {
    fn now_ns(&self) -> i128;
}

/// The system's clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as i128)
    }
}

/// For tests: `base` plus tokio's time since the clock was made, so a paused tokio runtime
/// (`start_paused`) moves it exactly as far as its timers.
pub struct TokioClock {
    base: i128,
    start: tokio::time::Instant,
}

impl TokioClock {
    /// Now is `base` nanoseconds since the epoch.
    pub fn at(base: i128) -> Self {
        Self {
            base,
            start: tokio::time::Instant::now(),
        }
    }
}

impl Clock for TokioClock {
    fn now_ns(&self) -> i128 {
        self.base + self.start.elapsed().as_nanos() as i128
    }
}

/// Sleep until `earliest + coalescing` on `clock` (now, if that's past); forever without one.
pub async fn wait_for(clock: &dyn Clock, earliest: Option<i128>, coalescing: i128) {
    match earliest {
        Some(d) => {
            let ns = (d + coalescing - clock.now_ns()).max(0);
            tokio::time::sleep(std::time::Duration::from_nanos(
                u64::try_from(ns).unwrap_or(u64::MAX),
            ))
            .await
        }
        None => std::future::pending().await,
    }
}
