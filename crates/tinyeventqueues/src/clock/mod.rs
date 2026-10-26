//! Time for the pipeline: the system clock, or a [`ManualClock`] that tests
//! (and simulations) move by hand.
//!
//! Every time the pipeline reads — debounce windows, cadence ticks, token
//! buckets, tombstone expiry — comes from one [`Clock`], the same type the
//! storage ports take, so a test drives the queues and the pipeline from one
//! source of time.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A host- or test-supplied millisecond clock.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// The system clock: milliseconds since the Unix epoch.
#[must_use]
pub fn system_clock() -> Clock {
    Arc::new(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            })
    })
}

/// A clock that only moves when told to.
///
/// ```
/// use tinyeventqueues::ManualClock;
///
/// let clock = ManualClock::new(1_000);
/// let read = clock.clock();
/// clock.advance(500);
/// assert_eq!(read(), 1_500);
/// ```
#[derive(Debug, Clone, Default)]
pub struct ManualClock {
    now: Arc<AtomicU64>,
}

impl ManualClock {
    /// A clock reading `now_ms`.
    #[must_use]
    pub fn new(now_ms: u64) -> Self {
        Self {
            now: Arc::new(AtomicU64::new(now_ms)),
        }
    }

    /// The current reading.
    #[must_use]
    pub fn now(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }

    /// Move the clock forward by `ms`.
    pub fn advance(&self, ms: u64) {
        self.now.fetch_add(ms, Ordering::SeqCst);
    }

    /// Set the clock to `now_ms`.
    pub fn set(&self, now_ms: u64) {
        self.now.store(now_ms, Ordering::SeqCst);
    }

    /// A [`Clock`] reading this one, for the pipeline and the storage ports.
    #[must_use]
    pub fn clock(&self) -> Clock {
        let now = Arc::clone(&self.now);
        Arc::new(move || now.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
