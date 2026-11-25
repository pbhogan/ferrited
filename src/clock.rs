//! Version generation and wall-clock helpers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const NODE_BITS: u32 = 8;
const COUNTER_BITS: u32 = 14;

pub fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

pub fn unix_secs() -> u64 {
    unix_millis() / 1000
}

/// Hybrid logical clock producing unique, strictly increasing versions: 42 bits of milliseconds,
/// 14 of counter, 8 of node ID. Counter overflow carries into the milliseconds, so the embedded
/// timestamp can run slightly ahead of the wall clock.
pub struct Clock {
    node_id: u8,
    state: AtomicU64,
    wall: fn() -> u64,
}

impl Clock {
    pub fn new(node_id: u8) -> Self {
        Self::with_wall(node_id, unix_millis)
    }

    /// Uses a custom millisecond source, for tests.
    pub fn with_wall(node_id: u8, wall: fn() -> u64) -> Self {
        Self {
            node_id,
            state: AtomicU64::new(0),
            wall,
        }
    }

    pub fn next(&self) -> u64 {
        let wall = (self.wall)() << COUNTER_BITS;
        let mut previous = self.state.load(Ordering::Relaxed);
        loop {
            let candidate = wall.max(previous + 1);
            match self.state.compare_exchange_weak(
                previous,
                candidate,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return (candidate << NODE_BITS) | self.node_id as u64,
                Err(actual) => previous = actual,
            }
        }
    }

    /// Makes every later version greater than `version`, which may come from another node or
    /// from before a restart.
    pub fn observe(&self, version: u64) {
        self.state
            .fetch_max(version >> NODE_BITS, Ordering::Relaxed);
    }

    pub fn millis_of(version: u64) -> u64 {
        version >> (NODE_BITS + COUNTER_BITS)
    }

    pub fn node_of(version: u64) -> u8 {
        version as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn versions_increase_and_carry_node_and_time() {
        let clock = Clock::with_wall(3, || 1_000);
        let first = clock.next();
        let second = clock.next();
        assert!(second > first);
        assert_eq!(Clock::node_of(first), 3);
        assert_eq!(Clock::millis_of(first), 1_000);
    }

    #[test]
    fn versions_increase_when_wall_clock_goes_backwards() {
        static WALL: AtomicU64 = AtomicU64::new(5_000);
        let clock = Clock::with_wall(1, || WALL.load(Ordering::Relaxed));
        let before = clock.next();
        WALL.store(4_000, Ordering::Relaxed);
        let after = clock.next();
        assert!(after > before);
    }

    #[test]
    fn observe_moves_later_versions_past_the_observed_one() {
        let clock = Clock::with_wall(1, || 1_000);
        let recovered = Clock::with_wall(2, || 50_000).next();
        clock.observe(recovered);
        assert!(clock.next() > recovered);
        // Observing an older version changes nothing.
        let before = clock.next();
        clock.observe(5);
        assert!(clock.next() > before);
    }

    #[test]
    fn versions_are_unique_across_threads() {
        let clock = Arc::new(Clock::new(1));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let clock = clock.clone();
                std::thread::spawn(move || (0..10_000).map(|_| clock.next()).collect::<Vec<_>>())
            })
            .collect();
        let mut seen = HashSet::new();
        for handle in handles {
            for version in handle.join().unwrap() {
                assert!(seen.insert(version));
            }
        }
        assert_eq!(seen.len(), 40_000);
    }
}
