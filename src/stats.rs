//! Server command counters.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum Stat {
    CmdGet,
    CmdSet,
    CmdFlush,
    CmdTouch,
    GetHits,
    GetMisses,
    DeleteMisses,
    DeleteHits,
    IncrMisses,
    IncrHits,
    DecrMisses,
    DecrHits,
    CasMisses,
    CasHits,
    CasBadval,
    TouchHits,
    TouchMisses,
    BytesRead,
    BytesWritten,
}

/// `BytesWritten` must stay the last variant.
pub const COUNT: usize = Stat::BytesWritten as usize + 1;

const NAMES: [&str; COUNT] = [
    "cmd_get",
    "cmd_set",
    "cmd_flush",
    "cmd_touch",
    "get_hits",
    "get_misses",
    "delete_misses",
    "delete_hits",
    "incr_misses",
    "incr_hits",
    "decr_misses",
    "decr_hits",
    "cas_misses",
    "cas_hits",
    "cas_badval",
    "touch_hits",
    "touch_misses",
    "bytes_read",
    "bytes_written",
];

/// Per-connection counters, merged into `Stats` in batches.
#[derive(Default)]
pub struct LocalStats {
    values: [u64; COUNT],
}

impl LocalStats {
    #[inline]
    pub fn bump(&mut self, stat: Stat) {
        self.values[stat as usize] += 1;
    }

    #[inline]
    pub fn add(&mut self, stat: Stat, amount: u64) {
        self.values[stat as usize] += amount;
    }
}

/// Process-wide counters.
pub struct Stats {
    totals: [AtomicU64; COUNT],
    pub curr_connections: AtomicU64,
    pub total_connections: AtomicU64,
    /// Connections closed at once because the server was at its connection limit.
    pub rejected_connections: AtomicU64,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            totals: std::array::from_fn(|_| AtomicU64::new(0)),
            curr_connections: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
        }
    }

    /// Adds the local counts to the totals and zeroes them.
    pub fn merge(&self, local: &mut LocalStats) {
        for (total, value) in self.totals.iter().zip(local.values.iter_mut()) {
            if *value != 0 {
                total.fetch_add(*value, Relaxed);
                *value = 0;
            }
        }
    }

    /// Zeroes every counter that `stats reset` clears. The number of open connections is kept.
    pub fn reset(&self) {
        for total in &self.totals {
            total.store(0, Relaxed);
        }
        self.total_connections.store(0, Relaxed);
        self.rejected_connections.store(0, Relaxed);
    }

    /// Every counter as (memcached stat name, value), in memcached's order.
    pub fn snapshot(&self) -> Vec<(&'static str, u64)> {
        NAMES
            .iter()
            .zip(self.totals.iter())
            .map(|(name, total)| (*name, total.load(Relaxed)))
            .collect()
    }
}

impl Default for Stats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_counts_merge_into_totals_and_reset() {
        let stats = Stats::new();
        let mut local = LocalStats::default();
        local.bump(Stat::CmdGet);
        local.bump(Stat::CmdGet);
        local.add(Stat::BytesRead, 100);
        stats.merge(&mut local);
        stats.merge(&mut local);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.len(), COUNT);
        assert!(snapshot.contains(&("cmd_get", 2)));
        assert!(snapshot.contains(&("bytes_read", 100)));
        assert!(snapshot.contains(&("get_hits", 0)));
    }

    #[test]
    fn reset_zeroes_totals_but_not_open_connections() {
        let stats = Stats::new();
        let mut local = LocalStats::default();
        local.bump(Stat::CmdGet);
        local.bump(Stat::BytesWritten);
        stats.merge(&mut local);
        stats.total_connections.store(3, Relaxed);
        stats.rejected_connections.store(4, Relaxed);
        stats.curr_connections.store(2, Relaxed);
        stats.reset();
        assert!(stats.snapshot().iter().all(|(_, value)| *value == 0));
        assert_eq!(stats.total_connections.load(Relaxed), 0);
        assert_eq!(stats.rejected_connections.load(Relaxed), 0);
        assert_eq!(stats.curr_connections.load(Relaxed), 2);
    }

    #[test]
    fn the_last_variant_has_the_last_slot() {
        let mut local = LocalStats::default();
        local.bump(Stat::BytesWritten);
        assert_eq!(local.values[COUNT - 1], 1);
        assert_eq!(NAMES[Stat::BytesWritten as usize], "bytes_written");
    }
}
