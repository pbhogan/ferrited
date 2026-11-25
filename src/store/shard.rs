use ahash::RandomState;
use indexmap::IndexMap;

use super::tombstones::Tombstones;
use super::types::Item;

/// One independently locked slice of the key space.
pub(super) struct Shard {
    pub map: IndexMap<Box<[u8]>, Item, RandomState>,
    /// Versions of deletes, expiries and evictions, kept only when peers are configured, so a
    /// peer's older write that arrives late cannot bring the key back. A key is never in both
    /// `map` and here.
    pub tombstones: Tombstones,
    /// The flush floor this shard was last purged against.
    pub swept_floor: u64,
    /// Memory charged for this shard's items and tombstones.
    pub used: usize,
    /// Part of `used` not yet added to the store's total.
    pub unreported: isize,
    /// Items stored in this shard since the last stats reset.
    pub total_items: u64,
    rng: u64,
}

impl Shard {
    pub fn new(seed: u64) -> Self {
        Self {
            map: IndexMap::with_hasher(RandomState::new()),
            tombstones: Tombstones::new(),
            swept_floor: 0,
            used: 0,
            unreported: 0,
            total_items: 0,
            rng: seed | 1,
        }
    }

    /// A uniformly random entry index.
    pub fn random_index(&mut self) -> Option<usize> {
        if self.map.is_empty() {
            return None;
        }
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        Some((self.rng % self.map.len() as u64) as usize)
    }
}
