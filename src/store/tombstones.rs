use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use ahash::RandomState;

use super::types::cost;

/// One shard's tombstones: the version of each deleted key, also ordered by version so the
/// oldest can be found without a scan, and the memory they are charged. Each key is allocated
/// once and shared by both indexes.
pub(super) struct Tombstones {
    versions: HashMap<Arc<[u8]>, u64, RandomState>,
    by_version: BTreeSet<(u64, Arc<[u8]>)>,
    bytes: usize,
}

impl Tombstones {
    pub fn new() -> Self {
        Self {
            versions: HashMap::with_hasher(RandomState::new()),
            by_version: BTreeSet::new(),
            bytes: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.versions.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// Memory charged for these tombstones: `cost(key, 0)` each.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The version of the tombstone for `key`, expired or not.
    pub fn get(&self, key: &[u8]) -> Option<u64> {
        self.versions.get(key).copied()
    }

    pub fn oldest(&self) -> Option<u64> {
        self.by_version.first().map(|(version, _)| *version)
    }

    /// Records that `key` was deleted at `version`, keeping the higher version if it already
    /// has a tombstone. Returns the bytes added to `bytes`.
    pub fn insert(&mut self, key: &[u8], version: u64) -> usize {
        if let Some((key, existing)) = self.versions.get_key_value(key) {
            let existing = *existing;
            if version > existing {
                let key = key.clone();
                self.by_version.remove(&(existing, key.clone()));
                self.by_version.insert((version, key.clone()));
                self.versions.insert(key, version);
                self.check();
            }
            return 0;
        }
        let key: Arc<[u8]> = key.into();
        let added = cost(key.len(), 0);
        self.by_version.insert((version, key.clone()));
        self.versions.insert(key, version);
        self.bytes += added;
        self.check();
        added
    }

    /// Removes the tombstone for `key`. Returns the bytes freed.
    pub fn remove(&mut self, key: &[u8]) -> usize {
        let Some((key, version)) = self.versions.remove_entry(key) else {
            return 0;
        };
        let freed = self.forget(&key);
        self.by_version.remove(&(version, key));
        self.check();
        freed
    }

    /// Removes the oldest tombstone. Returns its version and the bytes freed.
    pub fn pop_oldest(&mut self) -> Option<(u64, usize)> {
        let (version, key) = self.by_version.pop_first()?;
        self.versions.remove(&key);
        let freed = self.forget(&key);
        self.check();
        Some((version, freed))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], u64)> {
        self.versions
            .iter()
            .map(|(key, version)| (&key[..], *version))
    }

    /// Debug check that both indexes agree.
    #[inline]
    fn check(&self) {
        debug_assert_eq!(self.versions.len(), self.by_version.len());
    }

    fn forget(&mut self, key: &[u8]) -> usize {
        let freed = cost(key.len(), 0);
        self.bytes -= freed;
        freed
    }
}
