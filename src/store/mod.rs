//! Sharded in-memory item store.

mod lock;
mod shard;
mod tombstones;
mod types;

#[cfg(test)]
mod tests;

use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicU64, AtomicUsize, fence};
use std::sync::{Arc, OnceLock};

use ahash::RandomState;
use bytes::{Bytes, BytesMut};
use lock::ShardLock;

use crate::clock::Clock;
use crate::oplog::{Oplog, Record};
use shard::Shard;
pub use types::*;

pub struct Store {
    shards: Box<[ShardLock<Shard>]>,
    mask: usize,
    hasher: RandomState,
    clock: Arc<Clock>,
    memory_limit: usize,
    item_max: usize,
    /// Memory charged for all items, to within `slack` per shard. Shards report here only once
    /// their unreported usage reaches `slack`, so most mutations touch no shared counter.
    used: AtomicUsize,
    slack: usize,
    evictions: AtomicU64,
    reclaimed: AtomicU64,
    /// Items with a version below this are treated as absent.
    flush_floor: AtomicU64,
    /// Unix second at which a delayed flush takes effect; 0 means none pending.
    pending_flush: AtomicU64,
    /// Flushes in progress in the low 32 bits, finished flushes above. A mutation's version and
    /// floor are consistent only if this was unchanged, with nothing in progress, across both.
    flush_epoch: AtomicU64,
    /// Times `stamp` started over, so tests can see a mutation wait on a flush.
    #[cfg(test)]
    stamp_retries: AtomicU64,
    /// Pause points where a test forces an interleaving of a flush and a mutation.
    #[cfg(test)]
    hooks: parking_lot::Mutex<Hooks>,
    evict_cursor: AtomicUsize,
    /// Seconds a tombstone lives after the time in its version; 0 means tombstones are off.
    tombstone_ttl: AtomicU64,
    /// Memory each shard's tombstones may be charged; past it the oldest are evicted.
    tombstone_limit: AtomicUsize,
    /// Live tombstones evicted to stay within `tombstone_limit`.
    tombstones_evicted: AtomicU64,
    /// Unix second of the last sweep of tombstones.
    tombstones_swept: AtomicU64,
    /// Where change records go once persistence or replication is attached.
    oplog: OnceLock<Arc<Oplog>>,
}

/// Code a test runs once at a pause point.
#[cfg(test)]
type Hook = Box<dyn FnOnce() + Send>;

/// Test pause points. Each hook runs once, on the thread that reaches it.
#[cfg(test)]
#[derive(Default)]
struct Hooks {
    /// Where a flush has taken or observed its version and not yet raised the floor.
    flush: Option<Hook>,
    /// Where a lookup has found or missed its item and the caller has not yet acted on it.
    find: Option<Hook>,
}

/// The part of `Store::flush_epoch` that counts flushes in progress.
const FLUSHES_IN_PROGRESS: u64 = (1 << 32) - 1;
/// Added to `Store::flush_epoch` when a flush finishes: one more finished, one fewer in progress.
const FLUSH_FINISHED: u64 = (1 << 32) - 1;

fn dead(item: &Item, now: u64, floor: u64) -> bool {
    item.version < floor || (item.expiry != 0 && item.expiry <= now)
}

/// Least time between two sweeps of tombstones. Each sweep visits every shard, and a
/// tombstone outliving its lifetime by this much is harmless.
const TOMBSTONE_SWEEP_SECS: u64 = 10;

/// Parses a stored value as memcached does, via C's `strtoull`: optional leading whitespace and
/// sign, then digits ending at the end, whitespace or a NUL, after which anything is ignored.
fn parse_number(bytes: &[u8]) -> Option<u64> {
    let is_space = |byte: &u8| matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r');
    let start = bytes.iter().position(|byte| !is_space(byte))?;
    let (negative, digits) = match bytes[start] {
        b'-' => (true, &bytes[start + 1..]),
        b'+' => (false, &bytes[start + 1..]),
        _ => (false, &bytes[start..]),
    };
    let count = digits
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(digits.len());
    if count == 0 {
        return None;
    }
    if digits
        .get(count)
        .is_some_and(|after| !is_space(after) && *after != 0)
    {
        return None;
    }
    let mut magnitude = 0u64;
    for digit in &digits[..count] {
        magnitude = magnitude
            .checked_mul(10)?
            .checked_add(u64::from(digit - b'0'))?;
    }
    if !negative {
        return Some(magnitude);
    }
    // `strtoull` negates a signed number modulo 2^64. memcached then rejects it only if the
    // result would be negative as a signed 64-bit number.
    let wrapped = magnitude.wrapping_neg();
    ((wrapped as i64) >= 0).then_some(wrapped)
}

impl Store {
    pub fn new(config: StoreConfig, clock: Arc<Clock>) -> Self {
        let count = config.shards.max(1).next_power_of_two();
        let shards = (0..count)
            .map(|index| {
                ShardLock::new(Shard::new(
                    0x9E37_79B9_7F4A_7C15_u64.wrapping_mul(index as u64 + 1),
                ))
            })
            .collect();
        Self {
            shards,
            mask: count - 1,
            hasher: RandomState::new(),
            clock,
            memory_limit: config.memory_limit,
            item_max: config.item_max,
            used: AtomicUsize::new(0),
            slack: (config.memory_limit / (count * 64)).min(16 * 1024),
            evictions: AtomicU64::new(0),
            reclaimed: AtomicU64::new(0),
            flush_floor: AtomicU64::new(0),
            pending_flush: AtomicU64::new(0),
            flush_epoch: AtomicU64::new(0),
            #[cfg(test)]
            stamp_retries: AtomicU64::new(0),
            #[cfg(test)]
            hooks: parking_lot::Mutex::new(Hooks::default()),
            evict_cursor: AtomicUsize::new(0),
            tombstone_ttl: AtomicU64::new(0),
            tombstone_limit: AtomicUsize::new(0),
            tombstones_evicted: AtomicU64::new(0),
            tombstones_swept: AtomicU64::new(0),
            oplog: OnceLock::new(),
        }
    }

    pub fn get(&self, key: &[u8], now: u64, args: &GetArgs) -> Option<GetHit> {
        let mut shard = self.shard_for(key).lock();
        // A touch stamps a version and floor together, as in `set`; a read needs only a floor.
        let (touch_version, floor) = match args.touch {
            Some(_) => {
                let (version, floor) = self.stamp(now);
                (Some(version), floor)
            }
            None => (None, self.floor(now)),
        };
        let Some(index) = self.find(&mut shard, key, now, floor) else {
            let expiry = args.vivify?;
            let (version, floor) = self.stamp(now);
            self.find(&mut shard, key, now, floor);
            let cas = args.cas_override.unwrap_or(version);
            let item = Item {
                value: Bytes::new(),
                flags: 0,
                expiry,
                version,
                cas,
                last_access: now,
                fetched: false,
                stale: false,
                token_sent: true,
            };
            self.log_put(key, &item);
            self.insert(&mut shard, key, item, now, floor);
            drop(shard);
            self.evict_global(key, now, floor);
            return Some(GetHit {
                value: Bytes::new(),
                flags: 0,
                cas,
                expiry,
                last_access: now,
                fetched: false,
                stale: false,
                win: true,
                lost: false,
            });
        };
        let item = &mut shard.map[index];
        let ttl_low = args
            .recache
            .is_some_and(|below| item.expiry != 0 && item.expiry - now < below);
        let win = !item.token_sent && (item.stale || ttl_low);
        let hit = GetHit {
            value: item.value.clone(),
            flags: item.flags,
            cas: item.cas,
            expiry: args.touch.unwrap_or(item.expiry),
            last_access: item.last_access,
            fetched: item.fetched,
            stale: item.stale,
            win,
            lost: item.token_sent,
        };
        if win {
            item.token_sent = true;
        }
        if let (Some(expiry), Some(version)) = (args.touch, touch_version) {
            item.expiry = expiry;
            item.version = version;
            self.log_put(key, item);
        }
        if !args.no_bump {
            item.last_access = now;
            item.fetched = true;
        }
        Some(hit)
    }

    pub fn set(&self, key: &[u8], value: Bytes, now: u64, args: &SetArgs) -> SetOutcome {
        if value.len() > self.item_max || cost(key.len(), value.len()) > self.memory_limit {
            return SetOutcome::TooLarge;
        }
        let mut shard = self.shard_for(key).lock();
        // Stamped under the shard lock: every flush either finished before the floor was read
        // (so the old item is dead here) or took its version after this one (so it flushes the
        // new item). Pre-flush content is never carried forward past a finished flush.
        let (version, floor) = self.stamp(now);
        let existing = self.find(&mut shard, key, now, floor);
        let mut stale = false;
        // As in memcached: add ignores the CAS token; append and prepend check it only against
        // an existing item, never accepting an older token. A missing key is "not found" only
        // for set and replace.
        if let Some(expected) = args.cas
            && args.mode != SetMode::Add
        {
            let replaces = matches!(args.mode, SetMode::Set | SetMode::Replace);
            match existing {
                None if replaces => return SetOutcome::NotFound,
                None => {}
                Some(index) => {
                    let current = shard.map[index].cas;
                    if current != expected {
                        if replaces && args.invalidate && expected < current {
                            stale = true;
                        } else {
                            return SetOutcome::Exists;
                        }
                    }
                }
            }
        }
        let (value, flags, expiry) = match (args.mode, existing) {
            (SetMode::Set, _) | (SetMode::Add, None) | (SetMode::Replace, Some(_)) => {
                (value, args.flags, args.expiry)
            }
            (SetMode::Add, Some(_)) | (SetMode::Replace, None) => return SetOutcome::NotStored,
            (SetMode::Append | SetMode::Prepend, None) => match args.vivify {
                Some(expiry) => (value, args.flags, expiry),
                None => return SetOutcome::NotStored,
            },
            (mode, Some(index)) => {
                let old = &shard.map[index];
                let joined_len = old.value.len() + value.len();
                if joined_len > self.item_max || cost(key.len(), joined_len) > self.memory_limit {
                    return SetOutcome::TooLarge;
                }
                let mut joined = BytesMut::with_capacity(old.value.len() + value.len());
                if mode == SetMode::Append {
                    joined.extend_from_slice(&old.value);
                    joined.extend_from_slice(&value);
                } else {
                    joined.extend_from_slice(&value);
                    joined.extend_from_slice(&old.value);
                }
                (joined.freeze(), old.flags, old.expiry)
            }
        };
        let cas = args.cas_override.unwrap_or(version);
        let size = value.len();
        let item = Item {
            value,
            flags,
            expiry,
            version,
            cas,
            last_access: now,
            fetched: false,
            stale,
            token_sent: false,
        };
        self.log_put(key, &item);
        self.insert(&mut shard, key, item, now, floor);
        drop(shard);
        self.evict_global(key, now, floor);
        SetOutcome::Stored { cas, size }
    }

    pub fn delete(&self, key: &[u8], now: u64, args: &DeleteArgs) -> DeleteOutcome {
        let mut shard = self.shard_for(key).lock();
        // Stamped as in `set`.
        let (version, floor) = self.stamp(now);
        let Some(index) = self.find(&mut shard, key, now, floor) else {
            // With peers, a delete that finds nothing may be racing a write still in flight from
            // another node; a tombstone and a logged delete stop it wherever it arrives.
            let plain = args.cas.is_none() && !args.invalidate && !args.keep_empty;
            if plain && self.tombstone_ttl.load(Relaxed) > 0 {
                self.set_tombstone(&mut shard, key, version, now);
                self.log_delete(key, version);
            }
            return DeleteOutcome::NotFound;
        };
        if args
            .cas
            .is_some_and(|expected| shard.map[index].cas != expected)
        {
            return DeleteOutcome::Exists;
        }
        if args.keep_empty {
            // The empty item is a new one: it keeps nothing of the old item but its key.
            let item = Item {
                value: Bytes::new(),
                flags: args.flags,
                expiry: args.expiry.unwrap_or(0),
                version,
                cas: args.cas_override.unwrap_or(version),
                last_access: now,
                fetched: false,
                stale: args.invalidate,
                token_sent: false,
            };
            self.log_put(key, &item);
            self.insert(&mut shard, key, item, now, floor);
        } else if args.invalidate {
            let item = &mut shard.map[index];
            item.stale = true;
            item.token_sent = false;
            item.version = version;
            item.cas = args.cas_override.unwrap_or(version);
            if let Some(expiry) = args.expiry {
                item.expiry = expiry;
            }
            self.log_put(key, item);
        } else {
            self.remove_at(&mut shard, index);
            self.set_tombstone(&mut shard, key, version, now);
            self.log_delete(key, version);
        }
        DeleteOutcome::Deleted
    }

    /// Sets a new expiry. Returns whether the item exists.
    pub fn touch(&self, key: &[u8], now: u64, expiry: u64) -> bool {
        let mut shard = self.shard_for(key).lock();
        // Stamped as in `set`.
        let (version, floor) = self.stamp(now);
        let Some(index) = self.find(&mut shard, key, now, floor) else {
            return false;
        };
        let item = &mut shard.map[index];
        item.expiry = expiry;
        item.version = version;
        self.log_put(key, item);
        true
    }

    /// Starts sending a change record for every mutation to `oplog`. Call once, before serving.
    pub fn attach_oplog(&self, oplog: Arc<Oplog>) {
        assert!(
            self.oplog.set(oplog).is_ok(),
            "an oplog is already attached"
        );
    }

    /// Keeps a tombstone for each delete until `ttl` seconds after its version's time, so a
    /// peer's late older write cannot bring the key back. Each shard gets an equal share of
    /// `memory` and evicts its oldest tombstones past it, keeping the newest. Call before
    /// recovery, and only when peers are configured.
    pub fn enable_tombstones(&self, ttl: u64, memory: usize) {
        assert!(ttl > 0, "a tombstone must live at least a second");
        self.tombstone_limit
            .store(memory / self.shards.len(), Relaxed);
        self.tombstone_ttl.store(ttl, Relaxed);
    }

    /// Tombstones held, including expired ones not yet swept.
    pub fn tombstones(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.lock().tombstones.len() as u64)
            .sum()
    }

    /// Memory charged for tombstones, including expired ones not yet swept.
    pub fn tombstone_bytes(&self) -> u64 {
        self.shards
            .iter()
            .map(|shard| shard.lock().tombstones.bytes() as u64)
            .sum()
    }

    /// Live tombstones evicted to keep tombstone memory within its limit.
    pub fn tombstones_evicted(&self) -> u64 {
        self.tombstones_evicted.load(Relaxed)
    }

    /// Applies a recovered record without logging it. A record takes effect only if newer than
    /// the key's item and tombstone, so replay is idempotent and can correct a fuzzy snapshot.
    /// A delete, expired put or oversized put removes older state, leaving a tombstone if on.
    pub fn apply(&self, record: &Record, now: u64) {
        self.merge(record, now, false);
    }

    /// Applies a peer's record if newer than local state, logging it under the shard lock. A
    /// flush goes through the flush epoch like a local `flush_all`, so concurrent mutations
    /// cannot carry flushed content past it.
    pub fn apply_remote(&self, record: &Record, now: u64) -> Applied {
        self.merge(record, now, true)
    }

    fn merge(&self, record: &Record, now: u64, log: bool) -> Applied {
        let (key, version) = match record {
            // A remote flush moves the clock itself, inside the flush epoch.
            Record::Flush { version } if log => return self.flush_remote(*version),
            Record::Flush { version } => {
                // Recovery: nothing else changes the store, so the epoch is not needed.
                self.observe_flush(*version);
                self.flush_floor.fetch_max(*version, Relaxed);
                return Applied::Applied;
            }
            Record::Put { key, version, .. } | Record::Delete { key, version } => {
                self.clock.observe(*version);
                (key, *version)
            }
        };
        let mut shard = self.shard_for(key).lock();
        let floor = self.floor(now);
        let existing = shard.map.get_index_of(&key[..]);
        let newest = existing
            .map(|index| shard.map[index].version)
            .max(self.tombstone(&shard, key, now));
        // Below the floor, the record is older than a flush that has already removed its key.
        if newest.is_some_and(|newest| newest >= version) || version < floor {
            return Applied::Stale;
        }
        match record {
            Record::Put {
                cas,
                flags,
                expiry,
                stale,
                value,
                ..
            } if *expiry == 0 || *expiry > now => {
                if cost(key.len(), value.len()) > self.memory_limit {
                    // Too large to store, but newer than what is here: the older item must not
                    // outlive it, so the put takes effect as a delete at its version.
                    if let Some(index) = existing {
                        self.remove_at(&mut shard, index);
                    }
                    self.set_tombstone(&mut shard, key, version, now);
                    if log {
                        self.log_delete(key, version);
                    }
                    return Applied::Oversized;
                }
                let item = Item {
                    value: value.clone(),
                    flags: *flags,
                    expiry: *expiry,
                    version,
                    cas: *cas,
                    last_access: now,
                    fetched: false,
                    stale: *stale,
                    token_sent: false,
                };
                if log {
                    self.log(record.clone());
                }
                self.insert(&mut shard, key, item, now, floor);
                drop(shard);
                self.evict_global(key, now, floor);
            }
            _ => {
                if let Some(index) = existing {
                    self.remove_at(&mut shard, index);
                }
                self.set_tombstone(&mut shard, key, version, now);
                if log {
                    self.log(record.clone());
                }
            }
        }
        Applied::Applied
    }

    /// Raises the floor to a peer's flush version and logs it, using a local flush's epoch
    /// steps. The caller must not have moved the clock past `version` yet.
    fn flush_remote(&self, version: u64) -> Applied {
        if version <= self.flush_floor.load(Relaxed) {
            return Applied::Stale;
        }
        // Same order as `begin_flush`: mark the flush in progress, then move the clock, so a
        // mutation stamped above `version` restarts and reads the new floor. Moving the clock
        // first would let an append, incr or CAS carry pre-flush content past the flush.
        self.flush_epoch.fetch_add(1, AcqRel);
        fence(Release);
        self.observe_flush(version);
        self.finish_flush(version);
        Applied::Applied
    }

    /// Moves the clock past a flush version from a peer or the log.
    fn observe_flush(&self, version: u64) {
        self.clock.observe(version);
        #[cfg(test)]
        self.run_hook(|hooks| &mut hooks.flush);
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// One shard's live items as puts, then its live tombstones as deletes. With tombstones on,
    /// an expired but unremoved item becomes a delete at its version. Locks only while copying.
    pub fn shard_records(&self, shard: usize, now: u64) -> Vec<Record> {
        let floor = self.floor(now);
        let tombstones_on = self.tombstone_ttl.load(Relaxed) > 0;
        let shard = self.shards[shard].lock();
        let items = shard.map.iter().filter_map(|(key, item)| {
            let key = || Bytes::copy_from_slice(key);
            if !dead(item, now, floor) {
                return Some(Record::Put {
                    key: key(),
                    version: item.version,
                    cas: item.cas,
                    flags: item.flags,
                    expiry: item.expiry,
                    stale: item.stale,
                    value: item.value.clone(),
                });
            }
            (tombstones_on && item.version >= floor && !self.tombstone_expired(item.version, now))
                .then(|| Record::Delete {
                    key: key(),
                    version: item.version,
                })
        });
        let tombstones = shard
            .tombstones
            .iter()
            .filter(|&(_, version)| version >= floor && !self.tombstone_expired(version, now))
            .map(|(key, version)| Record::Delete {
                key: Bytes::copy_from_slice(key),
                version,
            });
        items.chain(tombstones).collect()
    }

    /// Items with a version below this are flushed.
    pub fn flush_floor(&self) -> u64 {
        self.flush_floor.load(Relaxed)
    }

    /// Logs the resulting state of `item`. Call with the item's shard lock held.
    #[inline]
    fn log_put(&self, key: &[u8], item: &Item) {
        if let Some(oplog) = self.oplog.get() {
            oplog.append(Record::Put {
                key: Bytes::copy_from_slice(key),
                version: item.version,
                cas: item.cas,
                flags: item.flags,
                expiry: item.expiry,
                stale: item.stale,
                value: item.value.clone(),
            });
        }
    }

    /// Whether a tombstone with this version has outlived its lifetime at `now`.
    fn tombstone_expired(&self, version: u64, now: u64) -> bool {
        (Clock::millis_of(version) / 1000).saturating_add(self.tombstone_ttl.load(Relaxed)) <= now
    }

    /// The version of the live tombstone for `key`, if any. Call with the key's shard lock held.
    fn tombstone(&self, shard: &Shard, key: &[u8], now: u64) -> Option<u64> {
        let version = shard.tombstones.get(key)?;
        (!self.tombstone_expired(version, now)).then_some(version)
    }

    /// Records a tombstone if they are on and it has not expired, then evicts the shard's
    /// oldest (possibly this one) down to its memory share, keeping at least one. Call with the
    /// key's shard lock held, after removing its item.
    #[inline]
    fn set_tombstone(&self, shard: &mut Shard, key: &[u8], version: u64, now: u64) {
        if self.tombstone_ttl.load(Relaxed) != 0 {
            self.record_tombstone(shard, key, version, now);
        }
    }

    /// `set_tombstone` with tombstones on.
    fn record_tombstone(&self, shard: &mut Shard, key: &[u8], version: u64, now: u64) {
        if self.tombstone_expired(version, now) {
            return;
        }
        let mut delta = shard.tombstones.insert(key, version) as isize;
        let limit = self.tombstone_limit.load(Relaxed);
        if shard.tombstones.bytes() > limit {
            let floor = self.flush_floor.load(Relaxed);
            let mut evicted = 0;
            while shard.tombstones.bytes() > limit && shard.tombstones.len() > 1 {
                let (oldest, freed) = shard.tombstones.pop_oldest().expect("more than one");
                delta -= freed as isize;
                // One expired or below the floor is only removed early, not evicted.
                if oldest >= floor && !self.tombstone_expired(oldest, now) {
                    evicted += 1;
                }
            }
            self.tombstones_evicted.fetch_add(evicted, Relaxed);
        }
        self.charge(shard, delta);
    }

    /// Removes the tombstone for `key`, if any. Call with the key's shard lock held. Without
    /// tombstones this is one length check.
    #[inline]
    fn clear_tombstone(&self, shard: &mut Shard, key: &[u8]) {
        if !shard.tombstones.is_empty() {
            self.remove_tombstone(shard, key);
        }
    }

    /// `clear_tombstone` on a shard holding tombstones.
    #[inline(never)]
    fn remove_tombstone(&self, shard: &mut Shard, key: &[u8]) {
        let freed = shard.tombstones.remove(key);
        self.charge(shard, -(freed as isize));
    }

    /// Logs a delete. Call with the key's shard lock held.
    #[inline]
    fn log_delete(&self, key: &[u8], version: u64) {
        if let Some(oplog) = self.oplog.get() {
            oplog.append(Record::Delete {
                key: Bytes::copy_from_slice(key),
                version,
            });
        }
    }

    /// Logs a record. Call with the record's shard lock held, if it has a key.
    #[inline]
    fn log(&self, record: Record) {
        if let Some(oplog) = self.oplog.get() {
            oplog.append(record);
        }
    }

    fn log_flush(&self, version: u64) {
        self.log(Record::Flush { version });
    }

    /// Item metadata for debugging, without changing access state.
    pub fn peek(&self, key: &[u8], now: u64) -> Option<ItemMeta> {
        let floor = self.floor(now);
        let mut shard = self.shard_for(key).lock();
        let index = self.find(&mut shard, key, now, floor)?;
        let item = &shard.map[index];
        Some(ItemMeta {
            expiry: item.expiry,
            last_access: item.last_access,
            cas: item.cas,
            fetched: item.fetched,
            size: item.value.len(),
        })
    }

    pub fn arith(&self, key: &[u8], now: u64, args: &ArithArgs) -> ArithOutcome {
        let mut shard = self.shard_for(key).lock();
        // Stamped as in `set`.
        let (version, floor) = self.stamp(now);
        let Some(index) = self.find(&mut shard, key, now, floor) else {
            let Some((expiry, initial)) = args.vivify else {
                return ArithOutcome::NotFound;
            };
            let cas = args.cas_override.unwrap_or(version);
            let item = Item {
                value: Bytes::from(initial.to_string()),
                flags: 0,
                expiry,
                version,
                cas,
                last_access: now,
                fetched: false,
                stale: false,
                token_sent: false,
            };
            self.log_put(key, &item);
            self.insert(&mut shard, key, item, now, floor);
            drop(shard);
            self.evict_global(key, now, floor);
            return ArithOutcome::Value {
                value: initial,
                cas,
                expiry,
            };
        };
        let old = &shard.map[index];
        if args.cas.is_some_and(|expected| old.cas != expected) {
            return ArithOutcome::Exists;
        }
        let Some(current) = parse_number(&old.value) else {
            return ArithOutcome::NonNumeric;
        };
        let value = if args.decr {
            current.saturating_sub(args.delta)
        } else {
            current.wrapping_add(args.delta)
        };
        let cas = args.cas_override.unwrap_or(version);
        let expiry = args.touch.unwrap_or(old.expiry);
        let item = Item {
            value: Bytes::from(value.to_string()),
            flags: old.flags,
            expiry,
            version,
            cas,
            last_access: now,
            fetched: old.fetched,
            stale: false,
            token_sent: false,
        };
        self.log_put(key, &item);
        self.insert(&mut shard, key, item, now, floor);
        drop(shard);
        self.evict_global(key, now, floor);
        ArithOutcome::Value { value, cas, expiry }
    }

    /// Invalidates every item now, or `delay` seconds from now.
    pub fn flush_all(&self, now: u64, delay: u64) {
        if delay == 0 {
            self.pending_flush.store(0, Relaxed);
            self.flush_now();
        } else {
            self.pending_flush.store(now.saturating_add(delay), Relaxed);
        }
    }

    /// Removes dead items: all below a new flush floor, and a random sample of expired ones.
    pub fn sweep(&self, now: u64) {
        const ROUNDS: usize = 16;
        const SAMPLES: usize = 20;
        let floor = self.floor(now);
        let swept = self.tombstones_swept.load(Relaxed);
        let sweep_tombstones = self.tombstone_ttl.load(Relaxed) > 0
            && now >= swept + TOMBSTONE_SWEEP_SECS
            && self
                .tombstones_swept
                .compare_exchange(swept, now, Relaxed, Relaxed)
                .is_ok();
        for shard in self.shards.iter() {
            let mut shard = shard.lock();
            if sweep_tombstones {
                // Expiry and the floor both come to the oldest versions first, so this stops at
                // the first tombstone that stays.
                let mut freed = 0;
                while let Some(oldest) = shard.tombstones.oldest()
                    && (oldest < floor || self.tombstone_expired(oldest, now))
                {
                    freed += shard.tombstones.pop_oldest().expect("not empty").1;
                }
                self.charge(&mut shard, -(freed as isize));
            }
            if shard.swept_floor < floor {
                let before = shard.map.len();
                let mut freed = 0;
                shard.map.retain(|key, item| {
                    let keep = item.version >= floor;
                    if !keep {
                        freed += cost(key.len(), item.value.len());
                    }
                    keep
                });
                let removed = (before - shard.map.len()) as u64;
                self.charge(&mut shard, -(freed as isize));
                self.reclaimed.fetch_add(removed, Relaxed);
                shard.swept_floor = floor;
            }
            for _ in 0..ROUNDS {
                let mut removed = 0;
                for _ in 0..SAMPLES {
                    let Some(index) = shard.random_index() else {
                        break;
                    };
                    if dead(&shard.map[index], now, floor) {
                        self.discard_at(&mut shard, index, now, floor);
                        removed += 1;
                    }
                }
                self.reclaimed.fetch_add(removed as u64, Relaxed);
                if removed < SAMPLES / 4 {
                    break;
                }
            }
        }
    }

    /// Zeroes the counters that `stats reset` clears. Current usage and item count are kept.
    pub fn reset_stats(&self) {
        for shard in self.shards.iter() {
            shard.lock().total_items = 0;
        }
        self.evictions.store(0, Relaxed);
        self.reclaimed.store(0, Relaxed);
    }

    pub fn stats(&self) -> StoreStats {
        let (mut bytes, mut curr_items, mut total_items) = (0, 0, 0);
        for shard in self.shards.iter() {
            let shard = shard.lock();
            bytes += shard.used as u64;
            curr_items += shard.map.len() as u64;
            total_items += shard.total_items;
        }
        StoreStats {
            bytes,
            curr_items,
            total_items,
            evictions: self.evictions.load(Relaxed),
            reclaimed: self.reclaimed.load(Relaxed),
            limit_maxbytes: self.memory_limit as u64,
        }
    }

    fn shard_for(&self, key: &[u8]) -> &ShardLock<Shard> {
        &self.shards[self.hasher.hash_one(key) as usize & self.mask]
    }

    /// The version below which items count as flushed. Activates a delayed flush that has come due.
    fn floor(&self, now: u64) -> u64 {
        let pending = self.pending_flush.load(Relaxed);
        if pending != 0
            && now >= pending
            && self
                .pending_flush
                .compare_exchange(pending, 0, Relaxed, Relaxed)
                .is_ok()
        {
            self.flush_now();
        }
        self.flush_floor.load(Relaxed)
    }

    /// A new version and the flush floor to judge it by, with no flush between them: any flush
    /// below the returned version is in the returned floor. Call with the key's shard lock
    /// held, before reading the item.
    fn stamp(&self, now: u64) -> (u64, u64) {
        let mut attempts = 0u32;
        loop {
            let before = self.flush_epoch.load(Acquire);
            let version = self.clock.next();
            let floor = self.floor(now);
            // Orders the clock read above before the second epoch read, pairing with the
            // release fence in `begin_flush`: if this version is above a flush's, that flush's
            // start is visible here.
            fence(Acquire);
            if before & FLUSHES_IN_PROGRESS == 0 && self.flush_epoch.load(Relaxed) == before {
                return (version, floor);
            }
            // A flush is between taking its version and raising the floor, which is brief and
            // lock-free, so wait.
            #[cfg(test)]
            self.stamp_retries.fetch_add(1, Relaxed);
            attempts += 1;
            if attempts < 64 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }

    /// Flushes everything stored so far.
    fn flush_now(&self) {
        let version = self.begin_flush();
        self.finish_flush(version);
    }

    /// Marks a flush as in progress and takes its version. Mutations wait in `stamp` until
    /// `finish_flush`.
    fn begin_flush(&self) -> u64 {
        self.flush_epoch.fetch_add(1, AcqRel);
        // Makes the epoch change visible to any mutation whose version is taken after this one.
        fence(Release);
        let version = self.clock.next();
        #[cfg(test)]
        self.run_hook(|hooks| &mut hooks.flush);
        version
    }

    /// Raises the floor to `version`, logs the flush, and lets waiting mutations go on.
    fn finish_flush(&self, version: u64) {
        self.flush_floor.fetch_max(version, Relaxed);
        self.log_flush(version);
        self.flush_epoch.fetch_add(FLUSH_FINISHED, Release);
    }

    /// Index of the live item for `key`. A dead item found on the way is removed.
    fn find(&self, shard: &mut Shard, key: &[u8], now: u64, floor: u64) -> Option<usize> {
        let index = shard.map.get_index_of(key);
        #[cfg(test)]
        self.run_hook(|hooks| &mut hooks.find);
        let index = index?;
        if dead(&shard.map[index], now, floor) {
            self.discard_at(shard, index, now, floor);
            self.reclaimed.fetch_add(1, Relaxed);
            return None;
        }
        Some(index)
    }

    /// Takes the hook that `pick` chooses, if one is set, and runs it.
    #[cfg(test)]
    fn run_hook(&self, pick: fn(&mut Hooks) -> &mut Option<Hook>) {
        let hook = pick(&mut self.hooks.lock()).take();
        if let Some(hook) = hook {
            hook();
        }
    }

    /// Removes the item at `index` for expiry or eviction. An item at or above the flush floor
    /// leaves a tombstone (if they are on); below it, the flush already stops older writes.
    fn discard_at(&self, shard: &mut Shard, index: usize, now: u64, floor: u64) {
        let Some((key, item)) = shard.map.swap_remove_index(index) else {
            return;
        };
        self.charge(shard, -(cost(key.len(), item.value.len()) as isize));
        if item.version >= floor {
            self.set_tombstone(shard, &key, item.version, now);
        }
    }

    fn remove_at(&self, shard: &mut Shard, index: usize) {
        if let Some((key, item)) = shard.map.swap_remove_index(index) {
            self.charge(shard, -(cost(key.len(), item.value.len()) as isize));
        }
    }

    /// Adds `delta` bytes to the shard's usage, and to the store's total once the shard's
    /// unreported part reaches the slack.
    #[inline]
    fn charge(&self, shard: &mut Shard, delta: isize) {
        if delta == 0 {
            return;
        }
        shard.used = shard.used.wrapping_add_signed(delta);
        shard.unreported += delta;
        if shard.unreported.unsigned_abs() > self.slack {
            let unreported = std::mem::take(&mut shard.unreported);
            if unreported > 0 {
                self.used.fetch_add(unreported as usize, Relaxed);
            } else {
                self.used.fetch_sub(unreported.unsigned_abs(), Relaxed);
            }
        }
    }

    /// Inserts or replaces `key`'s item, updates counters, and evicts if over the shard's limit.
    fn insert(&self, shard: &mut Shard, key: &[u8], item: Item, now: u64, floor: u64) {
        // The new item's version is above any tombstone for its key, so the tombstone is done.
        self.clear_tombstone(shard, key);
        let added = cost(key.len(), item.value.len()) as isize;
        let removed = match shard.map.get_index_of(key) {
            Some(index) => {
                let old = std::mem::replace(&mut shard.map[index], item);
                cost(key.len(), old.value.len()) as isize
            }
            None => {
                shard.map.insert(key.into(), item);
                0
            }
        };
        self.charge(shard, added - removed);
        shard.total_items += 1;
        while self.used.load(Relaxed) > self.memory_limit && shard.map.len() > 1 {
            if !self.evict_one(shard, Some(key), now, floor) {
                break;
            }
        }
    }

    /// Removes one item from a random sample: a dead one if any, else the least recently used.
    fn evict_one(&self, shard: &mut Shard, keep: Option<&[u8]>, now: u64, floor: u64) -> bool {
        const SAMPLES: usize = 5;
        let mut victim: Option<(usize, u64)> = None;
        for _ in 0..SAMPLES {
            let Some(index) = shard.random_index() else {
                break;
            };
            let (key, item) = shard.map.get_index(index).expect("index in range");
            if keep.is_some_and(|kept| kept == &key[..]) {
                continue;
            }
            let rank = if dead(item, now, floor) {
                0
            } else {
                item.last_access.max(1)
            };
            if victim.is_none_or(|(_, best)| rank < best) {
                victim = Some((index, rank));
            }
        }
        if victim.is_none()
            && let Some(kept) = keep
        {
            // Every sample hit the protected key; take the first other item instead.
            let other = shard.map.keys().position(|key| &key[..] != kept);
            victim = other.map(|index| {
                let rank = if dead(&shard.map[index], now, floor) {
                    0
                } else {
                    1
                };
                (index, rank)
            });
        }
        let Some((index, rank)) = victim else {
            return false;
        };
        self.discard_at(shard, index, now, floor);
        if rank == 0 {
            self.reclaimed.fetch_add(1, Relaxed);
        } else {
            self.evictions.fetch_add(1, Relaxed);
        }
        true
    }

    /// Evicts round-robin across shards until usage is under the limit, never removing `keep`.
    /// Stops once a full pass over every shard frees nothing. Call without holding a shard lock.
    fn evict_global(&self, keep: &[u8], now: u64, floor: u64) {
        let mut idle = 0;
        while self.used.load(Relaxed) > self.memory_limit && idle < self.shards.len() {
            let index = self.evict_cursor.fetch_add(1, Relaxed) & self.mask;
            let mut shard = self.shards[index].lock();
            if self.evict_one(&mut shard, Some(keep), now, floor) {
                idle = 0;
            } else {
                idle += 1;
            }
        }
    }
}
