//! The disk stage of the log thread, and compaction.

use std::fs;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::dir::DataDir;
use super::recover::{Recovered, set_position};
use super::segment::SegmentWriter;
use super::snapshot::{SnapshotInfo, SnapshotWriter};
use super::{PersistConfig, PersistStats};
use crate::clock::unix_secs;
use crate::oplog::{Logged, Position, RunStart};
use crate::store::Store;

/// Wait before retrying after a disk error.
const RETRY: Duration = Duration::from_secs(1);
/// Compact once the log after the last snapshot is this many times the snapshot's size.
const COMPACT_RATIO: u64 = 2;
/// Newest runs a snapshot carries; a peer whose position names an older run gets a full sync.
const RUNS_KEPT: usize = 32;

/// Writes numbered entries to segments, syncs them on a timer, rolls segments, and compacts
/// the log into snapshots.
pub struct Writer {
    dir: Arc<DataDir>,
    config: PersistConfig,
    store: Arc<Store>,
    stats: Arc<PersistStats>,
    /// `None` while degraded after a disk error.
    segment: Option<SegmentWriter>,
    segment_start: u64,
    next_seq: u64,
    /// Written to since the last sync.
    dirty: bool,
    /// Every entry numbered below this is written and synced.
    durable_seq: u64,
    /// The first entry that may have been lost to a disk error not yet covered by a snapshot.
    degraded_from: Option<u64>,
    /// The degraded marker is on disk.
    marker: bool,
    /// The history-lost marker may be on disk; the first snapshot covering losses removes it.
    history_marker: bool,
    last_sync: Instant,
    /// Log bytes written since the newest snapshot's start.
    log_bytes: u64,
    snapshot_bytes: u64,
    compaction: Option<Compaction>,
    /// Records were lost and no compaction started since has succeeded.
    loss_uncovered: bool,
    /// A compaction failed and must be retried regardless of log size.
    compact_wanted: bool,
    /// No retry of a failed segment creation or compaction before this.
    retry_at: Option<Instant>,
    /// Runs written so far, for snapshots.
    runs: Vec<RunStart>,
    /// The latest position on each peer's log, for snapshots.
    positions: Vec<Position>,
    /// The last link record, for snapshots.
    links: Option<Vec<u8>>,
}

/// What a snapshot carries besides the store's items and tombstones.
struct Meta {
    runs: Vec<RunStart>,
    positions: Vec<Position>,
    links: Option<Vec<u8>>,
}

/// A compaction running on its own thread.
struct Compaction {
    thread: JoinHandle<io::Result<SnapshotInfo>>,
    /// Began after records were lost, so its snapshot covers them.
    covers_loss: bool,
    /// Began after the last disk error, so its snapshot covers what that lost.
    covers_degraded: bool,
}

impl Compaction {
    /// Waits for the compaction, which also releases its hold on the data directory lock.
    fn join(self) -> io::Result<SnapshotInfo> {
        self.thread
            .join()
            .unwrap_or_else(|_| Err(io::Error::other("the compaction thread panicked")))
    }
}

impl Writer {
    pub(super) fn new(
        dir: Arc<DataDir>,
        config: PersistConfig,
        store: Arc<Store>,
        stats: Arc<PersistStats>,
        recovered: &Recovered,
    ) -> io::Result<Self> {
        let segment = SegmentWriter::create(&dir, recovered.next_seq)?;
        stats.log_bytes.store(recovered.log_bytes, Relaxed);
        Ok(Self {
            dir,
            config,
            store,
            stats,
            segment: Some(segment),
            segment_start: recovered.next_seq,
            next_seq: recovered.next_seq,
            dirty: false,
            durable_seq: recovered.next_seq,
            degraded_from: None,
            marker: recovered.degraded,
            history_marker: recovered.history_lost,
            last_sync: Instant::now(),
            log_bytes: recovered.log_bytes,
            snapshot_bytes: recovered.snapshot.map_or(0, |info| info.bytes),
            compaction: None,
            // A history hole or discarded positions are covered at once by a snapshot kept
            // alone, so no fallback can replay across the hole or resume from stale positions.
            loss_uncovered: recovered.history_lost || recovered.positions_discarded,
            // A snapshot of what was recovered removes an earlier run's degraded marker.
            compact_wanted: recovered.degraded,
            retry_at: None,
            runs: recovered.runs.clone(),
            positions: recovered.positions.clone(),
            // After a hole, the last link record read may be stale; carrying it into the
            // covering snapshot could make the node look last standing at a later start.
            links: if recovered.history_lost {
                None
            } else {
                recovered.links.clone()
            },
        })
    }

    /// Writes entries numbered from `first`, which must follow the last ones written. While
    /// degraded they are counted as lost. A loss entry triggers a compaction to cover it.
    pub fn write(&mut self, first: u64, entries: &[Logged]) {
        assert_eq!(
            first, self.next_seq,
            "entries must arrive in order without gaps"
        );
        self.next_seq += entries.len() as u64;
        // The counters count only client changes.
        let changes = entries
            .iter()
            .filter(|entry| matches!(entry, Logged::Record(_)))
            .count() as u64;
        let mut sync_now = false;
        // Tracked even while degraded, for the snapshot that later covers the loss.
        for entry in entries {
            match entry {
                Logged::Run(run) => self.runs.push(*run),
                Logged::Position(position) => set_position(&mut self.positions, *position),
                Logged::Links(up) => {
                    // A record that adds a peer keeps this node from looking last standing at
                    // its next start, so it is synced at once. Dropping peers can wait.
                    let before = self.links.as_deref().unwrap_or_default();
                    sync_now |= up.iter().any(|peer| !before.contains(peer));
                    self.links = Some(up.clone());
                }
                Logged::Record(_) | Logged::Loss => {}
            }
        }
        if entries.iter().any(|entry| matches!(entry, Logged::Loss)) {
            self.loss_uncovered = true;
        }
        let Some(segment) = &mut self.segment else {
            // Degraded: the compaction after recovery covers these.
            self.loss_uncovered |= !entries.is_empty();
            self.stats.lost.fetch_add(changes, Relaxed);
            return;
        };
        let mut written = 0;
        let mut result = Ok(());
        for entry in entries {
            match segment.append_logged(entry) {
                Ok(bytes) => written += bytes,
                Err(error) => {
                    result = Err(error);
                    break;
                }
            }
        }
        if result.is_ok() {
            match segment.write_out() {
                Ok(bytes) => written += bytes,
                Err(error) => result = Err(error),
            }
        }
        self.log_bytes += written;
        self.stats.bytes.fetch_add(written, Relaxed);
        self.stats.log_bytes.store(self.log_bytes, Relaxed);
        match result {
            Ok(()) => {
                self.dirty |= written > 0;
                self.stats.records.fetch_add(changes, Relaxed);
            }
            Err(error) => self.fail("writing the log", error),
        }
        if sync_now {
            self.sync();
        }
    }

    /// What a snapshot starting at `next_seq` carries besides items. Every entry below
    /// `next_seq` is written, so the store already holds what those positions cover.
    fn meta(&self) -> Meta {
        let skip = self.runs.len().saturating_sub(RUNS_KEPT);
        Meta {
            runs: self.runs[skip..].to_vec(),
            positions: self.positions.clone(),
            links: self.links.clone(),
        }
    }

    /// Syncs, rolls and compacts as needed, and retries a failed segment. Returns true when a
    /// segment was just begun after a disk error: the caller must then write a loss entry
    /// first. The covering compaction starts on a later call, so its snapshot is past it.
    pub fn maintain(&mut self) -> bool {
        let now = Instant::now();
        let retry_ok = self.retry_at.is_none_or(|at| now >= at);
        let mut resumed = false;
        if self.segment.is_none() && retry_ok {
            match self.begin_segment() {
                Ok(()) => {
                    eprintln!("persistence: writing again; compacting to cover lost records");
                    // Still degraded until that compaction succeeds.
                    resumed = true;
                    self.retry_at = None;
                }
                Err(error) => self.fail("starting a log segment", error),
            }
        }
        if now.duration_since(self.last_sync) >= self.config.fsync_interval {
            self.sync();
        }
        if self
            .segment
            .as_ref()
            .is_some_and(|segment| segment.bytes() >= self.config.segment_bytes)
        {
            self.roll();
        }
        self.collect_compaction();
        if resumed {
            return true;
        }
        let limit = self
            .config
            .segment_bytes
            .max(COMPACT_RATIO * self.snapshot_bytes);
        let wanted = self.compact_wanted || self.loss_uncovered || self.log_bytes > limit;
        if wanted && self.compaction.is_none() && self.segment.is_some() && retry_ok {
            self.start_compaction();
        }
        false
    }

    /// Syncs the current segment if it was written to since the last sync.
    fn sync(&mut self) {
        if !self.dirty {
            return;
        }
        self.last_sync = Instant::now();
        if let Some(segment) = &mut self.segment {
            match segment.sync() {
                Ok(()) => {
                    self.dirty = false;
                    self.durable_seq = self.next_seq;
                    self.stats.fsyncs.fetch_add(1, Relaxed);
                }
                Err(error) => self.fail("syncing the log", error),
            }
        }
    }

    /// Syncs the current segment and begins a new one at `next_seq`.
    fn roll(&mut self) {
        // An empty segment already starts at `next_seq`; creating another there would fail.
        if self.segment.is_none() || self.segment_start == self.next_seq {
            return;
        }
        self.sync();
        if self.segment.is_none() {
            return;
        }
        if let Err(error) = self.begin_segment() {
            self.fail("starting a log segment", error);
        }
    }

    fn begin_segment(&mut self) -> io::Result<()> {
        // A failed creation can leave a file behind; it holds no records yet.
        match fs::remove_file(self.dir.segment_path(self.next_seq)) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
        }
        self.segment = Some(SegmentWriter::create(&self.dir, self.next_seq)?);
        self.segment_start = self.next_seq;
        self.dirty = false;
        self.durable_seq = self.next_seq;
        Ok(())
    }

    fn start_compaction(&mut self) {
        self.roll();
        if self.segment.is_none() {
            return;
        }
        // Every loss marker so far lies before `start`, so the snapshot covers it.
        let start = self.next_seq;
        let covers_loss = self.loss_uncovered;
        let covers_degraded = self.degraded_from.is_some() || self.marker;
        let (store, dir) = (self.store.clone(), self.dir.clone());
        let meta = self.meta();
        let spawned = std::thread::Builder::new()
            .name("ferrited-compact".into())
            .spawn(move || compact(&store, &dir, start, unix_secs(), covers_loss, &meta));
        match spawned {
            Ok(thread) => {
                self.compaction = Some(Compaction {
                    thread,
                    covers_loss,
                    covers_degraded,
                });
                self.compact_wanted = false;
                self.loss_uncovered = false;
                // Log written during the walk counts toward the next compaction even if the
                // snapshot holds it; the segment-size minimum bounds the compaction rate.
                self.log_bytes = 0;
                self.stats.log_bytes.store(0, Relaxed);
            }
            Err(error) => self.fail("starting a compaction", error),
        }
    }

    /// Records the result of the compaction if it has finished.
    fn collect_compaction(&mut self) {
        if !self
            .compaction
            .as_ref()
            .is_some_and(|compaction| compaction.thread.is_finished())
        {
            return;
        }
        if let Some(compaction) = self.compaction.take() {
            let (covers_loss, covers_degraded) =
                (compaction.covers_loss, compaction.covers_degraded);
            self.record_compaction(compaction.join(), covers_loss, covers_degraded);
        }
    }

    fn record_compaction(
        &mut self,
        result: io::Result<SnapshotInfo>,
        covers_loss: bool,
        covers_degraded: bool,
    ) {
        match result {
            Ok(info) => {
                self.snapshot_bytes = info.bytes;
                self.stats.snapshots.fetch_add(1, Relaxed);
                self.stats.snapshot_items.store(info.items, Relaxed);
                if covers_degraded {
                    self.clear_degraded();
                }
                // A compaction covering losses also covers the hole recovery found.
                if covers_loss && self.history_marker {
                    match self.dir.remove_history_lost() {
                        Ok(()) => self.history_marker = false,
                        Err(error) => {
                            eprintln!(
                                "persistence: cannot remove the history-lost marker: {error}"
                            );
                            self.stats.errors.fetch_add(1, Relaxed);
                        }
                    }
                }
            }
            Err(error) => {
                eprintln!("persistence: compaction failed: {error}");
                self.stats.errors.fetch_add(1, Relaxed);
                self.compact_wanted = true;
                self.loss_uncovered |= covers_loss;
                self.retry_at = Some(Instant::now() + RETRY);
            }
        }
    }

    /// Final sync at shutdown, after every entry is written. Waits for a running compaction
    /// and compacts again if needed so the files describe the store; errors if they may not.
    /// Writes the clean-shutdown marker unless the oplog dropped changes since
    /// `dropped_before`, which may be on disk nowhere.
    pub fn finish(&mut self, dropped_before: u64) -> io::Result<()> {
        self.sync();
        // A failed sync drops the segment.
        let synced = self.segment.is_some();
        let mut covered = false;
        if let Some(compaction) = self.compaction.take() {
            let (covers_loss, covers_degraded) =
                (compaction.covers_loss, compaction.covers_degraded);
            // A failure sets `compact_wanted`, so the compaction below retries.
            self.record_compaction(compaction.join(), covers_loss, covers_degraded);
        }
        // Nothing more is coming, so a snapshot at `next_seq` covers any loss even without a
        // segment that starts there.
        let mut outcome = Ok(());
        if self.compact_wanted || self.loss_uncovered {
            self.roll();
            let covers_loss = self.loss_uncovered;
            let covers_degraded = self.degraded_from.is_some() || self.marker;
            let meta = self.meta();
            let result = compact(
                &self.store,
                &self.dir,
                self.next_seq,
                unix_secs(),
                covers_loss,
                &meta,
            );
            // Without a loss to cover, the synced log already holds every change.
            if let (Err(error), true) = (&result, covers_loss) {
                outcome = Err(io::Error::new(
                    error.kind(),
                    format!(
                        "the log could not be completed and the snapshot to cover it failed ({error}); recent changes may be lost"
                    ),
                ));
            }
            covered = covers_loss && result.is_ok();
            self.record_compaction(result, covers_loss, covers_degraded);
        }
        if outcome.is_ok() && (synced || covered) && self.stats.oplog.dropped() == dropped_before {
            // Best effort: without it, the next start only costs a full sync from each peer.
            if let Err(error) = self.dir.write_clean() {
                eprintln!("persistence: cannot write the clean-shutdown marker: {error}");
            }
        }
        outcome
    }

    /// Called once a compaction begun after the last disk error has succeeded.
    fn clear_degraded(&mut self) {
        if self.marker {
            match self.dir.remove_degraded() {
                Ok(()) => self.marker = false,
                Err(error) => {
                    eprintln!("persistence: cannot remove the degraded marker: {error}");
                    self.stats.errors.fetch_add(1, Relaxed);
                }
            }
        }
        if self.degraded_from.take().is_some() {
            eprintln!("persistence: a snapshot now covers the records lost to the disk error");
        }
        self.stats.degraded.store(false, Relaxed);
    }

    fn fail(&mut self, what: &str, error: io::Error) {
        eprintln!("persistence: {what} failed: {error}; serving from memory and retrying");
        self.stats.errors.fetch_add(1, Relaxed);
        self.stats.degraded.store(true, Relaxed);
        // Entries from the last sync on may not be on disk.
        let first_lost = *self.degraded_from.get_or_insert(self.durable_seq);
        // A running compaction began before this error, so it cannot cover what it loses.
        if let Some(compaction) = &mut self.compaction {
            compaction.covers_degraded = false;
        }
        // Best effort: retried with each error.
        if !self.marker {
            match self.dir.write_degraded(first_lost) {
                Ok(()) => self.marker = true,
                Err(error) => eprintln!(
                    "persistence: cannot write the degraded marker ({error}); a crash before the disk recovers would lose changes without a report at the next start"
                ),
            }
        }
        // Unsynced records, and those drained before a new segment begins, are lost.
        self.loss_uncovered = true;
        self.segment = None;
        self.dirty = false;
        self.retry_at = Some(Instant::now() + RETRY);
    }
}

/// Writes a snapshot that starts at `start` and prunes files no longer needed. A snapshot that
/// covers lost records is kept alone: falling back to an older one would replay across the loss.
fn compact(
    store: &Store,
    dir: &DataDir,
    start: u64,
    now: u64,
    covers_loss: bool,
    meta: &Meta,
) -> io::Result<SnapshotInfo> {
    let mut snapshot = SnapshotWriter::create(dir, start, store.flush_floor())?;
    for &run in &meta.runs {
        snapshot.add_run(run)?;
    }
    for &position in &meta.positions {
        snapshot.add_position(position)?;
    }
    if let Some(up) = &meta.links {
        snapshot.add_links(up)?;
    }
    for shard in 0..store.shard_count() {
        for record in store.shard_records(shard, now) {
            snapshot.add(&record)?;
        }
    }
    let info = snapshot.finish(dir)?;
    dir.prune(if covers_loss { 1 } else { 2 })?;
    Ok(info)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::backlog::{BATCH, Backlog, Pump};
    use crate::clock::Clock;
    use crate::oplog::{Oplog, Position, Record, RunStart};
    use crate::persist::recover::recover;
    use crate::persist::snapshot::SnapshotWriter;
    use crate::store::{GetArgs, SetArgs, StoreConfig};

    const NOW: u64 = 1_000_000;
    const PAUSE: Duration = Duration::from_millis(1);

    fn store() -> Arc<Store> {
        let config = StoreConfig {
            memory_limit: 1 << 26,
            item_max: 1 << 20,
            shards: 4,
        };
        Arc::new(Store::new(config, Arc::new(Clock::new(1))))
    }

    fn config(dir: &std::path::Path) -> PersistConfig {
        PersistConfig {
            dir: dir.to_path_buf(),
            fsync_interval: Duration::from_millis(20),
            segment_bytes: 64 * 1024,
        }
    }

    fn set(store: &Store, key: &str, value: &str) {
        store.set(
            key.as_bytes(),
            Bytes::from(value.to_string()),
            NOW,
            &SetArgs::default(),
        );
    }

    fn get(store: &Store, key: &str) -> Option<String> {
        store
            .get(key.as_bytes(), NOW, &GetArgs::default())
            .map(|hit| String::from_utf8(hit.value.to_vec()).unwrap())
    }

    /// Opens `path` for a hand-driven log in run 1, with an oplog of `BATCH + 1` changes.
    fn pump(path: &std::path::Path) -> (Pump, Arc<Store>, Arc<Oplog>) {
        let dir = Arc::new(DataDir::open(path).unwrap());
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        let oplog = Arc::new(Oplog::new(BATCH + 1, u64::MAX));
        let stats = Arc::new(PersistStats::new(oplog.clone()));
        let writer = Writer::new(dir, config(path), store.clone(), stats, &recovered).unwrap();
        let backlog = Arc::new(Backlog::new(0, recovered.next_seq, Vec::new()));
        let pump = Pump::new(oplog.clone(), backlog, Some(writer), recovered.next_seq, 1);
        store.attach_oplog(oplog.clone());
        (pump, store, oplog)
    }

    fn disk(pump: &mut Pump) -> &mut Writer {
        pump.disk.as_mut().unwrap()
    }

    /// Steps the pump until a compaction has finished.
    fn compact_once(pump: &mut Pump) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while disk(pump).stats.snapshots.load(Relaxed) == 0 {
            assert!(Instant::now() < deadline, "no compaction happened");
            pump.step();
            std::thread::sleep(PAUSE);
        }
    }

    #[test]
    fn a_dropped_delete_does_not_bring_its_key_back_after_a_crash() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, oplog) = pump(temp.path());
        // A full batch ahead of the put, so the pump's first pass leaves the put queued.
        for key in 0..BATCH {
            set(&store, &format!("f{key}"), "v");
        }
        set(&store, "k", "v");
        // The queue is full, so the delete is dropped.
        store.delete(b"k", NOW, &Default::default());
        assert_eq!(oplog.dropped(), 1);
        assert_eq!(get(&store, "k"), None);
        compact_once(&mut pump);
        // A crash: no final sync and compaction.
        drop(pump);
        drop(store);

        let dir = DataDir::open(temp.path()).unwrap();
        let second = self::store();
        let recovered = recover(&dir, &second, NOW).unwrap();
        assert_eq!(get(&second, "k"), None, "{:?}", recovered.warnings);
        assert_eq!(get(&second, "f0").as_deref(), Some("v"));
        assert!(recovered.warnings.is_empty(), "{:?}", recovered.warnings);
        assert_eq!(dir.snapshots().unwrap().len(), 1);
    }

    #[test]
    fn a_loss_with_no_covering_snapshot_is_reported_after_a_crash() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, _oplog) = pump(temp.path());
        set(&store, "a", "1");
        pump.step();
        // A crash before a compaction covers the loss.
        pump.emit_loss();
        disk(&mut pump).sync();
        drop(pump);

        let dir = DataDir::open(temp.path()).unwrap();
        let second = self::store();
        let recovered = recover(&dir, &second, NOW).unwrap();
        assert_eq!(recovered.warnings.len(), 1, "{:?}", recovered.warnings);
        assert!(
            recovered.warnings[0].contains("records were lost at about sequence 2"),
            "{:?}",
            recovered.warnings
        );
        assert_eq!(get(&second, "a").as_deref(), Some("1"));
    }

    #[test]
    fn a_damaged_snapshot_is_never_kept_in_place_of_a_good_one() {
        let temp = tempfile::tempdir().unwrap();
        {
            let dir = DataDir::open(temp.path()).unwrap();
            let put = |key: &str, version| Record::Put {
                key: Bytes::from(key.to_string()),
                version,
                cas: version,
                flags: 0,
                expiry: 0,
                stale: false,
                value: Bytes::from_static(b"v"),
            };
            let mut segment = SegmentWriter::create(&dir, 0).unwrap();
            segment.append(&put("a", 10)).unwrap();
            segment.append(&put("b", 11)).unwrap();
            segment.sync().unwrap();
            let mut snapshot = SnapshotWriter::create(&dir, 1, 0).unwrap();
            snapshot.add(&put("a", 10)).unwrap();
            snapshot.finish(&dir).unwrap();
            let mut snapshot = SnapshotWriter::create(&dir, 50, 0).unwrap();
            snapshot.add(&put("a", 10)).unwrap();
            snapshot.add(&put("b", 11)).unwrap();
            snapshot.finish(&dir).unwrap();
            let path = dir.snapshot_path(50);
            let bytes = fs::read(&path).unwrap();
            fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
        }
        // Recovery falls back to the snapshot at 1; a compaction kept alone prunes the rest.
        let (mut pump, store, _oplog) = pump(temp.path());
        assert_eq!(get(&store, "b").as_deref(), Some("v"));
        disk(&mut pump).loss_uncovered = true;
        compact_once(&mut pump);
        assert_eq!(disk(&mut pump).dir.snapshots().unwrap().len(), 1);
        assert!(disk(&mut pump).dir.snapshots().unwrap()[0] >= 50);
        drop(pump);
        drop(store);

        let dir = DataDir::open(temp.path()).unwrap();
        let second = self::store();
        let recovered = recover(&dir, &second, NOW).unwrap();
        assert!(recovered.warnings.is_empty(), "{:?}", recovered.warnings);
        assert_eq!(get(&second, "a").as_deref(), Some("v"));
        assert_eq!(get(&second, "b").as_deref(), Some("v"));
    }

    #[test]
    fn a_change_dropped_after_close_leaves_no_clean_marker() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, oplog) = pump(temp.path());
        set(&store, "a", "1");
        pump.step();
        let before = oplog.dropped();
        oplog.close();
        set(&store, "b", "1");
        assert_eq!(oplog.dropped(), before + 1);
        let clean = disk(&mut pump).dir.clean_path();
        disk(&mut pump).finish(before).unwrap();
        assert!(!clean.exists());
        disk(&mut pump).finish(oplog.dropped()).unwrap();
        assert!(clean.exists());
    }

    #[test]
    fn runs_positions_and_losses_are_not_counted_as_records() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, _store, _oplog) = pump(temp.path());
        let entries = [
            Logged::Run(RunStart { id: 7, first: 0 }),
            Logged::Loss,
            Logged::Record(Record::Flush { version: 1 }),
        ];
        let first = disk(&mut pump).next_seq;
        disk(&mut pump).write(first, &entries);
        assert_eq!(disk(&mut pump).stats.records.load(Relaxed), 1);
        assert!(disk(&mut pump).stats.bytes.load(Relaxed) > 0);
    }

    #[test]
    fn finish_succeeds_when_everything_is_saved() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, oplog) = pump(temp.path());
        set(&store, "a", "1");
        set(&store, "b", "1");
        pump.emit_loss();
        assert!(!oplog.is_empty());
        pump.finish().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn finish_reports_a_loss_it_could_not_cover() {
        use std::os::unix::fs::PermissionsExt;

        // Root ignores directory permissions.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, _oplog) = pump(temp.path());
        set(&store, "a", "1");
        disk(&mut pump).fail("testing", disk_error());
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o500)).unwrap();
        let result = pump.finish();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let message = result.unwrap_err().to_string();
        assert!(message.contains("recent changes may be lost"), "{message}");
    }

    fn disk_error() -> io::Error {
        io::Error::other("simulated disk error")
    }

    #[test]
    fn a_crash_while_degraded_is_reported_until_a_snapshot_covers_it() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, _oplog) = pump(temp.path());
        set(&store, "a", "1");
        pump.step();
        disk(&mut pump).sync();
        disk(&mut pump).fail("testing", disk_error());
        assert_eq!(
            disk(&mut pump).dir.read_degraded().unwrap(),
            Some(Some(2)),
            "the marker names the first entry not synced: after the run marker and the put"
        );
        set(&store, "b", "1");
        pump.step();
        assert_eq!(disk(&mut pump).stats.lost.load(Relaxed), 1);
        drop(pump);
        drop(store);

        let dir = DataDir::open(temp.path()).unwrap();
        let recovered = recover(&dir, &self::store(), NOW).unwrap();
        assert!(recovered.degraded);
        assert!(
            recovered
                .warnings
                .iter()
                .any(|w| w.contains("while the disk was failing")),
            "{:?}",
            recovered.warnings
        );
        drop(dir);
        let (mut second, store, _oplog) = self::pump(temp.path());
        assert_eq!(get(&store, "a").as_deref(), Some("1"));
        compact_once(&mut second);
        assert_eq!(disk(&mut second).dir.read_degraded().unwrap(), None);
    }

    #[test]
    fn degraded_stays_set_until_a_compaction_begun_after_the_error_succeeds() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, _oplog) = pump(temp.path());
        set(&store, "a", "1");
        pump.step();
        // A compaction running when the disk fails cannot cover the failure.
        disk(&mut pump).compact_wanted = true;
        pump.step();
        assert!(disk(&mut pump).compaction.is_some());
        disk(&mut pump).fail("testing", disk_error());
        while !disk(&mut pump)
            .compaction
            .as_ref()
            .unwrap()
            .thread
            .is_finished()
        {
            std::thread::sleep(PAUSE);
        }
        disk(&mut pump).retry_at = None;
        pump.step();
        let writer = disk(&mut pump);
        assert!(writer.segment.is_some());
        assert_eq!(writer.stats.snapshots.load(Relaxed), 1);
        assert!(writer.stats.degraded.load(Relaxed));
        assert!(writer.dir.read_degraded().unwrap().is_some());
        let deadline = Instant::now() + Duration::from_secs(10);
        while disk(&mut pump).stats.snapshots.load(Relaxed) < 2 {
            assert!(Instant::now() < deadline, "no second compaction");
            pump.step();
            std::thread::sleep(PAUSE);
        }
        assert!(!disk(&mut pump).stats.degraded.load(Relaxed));
        assert_eq!(disk(&mut pump).dir.read_degraded().unwrap(), None);
    }

    #[test]
    fn a_loss_entry_follows_a_disk_recovery_and_comes_before_the_covering_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, _oplog) = pump(temp.path());
        set(&store, "a", "1");
        pump.step();
        disk(&mut pump).fail("testing", disk_error());
        disk(&mut pump).retry_at = None;
        pump.step();
        compact_once(&mut pump);
        drop(pump);
        drop(store);
        let dir = DataDir::open(temp.path()).unwrap();
        let recovered = recover(&dir, &self::store(), NOW).unwrap();
        assert!(recovered.warnings.is_empty(), "{:?}", recovered.warnings);
    }

    #[test]
    fn a_clean_stop_leaves_the_marker_and_a_crash_does_not() {
        let temp = tempfile::tempdir().unwrap();
        let (pump, store, _oplog) = pump(temp.path());
        set(&store, "a", "1");
        pump.finish().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        assert!(dir.clean_path().exists());
        drop(dir);
        let (mut pump, _store, _oplog) = self::pump(temp.path());
        pump.step();
        drop(pump);
        let dir = DataDir::open(temp.path()).unwrap();
        assert!(!dir.clean_path().exists());
    }

    #[test]
    fn an_idle_writer_does_not_sync() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, _oplog) = pump(temp.path());
        set(&store, "a", "1");
        pump.step();
        let interval = disk(&mut pump).config.fsync_interval;
        std::thread::sleep(interval);
        pump.step();
        assert_eq!(disk(&mut pump).stats.fsyncs.load(Relaxed), 1);
        std::thread::sleep(interval);
        pump.step();
        assert_eq!(disk(&mut pump).stats.fsyncs.load(Relaxed), 1);
    }

    #[test]
    fn a_snapshot_carries_the_runs_and_positions_so_far() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, oplog) = pump(temp.path());
        let position = Position {
            peer: 2,
            run: 9,
            next: 33,
        };
        set(&store, "a", "1");
        oplog.append_position(position);
        pump.step();
        disk(&mut pump).compact_wanted = true;
        compact_once(&mut pump);
        pump.finish().unwrap();
        drop(store);
        let dir = DataDir::open(temp.path()).unwrap();
        for start in dir.segments().unwrap() {
            fs::remove_file(dir.segment_path(start)).unwrap();
        }
        let recovered = recover(&dir, &self::store(), NOW).unwrap();
        assert_eq!(recovered.runs, [RunStart { id: 1, first: 0 }]);
        assert_eq!(recovered.positions, [position]);
    }

    #[test]
    fn a_snapshot_carries_the_last_link_record() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, oplog) = pump(temp.path());
        set(&store, "a", "1");
        oplog.set_links(vec![2, 3]);
        pump.step();
        oplog.set_links(vec![3]);
        pump.step();
        disk(&mut pump).compact_wanted = true;
        compact_once(&mut pump);
        pump.finish().unwrap();
        drop(store);
        let dir = DataDir::open(temp.path()).unwrap();
        for start in dir.segments().unwrap() {
            fs::remove_file(dir.segment_path(start)).unwrap();
        }
        let recovered = recover(&dir, &self::store(), NOW).unwrap();
        assert_eq!(recovered.links, Some(vec![3]));
    }

    #[test]
    fn a_snapshot_after_a_hole_in_the_history_carries_no_link_record() {
        let temp = tempfile::tempdir().unwrap();
        {
            let dir = DataDir::open(temp.path()).unwrap();
            let mut segment = SegmentWriter::create(&dir, 0).unwrap();
            segment.append_links(&[]);
            segment.sync().unwrap();
            // A later link record may be among what the disk error lost.
            dir.write_degraded(1).unwrap();
        }
        let (mut pump, _store, _oplog) = pump(temp.path());
        compact_once(&mut pump);
        pump.finish().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(!recovered.history_lost, "{:?}", recovered.warnings);
        assert_eq!(recovered.links, None);
        assert!(!recovered.last_standing());
    }

    /// Logs `Links([])`, a record and `Links([2])`, then tears the last entry after a clean stop.
    fn torn_after_a_clean_stop(path: &std::path::Path) {
        let dir = DataDir::open(path).unwrap();
        let mut segment = SegmentWriter::create(&dir, 0).unwrap();
        segment.append_links(&[]);
        segment.append(&Record::Flush { version: 1 }).unwrap();
        segment.append_links(&[2]);
        segment.sync().unwrap();
        let file = dir.segment_path(0);
        let len = fs::metadata(&file).unwrap().len();
        fs::OpenOptions::new()
            .write(true)
            .open(&file)
            .unwrap()
            .set_len(len - 2)
            .unwrap();
        dir.write_clean().unwrap();
    }

    /// A directory a crash left, whose log holds this node's position on node 2's log.
    fn crashed_with_a_position(path: &std::path::Path) {
        let dir = DataDir::open(path).unwrap();
        let mut segment = SegmentWriter::create(&dir, 0).unwrap();
        segment
            .append_logged(&Logged::Position(Position {
                peer: 2,
                run: 9,
                next: 33,
            }))
            .unwrap();
        segment.sync().unwrap();
    }

    #[test]
    fn positions_discarded_after_a_crash_stay_discarded_after_a_clean_stop() {
        let temp = tempfile::tempdir().unwrap();
        crashed_with_a_position(temp.path());
        // Stops cleanly before node 2's full sync happens.
        let (pump, _store, _oplog) = pump(temp.path());
        pump.finish().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(!recovered.lossy, "{:?}", recovered.warnings);
        assert!(recovered.positions.is_empty(), "{:?}", recovered.positions);
        assert_eq!(dir.snapshots().unwrap().len(), 1);
    }

    #[test]
    fn positions_discarded_after_a_crash_are_hidden_by_a_snapshot_at_once() {
        let temp = tempfile::tempdir().unwrap();
        crashed_with_a_position(temp.path());
        let (mut pump, _store, _oplog) = pump(temp.path());
        compact_once(&mut pump);
        drop(pump);
        let dir = DataDir::open(temp.path()).unwrap();
        dir.write_clean().unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.positions.is_empty(), "{:?}", recovered.positions);
    }

    #[test]
    fn a_crash_without_positions_takes_no_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, store, _oplog) = pump(temp.path());
        set(&store, "a", "1");
        pump.step();
        drop(pump);
        let (mut pump, _store, _oplog) = self::pump(temp.path());
        pump.step();
        assert!(disk(&mut pump).compaction.is_none());
        assert_eq!(disk(&mut pump).stats.snapshots.load(Relaxed), 0);
    }

    #[test]
    fn a_hole_is_still_known_at_the_next_start_after_a_clean_stop() {
        let temp = tempfile::tempdir().unwrap();
        torn_after_a_clean_stop(temp.path());
        // Serves without logging a link record, then stops cleanly.
        let (pump, _store, _oplog) = pump(temp.path());
        pump.finish().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(!recovered.last_standing(), "{:?}", recovered.links);
        assert_eq!(dir.snapshots().unwrap().len(), 1);
        assert!(!dir.history_lost_path().exists());
    }

    #[test]
    fn a_hole_is_still_known_at_the_next_start_after_a_crash() {
        let temp = tempfile::tempdir().unwrap();
        torn_after_a_clean_stop(temp.path());
        let (pump, _store, _oplog) = pump(temp.path());
        drop(pump);
        let dir = DataDir::open(temp.path()).unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.history_lost, "{:?}", recovered.warnings);
        assert!(!recovered.last_standing());
    }

    #[test]
    fn the_snapshot_after_a_hole_is_written_at_once_and_kept_alone() {
        let temp = tempfile::tempdir().unwrap();
        {
            let dir = DataDir::open(temp.path()).unwrap();
            let mut segment = SegmentWriter::create(&dir, 0).unwrap();
            segment.append_links(&[]);
            segment.sync().unwrap();
            for start in [1, 2] {
                let mut snapshot = SnapshotWriter::create(&dir, start, 0).unwrap();
                snapshot.add_links(&[]).unwrap();
                snapshot.finish(&dir).unwrap();
            }
            // A later link record may be among what the disk error lost.
            dir.write_degraded(1).unwrap();
        }
        let (mut pump, _store, _oplog) = pump(temp.path());
        compact_once(&mut pump);
        let writer = disk(&mut pump);
        assert_eq!(writer.dir.snapshots().unwrap().len(), 1);
        assert_eq!(writer.dir.read_degraded().unwrap(), None);
        assert!(!writer.dir.history_lost_path().exists());
    }

    #[test]
    fn a_link_record_that_adds_a_peer_is_synced_at_once() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, _store, oplog) = pump(temp.path());
        // No timed syncs.
        disk(&mut pump).config.fsync_interval = Duration::from_secs(3600);
        pump.step();
        let fsyncs = |pump: &mut Pump| disk(pump).stats.fsyncs.load(Relaxed);
        let before = fsyncs(&mut pump);
        oplog.set_links(vec![2]);
        pump.step();
        assert_eq!(fsyncs(&mut pump), before + 1);
        oplog.set_links(vec![2, 3]);
        pump.step();
        assert_eq!(fsyncs(&mut pump), before + 2);
        oplog.set_links(vec![3]);
        pump.step();
        assert_eq!(fsyncs(&mut pump), before + 2);
    }

    #[test]
    fn a_snapshot_keeps_only_the_newest_runs() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pump, _store, _oplog) = pump(temp.path());
        let writer = disk(&mut pump);
        for id in 2..=40 {
            writer.runs.push(RunStart { id, first: id });
        }
        let runs = writer.meta().runs;
        assert_eq!(runs.len(), RUNS_KEPT);
        assert_eq!(runs.last(), Some(&RunStart { id: 40, first: 40 }));
    }
}
