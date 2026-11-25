//! Persistence: the change log on disk, snapshots, and recovery.

pub mod codec;
pub mod dir;
pub mod reader;
pub mod recover;
pub mod segment;
pub mod snapshot;
pub mod writer;

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use crate::oplog::Oplog;
use crate::store::Store;
use dir::DataDir;

#[derive(Clone, Debug)]
pub struct PersistConfig {
    pub dir: PathBuf,
    /// Time between fsyncs of the log.
    pub fsync_interval: Duration,
    /// Size at which a log segment is closed and a new one begun.
    pub segment_bytes: u64,
}

/// Counters reported by `stats` as `persist_*`.
pub struct PersistStats {
    oplog: Arc<Oplog>,
    pub records: AtomicU64,
    pub bytes: AtomicU64,
    pub fsyncs: AtomicU64,
    pub errors: AtomicU64,
    /// Records discarded while degraded.
    pub lost: AtomicU64,
    pub snapshots: AtomicU64,
    pub snapshot_items: AtomicU64,
    pub log_bytes: AtomicU64,
    pub degraded: AtomicBool,
}

impl PersistStats {
    pub(crate) fn new(oplog: Arc<Oplog>) -> Self {
        Self {
            oplog,
            records: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            fsyncs: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            lost: AtomicU64::new(0),
            snapshots: AtomicU64::new(0),
            snapshot_items: AtomicU64::new(0),
            log_bytes: AtomicU64::new(0),
            degraded: AtomicBool::new(false),
        }
    }

    /// Every counter as (stat name, value).
    pub fn entries(&self) -> Vec<(&'static str, u64)> {
        vec![
            ("persist_enabled", 1),
            ("persist_degraded", u64::from(self.degraded.load(Relaxed))),
            ("persist_records", self.records.load(Relaxed)),
            ("persist_bytes", self.bytes.load(Relaxed)),
            ("persist_fsyncs", self.fsyncs.load(Relaxed)),
            ("persist_errors", self.errors.load(Relaxed)),
            ("persist_dropped", self.oplog.dropped()),
            ("persist_lost", self.lost.load(Relaxed)),
            ("persist_snapshots", self.snapshots.load(Relaxed)),
            ("persist_snapshot_items", self.snapshot_items.load(Relaxed)),
            ("persist_log_bytes", self.log_bytes.load(Relaxed)),
        ]
    }
}

/// What opening the data directory produced.
pub struct Opened {
    /// The disk stage, ready to write entries from `recovered.next_seq` on.
    pub writer: writer::Writer,
    pub stats: Arc<PersistStats>,
    pub dir: Arc<DataDir>,
    pub recovered: recover::Recovered,
}

/// Opens and locks the data directory, recovers `store` from it, and creates the disk stage,
/// counting oplog drops in its stats. Blocks until recovery is complete.
pub fn open(
    config: &PersistConfig,
    store: &Arc<Store>,
    oplog: &Arc<Oplog>,
    now: u64,
) -> io::Result<Opened> {
    let dir = Arc::new(DataDir::open(&config.dir)?);
    let path = config.dir.display();
    dir.remove_temporaries()
        .map_err(|error| context(error, &format!("cannot clean up data directory {path}")))?;
    let recovered = recover::recover(&dir, store, now)
        .map_err(|error| context(error, &format!("cannot recover from data directory {path}")))?;
    for warning in &recovered.warnings {
        eprintln!("persistence: {warning}");
    }
    eprintln!("persistence: {}", recovered.describe(&dir));
    let stats = Arc::new(PersistStats::new(oplog.clone()));
    let writer = writer::Writer::new(
        dir.clone(),
        config.clone(),
        store.clone(),
        stats.clone(),
        &recovered,
    )
    .map_err(|error| {
        context(
            error,
            &format!("cannot start the log in data directory {path}"),
        )
    })?;
    Ok(Opened {
        writer,
        stats,
        dir,
        recovered,
    })
}

/// Puts `what` in front of an error's message, keeping its kind.
fn context(error: io::Error, what: &str) -> io::Error {
    io::Error::new(error.kind(), format!("{what}: {error}"))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::backlog::{Log, LogConfig};
    use crate::clock::{Clock, unix_secs};
    use crate::store::{GetArgs, SetArgs, StoreConfig};

    fn store() -> Arc<Store> {
        let config = StoreConfig {
            memory_limit: 1 << 26,
            item_max: 1 << 20,
            shards: 8,
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

    /// Starts the log with persistence in `dir` and an oplog of the given size.
    fn start_with(dir: &std::path::Path, store: &Arc<Store>, capacity: usize, bytes: u64) -> Log {
        let config = LogConfig {
            oplog_capacity: capacity,
            oplog_bytes: bytes,
            backlog_bytes: 0,
            persist: Some(config(dir)),
        };
        Log::start(&config, store, unix_secs()).unwrap().0
    }

    fn start(dir: &std::path::Path, store: &Arc<Store>) -> Log {
        start_with(dir, store, 65536, 64 << 20)
    }

    fn stats(log: &Log) -> Arc<PersistStats> {
        log.persist.clone().unwrap()
    }

    fn set(store: &Store, key: &str, value: &str) {
        store.set(
            key.as_bytes(),
            Bytes::from(value.to_string()),
            unix_secs(),
            &SetArgs::default(),
        );
    }

    fn get(store: &Store, key: &str) -> Option<String> {
        store
            .get(key.as_bytes(), unix_secs(), &GetArgs::default())
            .map(|hit| String::from_utf8(hit.value.to_vec()).unwrap())
    }

    fn dropped(stats: &PersistStats) -> u64 {
        stats
            .entries()
            .into_iter()
            .find(|&(name, _)| name == "persist_dropped")
            .unwrap()
            .1
    }

    #[test]
    fn data_survives_a_clean_restart() {
        let temp = tempfile::tempdir().unwrap();
        let first = store();
        let log = start(temp.path(), &first);
        set(&first, "a", "1");
        set(&first, "b", "2");
        first.delete(b"b", unix_secs(), &Default::default());
        log.shutdown().unwrap();

        let second = store();
        let log = start(temp.path(), &second);
        assert_eq!(get(&second, "a").as_deref(), Some("1"));
        assert_eq!(get(&second, "b"), None);
        // New writes get versions above the recovered ones.
        let before = second.peek(b"a", unix_secs()).unwrap().cas;
        set(&second, "a", "3");
        assert!(second.peek(b"a", unix_secs()).unwrap().cas > before);
        log.shutdown().unwrap();
    }

    #[test]
    fn shutdown_finishes_while_changes_keep_coming_and_counts_the_late_ones() {
        let temp = tempfile::tempdir().unwrap();
        let first = store();
        let log = start(temp.path(), &first);
        let stats = stats(&log);
        let running = Arc::new(AtomicBool::new(true));
        let producer = {
            let (store, running) = (first.clone(), running.clone());
            std::thread::spawn(move || {
                let mut key = 0u64;
                while running.load(Relaxed) {
                    set(&store, &format!("k{key}"), "v");
                    key += 1;
                }
                key
            })
        };
        std::thread::sleep(Duration::from_millis(50));
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            log.shutdown().unwrap();
            done.send(()).unwrap();
        });
        let result = finished.recv_timeout(Duration::from_secs(20));
        running.store(false, Relaxed);
        let written = producer.join().unwrap();
        result.expect("shutdown did not finish while changes kept coming");
        // Changes after the log thread finished are counted rather than left queued.
        let before = dropped(&stats);
        set(&first, "late", "v");
        assert_eq!(dropped(&stats), before + 1);
        assert!(written > 0);
    }

    #[test]
    fn compaction_snapshots_and_prunes_the_log() {
        let temp = tempfile::tempdir().unwrap();
        let first = store();
        let log = start(temp.path(), &first);
        let stats = stats(&log);
        let value = "x".repeat(1000);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let wait_for_snapshots = |count| {
            while stats.snapshots.load(Relaxed) < count {
                assert!(
                    std::time::Instant::now() < deadline,
                    "no compaction happened"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        // Each half passes the compaction threshold however the log thread batches it.
        for round in 0..10 {
            if round == 5 {
                wait_for_snapshots(1);
            }
            for key in 0..200 {
                set(&first, &format!("k{key}"), &format!("{round}{value}"));
            }
        }
        wait_for_snapshots(2);
        log.shutdown().unwrap();
        let dir = dir::DataDir::open(temp.path()).unwrap();
        let snapshots = dir.snapshots().unwrap();
        let segments = dir.segments().unwrap();
        assert!(snapshots.len() <= 2, "{snapshots:?}");
        // Pruning removed every segment that ends before the older kept snapshot.
        assert!(segments[0] >= snapshots[0], "{segments:?} {snapshots:?}");
        drop(dir);

        let second = store();
        let log = start(temp.path(), &second);
        for key in 0..200 {
            assert_eq!(get(&second, &format!("k{key}")), Some(format!("9{value}")));
        }
        log.shutdown().unwrap();
    }

    #[test]
    fn dropped_records_are_covered_by_a_compaction() {
        let temp = tempfile::tempdir().unwrap();
        let first = store();
        let log = start_with(temp.path(), &first, 1, 64 << 20);
        for key in 0..2000 {
            set(&first, &format!("k{key}"), "v");
        }
        assert!(dropped(&stats(&log)) > 0, "nothing was dropped");
        log.shutdown().unwrap();

        let second = store();
        let log = start(temp.path(), &second);
        for key in 0..2000 {
            assert_eq!(
                get(&second, &format!("k{key}")).as_deref(),
                Some("v"),
                "k{key}"
            );
        }
        log.shutdown().unwrap();
    }

    #[test]
    fn records_over_the_oplog_byte_budget_are_dropped_and_covered() {
        let temp = tempfile::tempdir().unwrap();
        let first = store();
        let log = start_with(temp.path(), &first, 65536, 4096);
        let value = "x".repeat(1000);
        for key in 0..2000 {
            set(&first, &format!("k{key}"), &value);
        }
        assert!(dropped(&stats(&log)) > 0, "nothing was dropped");
        log.shutdown().unwrap();

        let second = store();
        let log = start(temp.path(), &second);
        for key in 0..2000 {
            assert_eq!(
                get(&second, &format!("k{key}")),
                Some(value.clone()),
                "k{key}"
            );
        }
        log.shutdown().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_disk_error_degrades_and_recovers() {
        use std::os::unix::fs::PermissionsExt;

        // Root ignores directory permissions, so the disk error cannot be caused this way.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let first = store();
        let log = start(temp.path(), &first);
        let stats = stats(&log);
        let read_only = std::fs::Permissions::from_mode(0o500);
        std::fs::set_permissions(temp.path(), read_only).unwrap();
        // Fill past the segment size so creating the next segment fails.
        let value = "x".repeat(1000);
        for key in 0..100 {
            set(&first, &format!("k{key}"), &value);
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !stats.degraded.load(Relaxed) {
            assert!(std::time::Instant::now() < deadline, "never degraded");
            std::thread::sleep(Duration::from_millis(10));
        }
        // Dropped while degraded; only the compaction after recovery saves these.
        for key in 100..200 {
            set(&first, &format!("k{key}"), &value);
        }
        std::fs::set_permissions(temp.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        while stats.degraded.load(Relaxed) || stats.snapshots.load(Relaxed) == 0 {
            assert!(std::time::Instant::now() < deadline, "never recovered");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(stats.errors.load(Relaxed) > 0);
        log.shutdown().unwrap();

        let second = store();
        let log = start(temp.path(), &second);
        for key in 0..200 {
            assert_eq!(
                get(&second, &format!("k{key}")),
                Some(value.clone()),
                "k{key}"
            );
        }
        log.shutdown().unwrap();
    }
}
