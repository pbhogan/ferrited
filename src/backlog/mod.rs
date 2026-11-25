//! The numbered log: one thread drains the oplog, numbers every entry, writes it to disk when
//! persistence is on, and keeps the newest entries in memory for the peer senders.

mod pump;
mod window;

pub use pump::{BATCH, Pump};
pub use window::{Backlog, Read, entry_cost};

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::thread::JoinHandle;

use crate::oplog::Oplog;
use crate::persist::dir::DataDir;
use crate::persist::recover::Recovered;
use crate::persist::{self, PersistConfig, PersistStats};
use crate::store::Store;

/// Settings for the numbered log.
#[derive(Clone, Debug)]
pub struct LogConfig {
    /// Changes the oplog holds before the log thread must catch up.
    pub oplog_capacity: usize,
    /// Bytes of value the oplog holds before the log thread must catch up.
    pub oplog_bytes: u64,
    /// Bytes of entries the backlog keeps for peer senders; 0 without peers.
    pub backlog_bytes: u64,
    pub persist: Option<PersistConfig>,
}

/// The running log thread and what it shares.
pub struct Log {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<io::Result<()>>>,
    pub oplog: Arc<Oplog>,
    pub backlog: Arc<Backlog>,
    pub persist: Option<Arc<PersistStats>>,
    /// For senders that read entries older than the backlog.
    pub dir: Option<Arc<DataDir>>,
    /// This process's run ID.
    pub run: u64,
}

impl Log {
    /// Recovers `store` from disk if persistence is on, starts the log thread under a new run,
    /// and attaches the oplog. Blocks until recovery completes and returns what it found.
    pub fn start(
        config: &LogConfig,
        store: &Arc<Store>,
        now: u64,
    ) -> io::Result<(Self, Recovered)> {
        let oplog = Arc::new(Oplog::new(config.oplog_capacity, config.oplog_bytes));
        let (disk, persist_stats, dir, recovered) = match &config.persist {
            Some(persist) => {
                let opened = persist::open(persist, store, &oplog, now)?;
                (
                    Some(opened.writer),
                    Some(opened.stats),
                    Some(opened.dir),
                    opened.recovered,
                )
            }
            None => (None, None, None, Recovered::default()),
        };
        let backlog = Arc::new(Backlog::new(
            config.backlog_bytes,
            recovered.next_seq,
            recovered.runs.clone(),
        ));
        let run = new_run_id();
        let pump = Pump::new(
            oplog.clone(),
            backlog.clone(),
            disk,
            recovered.next_seq,
            run,
        );
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = stop.clone();
        let thread = std::thread::Builder::new()
            .name("ferrited-log".into())
            .spawn(move || pump.run(&thread_stop))?;
        // Attached last, so a failed start leaves the store unlogged.
        store.attach_oplog(oplog.clone());
        let log = Self {
            stop,
            thread: Some(thread),
            oplog,
            backlog,
            persist: persist_stats,
            dir,
            run,
        };
        Ok((log, recovered))
    }

    /// Stops the thread once every queued change is numbered, written and synced and any
    /// needed compaction has finished. Errors if the files may be missing changes.
    pub fn shutdown(mut self) -> io::Result<()> {
        self.stop.store(true, Relaxed);
        match self.thread.take() {
            Some(thread) => thread
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("the log thread panicked"))),
            None => Ok(()),
        }
    }
}

/// A random, nonzero run ID.
pub fn new_run_id() -> u64 {
    let mut bytes = [0u8; 8];
    getrandom::fill(&mut bytes).expect("the system's random source failed");
    u64::from_le_bytes(bytes).max(1)
}
