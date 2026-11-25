//! The log thread: drains the oplog, numbers each entry, and hands it to the disk stage and
//! then the backlog.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::Duration;

use super::Backlog;
use crate::oplog::{Change, Logged, Oplog, RunStart};
use crate::persist::writer::Writer;

/// Changes drained from the oplog per pass.
pub const BATCH: usize = 4096;
/// Sleep when the oplog was empty.
const IDLE: Duration = Duration::from_millis(1);

pub struct Pump {
    oplog: Arc<Oplog>,
    backlog: Arc<Backlog>,
    pub(crate) disk: Option<Writer>,
    next_seq: u64,
    changes: Vec<Change>,
    entries: Vec<Logged>,
}

impl Pump {
    /// Begins run `run` at sequence number `next_seq`, whose marker is its first entry.
    pub fn new(
        oplog: Arc<Oplog>,
        backlog: Arc<Backlog>,
        disk: Option<Writer>,
        next_seq: u64,
        run: u64,
    ) -> Self {
        let mut pump = Self {
            oplog,
            backlog,
            disk,
            next_seq,
            changes: Vec::with_capacity(BATCH),
            entries: Vec::with_capacity(BATCH),
        };
        pump.entries.push(Logged::Run(RunStart {
            id: run,
            first: next_seq,
        }));
        pump.publish();
        pump
    }

    /// Runs until `stop` is set, then finishes. Errors if the files may be missing changes.
    pub fn run(mut self, stop: &AtomicBool) -> io::Result<()> {
        loop {
            if stop.load(Relaxed) {
                return self.finish();
            }
            if self.step() == 0 {
                std::thread::sleep(IDLE);
            }
        }
    }

    /// One pass: numbers and publishes what the oplog holds, then lets the disk stage sync,
    /// roll and compact. Returns how many changes it drained.
    pub fn step(&mut self) -> usize {
        let mut drained = 0;
        // Logged ahead of changes queued meanwhile; links change on a scale of seconds, so order
        // within a pass does not matter.
        self.log_links();
        if self.oplog.take_lost() {
            eprintln!("ferrited: the oplog was full and changes were dropped");
            self.emit_loss();
            // Changes queued before the drop must be numbered before the covering compaction
            // starts, or replay after its snapshot could apply one over a later change the
            // snapshot captured.
            let queued = self.oplog.len();
            while drained < queued {
                let taken = self.drain((queued - drained).min(BATCH));
                if taken == 0 {
                    break;
                }
                drained += taken;
            }
        }
        drained += self.drain(BATCH);
        if self.disk.as_mut().is_some_and(|disk| disk.maintain()) {
            self.emit_loss();
        }
        drained
    }

    /// Numbers and publishes the link state replication set since the last pass, if any.
    fn log_links(&mut self) {
        if let Some(up) = self.oplog.take_links() {
            self.entries.push(Logged::Links(up));
            self.publish();
        }
    }

    /// Numbers and publishes a loss entry.
    pub(crate) fn emit_loss(&mut self) {
        self.entries.push(Logged::Loss);
        self.publish();
    }

    /// Drains up to `max` changes, then numbers and publishes them. Returns how many.
    fn drain(&mut self, max: usize) -> usize {
        self.changes.clear();
        self.oplog.drain(&mut self.changes, max);
        let count = self.changes.len();
        self.entries
            .extend(self.changes.drain(..).map(|change| match change {
                Change::Record(record) => Logged::Record(record),
                Change::Position(position) => Logged::Position(position),
            }));
        self.publish();
        count
    }

    /// Numbers the pending entries and writes them to disk before the backlog, so a sender that
    /// falls behind the backlog finds them in the files.
    fn publish(&mut self) {
        if self.entries.is_empty() {
            return;
        }
        let first = self.next_seq;
        self.next_seq += self.entries.len() as u64;
        if let Some(disk) = &mut self.disk {
            disk.write(first, &self.entries);
        }
        self.backlog.push(first, &mut self.entries);
    }

    /// Closes the oplog, publishes everything queued, then lets the disk stage finish. Errors if
    /// the files may be missing changes.
    pub fn finish(mut self) -> io::Result<()> {
        // Closing bounds the drain: later changes are dropped and noted as lost. A link state
        // set after this take is never logged, which is safe only because shutdown calls
        // `Readiness::stopping` before stopping replication and this thread.
        self.log_links();
        self.oplog.close();
        while self.drain(BATCH) > 0 {}
        // The loss entry and covering compaction come after every queued change.
        if self.oplog.take_lost() {
            self.emit_loss();
        }
        let dropped = self.oplog.dropped();
        let outcome = match &mut self.disk {
            Some(disk) => disk.finish(dropped),
            None => Ok(()),
        };
        // A change dropped after the loss was noted may be in neither log nor snapshot.
        // Shutdown stops writers first, so warn rather than lose it silently.
        let late = self.oplog.dropped() - dropped;
        if late > 0 {
            eprintln!(
                "ferrited: {late} change(s) made while the log was stopping may not be saved"
            );
        }
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backlog::Read;
    use crate::oplog::Record;

    #[test]
    fn the_latest_link_state_is_logged_once_before_the_changes_queued_with_it() {
        let oplog = Arc::new(Oplog::new(16, u64::MAX));
        let backlog = Arc::new(Backlog::new(1 << 20, 0, Vec::new()));
        let mut pump = Pump::new(oplog.clone(), backlog.clone(), None, 0, 7);
        oplog.append(Record::Flush { version: 1 });
        oplog.set_links(vec![2]);
        oplog.set_links(vec![2, 3]);
        pump.step();
        pump.step();
        let mut out = Vec::new();
        assert_eq!(backlog.read(0, 100, u64::MAX, &mut out), Read::Entries);
        assert_eq!(
            out,
            [
                Logged::Run(RunStart { id: 7, first: 0 }),
                Logged::Links(vec![2, 3]),
                Logged::Record(Record::Flush { version: 1 }),
            ]
        );
    }

    #[test]
    fn a_link_state_set_before_the_final_drain_is_logged() {
        let oplog = Arc::new(Oplog::new(16, u64::MAX));
        let backlog = Arc::new(Backlog::new(1 << 20, 0, Vec::new()));
        let pump = Pump::new(oplog.clone(), backlog.clone(), None, 0, 7);
        oplog.set_links(Vec::new());
        pump.finish().unwrap();
        let mut out = Vec::new();
        assert_eq!(backlog.read(0, 100, u64::MAX, &mut out), Read::Entries);
        assert_eq!(out.last(), Some(&Logged::Links(Vec::new())));
    }
}
