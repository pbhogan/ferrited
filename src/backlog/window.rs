//! The backlog: the newest numbered log entries, kept in memory for the peer senders.

use std::collections::VecDeque;

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::oplog::{Logged, RunStart};

/// Memory charged for an entry besides its key and value.
const ENTRY_OVERHEAD: u64 = 64;

/// Memory an entry is charged against the backlog's budget.
pub fn entry_cost(entry: &Logged) -> u64 {
    match entry {
        Logged::Record(record) => record.key_len() + record.value_len() + ENTRY_OVERHEAD,
        _ => ENTRY_OVERHEAD,
    }
}

/// What `Backlog::read` found.
#[derive(Debug, PartialEq, Eq)]
pub enum Read {
    /// Entries from the requested position on were copied out.
    Entries,
    /// The requested position is the head; nothing to read yet.
    UpToDate,
    /// The requested position has already left the backlog.
    Behind,
    /// The requested position is past the head.
    Ahead,
}

/// The newest numbered entries, bounded by bytes, and every run known.
pub struct Backlog {
    window: Mutex<Window>,
    /// The next sequence number, sent after every push to wake waiting senders.
    head: watch::Sender<u64>,
    budget: u64,
}

struct Window {
    entries: VecDeque<Logged>,
    /// Sequence number of `entries[0]`, or of the next entry when there are none.
    first: u64,
    bytes: u64,
    runs: Vec<RunStart>,
}

impl Backlog {
    /// An empty backlog whose next entry is `next`, holding up to `budget` bytes, that knows
    /// the recovered `runs`.
    pub fn new(budget: u64, next: u64, runs: Vec<RunStart>) -> Self {
        Self {
            window: Mutex::new(Window {
                entries: VecDeque::new(),
                first: next,
                bytes: 0,
                runs,
            }),
            head: watch::channel(next).0,
            budget,
        }
    }

    /// Moves in `entries`, numbered from `first`, drops the oldest down to the budget (always
    /// keeping the newest) and wakes the senders.
    pub fn push(&self, first: u64, entries: &mut Vec<Logged>) {
        if entries.is_empty() {
            return;
        }
        let next = first + entries.len() as u64;
        {
            let mut window = self.window.lock();
            assert_eq!(first, window.first + window.entries.len() as u64);
            for entry in entries.iter() {
                if let Logged::Run(run) = entry {
                    window.runs.push(*run);
                }
            }
            if self.budget == 0 {
                entries.clear();
                window.first = next;
            } else {
                for entry in entries.drain(..) {
                    window.bytes += entry_cost(&entry);
                    window.entries.push_back(entry);
                }
                while window.bytes > self.budget && window.entries.len() > 1 {
                    let oldest = window.entries.pop_front().expect("more than one entry");
                    window.bytes -= entry_cost(&oldest);
                    window.first += 1;
                }
            }
        }
        self.head.send_replace(next);
    }

    /// Copies up to `max_entries` from `from` on into `out`, stopping once they cost
    /// `max_bytes`, but always at least one.
    pub fn read(
        &self,
        from: u64,
        max_entries: usize,
        max_bytes: u64,
        out: &mut Vec<Logged>,
    ) -> Read {
        let window = self.window.lock();
        let head = window.first + window.entries.len() as u64;
        if from < window.first {
            return Read::Behind;
        }
        if from == head {
            return Read::UpToDate;
        }
        if from > head {
            return Read::Ahead;
        }
        let mut bytes = 0;
        let start = (from - window.first) as usize;
        for entry in window.entries.range(start..).take(max_entries.max(1)) {
            if bytes > 0 && bytes >= max_bytes {
                break;
            }
            bytes += entry_cost(entry);
            out.push(entry.clone());
        }
        Read::Entries
    }

    /// The sequence number the next entry will have.
    pub fn head(&self) -> u64 {
        let window = self.window.lock();
        window.first + window.entries.len() as u64
    }

    /// The sequence number of the oldest entry held, or the head when it holds none.
    pub fn first(&self) -> u64 {
        self.window.lock().first
    }

    /// Memory charged for the entries held.
    pub fn bytes(&self) -> u64 {
        self.window.lock().bytes
    }

    /// Every run known: those recovered from disk, then those begun since.
    pub fn runs(&self) -> Vec<RunStart> {
        self.window.lock().runs.clone()
    }

    /// Notified whenever entries are added. Mark the head seen (`borrow_and_update`) before
    /// calling `read`, or use `wait_for(|&head| head > next)`: marking it after `read` returns
    /// `UpToDate` can miss a push in between.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.head.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::oplog::Record;

    fn record(n: u64) -> Logged {
        Logged::Record(Record::Put {
            key: Bytes::from(format!("k{n}")),
            version: n,
            cas: n,
            flags: 0,
            expiry: 0,
            stale: false,
            value: Bytes::from(vec![b'v'; 100]),
        })
    }

    fn records(range: std::ops::Range<u64>) -> Vec<Logged> {
        range.map(record).collect()
    }

    #[test]
    fn reads_from_any_position_it_holds() {
        let backlog = Backlog::new(1 << 20, 10, Vec::new());
        let mut entries = records(0..5);
        backlog.push(10, &mut entries);
        assert!(entries.is_empty());
        assert_eq!(backlog.head(), 15);
        let mut out = Vec::new();
        assert_eq!(backlog.read(12, 10, u64::MAX, &mut out), Read::Entries);
        assert_eq!(out, records(2..5));
        assert_eq!(
            backlog.read(15, 10, u64::MAX, &mut Vec::new()),
            Read::UpToDate
        );
        assert_eq!(backlog.read(9, 10, u64::MAX, &mut Vec::new()), Read::Behind);
        assert_eq!(backlog.read(16, 10, u64::MAX, &mut Vec::new()), Read::Ahead);
    }

    #[test]
    fn reads_stop_at_the_entry_or_byte_limit_but_copy_at_least_one() {
        let backlog = Backlog::new(1 << 20, 0, Vec::new());
        backlog.push(0, &mut records(0..10));
        let mut out = Vec::new();
        backlog.read(0, 3, u64::MAX, &mut out);
        assert_eq!(out.len(), 3);
        out.clear();
        backlog.read(0, 100, 2 * entry_cost(&record(0)), &mut out);
        assert_eq!(out.len(), 2);
        out.clear();
        backlog.read(0, 100, 1, &mut out);
        assert_eq!(out.len(), 1);
        out.clear();
        backlog.read(0, 0, 0, &mut out);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn the_oldest_entries_leave_once_over_budget() {
        let cost = entry_cost(&record(0));
        let backlog = Backlog::new(3 * cost, 0, Vec::new());
        backlog.push(0, &mut records(0..5));
        assert_eq!(
            (backlog.first(), backlog.head(), backlog.bytes()),
            (2, 5, 3 * cost)
        );
    }

    #[test]
    fn an_entry_larger_than_the_budget_stays_until_the_next_arrives() {
        let backlog = Backlog::new(10, 0, Vec::new());
        backlog.push(0, &mut records(0..1));
        assert_eq!((backlog.first(), backlog.head()), (0, 1));
        backlog.push(1, &mut records(1..2));
        assert_eq!((backlog.first(), backlog.head()), (1, 2));
    }

    #[test]
    fn a_zero_budget_keeps_nothing_but_counts_and_learns_runs() {
        let recovered = RunStart { id: 1, first: 0 };
        let run = RunStart { id: 3, first: 8 };
        let backlog = Backlog::new(0, 7, vec![recovered]);
        backlog.push(7, &mut vec![record(0), Logged::Run(run)]);
        assert_eq!(
            (backlog.first(), backlog.head(), backlog.bytes()),
            (9, 9, 0)
        );
        assert_eq!(backlog.runs(), [recovered, run]);
    }

    #[tokio::test]
    async fn a_push_wakes_a_waiting_reader() {
        let backlog = Arc::new(Backlog::new(1 << 20, 0, Vec::new()));
        let mut head = backlog.subscribe();
        let waiter = tokio::spawn(async move { *head.wait_for(|&head| head > 0).await.unwrap() });
        let pusher = backlog.clone();
        std::thread::spawn(move || pusher.push(0, &mut records(0..1)))
            .join()
            .unwrap();
        let head = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head, 1);
    }
}
