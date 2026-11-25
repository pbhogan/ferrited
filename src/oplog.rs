//! The in-memory stream of change records from the store to its subscribers.
//!
//! The store appends one record per successful mutation while it still holds the key's shard
//! lock, so the records for one key are queued in the order their versions were taken.

use std::sync::atomic::{
    AtomicBool, AtomicU64,
    Ordering::{Acquire, Relaxed, Release},
};

use bytes::Bytes;
use crossbeam_queue::ArrayQueue;

/// One change, describing the resulting state rather than the operation, so applying a record
/// twice or out of order with respect to other keys gives the same result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    /// The complete state of one item after a set, touch, arithmetic, vivify or invalidate.
    Put {
        key: Bytes,
        version: u64,
        cas: u64,
        flags: u32,
        expiry: u64,
        stale: bool,
        value: Bytes,
    },
    /// The key was deleted by a change with this version.
    Delete { key: Bytes, version: u64 },
    /// Every item with a version below this one is flushed.
    Flush { version: u64 },
}

impl Record {
    pub fn version(&self) -> u64 {
        match self {
            Record::Put { version, .. }
            | Record::Delete { version, .. }
            | Record::Flush { version } => *version,
        }
    }

    /// Value bytes, which count toward the oplog's byte budget.
    #[inline]
    pub fn value_len(&self) -> u64 {
        match self {
            Record::Put { value, .. } => value.len() as u64,
            Record::Delete { .. } | Record::Flush { .. } => 0,
        }
    }

    /// Length of the record's key, or 0 for a flush.
    #[inline]
    pub fn key_len(&self) -> u64 {
        match self {
            Record::Put { key, .. } | Record::Delete { key, .. } => key.len() as u64,
            Record::Flush { .. } => 0,
        }
    }
}

/// Where a receiver has got to in a sender's log: it has applied every entry of run `run`
/// numbered below `next`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Position {
    /// The sender's node ID.
    pub peer: u8,
    pub run: u64,
    pub next: u64,
}

/// The start of a run: the log written by one process lifetime of a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunStart {
    pub id: u64,
    /// Sequence number of the run's marker, its first entry.
    pub first: u64,
}

/// One numbered entry of a node's log, as the backlog holds it and the files store it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Logged {
    Record(Record),
    /// Changes were lost here: dropped before they were numbered, or not written to disk.
    Loss,
    /// The first entry of a run.
    Run(RunStart),
    /// This node's position on a peer's log, logged after the records it covers.
    Position(Position),
    /// Peers this node had a live link with in either direction, ascending by node ID. Logged
    /// only while serving: on becoming ready and on each change. The last one tells a restart
    /// whether every peer was down when this node last served ("last node standing").
    Links(Vec<u8>),
}

/// What the oplog queues: a change to the store, or this node's position on a peer's log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    Record(Record),
    Position(Position),
}

impl Change {
    /// Value bytes, which count toward the oplog's byte budget.
    #[inline]
    fn value_len(&self) -> u64 {
        match self {
            Change::Record(record) => record.value_len(),
            Change::Position(_) => 0,
        }
    }
}

/// A queue of records bounded in count and value bytes. Appending never blocks: when full, over
/// budget or closed, the record is dropped, counted and reported as lost. Queued values share
/// the store's buffers and can outlive their items, so the byte budget bounds that memory.
pub struct Oplog {
    queue: ArrayQueue<Change>,
    /// Value bytes queued. A dropped record adds its size briefly before taking it back.
    bytes: AtomicU64,
    byte_budget: u64,
    dropped: AtomicU64,
    positions_dropped: AtomicU64,
    lost: AtomicBool,
    closed: AtomicBool,
    /// The latest link state replication has set and the log thread has not yet logged.
    links: parking_lot::Mutex<Option<Vec<u8>>>,
    /// Whether `links` holds a state to log, so the log thread checks with one atomic load.
    links_pending: AtomicBool,
}

impl Oplog {
    /// An oplog that holds up to `capacity` records and `byte_budget` bytes of value.
    pub fn new(capacity: usize, byte_budget: u64) -> Self {
        Self {
            queue: ArrayQueue::new(capacity.max(1)),
            bytes: AtomicU64::new(0),
            byte_budget,
            dropped: AtomicU64::new(0),
            positions_dropped: AtomicU64::new(0),
            lost: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            links: parking_lot::Mutex::new(None),
            links_pending: AtomicBool::new(false),
        }
    }

    /// Queues a record, or drops it if the queue is full, over its byte budget, or closed.
    #[inline]
    pub fn append(&self, record: Record) {
        self.push(Change::Record(record));
    }

    /// Queues this node's position on a peer's log behind every earlier record, returning
    /// whether it was queued. A dropped position is counted but is not a loss, since a later
    /// position covers the same records.
    pub fn append_position(&self, position: Position) -> bool {
        let queued =
            !self.closed.load(Relaxed) && self.queue.push(Change::Position(position)).is_ok();
        if !queued {
            self.positions_dropped.fetch_add(1, Relaxed);
        }
        queued
    }

    /// Asks the log thread to log `up` as this node's linked peers. Only the latest state is
    /// kept, outside the queue, so it is never dropped. Ignored once closed.
    pub fn set_links(&self, up: Vec<u8>) {
        if self.closed.load(Relaxed) {
            return;
        }
        *self.links.lock() = Some(up);
        self.links_pending.store(true, Release);
    }

    /// The link state set since the last call, if any.
    pub fn take_links(&self) -> Option<Vec<u8>> {
        if !self.links_pending.swap(false, Acquire) {
            return None;
        }
        self.links.lock().take()
    }

    /// Whether the queue is under half full in both count and bytes; an empty queue always has
    /// room. Replication receivers wait on this so a full sync cannot overflow the oplog. Check
    /// per chunk: one ~1 MiB frame of small records can exceed half the capacity.
    pub fn has_room(&self) -> bool {
        self.queue.len() * 2 < self.queue.capacity().max(2)
            && self.bytes.load(Relaxed).saturating_mul(2) < self.byte_budget.max(2)
    }

    #[inline]
    fn push(&self, change: Change) {
        let size = change.value_len();
        let queued = if size == 0 {
            !self.closed.load(Relaxed) && self.queue.push(change).is_ok()
        } else if self.bytes.fetch_add(size, Relaxed) + size > self.byte_budget
            || self.closed.load(Relaxed)
            || self.queue.push(change).is_err()
        {
            self.bytes.fetch_sub(size, Relaxed);
            false
        } else {
            true
        };
        if !queued {
            self.dropped.fetch_add(1, Relaxed);
            self.lost.store(true, Relaxed);
        }
    }

    /// Stops queuing: later appends are dropped and reported as lost. Queued records can still
    /// be drained.
    pub fn close(&self) {
        self.closed.store(true, Relaxed);
    }

    /// Moves up to `max` changes, oldest first, into `out`.
    pub fn drain(&self, out: &mut Vec<Change>, max: usize) {
        let mut released = 0;
        for _ in 0..max {
            match self.queue.pop() {
                Some(change) => {
                    released += change.value_len();
                    out.push(change);
                }
                None => break,
            }
        }
        if released > 0 {
            self.bytes.fetch_sub(released, Relaxed);
        }
    }

    /// Whether any record was dropped since the last call.
    pub fn take_lost(&self) -> bool {
        self.lost.swap(false, Relaxed)
    }

    /// Records dropped since the oplog was created.
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Relaxed)
    }

    /// Replication positions dropped since the oplog was created.
    pub fn positions_dropped(&self) -> u64 {
        self.positions_dropped.load(Relaxed)
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Value bytes queued.
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Relaxed)
    }

    pub fn byte_budget(&self) -> u64 {
        self.byte_budget
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flush(version: u64) -> Record {
        Record::Flush { version }
    }

    fn changes<const N: usize>(records: [Record; N]) -> Vec<Change> {
        records.into_iter().map(Change::Record).collect()
    }

    #[test]
    fn only_the_latest_link_state_is_taken_and_only_once() {
        let oplog = Oplog::new(1, u64::MAX);
        assert_eq!(oplog.take_links(), None);
        // A full queue does not stop it, and it is not a dropped change.
        oplog.append(flush(1));
        oplog.set_links(vec![2]);
        oplog.set_links(vec![2, 3]);
        assert_eq!(oplog.take_links(), Some(vec![2, 3]));
        assert_eq!(oplog.take_links(), None);
        assert_eq!(oplog.dropped(), 0);
        // Once closed, nothing more is taken.
        oplog.close();
        oplog.set_links(vec![4]);
        assert_eq!(oplog.take_links(), None);
    }

    #[test]
    fn drains_in_order_and_up_to_the_limit() {
        let oplog = Oplog::new(8, u64::MAX);
        for version in 1..=5 {
            oplog.append(flush(version));
        }
        let mut out = Vec::new();
        oplog.drain(&mut out, 3);
        assert_eq!(out, changes([flush(1), flush(2), flush(3)]));
        oplog.drain(&mut out, 10);
        assert_eq!(out.len(), 5);
        assert!(oplog.is_empty());
    }

    #[test]
    fn an_empty_queue_has_room_whatever_its_limits() {
        for (capacity, budget) in [(1, u64::MAX), (8, 1), (1, 1)] {
            let oplog = Oplog::new(capacity, budget);
            assert!(oplog.has_room(), "{capacity} {budget}");
        }
        let oplog = Oplog::new(1, u64::MAX);
        oplog.append(flush(1));
        assert!(!oplog.has_room());
        let mut out = Vec::new();
        oplog.drain(&mut out, 10);
        assert!(oplog.has_room());
    }

    #[test]
    fn a_full_queue_drops_and_reports_the_loss_once() {
        let oplog = Oplog::new(2, u64::MAX);
        for version in 1..=4 {
            oplog.append(flush(version));
        }
        assert_eq!(oplog.dropped(), 2);
        assert!(oplog.take_lost());
        assert!(!oplog.take_lost());
        let mut out = Vec::new();
        oplog.drain(&mut out, 10);
        assert_eq!(out, changes([flush(1), flush(2)]));
    }

    #[test]
    fn appends_after_close_are_dropped_and_reported() {
        let oplog = Oplog::new(8, u64::MAX);
        oplog.append(flush(1));
        oplog.close();
        oplog.append(flush(2));
        assert_eq!(oplog.dropped(), 1);
        assert!(oplog.take_lost());
        let mut out = Vec::new();
        oplog.drain(&mut out, 10);
        assert_eq!(out, changes([flush(1)]));
    }

    fn put(key: &str, size: usize) -> Record {
        Record::Put {
            key: Bytes::from(key.to_string()),
            version: 1,
            cas: 1,
            flags: 0,
            expiry: 0,
            stale: false,
            value: Bytes::from(vec![b'v'; size]),
        }
    }

    #[test]
    fn appends_over_the_byte_budget_are_dropped_and_reported() {
        let oplog = Oplog::new(8, 100);
        oplog.append(put("a", 60));
        // 120 bytes would be over the budget, so this one is dropped.
        oplog.append(put("b", 60));
        // Records without a value are not limited by the budget.
        oplog.append(flush(2));
        oplog.append(put("c", 40));
        assert_eq!(oplog.dropped(), 1);
        assert!(oplog.take_lost());
        assert_eq!(oplog.bytes(), 100);
        let mut out = Vec::new();
        oplog.drain(&mut out, 10);
        assert_eq!(out, changes([put("a", 60), flush(2), put("c", 40)]));
        // Draining releases the bytes, so the budget is available again.
        assert_eq!(oplog.bytes(), 0);
        oplog.append(put("b", 60));
        assert_eq!((oplog.dropped(), oplog.len()), (1, 1));
    }

    #[test]
    fn a_drop_for_a_full_queue_or_close_gives_back_its_bytes() {
        let oplog = Oplog::new(1, 1000);
        oplog.append(put("a", 10));
        oplog.append(put("b", 20));
        assert_eq!((oplog.dropped(), oplog.bytes()), (1, 10));
        oplog.close();
        oplog.append(put("c", 30));
        assert_eq!((oplog.dropped(), oplog.bytes()), (2, 10));
    }

    #[test]
    fn has_room_until_half_full_in_count_or_bytes() {
        let oplog = Oplog::new(4, 1000);
        assert!(oplog.has_room());
        oplog.append(flush(1));
        assert!(oplog.has_room());
        oplog.append(flush(2));
        assert!(!oplog.has_room());
        let oplog = Oplog::new(100, 1000);
        oplog.append(put("a", 500));
        assert!(!oplog.has_room());
    }

    #[test]
    fn positions_queue_in_order_with_records() {
        let oplog = Oplog::new(8, u64::MAX);
        let position = Position {
            peer: 2,
            run: 1,
            next: 10,
        };
        oplog.append(flush(1));
        oplog.append_position(position);
        oplog.append(flush(2));
        let mut out = Vec::new();
        oplog.drain(&mut out, 10);
        assert_eq!(
            out,
            [
                Change::Record(flush(1)),
                Change::Position(position),
                Change::Record(flush(2))
            ]
        );
    }

    #[test]
    fn a_dropped_position_is_counted_but_is_not_a_loss() {
        let oplog = Oplog::new(1, u64::MAX);
        let position = Position {
            peer: 2,
            run: 1,
            next: 10,
        };
        oplog.append(flush(1));
        assert!(!oplog.append_position(position));
        assert_eq!((oplog.positions_dropped(), oplog.dropped()), (1, 0));
        assert!(!oplog.take_lost());
        let mut out = Vec::new();
        oplog.drain(&mut out, 10);
        assert!(oplog.append_position(position));
        oplog.close();
        assert!(!oplog.append_position(position));
        assert_eq!(oplog.positions_dropped(), 2);
        assert!(!oplog.take_lost());
    }

    #[test]
    fn version_reads_every_kind() {
        let key = Bytes::from_static(b"k");
        let put = Record::Put {
            key: key.clone(),
            version: 7,
            cas: 7,
            flags: 0,
            expiry: 0,
            stale: false,
            value: Bytes::new(),
        };
        assert_eq!(put.version(), 7);
        assert_eq!(Record::Delete { key, version: 8 }.version(), 8);
        assert_eq!(flush(9).version(), 9);
    }
}
