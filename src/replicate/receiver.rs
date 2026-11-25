//! The receiving side: the replication listener, and a task per inbound link that applies a
//! peer's records and logs this node's position on the peer's log.

use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;

use super::Shared;
use super::command::{self, Sniffed};
use super::frame::{self, FRAME_MAX, Frame, Hello, Signing};
use super::handshake::{self, Me};
use super::link::{self, Frames, HEARTBEAT, SILENCE};
use super::ready::Readiness;
use super::status::LinkState;
use crate::clock::unix_secs;
use crate::oplog::{Position, Record};
use crate::store::Applied;

/// Least time between position entries for one link.
const POSITION_INTERVAL: Duration = Duration::from_millis(100);
/// How far a position may move with no record applied before it is logged anyway.
const POSITION_DRIFT: u64 = 1024;
/// Pause between checks for room in the oplog.
const ROOM_WAIT: Duration = Duration::from_millis(1);
/// Least time between acknowledgments.
const ACK_INTERVAL: Duration = Duration::from_millis(50);
/// Most inbound connections open at once, handshaking or linked; more are closed at once.
const MAX_INBOUND: usize = 1024;
/// Least time between reports of connections closed past `MAX_INBOUND`.
const REFUSED_REPORT_INTERVAL: Duration = Duration::from_secs(10);
/// Pause after a failed accept, such as one out of file descriptors.
const ACCEPT_RETRY: Duration = Duration::from_millis(100);
/// Least time between reports of the same handshake failure from the same host.
const FAILURE_REPORT_INTERVAL: Duration = Duration::from_secs(60);
/// Most distinct (host, reason) pairs remembered for `FAILURE_REPORT_INTERVAL`.
const FAILURES_KEPT: usize = 1024;

/// Accepts links until replication stops, then waits for every receiving link to finish.
pub(super) async fn listen(listener: TcpListener, shared: Arc<Shared>) {
    listen_with(listener, shared, MAX_INBOUND).await;
}

/// `listen` with at most `limit` inbound connections open at once.
async fn listen_with(listener: TcpListener, shared: Arc<Shared>, limit: usize) {
    let mut links = JoinSet::new();
    let mut stop = shared.stop.clone();
    let slots = Arc::new(Semaphore::new(limit));
    let mut refused = Refused::default();
    let failures = Arc::new(Mutex::new(Failures::default()));
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, address)) => {
                    while links.try_join_next().is_some() {}
                    match slots.clone().try_acquire_owned() {
                        Ok(slot) => {
                            let _ = stream.set_nodelay(true);
                            let failures = failures.clone();
                            links.spawn(receive(stream, address, shared.clone(), failures, slot));
                        }
                        Err(_) => refused.note(limit),
                    }
                }
                Err(error) => {
                    eprintln!("replication: accept failed: {error}");
                    tokio::select! {
                        _ = tokio::time::sleep(ACCEPT_RETRY) => {}
                        _ = link::stopped(&mut stop) => break,
                    }
                }
            },
            _ = link::stopped(&mut stop) => break,
        }
    }
    drop(listener);
    while links.join_next().await.is_some() {}
}

/// Answers on `listener` while this node recovers, until `done`, then returns it. Handshakes
/// complete with a hello that says this node is recovering and then close, so peers know it is
/// alive. Failures are not logged: they recur, and are logged, once this node has recovered.
pub(super) async fn answer_while_recovering(
    listener: TcpListener,
    me: Me,
    readiness: Arc<Readiness>,
    mut done: watch::Receiver<bool>,
) -> TcpListener {
    let mut answers = JoinSet::new();
    let slots = Arc::new(Semaphore::new(MAX_INBOUND));
    let mut refused = Refused::default();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, address)) => {
                    while answers.try_join_next().is_some() {}
                    match slots.clone().try_acquire_owned() {
                        Ok(slot) => {
                            let _ = stream.set_nodelay(true);
                            let answer =
                                answer_recovering(stream, address, me.clone(), readiness.clone(), slot);
                            answers.spawn(answer);
                        }
                        Err(_) => refused.note(MAX_INBOUND),
                    }
                }
                Err(error) => {
                    eprintln!("replication: accept failed: {error}");
                    tokio::select! {
                        _ = tokio::time::sleep(ACCEPT_RETRY) => {}
                        _ = link::stopped(&mut done) => break,
                    }
                }
            },
            _ = link::stopped(&mut done) => break,
        }
    }
    // Dropping `answers` aborts any answer still running; the peer asks again.
    listener
}

/// Answers one connection while this node recovers.
async fn answer_recovering(
    mut stream: TcpStream,
    address: SocketAddr,
    me: Me,
    readiness: Arc<Readiness>,
    _slot: OwnedSemaphorePermit,
) {
    match command::sniff(&stream).await {
        Sniffed::Status => {
            command::answer(stream, address, &readiness, me.secret.is_some()).await;
        }
        Sniffed::Frames => {
            // Proofs included, so the peer can trust the answer.
            let _ = handshake::accept(&mut stream, &me, |_| Ok(()), |_| None).await;
        }
        Sniffed::Nothing => {}
    }
}

/// Connections closed past the limit, reported at most every `REFUSED_REPORT_INTERVAL`.
#[derive(Default)]
struct Refused {
    unreported: u64,
    reported_at: Option<Instant>,
}

impl Refused {
    fn note(&mut self, limit: usize) {
        self.unreported += 1;
        if self
            .reported_at
            .is_none_or(|at| at.elapsed() >= REFUSED_REPORT_INTERVAL)
        {
            eprintln!(
                "replication: closed {} inbound connection(s) past the limit of {limit} open at once",
                self.unreported
            );
            self.unreported = 0;
            self.reported_at = Some(Instant::now());
        }
    }
}

/// Handshake failures per host and reason: the first is reported at once, repeats at most
/// every `FAILURE_REPORT_INTERVAL` with a count, so a peer with the wrong secret does not log a
/// line per attempt.
#[derive(Default)]
struct Failures {
    seen: HashMap<(IpAddr, String), Repeats>,
}

struct Repeats {
    reported_at: Instant,
    unreported: u64,
}

impl Failures {
    /// Notes a failure and returns the line to log, if one is due.
    fn note(&mut self, address: SocketAddr, reason: String, now: Instant) -> Option<String> {
        let due = |at: Instant| now.saturating_duration_since(at) >= FAILURE_REPORT_INTERVAL;
        if self.seen.len() >= FAILURES_KEPT {
            self.seen.retain(|_, repeats| !due(repeats.reported_at));
        }
        let line = format!("replication: link from {address}: {reason}");
        let key = (address.ip(), reason);
        match self.seen.get_mut(&key) {
            None => {
                // Past the bound, a new pair is reported but not remembered.
                if self.seen.len() < FAILURES_KEPT {
                    let repeats = Repeats {
                        reported_at: now,
                        unreported: 0,
                    };
                    self.seen.insert(key, repeats);
                }
                Some(line)
            }
            Some(repeats) if due(repeats.reported_at) => {
                let unreported = std::mem::take(&mut repeats.unreported);
                let since = now.saturating_duration_since(repeats.reported_at).as_secs();
                repeats.reported_at = now;
                Some(match unreported {
                    0 => line,
                    _ => format!("{line} ({unreported} more like it in the last {since}s)"),
                })
            }
            Some(repeats) => {
                repeats.unreported += 1;
                None
            }
        }
    }
}

/// Holds a node's place as its one live inbound link until dropped.
struct Inbound {
    shared: Arc<Shared>,
    node_id: u8,
}

impl Drop for Inbound {
    fn drop(&mut self) {
        // Before freeing the place: a new link from the peer could otherwise register first,
        // and this `inbound_down` would remove its progress.
        self.shared
            .readiness
            .inbound_down(self.node_id, Instant::now());
        self.shared.inbound.lock().remove(&self.node_id);
        self.shared
            .status
            .set_inbound(self.node_id, LinkState::Down);
    }
}

fn register(shared: &Arc<Shared>, hello: &Hello) -> Result<Inbound, String> {
    if !shared.inbound.lock().insert(hello.node_id) {
        return Err(format!(
            "node {} already has a live link to this node",
            hello.node_id
        ));
    }
    shared
        .status
        .set_inbound(hello.node_id, LinkState::Streaming);
    shared.readiness.inbound_up(hello.node_id, hello.head);
    Ok(Inbound {
        shared: shared.clone(),
        node_id: hello.node_id,
    })
}

/// Shakes hands with a connecting sender and applies what it sends until the link ends.
async fn receive(
    mut stream: TcpStream,
    address: SocketAddr,
    shared: Arc<Shared>,
    failures: Arc<Mutex<Failures>>,
    _slot: OwnedSemaphorePermit,
) {
    let mut stop = shared.stop.clone();
    let sniffed = tokio::select! {
        sniffed = command::sniff(&stream) => sniffed,
        _ = link::stopped(&mut stop) => return,
    };
    match sniffed {
        Sniffed::Status => {
            let secret = shared.config.secret.is_some();
            // So a client that never ends its line cannot hold up shutdown.
            tokio::select! {
                () = command::answer(stream, address, &shared.readiness, secret) => {}
                _ = link::stopped(&mut stop) => {}
            }
            return;
        }
        Sniffed::Nothing => return,
        Sniffed::Frames => {}
    }
    let me = shared.me();
    let mut offered = None;
    let handshake = handshake::accept(
        &mut stream,
        &me,
        |hello| register(&shared, hello),
        |peer| {
            offered = shared.positions.lock().get(&peer).copied();
            offered.map(|position| (position.run, position.next))
        },
    );
    let accepted = tokio::select! {
        accepted = handshake => accepted,
        _ = link::stopped(&mut stop) => return,
    };
    let (hello, _inbound, keys) = match accepted {
        Ok(accepted) => accepted,
        Err(failure) => {
            let line = failures
                .lock()
                .note(address, failure.to_string(), Instant::now());
            if let Some(line) = line {
                eprintln!("{line}");
            }
            return;
        }
    };
    eprintln!(
        "replication: receiving from node {} at {address}",
        hello.node_id
    );
    // In the sender's current run it resumes exactly at the offered position, so the link
    // starts there and acknowledges it at once. From an earlier run the sender may resume
    // before it, so the link waits for the first batch.
    let resumed = offered.filter(|position| position.run == hello.run);
    let (read, write) = stream.into_split();
    let (signing, checking) = keys.map(|keys| (keys.send, keys.receive)).unzip();
    let frames = Frames::spawn(read, FRAME_MAX, checking);
    let mut link = Link::new(shared, hello.node_id, hello.run, resumed, write, signing);
    link.serve(frames).await;
}

enum Event {
    Frame(io::Result<Frame>),
    Tick,
    Stop,
}

struct Link {
    shared: Arc<Shared>,
    peer: u8,
    run: u64,
    write: OwnedWriteHalf,
    signing: Option<Signing>,
    buf: Vec<u8>,
    stop: watch::Receiver<bool>,
    /// This node's position on the peer's log, once known.
    position: Option<Position>,
    logged: Option<Position>,
    logged_at: Option<Instant>,
    /// A due position was dropped by a full oplog; log it at the next chance.
    log_retry: bool,
    acked: Option<u64>,
    acked_at: Option<Instant>,
    written_at: Instant,
    syncing: bool,
    applied_since_log: bool,
}

impl Link {
    /// A link from `peer`'s run `run`, starting from `resumed`. If `resumed` is not the last
    /// position logged for `peer` (a full oplog dropped it), it is logged at the first chance.
    fn new(
        shared: Arc<Shared>,
        peer: u8,
        run: u64,
        resumed: Option<Position>,
        write: OwnedWriteHalf,
        signing: Option<Signing>,
    ) -> Self {
        let logged = shared.logged.lock().get(&peer).copied();
        if let Some(resumed) = resumed {
            shared.readiness.applied(peer, resumed.next);
        }
        Self {
            stop: shared.stop.clone(),
            shared,
            peer,
            run,
            write,
            signing,
            buf: Vec::new(),
            position: resumed,
            logged,
            logged_at: None,
            log_retry: resumed.is_some() && resumed != logged,
            acked: None,
            acked_at: None,
            written_at: Instant::now(),
            syncing: false,
            applied_since_log: false,
        }
    }

    /// Applies frames until the link fails or replication stops, then logs the position.
    async fn serve(&mut self, frames: Frames) {
        let result = self.run(frames).await;
        self.log_position(true);
        if let Err(error) = &result
            && frame::is_bad_tag(error)
        {
            self.shared.status.bad_frames.fetch_add(1, Relaxed);
        }
        match result {
            Err(error) if !link::is_stopping(&error) => eprintln!(
                "replication: the link from node {} ended: {error}",
                self.peer
            ),
            _ => {}
        }
    }

    /// Applies frames until the link fails, or fails with `link::stopping` on stop.
    async fn run(&mut self, mut frames: Frames) -> io::Result<()> {
        let mut tick = tokio::time::interval(HEARTBEAT);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut heard = Instant::now();
        loop {
            let event = tokio::select! {
                frame = frames.recv() => Event::Frame(frame),
                _ = tick.tick() => Event::Tick,
                _ = link::stopped(&mut self.stop) => Event::Stop,
            };
            match event {
                Event::Frame(frame) => {
                    self.handle(frame?).await?;
                    // Handling may have waited long for oplog room while the peer's frames
                    // queued behind it, so silence counts from now.
                    heard = Instant::now();
                }
                Event::Tick => {
                    // A frame waiting to be handled means the peer is not silent.
                    if heard.elapsed() > SILENCE && !frames.is_waiting() {
                        return Err(link::timed_out("the peer went silent"));
                    }
                    self.log_position(false);
                }
                Event::Stop => return Err(link::stopping()),
            }
            self.reply().await?;
        }
    }

    /// Fails with `link::stopping` if replication stopped before every record was applied,
    /// so the position must not move past them.
    async fn handle(&mut self, frame: Frame) -> io::Result<()> {
        match frame {
            Frame::Batch { next, records } => {
                if self.syncing {
                    return Err(link::invalid("a batch arrived during a full sync".into()));
                }
                self.apply(&records).await?;
                self.advance(next);
                self.log_position(false);
            }
            Frame::SyncStart => {
                self.syncing = true;
                self.shared.status.full_syncs_received.fetch_add(1, Relaxed);
                self.shared
                    .status
                    .set_inbound(self.peer, LinkState::Syncing);
            }
            Frame::SyncItems(records) => {
                if !self.syncing {
                    return Err(link::invalid(
                        "sync items arrived outside a full sync".into(),
                    ));
                }
                self.apply(&records).await?;
            }
            Frame::SyncEnd { next } => {
                if !self.syncing {
                    return Err(link::invalid(
                        "a sync end arrived outside a full sync".into(),
                    ));
                }
                self.syncing = false;
                self.shared
                    .status
                    .set_inbound(self.peer, LinkState::Streaming);
                self.advance(next);
                self.log_position(true);
            }
            Frame::Heartbeat => {}
            other => {
                return Err(link::invalid(format!(
                    "unexpected {} from a sender",
                    other.name()
                )));
            }
        }
        Ok(())
    }

    /// Waits for oplog room. A full sync applied at network speed would otherwise overflow it,
    /// and the resulting loss entry would make this node full-sync all its own peers. Keeps
    /// heartbeating, so the sender knows this link is alive however long the disk stalls.
    async fn wait_for_room(&mut self) -> io::Result<()> {
        while !self.shared.oplog.has_room() {
            if *self.stop.borrow() {
                return Err(link::stopping());
            }
            self.reply().await?;
            tokio::time::sleep(ROOM_WAIT).await;
        }
        Ok(())
    }

    /// Applies `records` in chunks of at most a quarter of the oplog's capacity and byte budget
    /// (or one record), waiting for room before each, since one frame can exceed the room.
    async fn apply(&mut self, records: &[Record]) -> io::Result<()> {
        let max_count = (self.shared.oplog.capacity() / 4).max(1);
        let max_bytes = self.shared.oplog.byte_budget() / 4;
        let mut rest = records;
        while !rest.is_empty() {
            let (chunk, after) = rest.split_at(chunk_len(rest, max_count, max_bytes));
            rest = after;
            self.wait_for_room().await?;
            self.applied_since_log = true;
            let now = unix_secs();
            let status = &self.shared.status;
            for record in chunk {
                let counter = match self.shared.store.apply_remote(record, now) {
                    Applied::Applied => &status.records_applied,
                    Applied::Stale => &status.records_stale,
                    Applied::Oversized => &status.records_oversized,
                };
                counter.fetch_add(1, Relaxed);
            }
        }
        Ok(())
    }

    /// Every entry of the peer's log below `next` is applied.
    fn advance(&mut self, next: u64) {
        let position = Position {
            peer: self.peer,
            run: self.run,
            next,
        };
        self.position = Some(position);
        self.shared.positions.lock().insert(self.peer, position);
        self.shared.readiness.applied(self.peer, next);
    }

    /// Logs the position when `force`d, or at most every `POSITION_INTERVAL` once records were
    /// applied or it drifted `POSITION_DRIFT`. An empty batch alone does not earn an entry, or
    /// two idle nodes would log each other's positions forever. The position follows the records
    /// it covers in the log. One a full oplog drops is retried at least every `HEARTBEAT`.
    fn log_position(&mut self, force: bool) {
        let Some(position) = self.position else {
            return;
        };
        if self.logged == Some(position) {
            return;
        }
        let due = self
            .logged_at
            .is_none_or(|at| at.elapsed() >= POSITION_INTERVAL);
        let drifted = self.logged.is_none_or(|logged| {
            logged.run != position.run || position.next >= logged.next + POSITION_DRIFT
        });
        if !(force || self.log_retry || (due && (self.applied_since_log || drifted))) {
            return;
        }
        if !self.shared.oplog.append_position(position) {
            self.log_retry = true;
            return;
        }
        self.logged = Some(position);
        self.shared.logged.lock().insert(self.peer, position);
        self.logged_at = Some(Instant::now());
        self.log_retry = false;
        self.applied_since_log = false;
    }

    /// Acknowledges at most every `ACK_INTERVAL`, else heartbeats after `HEARTBEAT` of quiet.
    /// Fails with `link::stopping` if replication stops while the peer is not reading.
    async fn reply(&mut self) -> io::Result<()> {
        let next = self.position.map(|position| position.next);
        let ack_due = self.acked_at.is_none_or(|at| at.elapsed() >= ACK_INTERVAL);
        let frame = match next {
            Some(next) if Some(next) != self.acked && ack_due => {
                self.acked = Some(next);
                self.acked_at = Some(Instant::now());
                Frame::Ack { next }
            }
            _ if self.written_at.elapsed() >= HEARTBEAT => Frame::Heartbeat,
            _ => return Ok(()),
        };
        tokio::select! {
            sent = link::send(&mut self.write, &frame, &mut self.buf, self.signing.as_mut()) => sent?,
            _ = link::stopped(&mut self.stop) => return Err(link::stopping()),
        }
        self.written_at = Instant::now();
        Ok(())
    }
}

/// How many leading `records` fit in `max_count` and `max_bytes` of value, at least one.
fn chunk_len(records: &[Record], max_count: usize, max_bytes: u64) -> usize {
    let mut bytes = 0;
    records
        .iter()
        .take(max_count)
        .take_while(|record| {
            bytes += record.value_len();
            bytes <= max_bytes
        })
        .count()
        .max(1)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::backlog::Backlog;
    use crate::clock::Clock;
    use crate::oplog::{Change, Oplog};
    use crate::replicate::frame::{self, HANDSHAKE_FRAME_MAX, Signing};
    use crate::replicate::handshake::{Accepted, Failure, Keys, Me};
    use crate::replicate::test_shared;
    use crate::store::{GetArgs, Store, StoreConfig};

    struct Receiving {
        shared: Arc<Shared>,
        store: Arc<Store>,
        oplog: Arc<Oplog>,
        stop: watch::Sender<bool>,
        listening: tokio::task::JoinHandle<()>,
        address: SocketAddr,
    }

    /// Node 2, listening, with tombstones and an oplog of its own.
    async fn receiving() -> Receiving {
        receiving_with(1024).await
    }

    /// Node 2 with an oplog of `capacity` changes.
    async fn receiving_with(capacity: usize) -> Receiving {
        receiving_with_budget(capacity, u64::MAX).await
    }

    /// Node 2 with an oplog of `capacity` changes and `byte_budget` bytes of value.
    async fn receiving_with_budget(capacity: usize, byte_budget: u64) -> Receiving {
        receiving_node(capacity, byte_budget, None).await
    }

    /// Node 2 with the secret `SECRET`.
    async fn receiving_signed() -> Receiving {
        receiving_node(1024, u64::MAX, Some(SECRET.to_vec())).await
    }

    /// Node 2 with the given oplog limits and secret.
    async fn receiving_node(
        capacity: usize,
        byte_budget: u64,
        secret: Option<Vec<u8>>,
    ) -> Receiving {
        let config = StoreConfig {
            memory_limit: 1 << 24,
            item_max: 1 << 20,
            shards: 4,
        };
        let store = Arc::new(Store::new(config, Arc::new(Clock::new(2))));
        store.enable_tombstones(3600, usize::MAX);
        let oplog = Arc::new(Oplog::new(capacity, byte_budget));
        store.attach_oplog(oplog.clone());
        let backlog = Arc::new(Backlog::new(1 << 20, 0, Vec::new()));
        let (stop, stop_receiver) = watch::channel(false);
        let shared = test_shared(
            2,
            store.clone(),
            oplog.clone(),
            backlog,
            stop_receiver,
            secret,
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let listening = tokio::spawn(listen(listener, shared.clone()));
        Receiving {
            shared,
            store,
            oplog,
            stop,
            listening,
            address,
        }
    }

    /// Connects as node 1, run 5, and shakes hands.
    async fn sender(address: SocketAddr) -> Result<TcpStream, Failure> {
        Ok(sender_accepted(address).await?.0)
    }

    /// `sender`, also returning what the receiver answered.
    async fn sender_accepted(address: SocketAddr) -> Result<(TcpStream, Accepted), Failure> {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let me = Me {
            node_id: 1,
            run: 5,
            head: 0,
            recovering: false,
            secret: None,
        };
        let accepted = handshake::connect(&mut stream, &me).await?;
        Ok((stream, accepted))
    }

    const SECRET: &[u8] = b"s";

    /// Connects as node 1, run 5, with `SECRET`, and shakes hands.
    async fn signed_sender(address: SocketAddr) -> (TcpStream, Keys, Accepted) {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let me = Me {
            node_id: 1,
            run: 5,
            head: 0,
            recovering: false,
            secret: Some(SECRET.to_vec()),
        };
        let mut accepted = handshake::connect(&mut stream, &me).await.unwrap();
        let keys = accepted.keys.take().expect("a signed link");
        (stream, keys, accepted)
    }

    /// `frame` signed as the next frame of `signing`, as it would be written.
    async fn signed_bytes(frame: &Frame, signing: &mut Signing) -> Vec<u8> {
        let mut bytes = Vec::new();
        frame::write_frame_with(&mut bytes, frame, &mut Vec::new(), Some(signing))
            .await
            .unwrap();
        bytes
    }

    /// Reads signed frames until an acknowledgment, and returns its position.
    async fn signed_ack(stream: &mut TcpStream, keys: &mut Keys) -> u64 {
        let mut buf = Vec::new();
        loop {
            let read = frame::read_frame_with(
                stream,
                HANDSHAKE_FRAME_MAX,
                &mut buf,
                Some(&mut keys.receive),
            );
            match read.await.unwrap() {
                Frame::Ack { next } => return next,
                Frame::Heartbeat => {}
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    /// Checks that the node drops the link promptly and counts one bad frame.
    async fn dropped(node: &Receiving, stream: &mut TcpStream, keys: &mut Keys) {
        let mut buf = Vec::new();
        let closed = async {
            loop {
                let read = frame::read_frame_with(
                    stream,
                    HANDSHAKE_FRAME_MAX,
                    &mut buf,
                    Some(&mut keys.receive),
                );
                match read.await {
                    Ok(Frame::Ack { .. } | Frame::Heartbeat) => {}
                    Ok(other) => panic!("unexpected {other:?}"),
                    Err(error) => {
                        assert!(!frame::is_bad_tag(&error), "{error}");
                        return;
                    }
                }
            }
        };
        tokio::time::timeout(SILENCE / 2, closed)
            .await
            .expect("the node dropped the link before the peer went silent");
        let deadline = Instant::now() + Duration::from_secs(10);
        while node.shared.status.bad_frames.load(Relaxed) != 1 {
            assert!(Instant::now() < deadline, "the bad frame was never counted");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn batch(next: u64, key: &str) -> Frame {
        Frame::Batch {
            next,
            records: vec![put(key, "v")],
        }
    }

    /// Whether node 2 holds `key`.
    fn holds(node: &Receiving, key: &str) -> bool {
        node.store
            .get(key.as_bytes(), unix_secs(), &GetArgs::default())
            .is_some()
    }

    /// Node 2 with a signed link from node 1 that has applied and acknowledged one batch.
    async fn signed_link() -> (Receiving, TcpStream, Keys) {
        let node = receiving_signed().await;
        let (mut stream, mut keys, _) = signed_sender(node.address).await;
        let first = signed_bytes(&batch(10, "good"), &mut keys.send).await;
        stream.write_all(&first).await.unwrap();
        assert_eq!(signed_ack(&mut stream, &mut keys).await, 10);
        assert!(holds(&node, "good"));
        (node, stream, keys)
    }

    /// What the node has applied in all, stale records included, and its position on node 1.
    fn progress(node: &Receiving) -> (u64, u64, Option<u64>) {
        let status = &node.shared.status;
        (
            status.records_applied.load(Relaxed),
            status.records_stale.load(Relaxed),
            node.shared.positions.lock().get(&1).map(|p| p.next),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_signed_frame_changed_on_the_way_drops_the_link_and_is_not_applied() {
        let (node, mut stream, mut keys) = signed_link().await;
        let mut bytes = signed_bytes(&batch(20, "bad"), &mut keys.send).await;
        // The key's last byte: still a well-formed batch, of another key.
        let at = bytes
            .windows(3)
            .rposition(|window| window == b"bad")
            .unwrap();
        bytes[at + 1] = b'd';
        stream.write_all(&bytes).await.unwrap();
        dropped(&node, &mut stream, &mut keys).await;
        assert!(!holds(&node, "bad") && !holds(&node, "bdd"));
        assert_eq!(progress(&node), (1, 0, Some(10)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_signed_frame_whose_length_was_raised_drops_the_link_before_its_payload() {
        for raised in [frame::FRAME_MAX - 1, 100] {
            let (node, mut stream, mut keys) = signed_link().await;
            let mut bytes = signed_bytes(&batch(20, "bad"), &mut keys.send).await;
            bytes[..4].copy_from_slice(&raised.to_le_bytes());
            // Only the header and its tag: the link must drop without awaiting the payload.
            stream
                .write_all(&bytes[..frame::HEADER_LEN + frame::TAG_LEN])
                .await
                .unwrap();
            dropped(&node, &mut stream, &mut keys).await;
            assert!(!holds(&node, "bad"));
            assert_eq!(progress(&node), (1, 0, Some(10)));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_frame_inserted_into_a_signed_link_drops_it_and_is_not_applied() {
        let (node, mut stream, mut keys) = signed_link().await;
        let mut plain = Vec::new();
        batch(20, "bad").encode(&mut plain);
        let tag = [0; frame::TAG_LEN];
        let (header, payload) = plain.split_at(frame::HEADER_LEN);
        let forged = [header, &tag, payload, &tag].concat();
        stream.write_all(&forged).await.unwrap();
        dropped(&node, &mut stream, &mut keys).await;
        assert!(!holds(&node, "bad"));
        assert_eq!(progress(&node), (1, 0, Some(10)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_frame_dropped_from_a_signed_link_drops_it_at_the_next_frame() {
        let (node, mut stream, mut keys) = signed_link().await;
        let _lost = signed_bytes(&batch(20, "lost"), &mut keys.send).await;
        let next = signed_bytes(&batch(30, "after"), &mut keys.send).await;
        stream.write_all(&next).await.unwrap();
        dropped(&node, &mut stream, &mut keys).await;
        assert!(!holds(&node, "after"));
        assert_eq!(progress(&node), (1, 0, Some(10)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_frame_replayed_from_an_earlier_link_drops_the_link_and_is_not_applied() {
        let node = receiving_signed().await;
        let (mut stream, mut keys, _) = signed_sender(node.address).await;
        let first = signed_bytes(&batch(10, "old"), &mut keys.send).await;
        stream.write_all(&first).await.unwrap();
        assert_eq!(signed_ack(&mut stream, &mut keys).await, 10);
        drop(stream);
        // Replays the earlier link's first frame as the next link's first.
        let deadline = Instant::now() + Duration::from_secs(10);
        let (mut stream, mut keys, accepted) = loop {
            let mut stream = TcpStream::connect(node.address).await.unwrap();
            let me = Me {
                node_id: 1,
                run: 5,
                head: 0,
                recovering: false,
                secret: Some(SECRET.to_vec()),
            };
            match handshake::connect(&mut stream, &me).await {
                Ok(mut accepted) => {
                    let keys = accepted.keys.take().unwrap();
                    break (stream, keys, accepted);
                }
                Err(_) => {
                    assert!(
                        Instant::now() < deadline,
                        "the first link was never released"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        assert_eq!(accepted.position, Some((5, 10)));
        stream.write_all(&first).await.unwrap();
        dropped(&node, &mut stream, &mut keys).await;
        assert_eq!(progress(&node), (1, 0, Some(10)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unsigned_frame_on_a_signed_link_drops_it() {
        let (node, mut stream, mut keys) = signed_link().await;
        write_frames(&mut stream, &[batch(20, "bad")]).await;
        // Untagged, its payload's first bytes are read as the header tag.
        let heartbeats = [(); 4].map(|()| Frame::Heartbeat);
        write_frames(&mut stream, &heartbeats).await;
        dropped(&node, &mut stream, &mut keys).await;
        assert!(!holds(&node, "bad"));
        assert_eq!(progress(&node), (1, 0, Some(10)));
    }

    /// Two ends of a loopback connection: (accepted, connecting).
    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (server, client)
    }

    async fn write_frames(stream: &mut TcpStream, frames: &[Frame]) {
        let mut buf = Vec::new();
        for frame in frames {
            frame::write_frame(stream, frame, &mut buf).await.unwrap();
        }
    }

    fn positions(changes: &[Change]) -> Vec<u64> {
        changes
            .iter()
            .filter_map(|change| match change {
                Change::Position(position) => Some(position.next),
                Change::Record(_) => None,
            })
            .collect()
    }

    fn put(key: &str, value: &str) -> Record {
        let version = Clock::new(1).next();
        Record::Put {
            key: Bytes::from(key.to_string()),
            version,
            cas: version,
            flags: 0,
            expiry: 0,
            stale: false,
            value: Bytes::from(value.to_string()),
        }
    }

    /// Reads frames until an acknowledgment, and returns its position.
    async fn ack(stream: &mut TcpStream) -> u64 {
        let mut buf = Vec::new();
        loop {
            match frame::read_frame(stream, HANDSHAKE_FRAME_MAX, &mut buf)
                .await
                .unwrap()
            {
                Frame::Ack { next } => return next,
                Frame::Heartbeat => {}
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    fn drained(oplog: &Oplog) -> Vec<Change> {
        let mut out = Vec::new();
        oplog.drain(&mut out, usize::MAX);
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_receiver_applies_a_batch_acknowledges_it_and_logs_its_position_after_it() {
        let node = receiving().await;
        let mut stream = sender(node.address).await.unwrap();
        let record = put("k", "v");
        let batch = Frame::Batch {
            next: 10,
            records: vec![record.clone()],
        };
        frame::write_frame(&mut stream, &batch, &mut Vec::new())
            .await
            .unwrap();
        assert_eq!(ack(&mut stream).await, 10);
        let hit = node.store.get(b"k", unix_secs(), &GetArgs::default());
        assert_eq!(&hit.unwrap().value[..], b"v");
        assert_eq!(node.shared.status.records_applied.load(Relaxed), 1);
        node.stop.send_replace(true);
        node.listening.await.unwrap();
        let position = Position {
            peer: 1,
            run: 5,
            next: 10,
        };
        assert_eq!(
            drained(&node.oplog),
            [Change::Record(record), Change::Position(position)]
        );
        assert_eq!(node.shared.positions.lock().get(&1), Some(&position));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_listener_answers_the_status_command() {
        let node = receiving().await;
        let mut stream = TcpStream::connect(node.address).await.unwrap();
        stream.write_all(b"status\r\n").await.unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).await.unwrap();
        assert!(reply.starts_with("STAT state recovering\r\n"), "{reply}");
        assert!(reply.ends_with("END\r\n"), "{reply}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_receiving_link_reports_its_catch_up_progress() {
        let node = receiving().await;
        let mut stream = TcpStream::connect(node.address).await.unwrap();
        let me = Me {
            node_id: 1,
            run: 5,
            head: 10,
            recovering: false,
            secret: None,
        };
        handshake::connect(&mut stream, &me).await.unwrap();
        let progress = |node: &Receiving| -> Vec<String> {
            node.shared
                .readiness
                .lines(Instant::now())
                .into_iter()
                .filter(|(name, _)| name.starts_with("peer:node-1:"))
                .map(|(_, value)| value)
                .collect()
        };
        assert_eq!(progress(&node), ["10", "none", "0"]);
        let batch = Frame::Batch {
            next: 10,
            records: vec![put("k", "v")],
        };
        write_frames(&mut stream, &[batch]).await;
        assert_eq!(ack(&mut stream).await, 10);
        assert_eq!(progress(&node), ["10", "10", "1"]);
        // The link ends, and its progress goes with it.
        drop(stream);
        let deadline = Instant::now() + Duration::from_secs(10);
        while !progress(&node).is_empty() {
            assert!(Instant::now() < deadline, "the link never ended");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_batch_without_records_moves_the_position_but_logs_no_entry() {
        let node = receiving().await;
        let mut stream = sender(node.address).await.unwrap();
        let mut buf = Vec::new();
        let first = Frame::Batch {
            next: 10,
            records: vec![put("k", "v")],
        };
        frame::write_frame(&mut stream, &first, &mut buf)
            .await
            .unwrap();
        assert_eq!(ack(&mut stream).await, 10);
        // Past the interval, so only the lack of records keeps it out of the log.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let empty = Frame::Batch {
            next: 12,
            records: Vec::new(),
        };
        frame::write_frame(&mut stream, &empty, &mut buf)
            .await
            .unwrap();
        assert_eq!(ack(&mut stream).await, 12);
        tokio::time::sleep(Duration::from_millis(150)).await;
        let positions: Vec<_> = drained(&node.oplog)
            .into_iter()
            .filter_map(|change| match change {
                Change::Position(position) => Some(position.next),
                Change::Record(_) => None,
            })
            .collect();
        assert_eq!(positions, [10]);
        // The handshake still offers the latest position.
        assert_eq!(
            node.shared.positions.lock().get(&1).map(|p| p.next),
            Some(12)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_link_resumed_in_the_same_run_acknowledges_at_once() {
        let node = receiving().await;
        let position = Position {
            peer: 1,
            run: 5,
            next: 40,
        };
        node.shared.positions.lock().insert(1, position);
        node.shared.logged.lock().insert(1, position);
        let mut stream = sender(node.address).await.unwrap();
        assert_eq!(ack(&mut stream).await, 40);
        // Already logged, so not logged again.
        node.stop.send_replace(true);
        node.listening.await.unwrap();
        assert!(drained(&node.oplog).is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_same_run_resume_at_the_head_is_caught_up_without_a_batch() {
        let node = receiving().await;
        let position = Position {
            peer: 1,
            run: 5,
            next: 40,
        };
        node.shared.positions.lock().insert(1, position);
        node.shared.logged.lock().insert(1, position);
        let mut stream = TcpStream::connect(node.address).await.unwrap();
        let me = Me {
            node_id: 1,
            run: 5,
            head: 40,
            recovering: false,
            secret: None,
        };
        handshake::connect(&mut stream, &me).await.unwrap();
        assert_eq!(ack(&mut stream).await, 40);
        let lines = node.shared.readiness.lines(Instant::now());
        let caught_up = lines
            .iter()
            .find(|(name, _)| name == "peer:node-1:caught_up")
            .map(|(_, value)| value.as_str());
        assert_eq!(caught_up, Some("1"), "{lines:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_status_command_left_unfinished_does_not_hold_shutdown() {
        let node = receiving().await;
        let mut stream = TcpStream::connect(node.address).await.unwrap();
        // A command prefix with no line break.
        stream.write_all(b"stat").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        node.stop.send_replace(true);
        node.listening.await.unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
        drop(stream);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_receiver_waits_for_room_in_the_oplog() {
        let node = receiving().await;
        // Fill the oplog past half, as a lagging log thread would.
        for index in 0..600 {
            node.oplog.append(put(&format!("filler{index}"), "v"));
        }
        let mut stream = sender(node.address).await.unwrap();
        let batch = Frame::Batch {
            next: 10,
            records: vec![put("k", "v")],
        };
        frame::write_frame(&mut stream, &batch, &mut Vec::new())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            node.store
                .get(b"k", unix_secs(), &GetArgs::default())
                .is_none()
        );
        // The log thread catches up.
        drained(&node.oplog);
        assert_eq!(ack(&mut stream).await, 10);
        assert!(
            node.store
                .get(b"k", unix_secs(), &GetArgs::default())
                .is_some()
        );
        assert_eq!(node.oplog.dropped(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_frame_larger_than_the_oplog_is_applied_in_chunks_that_fit() {
        // Room for 8 changes, so records are applied 2 at a time.
        let node = receiving_with(8).await;
        let oplog = node.oplog.clone();
        let draining = tokio::spawn(async move {
            let mut out = Vec::new();
            loop {
                tokio::time::sleep(Duration::from_millis(2)).await;
                out.clear();
                oplog.drain(&mut out, 1);
            }
        });
        let mut stream = sender(node.address).await.unwrap();
        let records = (0..40)
            .map(|index| put(&format!("k{index}"), "v"))
            .collect();
        let batch = Frame::Batch { next: 50, records };
        frame::write_frame(&mut stream, &batch, &mut Vec::new())
            .await
            .unwrap();
        assert_eq!(ack(&mut stream).await, 50);
        draining.abort();
        assert_eq!(node.shared.status.records_applied.load(Relaxed), 40);
        assert_eq!(node.oplog.dropped(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_frame_larger_than_the_oplogs_byte_budget_is_applied_in_chunks_that_fit() {
        // A 400-byte value budget, so 100-byte records are applied one at a time.
        let node = receiving_with_budget(1024, 400).await;
        let oplog = node.oplog.clone();
        let draining = tokio::spawn(async move {
            let mut out = Vec::new();
            loop {
                tokio::time::sleep(Duration::from_millis(2)).await;
                out.clear();
                oplog.drain(&mut out, 1);
            }
        });
        let mut stream = sender(node.address).await.unwrap();
        let value = "v".repeat(100);
        let records = (0..20)
            .map(|index| put(&format!("k{index}"), &value))
            .collect();
        let batch = Frame::Batch { next: 30, records };
        frame::write_frame(&mut stream, &batch, &mut Vec::new())
            .await
            .unwrap();
        assert_eq!(ack(&mut stream).await, 30);
        draining.abort();
        assert_eq!(node.shared.status.records_applied.load(Relaxed), 20);
        assert_eq!(node.oplog.dropped(), 0);
    }

    #[test]
    fn a_chunk_is_bounded_by_count_and_by_bytes_and_holds_at_least_one_record() {
        let small: Vec<_> = (0..10).map(|_| put("k", "v")).collect();
        assert_eq!(chunk_len(&small, 4, u64::MAX), 4);
        assert_eq!(chunk_len(&small, 100, 3), 3);
        assert_eq!(chunk_len(&small, 100, 100), 10);
        let large = [put("k", &"v".repeat(50)), put("k", "v")];
        assert_eq!(chunk_len(&large, 100, 10), 1);
        assert_eq!(chunk_len(&large, 100, 51), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_sync_is_applied_and_sets_the_position_at_its_end() {
        let node = receiving().await;
        let mut stream = sender(node.address).await.unwrap();
        let older = put("gone", "v");
        // One step of node 1's counter after the put, so it is newer even within a millisecond.
        let delete = Record::Delete {
            key: Bytes::from_static(b"gone"),
            version: older.version() + (1 << 8),
        };
        node.store.apply_remote(&older, unix_secs());
        let mut buf = Vec::new();
        for frame in [
            Frame::SyncStart,
            Frame::SyncItems(vec![put("k", "v"), delete]),
            Frame::SyncEnd { next: 20 },
        ] {
            frame::write_frame(&mut stream, &frame, &mut buf)
                .await
                .unwrap();
        }
        assert_eq!(ack(&mut stream).await, 20);
        assert!(
            node.store
                .get(b"gone", unix_secs(), &GetArgs::default())
                .is_none()
        );
        assert!(
            node.store
                .get(b"k", unix_secs(), &GetArgs::default())
                .is_some()
        );
        assert_eq!(node.store.tombstones(), 1);
        assert_eq!(node.shared.status.full_syncs_received.load(Relaxed), 1);
        assert_eq!(
            node.shared.positions.lock().get(&1).map(|p| p.next),
            Some(20)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_second_link_from_the_same_node_is_refused_until_the_first_ends() {
        let node = receiving().await;
        let first = sender(node.address).await.unwrap();
        match sender(node.address).await {
            Err(Failure::RefusedBy(reason)) => assert!(reason.contains("live link"), "{reason}"),
            other => panic!("expected a refusal, got {other:?}"),
        }
        drop(first);
        let deadline = Instant::now() + Duration::from_secs(10);
        while sender(node.address).await.is_err() {
            assert!(
                Instant::now() < deadline,
                "the first link was never released"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_receiver_stalled_on_a_full_oplog_keeps_the_link_and_completes() {
        let node = receiving().await;
        for index in 0..600 {
            node.oplog.append(put(&format!("filler{index}"), "v"));
        }
        let mut stream = sender(node.address).await.unwrap();
        // The second batch waits behind the first, which waits for room.
        let batches = [
            Frame::Batch {
                next: 10,
                records: vec![put("k1", "v")],
            },
            Frame::Batch {
                next: 12,
                records: vec![put("k2", "v")],
            },
        ];
        write_frames(&mut stream, &batches).await;
        // The stall outlasts the silence limit, and the receiver heartbeats throughout.
        let mut buf = Vec::new();
        let until = Instant::now() + SILENCE * 2;
        let mut heartbeats = 0;
        while Instant::now() < until {
            let read = frame::read_frame(&mut stream, HANDSHAKE_FRAME_MAX, &mut buf);
            match tokio::time::timeout(SILENCE, read).await {
                Ok(Ok(Frame::Heartbeat)) => heartbeats += 1,
                other => panic!("expected heartbeats during the stall, got {other:?}"),
            }
        }
        assert!(heartbeats >= 5, "{heartbeats}");
        assert!(
            node.store
                .get(b"k1", unix_secs(), &GetArgs::default())
                .is_none()
        );
        // Once the log thread catches up, both batches apply in order on the same link.
        drained(&node.oplog);
        let mut acked = ack(&mut stream).await;
        if acked == 10 {
            acked = ack(&mut stream).await;
        }
        assert_eq!(acked, 12);
        assert!(
            node.store
                .get(b"k2", unix_secs(), &GetArgs::default())
                .is_some()
        );
        assert_eq!(node.oplog.dropped(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_while_waiting_for_room_logs_the_position_reached_before() {
        let node = receiving().await;
        let mut stream = sender(node.address).await.unwrap();
        let first = Frame::Batch {
            next: 10,
            records: vec![put("k1", "v")],
        };
        write_frames(&mut stream, &[first]).await;
        assert_eq!(ack(&mut stream).await, 10);
        for index in 0..600 {
            node.oplog.append(put(&format!("filler{index}"), "v"));
        }
        let second = Frame::Batch {
            next: 20,
            records: vec![put("k2", "v")],
        };
        write_frames(&mut stream, &[second]).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        node.stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), node.listening)
            .await
            .expect("the receiver stopped promptly")
            .unwrap();
        assert!(
            node.store
                .get(b"k2", unix_secs(), &GetArgs::default())
                .is_none()
        );
        assert_eq!(positions(&drained(&node.oplog)), [10]);
        assert_eq!(
            node.shared.positions.lock().get(&1).map(|p| p.next),
            Some(10)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_during_a_stalled_reply_is_prompt_and_logs_the_position() {
        let node = receiving().await;
        let (server, _client) = tcp_pair().await;
        let (read, write) = server.into_split();
        // Fill the socket's buffers so the link's next write blocks.
        let junk = vec![0; 1 << 16];
        for _ in 0..3 {
            loop {
                match write.try_write(&junk) {
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) => panic!("{error}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let resumed = Position {
            peer: 1,
            run: 5,
            next: 10,
        };
        let mut link = Link::new(node.shared.clone(), 1, 5, Some(resumed), write, None);
        // Too little progress to log by itself, but owed an acknowledgment.
        link.advance(11);
        let frames = Frames::spawn(read, FRAME_MAX, None);
        let serving = tokio::spawn(async move { link.serve(frames).await });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!serving.is_finished());
        node.stop.send_replace(true);
        tokio::time::timeout(SILENCE / 2, serving)
            .await
            .expect("the link stopped promptly")
            .unwrap();
        assert_eq!(positions(&drained(&node.oplog)), [11]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_position_dropped_by_a_full_oplog_is_logged_later_and_is_no_loss() {
        let node = receiving_with(2).await;
        let (server, _client) = tcp_pair().await;
        let (_read, write) = server.into_split();
        let resumed = Position {
            peer: 1,
            run: 5,
            next: 10,
        };
        let mut link = Link::new(node.shared.clone(), 1, 5, Some(resumed), write, None);
        node.oplog.append(put("a", "v"));
        node.oplog.append(put("b", "v"));
        // Forced, as at a full sync's end; otherwise too small a step to log.
        link.advance(11);
        link.log_position(true);
        assert_eq!(node.oplog.positions_dropped(), 1);
        assert!(!node.oplog.take_lost());
        drained(&node.oplog);
        link.log_position(false);
        assert_eq!(positions(&drained(&node.oplog)), [11]);
        link.log_position(false);
        assert!(drained(&node.oplog).is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_final_position_dropped_by_a_full_oplog_is_logged_by_the_next_link() {
        let node = receiving_with(2).await;
        let resumed = Position {
            peer: 1,
            run: 5,
            next: 10,
        };
        node.shared.positions.lock().insert(1, resumed);
        node.shared.logged.lock().insert(1, resumed);
        let (server, _client) = tcp_pair().await;
        let (_read, write) = server.into_split();
        let mut link = Link::new(node.shared.clone(), 1, 5, Some(resumed), write, None);
        node.oplog.append(put("a", "v"));
        node.oplog.append(put("b", "v"));
        // The link ends, and the oplog has no room for its final position.
        link.advance(11);
        link.log_position(true);
        drop(link);
        assert_eq!(node.oplog.positions_dropped(), 1);
        drained(&node.oplog);
        // The next link from the peer resumes at the newer position, and logs it.
        let offered = node.shared.positions.lock().get(&1).copied();
        assert_eq!(offered.map(|p| p.next), Some(11));
        let (server, _client) = tcp_pair().await;
        let (_read, write) = server.into_split();
        let mut link = Link::new(node.shared.clone(), 1, 5, offered, write, None);
        link.log_position(false);
        assert_eq!(positions(&drained(&node.oplog)), [11]);
        assert_eq!(node.shared.logged.lock().get(&1).map(|p| p.next), Some(11));
        link.log_position(false);
        assert!(drained(&node.oplog).is_empty());
    }

    #[test]
    fn a_repeated_handshake_failure_is_reported_at_most_once_a_minute_with_a_count() {
        let mut failures = Failures::default();
        let start = Instant::now();
        let first: SocketAddr = "10.0.0.2:50000".parse().unwrap();
        let again: SocketAddr = "10.0.0.2:50001".parse().unwrap();
        let other: SocketAddr = "10.0.0.3:50000".parse().unwrap();
        let reason = || "refused the link: wrong secret".to_string();
        let line = failures.note(first, reason(), start).unwrap();
        assert_eq!(
            line,
            "replication: link from 10.0.0.2:50000: refused the link: wrong secret"
        );
        // The same host and reason, from another port, within the interval: counted only.
        let soon = start + Duration::from_secs(1);
        assert_eq!(failures.note(again, reason(), soon), None);
        assert_eq!(failures.note(again, reason(), soon), None);
        // Another host, or another reason, is reported at once.
        assert!(failures.note(other, reason(), soon).is_some());
        assert!(failures.note(first, "timed out".into(), soon).is_some());
        let later = start + FAILURE_REPORT_INTERVAL;
        let line = failures.note(again, reason(), later).unwrap();
        assert!(line.ends_with("(2 more like it in the last 60s)"), "{line}");
        // Nothing in between: reported plainly.
        let line = failures
            .note(again, reason(), later + FAILURE_REPORT_INTERVAL)
            .unwrap();
        assert!(line.ends_with("wrong secret"), "{line}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connections_past_the_limit_are_closed_at_once() {
        let node = receiving().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let listening = tokio::spawn(listen_with(listener, node.shared.clone(), 2));
        let mut held = Vec::new();
        for _ in 0..2 {
            held.push(TcpStream::connect(address).await.unwrap());
        }
        // Well within the handshake timeout, which would close it too.
        let mut byte = [0; 1];
        let mut third = TcpStream::connect(address).await.unwrap();
        let read = tokio::time::timeout(Duration::from_secs(2), third.read(&mut byte))
            .await
            .expect("the third connection was closed at once");
        assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
        // Closing one makes room for another.
        drop(held.pop());
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut fourth = TcpStream::connect(address).await.unwrap();
        let read = tokio::time::timeout(Duration::from_millis(500), fourth.read(&mut byte)).await;
        assert!(read.is_err(), "the fourth connection was closed: {read:?}");
        node.stop.send_replace(true);
        listening.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_sync_cut_short_keeps_the_old_position_until_the_next_one_ends() {
        let node = receiving().await;
        let (mut stream, accepted) = sender_accepted(node.address).await.unwrap();
        assert_eq!(accepted.position, None);
        let batch = Frame::Batch {
            next: 10,
            records: vec![put("a", "v")],
        };
        write_frames(&mut stream, &[batch]).await;
        assert_eq!(ack(&mut stream).await, 10);
        write_frames(
            &mut stream,
            &[Frame::SyncStart, Frame::SyncItems(vec![put("b", "v")])],
        )
        .await;
        let deadline = Instant::now() + Duration::from_secs(10);
        while node.shared.status.records_applied.load(Relaxed) < 2 {
            assert!(
                Instant::now() < deadline,
                "the sync items were never applied"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(stream);
        // The next link is offered the position from before the cut-short sync.
        let (mut stream, accepted) = loop {
            match sender_accepted(node.address).await {
                Ok(linked) => break linked,
                Err(_) => {
                    assert!(
                        Instant::now() < deadline,
                        "the first link was never released"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };
        assert_eq!(accepted.position, Some((5, 10)));
        write_frames(
            &mut stream,
            &[
                Frame::SyncStart,
                Frame::SyncItems(vec![put("c", "v")]),
                Frame::SyncEnd { next: 30 },
            ],
        )
        .await;
        // A same-run resume is acknowledged at once, before the sync ends.
        let mut acked = ack(&mut stream).await;
        if acked == 10 {
            acked = ack(&mut stream).await;
        }
        assert_eq!(acked, 30);
        assert_eq!(
            node.shared.positions.lock().get(&1).map(|p| p.next),
            Some(30)
        );
        assert_eq!(positions(&drained(&node.oplog)), [10, 30]);
    }
}
