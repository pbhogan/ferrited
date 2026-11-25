//! Event loops serving client connections, each on its own thread with its own poll,
//! connections and counters. Requests execute inline: nothing waits but a shard lock.

use std::collections::VecDeque;
use std::io::{self, IoSlice};
use std::os::fd::AsRawFd;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Acquire, Ordering::Relaxed};
use std::time::Duration;

use bytes::Buf;
use mio::{Events, Interest, Poll, Token, Waker};

use super::Context;
use super::session::{Outcome, Session};
use super::stream::Stream;
use crate::stats::{LocalStats, Stat};

/// The waker's token; connection tokens are slot indexes, which never reach it.
const WAKE: Token = Token(usize::MAX);
/// Requests one connection may execute in one turn before the loop serves the others.
pub(crate) const REQUESTS_PER_TURN: usize = 64;
/// Reads one connection may make in one turn.
const READS_PER_TURN: usize = 4;
const EVENTS: usize = 1024;
const WRITE_SLICES: usize = 64;

/// What other threads use to reach a loop.
#[derive(Clone)]
pub(crate) struct Handle {
    inbox: Arc<parking_lot::Mutex<Vec<Stream>>>,
    waker: Arc<Waker>,
    /// Connections handed to the loop and not yet closed.
    open: Arc<AtomicUsize>,
}

impl Handle {
    /// Gives the loop a new connection, already counted in `curr_connections`.
    pub fn hand(&self, stream: Stream) {
        self.open.fetch_add(1, Relaxed);
        self.inbox.lock().push(stream);
        let _ = self.waker.wake();
    }

    pub fn open(&self) -> usize {
        self.open.load(Relaxed)
    }

    pub fn waker(&self) -> Arc<Waker> {
        self.waker.clone()
    }
}

/// What the loop does with a connection after a turn.
#[derive(Debug, PartialEq, Eq)]
enum Next {
    /// Wait for the socket to become readable or writable.
    Wait,
    /// Serve it again next turn: it has buffered requests or unread input.
    Again,
    Close,
}

struct Conn {
    stream: Stream,
    session: Session,
    /// Replies wait for the socket to take more; nothing is read or executed meanwhile.
    blocked: bool,
    queued: bool,
    /// Close once replies are written.
    closing: bool,
    /// The peer closed its side or the socket has an error. A short read then does not mean
    /// drained: the end of input may have come in the same edge, and no event will follow.
    read_closed: bool,
    /// The turn in which the connection was last served, so it is served once per turn.
    served_in: u64,
}

/// One event loop: its poll, its connections, and the run queue of connections with work left.
pub(crate) struct EventLoop {
    ctx: Arc<Context>,
    poll: Poll,
    events: Events,
    inbox: Arc<parking_lot::Mutex<Vec<Stream>>>,
    open: Arc<AtomicUsize>,
    conns: Vec<Option<Conn>>,
    free: Vec<usize>,
    queue: VecDeque<usize>,
    /// Slots with events this turn, kept to reuse the allocation.
    ready: Vec<usize>,
    stats: LocalStats,
    stopping: bool,
    turns: u64,
    /// Makes the next connection served panic, for tests.
    #[cfg(test)]
    pub(crate) panic_next: bool,
}

impl EventLoop {
    pub fn new(ctx: Arc<Context>) -> io::Result<(Self, Handle)> {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKE)?);
        let inbox = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let open = Arc::new(AtomicUsize::new(0));
        let handle = Handle {
            inbox: inbox.clone(),
            waker,
            open: open.clone(),
        };
        let event_loop = Self {
            ctx,
            poll,
            events: Events::with_capacity(EVENTS),
            inbox,
            open,
            conns: Vec::new(),
            free: Vec::new(),
            queue: VecDeque::new(),
            ready: Vec::new(),
            stats: LocalStats::default(),
            stopping: false,
            turns: 0,
            #[cfg(test)]
            panic_next: false,
        };
        Ok((event_loop, handle))
    }

    /// Serves connections until shutdown has closed them all.
    pub fn run(mut self) {
        while self.turn(None) {}
    }

    /// Polls up to `timeout` (not at all while work is queued), then serves queued connections
    /// and those with events, each at most once. Returns false once the loop has finished.
    pub fn turn(&mut self, timeout: Option<Duration>) -> bool {
        let timeout = match self.queue.is_empty() {
            true => timeout,
            false => Some(Duration::ZERO),
        };
        if let Err(error) = self.poll.poll(&mut self.events, timeout)
            && error.kind() != io::ErrorKind::Interrupted
        {
            eprintln!("ferrited: event loop poll failed: {error}");
        }
        if self.ctx.close_now.load(Relaxed) {
            self.close_all();
            return false;
        }
        self.turns += 1;
        let mut ready = std::mem::take(&mut self.ready);
        for event in self.events.iter() {
            let slot = match event.token() {
                WAKE => continue,
                token => token.0,
            };
            if (event.is_read_closed() || event.is_error())
                && let Some(conn) = self.conns.get_mut(slot).and_then(Option::as_mut)
            {
                conn.read_closed = true;
            }
            ready.push(slot);
        }
        // Every turn, not only on a wake-up, since a wake-up may arrive with other events.
        self.take_new();
        if !self.stopping && self.ctx.stopping.load(Relaxed) {
            self.stopping = true;
            for slot in 0..self.conns.len() {
                self.enqueue(slot);
            }
        }
        // One clock reading per turn, so requests that arrive together agree on the time.
        let now = (self.ctx.wall_clock)();
        // Only the connections queued before this turn; any queued again wait for the next.
        for _ in 0..self.queue.len() {
            let Some(slot) = self.queue.pop_front() else {
                break;
            };
            if let Some(conn) = self.conns[slot].as_mut() {
                conn.queued = false;
                self.serve(slot, now);
            }
        }
        for slot in ready.drain(..) {
            self.serve(slot, now);
        }
        self.ready = ready;
        self.ctx.stats.merge(&mut self.stats);
        // Not while the acceptor may still hand over a connection. Acquire pairs with `drain`'s
        // release, so `open` then counts every connection handed over.
        !(self.stopping && !self.ctx.accepting.load(Acquire) && self.open.load(Relaxed) == 0)
    }

    /// Registers handed-over connections and queues each, since input that arrived before
    /// registration may not raise an event.
    fn take_new(&mut self) {
        let streams = std::mem::take(&mut *self.inbox.lock());
        for mut stream in streams {
            let slot = self.free.pop().unwrap_or_else(|| {
                self.conns.push(None);
                self.conns.len() - 1
            });
            if let Err(error) =
                self.poll
                    .registry()
                    .register(&mut stream, Token(slot), Interest::READABLE)
            {
                eprintln!("ferrited: cannot watch a new connection: {error}");
                self.free.push(slot);
                self.forget_one();
                continue;
            }
            self.conns[slot] = Some(Conn {
                stream,
                session: Session::new(),
                blocked: false,
                queued: false,
                closing: false,
                read_closed: false,
                served_in: 0,
            });
            self.enqueue(slot);
        }
    }

    fn enqueue(&mut self, slot: usize) {
        if let Some(conn) = self.conns[slot].as_mut()
            && !conn.queued
        {
            conn.queued = true;
            self.queue.push_back(slot);
        }
    }

    /// Gives the connection in `slot` one turn (harmless if the slot was reused). One already
    /// served this turn is queued for the next instead, since its event will not come again.
    fn serve(&mut self, slot: usize, now: u64) {
        let Some(conn) = self.conns.get_mut(slot).and_then(Option::as_mut) else {
            return;
        };
        if conn.served_in == self.turns {
            self.enqueue(slot);
            return;
        }
        conn.served_in = self.turns;
        let was_blocked = conn.blocked;
        let ctx = &self.ctx;
        let stats = &mut self.stats;
        #[cfg(test)]
        let panic_next = std::mem::take(&mut self.panic_next);
        // A panic while serving one connection closes that connection, not the loop.
        let next = catch_unwind(AssertUnwindSafe(|| {
            #[cfg(test)]
            assert!(!panic_next, "deliberate panic while serving a connection");
            drive(conn, ctx, now, stats)
        }));
        let next = match next {
            Ok(Ok(next)) => next,
            Ok(Err(_)) => Next::Close,
            Err(_) => {
                eprintln!("ferrited: a connection panicked and was closed");
                Next::Close
            }
        };
        match next {
            Next::Close => return self.close(slot),
            Next::Again => self.enqueue(slot),
            Next::Wait => {}
        }
        let conn = self.conns[slot]
            .as_mut()
            .expect("served connection is open");
        // Writability is watched only while replies wait for the socket.
        if conn.blocked != was_blocked {
            let interest = match conn.blocked {
                true => Interest::READABLE | Interest::WRITABLE,
                false => Interest::READABLE,
            };
            if self
                .poll
                .registry()
                .reregister(&mut conn.stream, Token(slot), interest)
                .is_err()
            {
                self.close(slot);
            }
        }
    }

    fn close(&mut self, slot: usize) {
        if self.conns[slot].take().is_some() {
            self.free.push(slot);
            self.forget_one();
        }
    }

    /// Closes every connection at once, including any still in the inbox.
    fn close_all(&mut self) {
        for slot in 0..self.conns.len() {
            self.close(slot);
        }
        for stream in std::mem::take(&mut *self.inbox.lock()) {
            drop(stream);
            self.forget_one();
        }
        self.ctx.stats.merge(&mut self.stats);
    }

    fn forget_one(&self) {
        self.open.fetch_sub(1, Relaxed);
        self.ctx.stats.curr_connections.fetch_sub(1, Relaxed);
    }
}

/// One turn of one connection: writes leftover replies, then executes buffered requests and
/// reads more, within the turn's budgets.
fn drive(conn: &mut Conn, ctx: &Context, now: u64, stats: &mut LocalStats) -> io::Result<Next> {
    if !conn.session.output().is_empty() {
        write_out(conn, stats)?;
        if conn.blocked {
            return Ok(Next::Wait);
        }
    }
    if conn.closing {
        return Ok(Next::Close);
    }
    let mut budget = REQUESTS_PER_TURN;
    let mut reads = 0;
    let mut drained = false;
    loop {
        match conn.session.run(ctx, now, &mut budget, stats) {
            Outcome::NeedInput => {
                // Write each batch before the next read, so a batch costs one write.
                write_out(conn, stats)?;
                // The shared flag, not the loop's copy: a `shutdown` served earlier this turn
                // may have set it.
                if ctx.stopping.load(Relaxed) {
                    return Ok(close_after_write(conn));
                }
                if conn.blocked || (drained && !conn.read_closed) {
                    return Ok(Next::Wait);
                }
                if reads == READS_PER_TURN {
                    return Ok(Next::Again);
                }
                let room = conn.session.read_room();
                match conn.session.read_from(conn.stream.as_raw_fd(), stats) {
                    Ok(0) => return Ok(Next::Close),
                    Ok(read) => {
                        reads += 1;
                        // A short read drained the socket. A closed peer is read until 0,
                        // since its end of input raises no further event.
                        drained = read < room;
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(Next::Wait);
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(error),
                }
            }
            Outcome::Budget => {
                write_out(conn, stats)?;
                return Ok(match conn.blocked {
                    true => Next::Wait,
                    false => Next::Again,
                });
            }
            Outcome::OutputFull => {
                write_out(conn, stats)?;
                if conn.blocked {
                    return Ok(Next::Wait);
                }
            }
            Outcome::Close => {
                write_out(conn, stats)?;
                return Ok(close_after_write(conn));
            }
            Outcome::Shutdown => {
                ctx.begin_shutdown();
                write_out(conn, stats)?;
                return Ok(close_after_write(conn));
            }
        }
    }
}

/// Closes the connection now if its replies are all written, or once they are.
fn close_after_write(conn: &mut Conn) -> Next {
    if conn.blocked {
        conn.closing = true;
        return Next::Wait;
    }
    Next::Close
}

/// Writes as many replies as the socket takes, setting `blocked` if some are left.
fn write_out(conn: &mut Conn, stats: &mut LocalStats) -> io::Result<()> {
    conn.blocked = false;
    let out = conn.session.output();
    while out.has_remaining() {
        let mut slices = [IoSlice::new(&[]); WRITE_SLICES];
        let filled = out.chunks_vectored(&mut slices);
        match conn.stream.write_vectored(&slices[..filled]) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => {
                out.advance(written);
                stats.add(Stat::BytesWritten, written as u64);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                conn.blocked = true;
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
