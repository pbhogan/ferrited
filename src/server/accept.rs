//! The acceptor thread: enforces the connection limit and hands each connection to the event
//! loop serving the fewest.

use std::io::{self, IoSlice};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use mio::{Events, Interest, Poll, Token, Waker};

use super::Context;
use super::eventloop::Handle;
use super::stream::{Listener, Stream};

const LISTENER: Token = Token(0);
const WAKE: Token = Token(1);
/// Wait before accepting again after a failure, such as running out of file descriptors.
const RETRY: Duration = Duration::from_millis(100);
/// What memcached sends a client over the connection limit before closing it.
pub(crate) const TOO_MANY: &[u8] = b"ERROR Too many open connections\r\n";

pub(crate) struct Acceptor {
    ctx: Arc<Context>,
    poll: Poll,
    listener: Listener,
    loops: Vec<Handle>,
    /// Where the search for the least busy loop starts, so ties rotate.
    next: usize,
}

impl Acceptor {
    pub fn new(
        ctx: Arc<Context>,
        mut listener: Listener,
        loops: Vec<Handle>,
    ) -> io::Result<(Self, Arc<Waker>)> {
        assert!(!loops.is_empty(), "the acceptor needs an event loop");
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKE)?);
        poll.registry()
            .register(&mut listener, LISTENER, Interest::READABLE)?;
        let acceptor = Self {
            ctx,
            poll,
            listener,
            loops,
            next: 0,
        };
        Ok((acceptor, waker))
    }

    /// Accepts connections until shutdown begins. The listener closes when this returns.
    pub fn run(mut self) {
        let mut events = Events::with_capacity(8);
        let mut retry_at: Option<Instant> = None;
        while !self.ctx.stopping.load(Relaxed) {
            let timeout = retry_at.map(|at| at.saturating_duration_since(Instant::now()));
            if let Err(error) = self.poll.poll(&mut events, timeout)
                && error.kind() != io::ErrorKind::Interrupted
            {
                eprintln!("ferrited: acceptor poll failed: {error}");
            }
            if self.ctx.stopping.load(Relaxed) {
                break;
            }
            // Edge-triggered: accept until it would block; after an error, retry on a timer.
            retry_at = self.accept_waiting();
        }
    }

    /// Accepts every waiting connection; returns when to retry if an accept failed. Stops once
    /// shutdown begins, since a finishing loop might never serve a new connection.
    fn accept_waiting(&mut self) -> Option<Instant> {
        loop {
            if self.ctx.stopping.load(Relaxed) {
                return None;
            }
            match self.listener.accept() {
                Ok(stream) => self.admit(stream),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return None,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted | io::ErrorKind::ConnectionAborted
                    ) => {}
                Err(error) => {
                    eprintln!("accept failed: {error}");
                    return Some(Instant::now() + RETRY);
                }
            }
        }
    }

    /// Hands a new connection to a loop, or turns it away at the connection limit.
    fn admit(&mut self, mut stream: Stream) {
        let stats = &self.ctx.stats;
        // Only this thread opens connections, so the count cannot rise before the increment.
        if stats.curr_connections.load(Relaxed) >= self.ctx.max_connections as u64 {
            let _ = stream.write_vectored(&[IoSlice::new(TOO_MANY)]);
            stats.rejected_connections.fetch_add(1, Relaxed);
            return;
        }
        stats.curr_connections.fetch_add(1, Relaxed);
        stats.total_connections.fetch_add(1, Relaxed);
        let count = self.loops.len();
        let chosen = (0..count)
            .map(|offset| (self.next + offset) % count)
            .min_by_key(|&index| self.loops[index].open())
            .expect("there is at least one loop");
        self.next = (chosen + 1) % count;
        self.loops[chosen].hand(stream);
    }
}
