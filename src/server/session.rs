//! A connection's protocol state, apart from its socket: unexecuted input and unwritten
//! replies. The event loop moves the bytes; the session parses and executes requests.

use std::io;
use std::os::fd::RawFd;

use bytes::{Buf, BufMut, BytesMut};

use super::reply::Reply;
use super::{Context, Flow, exec};
use crate::protocol;
use crate::stats::{LocalStats, Stat};
use crate::store::DeleteArgs;

/// The read buffer's starting size and growth step.
pub(crate) const READ_BUFFER: usize = 16 * 1024;
/// Unparsed bytes at which a batch is checked for a large data block still arriving.
const PENDING_CHECK: usize = 1024;
/// A read buffer that has held more than this is replaced once it is empty.
const IDLE_READ_BUFFER: usize = 64 * 1024;
/// Replies holding more inline bytes than this are written before the next request runs.
const FLUSH_BYTES: usize = 64 * 1024;
/// Replies holding more chunks than this are written before the next request runs.
const FLUSH_CHUNKS: usize = 64;

/// Why `Session::run` returned.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// No complete request is buffered.
    NeedInput,
    /// The request budget ran out.
    Budget,
    /// The replies are over the output bound; write them before running more.
    OutputFull,
    /// Write the replies, then close the connection.
    Close,
    /// Write the replies, close the connection, and stop the server.
    Shutdown,
}

pub struct Session {
    buf: BytesMut,
    out: Reply,
    /// Bytes still to discard from future reads: the rest of a value that was too large.
    skip: usize,
    /// The most bytes `buf` has held since it was allocated.
    peak: usize,
    /// Bytes `buf` must hold before its first request can be complete; parsing waits till then.
    need: usize,
}

impl Session {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(READ_BUFFER),
            out: Reply::new(),
            skip: 0,
            peak: 0,
            need: 0,
        }
    }

    /// Reads once from `fd` into the buffer; returns the bytes read (0 at end of input).
    pub fn read_from(&mut self, fd: RawFd, stats: &mut LocalStats) -> io::Result<usize> {
        if self.buf.capacity() - self.buf.len() < READ_BUFFER / 4 {
            self.buf.reserve(READ_BUFFER);
        }
        let spare = self.buf.chunk_mut();
        // SAFETY: the kernel writes at most `spare.len()` bytes into capacity `BytesMut` owns,
        // and no Rust reference to uninitialized memory is made.
        let read = unsafe { libc::read(fd, spare.as_mut_ptr().cast(), spare.len()) };
        if read < 0 {
            return Err(io::Error::last_os_error());
        }
        let read = read as usize;
        // SAFETY: the kernel filled the first `read` bytes of the spare capacity.
        unsafe { self.buf.advance_mut(read) };
        stats.add(Stat::BytesRead, read as u64);
        self.peak = self.peak.max(self.buf.len());
        Ok(read)
    }

    /// Spare room in the buffer; a read returning less has drained the socket.
    pub fn read_room(&self) -> usize {
        self.buf.capacity() - self.buf.len()
    }

    /// Appends bytes as if read from the socket, for tests and the `hotpath` example.
    pub fn feed(&mut self, bytes: &[u8], stats: &mut LocalStats) {
        self.buf.extend_from_slice(bytes);
        stats.add(Stat::BytesRead, bytes.len() as u64);
        self.peak = self.peak.max(self.buf.len());
    }

    /// Executes up to `*budget` buffered requests, decrementing it for each and appending their
    /// replies. `now` is the Unix time they all see.
    pub fn run(
        &mut self,
        ctx: &Context,
        now: u64,
        budget: &mut usize,
        stats: &mut LocalStats,
    ) -> Outcome {
        loop {
            if self.output_full() {
                return Outcome::OutputFull;
            }
            if self.skip > 0 {
                let discard = self.skip.min(self.buf.len());
                self.buf.advance(discard);
                self.skip -= discard;
                if self.skip > 0 {
                    return Outcome::NeedInput;
                }
            }
            if self.buf.len() < self.need {
                return Outcome::NeedInput;
            }
            self.need = 0;
            if *budget == 0 {
                return Outcome::Budget;
            }
            let Some(step) = step(ctx, &self.buf, now, &mut self.out, stats) else {
                self.await_input(ctx.item_max);
                return Outcome::NeedInput;
            };
            *budget -= 1;
            self.buf.advance(step.consumed);
            self.skip = step.skip;
            match step.flow {
                Flow::Continue => {}
                Flow::Close => return Outcome::Close,
                Flow::Shutdown => return Outcome::Shutdown,
            }
        }
    }

    pub(crate) fn output(&mut self) -> &mut Reply {
        &mut self.out
    }

    /// Discards pending replies, copying them to `capture` if given; returns their length. For
    /// the `hotpath` example, which has no socket.
    pub fn take_output(&mut self, capture: Option<&mut Vec<u8>>) -> usize {
        let len = self.out.len();
        match capture {
            Some(capture) => capture.extend_from_slice(&self.out.copy_to_bytes(len)),
            None => self.out.advance(len),
        }
        len
    }

    /// Whether replies must be written before another request runs, so a slow reader cannot
    /// make the server queue replies without bound.
    fn output_full(&self) -> bool {
        self.out.inline_len() > FLUSH_BYTES || self.out.chunk_count() > FLUSH_CHUNKS
    }

    /// Called when no complete request is buffered. Releases a buffer that grew large, and
    /// makes room at once for a large data block still arriving.
    fn await_input(&mut self, item_max: usize) {
        release_idle(&mut self.buf, &mut self.peak);
        // Only a large leftover is worth checking: checking every partial request slows small
        // pipelined sets, which arrive in a read or two anyway.
        if self.buf.len() >= PENDING_CHECK
            && let Some(total) = protocol::pending_len(&self.buf, item_max)
            && total > READ_BUFFER
            && total > self.buf.len()
        {
            self.need = total;
            self.buf.reserve(total - self.buf.len());
        }
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

/// Replaces an empty read buffer that grew large, so one big request does not pin its memory.
pub(crate) fn release_idle(buf: &mut BytesMut, peak: &mut usize) {
    // An emptied `BytesMut` under-reports its allocation in `capacity()`, so use `peak`.
    if buf.is_empty() && *peak > IDLE_READ_BUFFER {
        *buf = BytesMut::with_capacity(READ_BUFFER);
        *peak = 0;
    }
}

/// The result of handling the request at the front of the read buffer.
struct Step {
    consumed: usize,
    /// Bytes still to discard from future reads.
    skip: usize,
    flow: Flow,
}

/// Executes the first request in `buf`, appending its reply to `out`; `None` if incomplete.
fn step(
    ctx: &Context,
    buf: &[u8],
    now: u64,
    out: &mut Reply,
    stats: &mut LocalStats,
) -> Option<Step> {
    match protocol::parse(buf, ctx.item_max) {
        Ok(None) => None,
        Ok(Some(request)) => Some(Step {
            consumed: request.consumed,
            skip: 0,
            flow: exec::execute(ctx, request.cmd, now, out, stats),
        }),
        Err(failure) => {
            if let Some(key) = &failure.unlink {
                ctx.store.delete(key, now, &DeleteArgs::default());
            }
            if !failure.silent {
                out.extend_from_slice(failure.error.message().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            if failure.error.is_fatal() {
                return Some(Step {
                    consumed: failure.consumed,
                    skip: 0,
                    flow: Flow::Close,
                });
            }
            let swallowed = failure.swallow.min(buf.len() - failure.consumed);
            Some(Step {
                consumed: failure.consumed + swallowed,
                skip: failure.swallow - swallowed,
                flow: Flow::Continue,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use clap::Parser;

    use super::*;
    use crate::config::Config;

    fn context() -> std::sync::Arc<Context> {
        Context::new(&Config::parse_from(["ferrited"]))
    }

    /// Runs everything buffered and returns the outcome and replies.
    fn run_all(session: &mut Session, ctx: &Context) -> (Outcome, String) {
        let mut budget = usize::MAX;
        let outcome = session.run(ctx, 0, &mut budget, &mut LocalStats::default());
        let out = session.output();
        let replies = out.copy_to_bytes(out.remaining());
        (outcome, String::from_utf8(replies.to_vec()).unwrap())
    }

    #[test]
    fn runs_complete_requests_and_keeps_a_partial_one() {
        let ctx = context();
        let mut session = Session::new();
        session.feed(b"mn\r\nmn\r\nm", &mut LocalStats::default());
        assert_eq!(
            run_all(&mut session, &ctx),
            (Outcome::NeedInput, "MN\r\nMN\r\n".into())
        );
        session.feed(b"n\r\n", &mut LocalStats::default());
        assert_eq!(
            run_all(&mut session, &ctx),
            (Outcome::NeedInput, "MN\r\n".into())
        );
    }

    #[test]
    fn stops_when_the_budget_runs_out() {
        let ctx = context();
        let mut session = Session::new();
        session.feed(&b"mn\r\n".repeat(5), &mut LocalStats::default());
        let mut budget = 3;
        let mut stats = LocalStats::default();
        assert_eq!(
            session.run(&ctx, 0, &mut budget, &mut stats),
            Outcome::Budget
        );
        assert_eq!((budget, session.output().remaining()), (0, 12));
        budget = 3;
        assert_eq!(
            session.run(&ctx, 0, &mut budget, &mut stats),
            Outcome::NeedInput
        );
        assert_eq!((budget, session.output().remaining()), (1, 20));
    }

    #[test]
    fn stops_when_the_replies_are_over_the_output_bound() {
        let ctx = context();
        let mut session = Session::new();
        let value = "v".repeat(1000);
        session.feed(
            format!("set k 0 0 {}\r\n{value}\r\n", value.len()).as_bytes(),
            &mut LocalStats::default(),
        );
        session.feed(&b"get k\r\n".repeat(200), &mut LocalStats::default());
        let (outcome, replies) = run_all(&mut session, &ctx);
        assert_eq!(outcome, Outcome::OutputFull);
        // Values under 1 KiB are copied, so the inline bound stops the batch.
        let gets = replies.matches("END\r\n").count();
        assert!(gets > 50 && gets < 200, "{gets} gets ran");
        let mut total = gets;
        loop {
            let (outcome, more) = run_all(&mut session, &ctx);
            total += more.matches("END\r\n").count();
            if outcome == Outcome::NeedInput {
                break;
            }
            assert_eq!(outcome, Outcome::OutputFull);
        }
        assert_eq!(total, 200);
    }

    #[test]
    fn quit_closes_after_the_replies_before_it() {
        let ctx = context();
        let mut session = Session::new();
        session.feed(b"mn\r\nquit\r\nmn\r\n", &mut LocalStats::default());
        assert_eq!(
            run_all(&mut session, &ctx),
            (Outcome::Close, "MN\r\n".into())
        );
    }

    #[test]
    fn a_large_value_still_arriving_is_given_room_and_not_parsed_until_complete() {
        let ctx = context();
        let mut session = Session::new();
        let len = 256 * 1024;
        let mut first = format!("set big 0 0 {len}\r\n").into_bytes();
        first.resize(first.len() + 4096, b'v');
        session.feed(&first, &mut LocalStats::default());
        assert_eq!(run_all(&mut session, &ctx).0, Outcome::NeedInput);
        assert!(session.read_room() >= len + 2 - 4096);
        let mut rest = vec![b'v'; len - 4096];
        rest.extend_from_slice(b"\r\n");
        session.feed(&rest, &mut LocalStats::default());
        assert_eq!(
            run_all(&mut session, &ctx),
            (Outcome::NeedInput, "STORED\r\n".into())
        );
    }

    #[test]
    fn an_idle_read_buffer_that_grew_large_is_replaced() {
        let mut buf = BytesMut::with_capacity(READ_BUFFER);
        let mut peak = 0;
        buf.extend_from_slice(&vec![0u8; 1024 * 1024]);
        peak = peak.max(buf.len());
        buf.advance(1024 * 1024);
        release_idle(&mut buf, &mut peak);
        // Reserving reclaims any allocation still behind the buffer.
        buf.reserve(1);
        assert!(
            buf.capacity() <= IDLE_READ_BUFFER,
            "kept {} bytes",
            buf.capacity()
        );
        assert_eq!(peak, 0);

        // A buffer that still holds bytes, or never grew, is left alone.
        buf.extend_from_slice(b"partial request");
        peak = 1024 * 1024;
        let pointer = buf.as_ptr();
        release_idle(&mut buf, &mut peak);
        assert_eq!((buf.as_ptr(), &buf[..]), (pointer, &b"partial request"[..]));
        buf.clear();
        peak = IDLE_READ_BUFFER;
        release_idle(&mut buf, &mut peak);
        assert_eq!(buf.as_ptr(), pointer);
    }
}
