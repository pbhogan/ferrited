//! The queue of reply bytes waiting to be written to one connection.

use std::collections::VecDeque;
use std::io::{self, IoSlice};

use bytes::{Buf, Bytes};

/// Values at least this long are queued by reference instead of being copied.
pub(crate) const SHARE_THRESHOLD: usize = 1024;
/// An inline buffer that grew beyond this is released once everything is written.
const MAX_RETAINED: usize = 256 * 1024;

struct Chunk {
    bytes: Bytes,
    /// Copied into the reply rather than shared with the store.
    inline: bool,
}

/// Reply bytes in send order: queued chunks, then an inline buffer for small writes. Large
/// values are shared with the store, so a reply's memory cost is its inline bytes.
#[derive(Default)]
pub(crate) struct Reply {
    chunks: VecDeque<Chunk>,
    /// Inline bytes after the last chunk. Bytes before `tail_start` have been written.
    tail: Vec<u8>,
    tail_start: usize,
    len: usize,
    inline: usize,
}

impl Reply {
    pub fn new() -> Self {
        Self::default()
    }

    /// Unwritten bytes in total.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Unwritten bytes held in the reply's own memory.
    pub fn inline_len(&self) -> usize {
        self.inline
    }

    /// Queued chunks, not counting the inline buffer.
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    pub fn push(&mut self, byte: u8) {
        self.extend_from_slice(&[byte]);
    }

    #[inline]
    pub fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.tail.extend_from_slice(bytes);
        self.len += bytes.len();
        self.inline += bytes.len();
    }

    /// Appends the decimal digits of `value`, much cheaper than `write!`.
    #[inline]
    pub fn number(&mut self, value: u64) {
        let count = value.checked_ilog10().unwrap_or(0) as usize + 1;
        let mut digits = [0u8; 20];
        let mut rest = value;
        for digit in digits[..count].iter_mut().rev() {
            *digit = b'0' + (rest % 10) as u8;
            rest /= 10;
        }
        // Copying the whole fixed-size array then trimming avoids a `memmove` call.
        let end = self.tail.len() + count;
        self.tail.extend_from_slice(&digits);
        self.tail.truncate(end);
        self.len += count;
        self.inline += count;
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.tail.reserve(additional);
    }

    /// Appends `value` in decimal, with a leading `-` if negative.
    #[inline]
    pub fn signed(&mut self, value: i64) {
        if value < 0 {
            self.push(b'-');
        }
        self.number(value.unsigned_abs());
    }

    /// Appends a stored value: by reference if it is large, copied otherwise.
    pub fn value(&mut self, value: &Bytes) {
        if value.len() < SHARE_THRESHOLD {
            self.extend_from_slice(value);
            return;
        }
        if self.tail.len() > self.tail_start {
            let mut sealed = Bytes::from(std::mem::take(&mut self.tail));
            sealed.advance(self.tail_start);
            self.tail_start = 0;
            self.chunks.push_back(Chunk {
                bytes: sealed,
                inline: true,
            });
        }
        self.len += value.len();
        self.chunks.push_back(Chunk {
            bytes: value.clone(),
            inline: false,
        });
    }

    fn unwritten_tail(&self) -> &[u8] {
        &self.tail[self.tail_start..]
    }
}

impl io::Write for Reply {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Reading consumes the reply front to back, as it is written to a socket.
impl Buf for Reply {
    fn remaining(&self) -> usize {
        self.len
    }

    fn chunk(&self) -> &[u8] {
        match self.chunks.front() {
            Some(chunk) => &chunk.bytes,
            None => self.unwritten_tail(),
        }
    }

    fn chunks_vectored<'a>(&'a self, slices: &mut [IoSlice<'a>]) -> usize {
        let tail = self.unwritten_tail();
        let parts = self
            .chunks
            .iter()
            .map(|chunk| &chunk.bytes[..])
            .chain((!tail.is_empty()).then_some(tail));
        let mut filled = 0;
        for (slice, part) in slices.iter_mut().zip(parts) {
            *slice = IoSlice::new(part);
            filled += 1;
        }
        filled
    }

    fn advance(&mut self, mut count: usize) {
        assert!(count <= self.len, "advanced past the end of the reply");
        self.len -= count;
        while count > 0 {
            let Some(chunk) = self.chunks.front_mut() else {
                self.tail_start += count;
                self.inline -= count;
                break;
            };
            let taken = count.min(chunk.bytes.len());
            if chunk.inline {
                self.inline -= taken;
            }
            count -= taken;
            if taken == chunk.bytes.len() {
                self.chunks.pop_front();
            } else {
                chunk.bytes.advance(taken);
            }
        }
        if self.len == 0 {
            self.tail_start = 0;
            if self.tail.capacity() > MAX_RETAINED {
                self.tail = Vec::new();
            } else {
                self.tail.clear();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    /// Drains the reply through `chunk`/`advance`, `step` bytes at a time.
    fn drain(reply: &mut Reply, step: usize) -> Vec<u8> {
        let mut written = Vec::new();
        while reply.has_remaining() {
            let chunk = reply.chunk();
            let taken = chunk.len().min(step);
            written.extend_from_slice(&chunk[..taken]);
            reply.advance(taken);
        }
        written
    }

    fn sample(large: &Bytes) -> Reply {
        let mut reply = Reply::new();
        reply.extend_from_slice(b"VALUE k 0 ");
        write!(reply, "{}\r\n", large.len()).unwrap();
        reply.value(large);
        reply.extend_from_slice(b"\r\n");
        reply.value(&Bytes::from_static(b"small"));
        reply.push(b'!');
        reply
    }

    #[test]
    fn keeps_bytes_in_order_across_inline_and_shared_chunks() {
        let large = Bytes::from(vec![b'x'; SHARE_THRESHOLD]);
        let mut expected = format!("VALUE k 0 {}\r\n", large.len()).into_bytes();
        expected.extend_from_slice(&large);
        expected.extend_from_slice(b"\r\nsmall!");
        for step in [1, 7, 1000, usize::MAX] {
            let mut reply = sample(&large);
            assert_eq!(reply.len(), expected.len());
            assert_eq!(drain(&mut reply, step), expected, "step {step}");
            assert!(reply.is_empty());
            assert_eq!((reply.inline_len(), reply.chunk_count()), (0, 0));
        }
    }

    #[test]
    fn large_values_are_shared_and_small_ones_copied() {
        let large = Bytes::from(vec![b'x'; SHARE_THRESHOLD]);
        let mut reply = Reply::new();
        reply.value(&large);
        assert_eq!((reply.len(), reply.inline_len()), (SHARE_THRESHOLD, 0));
        assert_eq!(reply.chunk().as_ptr(), large.as_ptr());
        reply.value(&large.slice(1..));
        assert_eq!(reply.inline_len(), SHARE_THRESHOLD - 1);
        assert_eq!(reply.chunk_count(), 1);
    }

    #[test]
    fn vectored_view_lists_every_chunk_then_the_inline_buffer() {
        let large = Bytes::from(vec![b'x'; SHARE_THRESHOLD]);
        let reply = sample(&large);
        let mut slices = [IoSlice::new(&[]); 8];
        let filled = reply.chunks_vectored(&mut slices);
        let lengths: Vec<usize> = slices[..filled].iter().map(|slice| slice.len()).collect();
        assert_eq!(lengths, [16, SHARE_THRESHOLD, 8]);
        let mut one = [IoSlice::new(&[])];
        assert_eq!(reply.chunks_vectored(&mut one), 1);
    }

    #[test]
    fn writes_numbers_in_decimal() {
        let mut reply = Reply::new();
        for value in [0, 7, 10, 99, 1234567890, u64::MAX] {
            reply.number(value);
            reply.push(b' ');
        }
        for value in [0, -1, -10, 42, i64::MIN, i64::MAX] {
            reply.signed(value);
            reply.push(b' ');
        }
        let expected = format!(
            "0 7 10 99 1234567890 {} 0 -1 -10 42 {} {} ",
            u64::MAX,
            i64::MIN,
            i64::MAX
        );
        assert_eq!(drain(&mut reply, usize::MAX), expected.as_bytes());
    }

    #[test]
    fn can_be_refilled_after_a_partial_write() {
        let mut reply = Reply::new();
        reply.extend_from_slice(b"abcdef");
        reply.advance(4);
        reply.extend_from_slice(b"gh");
        assert_eq!((reply.len(), reply.inline_len()), (4, 4));
        assert_eq!(drain(&mut reply, 3), b"efgh");
        reply.extend_from_slice(b"ij");
        assert_eq!(drain(&mut reply, 3), b"ij");
    }
}
