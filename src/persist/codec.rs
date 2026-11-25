//! The entry encoding shared by log segments and snapshots.
//!
//! Each entry is framed as `[payload length: u32 LE][CRC-32 of payload: u32 LE][payload]`,
//! so a reader can tell a complete entry from one cut short by a crash.

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

use bytes::Bytes;

use crate::oplog::{Position, Record, RunStart};

/// Encoding version written in, and required of, every file header.
pub const FORMAT: u32 = 1;
/// Length of a framed file header; a file no longer than this holds no records.
pub const HEADER_LEN: u64 = 8 + 1 + 1 + 4 + 8 + 8;
/// Longest payload; readers treat a longer length field as corruption, writers refuse one.
const MAX_PAYLOAD: u32 = 1 << 30;
/// Bytes a Put payload needs besides its value, with a 255-byte key.
const MAX_PUT_OVERHEAD: usize = 1 + 8 + 8 + 4 + 8 + 1 + 1 + u8::MAX as usize;
/// Largest value a record can hold; the item size limit must not exceed it while
/// persistence is on.
pub const MAX_VALUE: usize = MAX_PAYLOAD as usize - MAX_PUT_OVERHEAD;

const TAG_PUT: u8 = 1;
const TAG_DELETE: u8 = 2;
const TAG_FLUSH: u8 = 3;
const TAG_HEADER: u8 = 4;
const TAG_END: u8 = 5;
const TAG_LOSS: u8 = 6;
const TAG_RUN: u8 = 7;
const TAG_POSITION: u8 = 8;
const TAG_LINKS: u8 = 9;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    Segment,
    Snapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    Record(Record),
    /// The first entry of every file. `start` is a segment's first sequence number, or the
    /// first one a snapshot does not cover; `floor` is a snapshot's flush floor (0 in segments).
    Header {
        kind: FileKind,
        format: u32,
        start: u64,
        floor: u64,
    },
    /// The last entry of a complete snapshot, with the number of records before it.
    End {
        count: u64,
    },
    /// Records were dropped before reaching the log here. It takes a sequence number, so a
    /// snapshot that starts after it covers the loss.
    Loss,
    /// The first entry of a run, or in a snapshot, a run its log may still need.
    Run(RunStart),
    /// A receiver's position on a peer's log.
    Position(Position),
    /// The peers a node was linked with, by node ID (see `Logged::Links`).
    Links(Vec<u8>),
    /// An intact file header of a kind or format this version cannot read.
    Unsupported(Unsupported),
}

/// The kind and format from a file header this version cannot read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unsupported {
    pub kind: u8,
    pub format: u32,
}

impl Unsupported {
    /// Stops startup rather than treat the file as damaged and discard it.
    pub fn error(self, path: &Path) -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} has file format {} (kind {}), but this version of ferrited reads only format {FORMAT}; refusing to start rather than discard it",
                path.display(),
                self.format,
                self.kind
            ),
        )
    }
}

/// Returns the header's kind and format if this version cannot read them; `None` for a
/// readable, damaged or unreadable file (the full read reports those).
pub fn unsupported_format(path: &Path) -> Option<Unsupported> {
    let mut input = BufReader::new(File::open(path).ok()?);
    match read_entry(&mut input, &mut Vec::new()).ok()? {
        ReadOutcome::Entry(Entry::Unsupported(unsupported)) => Some(unsupported),
        _ => None,
    }
}

/// What reading the next entry found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadOutcome {
    Entry(Entry),
    /// The input ended exactly between entries.
    Eof,
    /// The input ended inside an entry, or the entry failed its checksum or could not be decoded.
    Torn,
}

/// Appends one framed record to `out`.
pub fn encode_record(record: &Record, out: &mut Vec<u8>) {
    frame(out, |out| match record {
        Record::Put {
            key,
            version,
            cas,
            flags,
            expiry,
            stale,
            value,
        } => {
            out.push(TAG_PUT);
            out.extend_from_slice(&version.to_le_bytes());
            out.extend_from_slice(&cas.to_le_bytes());
            out.extend_from_slice(&flags.to_le_bytes());
            out.extend_from_slice(&expiry.to_le_bytes());
            out.push(u8::from(*stale));
            push_key(out, key);
            out.extend_from_slice(value);
        }
        Record::Delete { key, version } => {
            out.push(TAG_DELETE);
            out.extend_from_slice(&version.to_le_bytes());
            push_key(out, key);
        }
        Record::Flush { version } => {
            out.push(TAG_FLUSH);
            out.extend_from_slice(&version.to_le_bytes());
        }
    });
}

/// Appends a framed file header to `out`.
pub fn encode_header(kind: FileKind, start: u64, floor: u64, out: &mut Vec<u8>) {
    frame(out, |out| {
        out.push(TAG_HEADER);
        out.push(match kind {
            FileKind::Segment => 1,
            FileKind::Snapshot => 2,
        });
        out.extend_from_slice(&FORMAT.to_le_bytes());
        out.extend_from_slice(&start.to_le_bytes());
        out.extend_from_slice(&floor.to_le_bytes());
    });
}

/// Appends a framed snapshot end marker to `out`.
pub fn encode_end(count: u64, out: &mut Vec<u8>) {
    frame(out, |out| {
        out.push(TAG_END);
        out.extend_from_slice(&count.to_le_bytes());
    });
}

/// Appends a framed loss marker to `out`.
pub fn encode_loss(out: &mut Vec<u8>) {
    frame(out, |out| out.push(TAG_LOSS));
}

/// Appends a framed run marker to `out`.
pub fn encode_run(run: RunStart, out: &mut Vec<u8>) {
    frame(out, |out| {
        out.push(TAG_RUN);
        out.extend_from_slice(&run.id.to_le_bytes());
        out.extend_from_slice(&run.first.to_le_bytes());
    });
}

/// Appends a framed receiver position to `out`.
pub fn encode_position(position: Position, out: &mut Vec<u8>) {
    frame(out, |out| {
        out.push(TAG_POSITION);
        out.push(position.peer);
        out.extend_from_slice(&position.run.to_le_bytes());
        out.extend_from_slice(&position.next.to_le_bytes());
    });
}

/// Appends a framed link record to `out`: one byte per linked peer's node ID.
pub fn encode_links(up: &[u8], out: &mut Vec<u8>) {
    frame(out, |out| {
        out.push(TAG_LINKS);
        out.extend_from_slice(up);
    });
}

/// Reads the next entry. `scratch` is reused between calls to avoid allocating.
pub fn read_entry<R: Read>(input: &mut R, scratch: &mut Vec<u8>) -> io::Result<ReadOutcome> {
    let mut header = [0u8; 8];
    match read_full(input, &mut header)? {
        0 => return Ok(ReadOutcome::Eof),
        8 => {}
        _ => return Ok(ReadOutcome::Torn),
    }
    let len = u32::from_le_bytes(header[..4].try_into().unwrap());
    let crc = u32::from_le_bytes(header[4..].try_into().unwrap());
    if len > MAX_PAYLOAD {
        return Ok(ReadOutcome::Torn);
    }
    // Grow only as data arrives, so a corrupt length cannot force a huge allocation.
    scratch.clear();
    input.by_ref().take(u64::from(len)).read_to_end(scratch)?;
    if scratch.len() != len as usize || crc32fast::hash(scratch) != crc {
        return Ok(ReadOutcome::Torn);
    }
    Ok(decode(scratch).map_or(ReadOutcome::Torn, ReadOutcome::Entry))
}

fn frame(out: &mut Vec<u8>, payload: impl FnOnce(&mut Vec<u8>)) {
    let start = out.len();
    out.extend_from_slice(&[0; 8]);
    payload(out);
    let len = out.len() - start - 8;
    assert!(
        len <= MAX_PAYLOAD as usize,
        "entry payload of {len} bytes exceeds the {MAX_PAYLOAD}-byte limit readers accept"
    );
    let len = len as u32;
    let crc = crc32fast::hash(&out[start + 8..]);
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    out[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
}

fn push_key(out: &mut Vec<u8>, key: &[u8]) {
    let len = u8::try_from(key.len()).expect("keys are at most 255 bytes");
    out.push(len);
    out.extend_from_slice(key);
}

/// Reads until `buf` is full or the input ends, and returns the number of bytes read.
fn read_full<R: Read>(input: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match input.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(filled)
}

fn decode(payload: &[u8]) -> Option<Entry> {
    let mut cursor = Cursor(payload);
    let entry = match cursor.u8()? {
        TAG_PUT => {
            let version = cursor.u64()?;
            let cas = cursor.u64()?;
            let flags = cursor.u32()?;
            let expiry = cursor.u64()?;
            let stale = cursor.u8()? != 0;
            let key = cursor.key()?;
            let value = Bytes::copy_from_slice(cursor.rest());
            Entry::Record(Record::Put {
                key,
                version,
                cas,
                flags,
                expiry,
                stale,
                value,
            })
        }
        TAG_DELETE => {
            let version = cursor.u64()?;
            let key = cursor.key()?;
            Entry::Record(Record::Delete { key, version })
        }
        TAG_FLUSH => Entry::Record(Record::Flush {
            version: cursor.u64()?,
        }),
        TAG_HEADER => {
            let (kind, format) = (cursor.u8()?, cursor.u32()?);
            let supported = format == FORMAT;
            let kind = match kind {
                1 if supported => FileKind::Segment,
                2 if supported => FileKind::Snapshot,
                // The checksum passed, so this is a real header; its layout is unknown, so the
                // rest is not checked.
                _ => return Some(Entry::Unsupported(Unsupported { kind, format })),
            };
            Entry::Header {
                kind,
                format,
                start: cursor.u64()?,
                floor: cursor.u64()?,
            }
        }
        TAG_END => Entry::End {
            count: cursor.u64()?,
        },
        TAG_LOSS => Entry::Loss,
        TAG_RUN => Entry::Run(RunStart {
            id: cursor.u64()?,
            first: cursor.u64()?,
        }),
        TAG_POSITION => Entry::Position(Position {
            peer: cursor.u8()?,
            run: cursor.u64()?,
            next: cursor.u64()?,
        }),
        TAG_LINKS => Entry::Links(cursor.rest().to_vec()),
        _ => return None,
    };
    cursor.0.is_empty().then_some(entry)
}

struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.0.len() < len {
            return None;
        }
        let (taken, rest) = self.0.split_at(len);
        self.0 = rest;
        Some(taken)
    }

    fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.0)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn key(&mut self) -> Option<Bytes> {
        let len = usize::from(self.u8()?);
        Some(Bytes::copy_from_slice(self.take(len)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> Vec<Record> {
        vec![
            Record::Put {
                key: Bytes::from_static(b"key"),
                version: 1 << 40,
                cas: 12345,
                flags: u32::MAX,
                expiry: 1_900_000_000,
                stale: true,
                value: Bytes::from(vec![7u8; 3000]),
            },
            Record::Put {
                key: Bytes::from(vec![b'k'; 250]),
                version: 2,
                cas: 2,
                flags: 0,
                expiry: 0,
                stale: false,
                value: Bytes::new(),
            },
            Record::Delete {
                key: Bytes::from_static(b"gone"),
                version: 3,
            },
            Record::Flush { version: 4 },
        ]
    }

    fn read_all(bytes: &[u8]) -> Vec<ReadOutcome> {
        let mut input = bytes;
        let mut scratch = Vec::new();
        let mut outcomes = Vec::new();
        loop {
            let outcome = read_entry(&mut input, &mut scratch).unwrap();
            let done = outcome != ReadOutcome::Eof && !matches!(outcome, ReadOutcome::Entry(_));
            let eof = outcome == ReadOutcome::Eof;
            outcomes.push(outcome);
            if done || eof {
                return outcomes;
            }
        }
    }

    #[test]
    fn every_entry_round_trips() {
        let run = RunStart { id: 99, first: 10 };
        let position = Position {
            peer: 3,
            run: 7,
            next: 1234,
        };
        let mut bytes = Vec::new();
        encode_header(FileKind::Snapshot, 10, 20, &mut bytes);
        for record in samples() {
            encode_record(&record, &mut bytes);
        }
        encode_loss(&mut bytes);
        encode_run(run, &mut bytes);
        encode_position(position, &mut bytes);
        encode_links(&[2, 5], &mut bytes);
        encode_end(4, &mut bytes);
        let mut expected = vec![ReadOutcome::Entry(Entry::Header {
            kind: FileKind::Snapshot,
            format: FORMAT,
            start: 10,
            floor: 20,
        })];
        expected.extend(
            samples()
                .into_iter()
                .map(|r| ReadOutcome::Entry(Entry::Record(r))),
        );
        expected.push(ReadOutcome::Entry(Entry::Loss));
        expected.push(ReadOutcome::Entry(Entry::Run(run)));
        expected.push(ReadOutcome::Entry(Entry::Position(position)));
        expected.push(ReadOutcome::Entry(Entry::Links(vec![2, 5])));
        expected.push(ReadOutcome::Entry(Entry::End { count: 4 }));
        expected.push(ReadOutcome::Eof);
        assert_eq!(read_all(&bytes), expected);
    }

    #[test]
    fn an_empty_link_record_round_trips() {
        let mut bytes = Vec::new();
        encode_links(&[], &mut bytes);
        assert_eq!(
            read_all(&bytes),
            [
                ReadOutcome::Entry(Entry::Links(Vec::new())),
                ReadOutcome::Eof
            ]
        );
    }

    #[test]
    fn the_unsupported_format_error_names_the_format_this_version_reads() {
        let unsupported = Unsupported { kind: 1, format: 9 };
        let message = unsupported.error(Path::new("log-1.seg")).to_string();
        assert!(message.contains("reads only format 1"), "{message}");
    }

    #[test]
    fn a_cut_anywhere_inside_an_entry_reads_as_torn() {
        let mut bytes = Vec::new();
        encode_record(&samples()[2], &mut bytes);
        for cut in 1..bytes.len() {
            assert_eq!(read_all(&bytes[..cut]), [ReadOutcome::Torn], "cut at {cut}");
        }
    }

    #[test]
    fn a_changed_byte_reads_as_torn() {
        let mut bytes = Vec::new();
        encode_record(&samples()[0], &mut bytes);
        for index in [0, 5, 9, 30, bytes.len() - 1] {
            let mut damaged = bytes.clone();
            damaged[index] ^= 0x40;
            assert_eq!(read_all(&damaged)[0], ReadOutcome::Torn, "byte {index}");
        }
    }

    #[test]
    fn a_huge_length_field_reads_as_torn_without_allocating_it() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(512u32 << 20).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&[1; 10]);
        let mut input = bytes.as_slice();
        let mut scratch = Vec::new();
        assert_eq!(
            read_entry(&mut input, &mut scratch).unwrap(),
            ReadOutcome::Torn
        );
        assert!(scratch.capacity() < 1 << 20, "{}", scratch.capacity());
    }

    #[test]
    fn a_zero_filled_tail_reads_as_torn() {
        for len in [8, 9, 16, 100, 4096] {
            assert_eq!(read_all(&vec![0; len]), [ReadOutcome::Torn], "{len} zeros");
        }
    }

    #[test]
    fn max_value_leaves_room_for_the_largest_put() {
        let mut bytes = Vec::new();
        encode_record(
            &Record::Put {
                key: Bytes::from(vec![b'k'; 255]),
                version: 1,
                cas: 1,
                flags: 0,
                expiry: 0,
                stale: false,
                value: Bytes::new(),
            },
            &mut bytes,
        );
        assert_eq!(bytes.len() - 8 + MAX_VALUE, MAX_PAYLOAD as usize);
    }

    #[test]
    #[should_panic(expected = "keys are at most 255 bytes")]
    fn a_key_longer_than_255_bytes_is_refused() {
        let record = Record::Delete {
            key: Bytes::from(vec![b'k'; 256]),
            version: 1,
        };
        encode_record(&record, &mut Vec::new());
    }

    #[test]
    fn header_len_is_the_length_of_a_header() {
        let mut bytes = Vec::new();
        encode_header(FileKind::Segment, u64::MAX, 0, &mut bytes);
        assert_eq!(bytes.len() as u64, HEADER_LEN);
    }

    #[test]
    fn a_header_of_an_unknown_format_or_kind_reads_as_unsupported() {
        let header = |kind: u8, format: u32, extra: &[u8]| {
            let mut bytes = Vec::new();
            frame(&mut bytes, |out| {
                out.push(TAG_HEADER);
                out.push(kind);
                out.extend_from_slice(&format.to_le_bytes());
                out.extend_from_slice(extra);
            });
            read_all(&bytes)[0].clone()
        };
        let unsupported =
            |kind, format| ReadOutcome::Entry(Entry::Unsupported(Unsupported { kind, format }));
        assert_eq!(header(1, FORMAT + 1, &[0; 40]), unsupported(1, FORMAT + 1));
        assert_eq!(header(9, FORMAT, &[0; 16]), unsupported(9, FORMAT));
        // The current format with the wrong length is damaged, not unsupported.
        assert_eq!(header(1, FORMAT, &[0; 3]), ReadOutcome::Torn);
    }

    #[test]
    fn an_unknown_tag_reads_as_torn() {
        let mut bytes = Vec::new();
        frame(&mut bytes, |out| out.push(99));
        assert_eq!(read_all(&bytes), [ReadOutcome::Torn]);
    }
}
