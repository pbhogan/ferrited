//! Log segments: append-only files named by the sequence number of their first entry.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::Path;

use super::codec::{self, Entry, FileKind, ReadOutcome};
use super::dir::DataDir;
use crate::oplog::{Logged, Position, Record, RunStart};

/// Buffered bytes at which `append` writes the buffer out.
pub const WRITE_AT: usize = 1 << 20;

/// Appends records to one log segment. After any error, discard it and start a new segment: a
/// partial write leaves the file offset and buffer out of step.
pub struct SegmentWriter {
    file: File,
    buf: Vec<u8>,
    start: u64,
    count: u64,
    bytes: u64,
}

impl SegmentWriter {
    /// Creates the segment starting at `start` and makes it durable. On failure, removes the
    /// file so a retry can create it.
    pub fn create(dir: &DataDir, start: u64) -> io::Result<Self> {
        let path = dir.segment_path(start);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut segment = Self {
            file,
            buf: Vec::with_capacity(WRITE_AT),
            start,
            count: 0,
            bytes: 0,
        };
        codec::encode_header(FileKind::Segment, start, 0, &mut segment.buf);
        if let Err(error) = segment.sync().and_then(|()| dir.sync()) {
            drop(segment);
            let _ = fs::remove_file(&path);
            return Err(error);
        }
        Ok(segment)
    }

    /// Buffers one record, writing the buffer out (unsynced) once it reaches `WRITE_AT`.
    /// Returns the bytes written, usually 0.
    pub fn append(&mut self, record: &Record) -> io::Result<u64> {
        codec::encode_record(record, &mut self.buf);
        self.count += 1;
        if self.buf.len() >= WRITE_AT {
            self.write_out()
        } else {
            Ok(0)
        }
    }

    /// Buffers a loss marker, which takes the next sequence number.
    pub fn append_loss(&mut self) {
        codec::encode_loss(&mut self.buf);
        self.count += 1;
    }

    /// Buffers a run marker, which takes the next sequence number.
    pub fn append_run(&mut self, run: RunStart) {
        codec::encode_run(run, &mut self.buf);
        self.count += 1;
    }

    /// Buffers a receiver position, which takes the next sequence number.
    pub fn append_position(&mut self, position: Position) {
        codec::encode_position(position, &mut self.buf);
        self.count += 1;
    }

    /// Buffers a link record, which takes the next sequence number.
    pub fn append_links(&mut self, up: &[u8]) {
        codec::encode_links(up, &mut self.buf);
        self.count += 1;
    }

    /// Appends any numbered entry; returns the bytes written, as `append` does.
    pub fn append_logged(&mut self, entry: &Logged) -> io::Result<u64> {
        match entry {
            Logged::Record(record) => return self.append(record),
            Logged::Loss => self.append_loss(),
            Logged::Run(run) => self.append_run(*run),
            Logged::Position(position) => self.append_position(*position),
            Logged::Links(up) => self.append_links(up),
        }
        Ok(0)
    }

    /// Writes the buffer out without syncing; returns the bytes written.
    pub fn write_out(&mut self) -> io::Result<u64> {
        let written = self.buf.len() as u64;
        self.file.write_all(&self.buf)?;
        self.buf.clear();
        self.bytes += written;
        Ok(written)
    }

    /// Writes the buffer out and syncs the file's data.
    pub fn sync(&mut self) -> io::Result<()> {
        self.write_out()?;
        self.file.sync_data()
    }

    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Sequence number of the next appended entry.
    pub fn next(&self) -> u64 {
        self.start + self.count
    }
}

/// What reading a segment found.
#[derive(Debug, PartialEq, Eq)]
pub struct SegmentScan {
    /// Entries read before the end or the first torn entry, loss markers included. The next
    /// sequence number is the segment's start plus this.
    pub entries: u64,
    /// Sequence numbers of loss markers.
    pub losses: Vec<u64>,
    pub runs: Vec<RunStart>,
    pub positions: Vec<(u64, Position)>,
    pub links: Vec<(u64, Vec<u8>)>,
    /// Length of the file's valid prefix.
    pub valid_len: u64,
    /// Reading stopped at a torn or corrupt entry, or a missing or wrong header.
    pub torn: bool,
}

/// Reads the segment that starts at `start`, passing each record and its sequence number to
/// `apply`, up to the first torn or corrupt entry. An unsupported format is an `InvalidData`
/// error rather than a torn segment, so the file is never discarded as damaged.
pub fn scan_segment(
    path: &Path,
    start: u64,
    mut apply: impl FnMut(u64, Record),
) -> io::Result<SegmentScan> {
    let mut input = Counting::new(BufReader::new(File::open(path)?));
    let mut scratch = Vec::new();
    let mut scan = SegmentScan {
        entries: 0,
        losses: Vec::new(),
        runs: Vec::new(),
        positions: Vec::new(),
        links: Vec::new(),
        valid_len: 0,
        torn: true,
    };
    match codec::read_entry(&mut input, &mut scratch)? {
        ReadOutcome::Entry(Entry::Header {
            kind: FileKind::Segment,
            start: header_start,
            ..
        }) if header_start == start => scan.valid_len = input.count,
        ReadOutcome::Entry(Entry::Unsupported(unsupported)) => {
            return Err(unsupported.error(path));
        }
        _ => return Ok(scan),
    }
    loop {
        match codec::read_entry(&mut input, &mut scratch)? {
            ReadOutcome::Entry(Entry::Record(record)) => {
                apply(start + scan.entries, record);
                scan.entries += 1;
                scan.valid_len = input.count;
            }
            ReadOutcome::Entry(Entry::Loss) => {
                scan.losses.push(start + scan.entries);
                scan.entries += 1;
                scan.valid_len = input.count;
            }
            ReadOutcome::Entry(Entry::Run(run)) => {
                scan.runs.push(run);
                scan.entries += 1;
                scan.valid_len = input.count;
            }
            ReadOutcome::Entry(Entry::Position(position)) => {
                scan.positions.push((start + scan.entries, position));
                scan.entries += 1;
                scan.valid_len = input.count;
            }
            ReadOutcome::Entry(Entry::Links(up)) => {
                scan.links.push((start + scan.entries, up));
                scan.entries += 1;
                scan.valid_len = input.count;
            }
            ReadOutcome::Eof => {
                scan.torn = false;
                return Ok(scan);
            }
            _ => return Ok(scan),
        }
    }
}

/// Counts the bytes read through it.
pub(super) struct Counting<R> {
    inner: R,
    pub count: u64,
}

impl<R> Counting<R> {
    pub fn new(inner: R) -> Self {
        Self { inner, count: 0 }
    }
}

impl<R: Read> Read for Counting<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.count += read as u64;
        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::oplog::{Position, RunStart};

    fn record(version: u64) -> Record {
        Record::Put {
            key: Bytes::from(format!("k{version}")),
            version,
            cas: version,
            flags: 0,
            expiry: 0,
            stale: false,
            value: Bytes::from(vec![b'v'; 100]),
        }
    }

    fn scan(dir: &DataDir, start: u64) -> (SegmentScan, Vec<(u64, Record)>) {
        let mut seen = Vec::new();
        let scan = scan_segment(&dir.segment_path(start), start, |seq, r| {
            seen.push((seq, r))
        })
        .unwrap();
        (scan, seen)
    }

    #[test]
    fn records_read_back_with_their_sequence_numbers() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let mut segment = SegmentWriter::create(&dir, 40).unwrap();
        for version in 1..=3 {
            segment.append(&record(version)).unwrap();
        }
        segment.sync().unwrap();
        assert_eq!(segment.next(), 43);
        let (scan, seen) = scan(&dir, 40);
        assert!(!scan.torn);
        assert_eq!(scan.entries, 3);
        assert_eq!(scan.valid_len, segment.bytes());
        assert_eq!(seen, [(40, record(1)), (41, record(2)), (42, record(3))]);
    }

    #[test]
    fn append_writes_the_buffer_out_once_it_passes_the_threshold() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let mut segment = SegmentWriter::create(&dir, 0).unwrap();
        let big = Record::Put {
            key: Bytes::from_static(b"k"),
            version: 1,
            cas: 1,
            flags: 0,
            expiry: 0,
            stale: false,
            value: Bytes::from(vec![b'v'; 100_000]),
        };
        let mut encoded = Vec::new();
        codec::encode_record(&big, &mut encoded);
        let mut written = 0;
        for _ in 0..100 {
            written += segment.append(&big).unwrap();
            assert!(segment.buffered() < WRITE_AT, "{}", segment.buffered());
        }
        assert!(written > 9 * WRITE_AT as u64, "{written}");
        segment.sync().unwrap();
        assert_eq!(
            segment.bytes(),
            fs::metadata(dir.segment_path(0)).unwrap().len()
        );
        let (scan, seen) = scan(&dir, 0);
        assert!(!scan.torn);
        assert_eq!(seen.len(), 100);
    }

    #[test]
    fn loss_markers_take_a_sequence_number_and_are_reported() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let mut segment = SegmentWriter::create(&dir, 10).unwrap();
        segment.append_loss();
        segment.append(&record(1)).unwrap();
        segment.append_loss();
        segment.append(&record(2)).unwrap();
        segment.sync().unwrap();
        assert_eq!(segment.next(), 14);
        let (scan, seen) = scan(&dir, 10);
        assert!(!scan.torn);
        assert_eq!((scan.entries, scan.losses), (4, vec![10, 12]));
        assert_eq!(seen, [(11, record(1)), (13, record(2))]);
    }

    #[test]
    fn a_torn_tail_stops_the_scan_at_the_last_whole_record() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let mut segment = SegmentWriter::create(&dir, 0).unwrap();
        segment.append(&record(1)).unwrap();
        segment.sync().unwrap();
        let whole = segment.bytes();
        segment.append(&record(2)).unwrap();
        segment.sync().unwrap();
        let file = OpenOptions::new()
            .write(true)
            .open(dir.segment_path(0))
            .unwrap();
        file.set_len(whole + 20).unwrap();
        let (scan, seen) = scan(&dir, 0);
        assert!(scan.torn);
        assert_eq!((scan.entries, scan.valid_len, seen.len()), (1, whole, 1));
    }

    #[test]
    fn a_missing_or_mismatched_header_is_torn_with_nothing_valid() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        File::create(dir.segment_path(0)).unwrap();
        assert_eq!(
            scan(&dir, 0).0,
            SegmentScan {
                entries: 0,
                losses: Vec::new(),
                runs: Vec::new(),
                positions: Vec::new(),
                links: Vec::new(),
                valid_len: 0,
                torn: true
            }
        );
        SegmentWriter::create(&dir, 5).unwrap();
        std::fs::rename(dir.segment_path(5), dir.segment_path(6)).unwrap();
        assert!(scan(&dir, 6).0.torn);
    }

    #[test]
    fn an_unsupported_format_is_an_error_naming_the_file() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let mut bytes = Vec::new();
        codec::encode_header(FileKind::Segment, 0, 0, &mut bytes);
        // Rewrite the format field and the checksum to match.
        bytes[10..14].copy_from_slice(&(codec::FORMAT + 1).to_le_bytes());
        let crc = crc32fast::hash(&bytes[8..]);
        bytes[4..8].copy_from_slice(&crc.to_le_bytes());
        codec::encode_record(&record(1), &mut bytes);
        let path = dir.segment_path(0);
        fs::write(&path, &bytes).unwrap();
        let error = scan_segment(&path, 0, |_, _| {}).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let message = error.to_string();
        assert!(message.contains(&path.display().to_string()), "{message}");
        assert!(
            message.contains(&format!("format {}", codec::FORMAT + 1)),
            "{message}"
        );
    }

    #[test]
    fn a_snapshot_file_does_not_scan_as_a_segment() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let mut writer = crate::persist::snapshot::SnapshotWriter::create(&dir, 3, 0).unwrap();
        writer.add(&record(1)).unwrap();
        writer.finish(&dir).unwrap();
        let mut seen = 0;
        let scan = scan_segment(&dir.snapshot_path(3), 3, |_, _| seen += 1).unwrap();
        assert_eq!(
            scan,
            SegmentScan {
                entries: 0,
                losses: Vec::new(),
                runs: Vec::new(),
                positions: Vec::new(),
                links: Vec::new(),
                valid_len: 0,
                torn: true
            }
        );
        assert_eq!(seen, 0);
    }

    #[test]
    fn link_records_take_sequence_numbers_and_are_reported() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let mut segment = SegmentWriter::create(&dir, 30).unwrap();
        segment.append_logged(&Logged::Links(vec![2, 3])).unwrap();
        segment.append(&record(1)).unwrap();
        segment.append_logged(&Logged::Links(Vec::new())).unwrap();
        segment.sync().unwrap();
        assert_eq!(segment.next(), 33);
        let (scan, seen) = scan(&dir, 30);
        assert!(!scan.torn);
        assert_eq!(scan.entries, 3);
        assert_eq!(scan.links, [(30, vec![2, 3]), (32, Vec::new())]);
        assert_eq!(seen, [(31, record(1))]);
    }

    #[test]
    fn run_markers_and_positions_take_sequence_numbers_and_are_reported() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let mut segment = SegmentWriter::create(&dir, 20).unwrap();
        let run = RunStart { id: 77, first: 20 };
        let position = Position {
            peer: 2,
            run: 9,
            next: 400,
        };
        segment.append_run(run);
        segment.append(&record(1)).unwrap();
        segment.append_position(position);
        segment.sync().unwrap();
        assert_eq!(segment.next(), 23);
        let (scan, seen) = scan(&dir, 20);
        assert!(!scan.torn);
        assert_eq!(scan.entries, 3);
        assert_eq!(scan.runs, [run]);
        assert_eq!(scan.positions, [(22, position)]);
        assert_eq!(seen, [(21, record(1))]);
    }
}
