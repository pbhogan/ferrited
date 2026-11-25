//! Reads the log back from a sequence number, for a peer that has fallen behind the backlog.

use std::fs::File;
use std::io::{self, BufReader, Seek, SeekFrom};
use std::sync::Arc;

use super::codec::{self, Entry, FileKind, ReadOutcome};
use super::dir::DataDir;
use super::segment::Counting;
use crate::backlog::entry_cost;
use crate::oplog::Logged;

/// What `LogReader::read` found.
#[derive(Debug, PartialEq, Eq)]
pub enum Step {
    /// Entries were added to the output, and there may be more.
    More,
    /// Nothing more is written from `next()` on yet.
    End,
    /// An entry from `next()` on is missing: a loss marker, a gap between segments, or a
    /// segment that is gone or unreadable.
    Lost,
}

/// How opening a segment went.
enum Opened {
    Ready,
    /// The segment is gone, or its header is wrong.
    Missing,
    /// The segment is being created and its header is incomplete.
    Incomplete,
}

pub struct LogReader {
    dir: Arc<DataDir>,
    segment: u64,
    /// The open segment, or `None` until it is opened again at `offset`.
    file: Option<Counting<BufReader<File>>>,
    /// Byte offset just past the last whole entry read from the segment; 0 before its header.
    offset: u64,
    /// Sequence number of the next entry in the segment.
    seq: u64,
    /// Entries before this are skipped.
    from: u64,
}

impl LogReader {
    /// A reader from `from` on, or `None` if no segment on disk can hold it.
    pub fn open(dir: Arc<DataDir>, from: u64) -> io::Result<Option<Self>> {
        let segments = dir.segments()?;
        let Some(&segment) = segments.iter().rev().find(|&&start| start <= from) else {
            return Ok(None);
        };
        let mut reader = Self {
            dir,
            segment,
            file: None,
            offset: 0,
            seq: segment,
            from,
        };
        match reader.open_segment()? {
            Opened::Ready => Ok(Some(reader)),
            Opened::Missing | Opened::Incomplete => Ok(None),
        }
    }

    pub fn next(&self) -> u64 {
        self.seq.max(self.from)
    }

    /// Adds entries from `next()` on to `out`: at most `max_entries`, stopping once they cost
    /// `max_bytes`. Reads at least one entry when there is one, as `Backlog::read` does.
    pub fn read(
        &mut self,
        out: &mut Vec<Logged>,
        max_entries: usize,
        max_bytes: u64,
    ) -> io::Result<Step> {
        let mut scratch = Vec::new();
        let (mut added, mut bytes) = (0, 0);
        // Whether the segment has been read to its end again since a later segment was seen.
        let mut rechecked = false;
        loop {
            if added > 0 && (added >= max_entries || bytes >= max_bytes) {
                return Ok(Step::More);
            }
            if self.file.is_none() {
                match self.open_segment()? {
                    Opened::Ready => {}
                    Opened::Missing => return Ok(Step::Lost),
                    Opened::Incomplete => {
                        return Ok(if added > 0 { Step::More } else { Step::End });
                    }
                }
            }
            let file = self.file.as_mut().expect("opened above");
            let entry = match codec::read_entry(file, &mut scratch)? {
                ReadOutcome::Entry(entry) => entry,
                ReadOutcome::Eof | ReadOutcome::Torn => {
                    self.file = None;
                    match self.following()? {
                        Some(start) if start == self.seq => {
                            self.segment = start;
                            self.offset = 0;
                        }
                        // More may have been written to this segment before the next began;
                        // read it once more before calling the difference a gap.
                        Some(_) if !rechecked => rechecked = true,
                        Some(_) => return Ok(Step::Lost),
                        None => return Ok(if added > 0 { Step::More } else { Step::End }),
                    }
                    continue;
                }
            };
            self.offset = file.count;
            rechecked = false;
            let seq = self.seq;
            self.seq += 1;
            let logged = match entry {
                Entry::Record(record) => Logged::Record(record),
                Entry::Loss => Logged::Loss,
                Entry::Run(run) => Logged::Run(run),
                Entry::Position(position) => Logged::Position(position),
                Entry::Links(up) => Logged::Links(up),
                // Nothing else follows a segment's header.
                _ => return Ok(Step::Lost),
            };
            if seq < self.from {
                continue;
            }
            if logged == Logged::Loss {
                return Ok(Step::Lost);
            }
            bytes += entry_cost(&logged);
            out.push(logged);
            added += 1;
        }
    }

    /// Opens the current segment at `offset`, checking its header when at its start.
    fn open_segment(&mut self) -> io::Result<Opened> {
        let file = match File::open(self.dir.segment_path(self.segment)) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Opened::Missing),
            Err(error) => return Err(error),
        };
        let mut input = BufReader::with_capacity(1 << 16, file);
        if self.offset > 0 {
            input.seek(SeekFrom::Start(self.offset))?;
            let mut counting = Counting::new(input);
            counting.count = self.offset;
            self.file = Some(counting);
            return Ok(Opened::Ready);
        }
        let mut counting = Counting::new(input);
        match codec::read_entry(&mut counting, &mut Vec::new())? {
            ReadOutcome::Entry(Entry::Header {
                kind: FileKind::Segment,
                start,
                ..
            }) if start == self.segment => {}
            ReadOutcome::Eof | ReadOutcome::Torn => return Ok(Opened::Incomplete),
            _ => return Ok(Opened::Missing),
        }
        self.offset = counting.count;
        self.file = Some(counting);
        Ok(Opened::Ready)
    }

    /// The start of the next segment, if any.
    fn following(&self) -> io::Result<Option<u64>> {
        Ok(self
            .dir
            .segments()?
            .into_iter()
            .find(|&start| start > self.segment))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::oplog::{Record, RunStart};
    use crate::persist::segment::SegmentWriter;

    fn record(n: u64) -> Logged {
        Logged::Record(Record::Put {
            key: Bytes::from(format!("k{n}")),
            version: n,
            cas: n,
            flags: 0,
            expiry: 0,
            stale: false,
            value: Bytes::from_static(b"v"),
        })
    }

    fn append(segment: &mut SegmentWriter, entry: &Logged) {
        segment.append_logged(entry).unwrap();
    }

    fn segment(dir: &DataDir, start: u64, entries: &[Logged]) {
        let mut segment = SegmentWriter::create(dir, start).unwrap();
        for entry in entries {
            append(&mut segment, entry);
        }
        segment.sync().unwrap();
    }

    /// Reads two entries at a time until the reader stops for something other than `More`.
    fn read_all(reader: &mut LogReader) -> (Vec<Logged>, Step) {
        let mut out = Vec::new();
        loop {
            match reader.read(&mut out, 2, u64::MAX).unwrap() {
                Step::More => continue,
                step => return (out, step),
            }
        }
    }

    fn dir() -> (tempfile::TempDir, Arc<DataDir>) {
        let temp = tempfile::tempdir().unwrap();
        let dir = Arc::new(DataDir::open(temp.path()).unwrap());
        (temp, dir)
    }

    #[test]
    fn link_records_are_read_back() {
        let (_temp, dir) = dir();
        segment(&dir, 0, &[record(0), Logged::Links(vec![2]), record(2)]);
        let mut reader = LogReader::open(dir, 0).unwrap().unwrap();
        let (entries, _) = read_all(&mut reader);
        assert_eq!(entries, [record(0), Logged::Links(vec![2]), record(2)]);
    }

    #[test]
    fn reads_across_segments_from_a_position() {
        let (_temp, dir) = dir();
        let run = Logged::Run(RunStart { id: 5, first: 4 });
        segment(&dir, 0, &[record(0), record(1), record(2)]);
        segment(&dir, 3, &[record(3), run.clone(), record(5)]);
        let mut reader = LogReader::open(dir, 1).unwrap().unwrap();
        let (entries, step) = read_all(&mut reader);
        assert_eq!(entries, [record(1), record(2), record(3), run, record(5)]);
        assert_eq!((step, reader.next()), (Step::End, 6));
    }

    #[test]
    fn zero_limits_still_read_one_entry() {
        let (_temp, dir) = dir();
        segment(&dir, 0, &[record(0), record(1)]);
        for (entries, bytes) in [(0, u64::MAX), (10, 0), (0, 0)] {
            let mut reader = LogReader::open(dir.clone(), 0).unwrap().unwrap();
            let mut out = Vec::new();
            assert_eq!(reader.read(&mut out, entries, bytes).unwrap(), Step::More);
            assert_eq!(out, [record(0)]);
        }
    }

    #[test]
    fn a_position_before_every_segment_cannot_be_read() {
        let (_temp, dir) = dir();
        segment(&dir, 5, &[record(5)]);
        assert!(LogReader::open(dir, 2).unwrap().is_none());
    }

    #[test]
    fn a_loss_marker_after_the_position_is_lost_and_one_before_it_is_not() {
        let (_temp, dir) = dir();
        segment(&dir, 0, &[record(0), Logged::Loss, record(2)]);
        let mut reader = LogReader::open(dir.clone(), 0).unwrap().unwrap();
        assert_eq!(read_all(&mut reader), (vec![record(0)], Step::Lost));
        let mut reader = LogReader::open(dir, 2).unwrap().unwrap();
        assert_eq!(read_all(&mut reader), (vec![record(2)], Step::End));
    }

    #[test]
    fn a_gap_between_segments_is_lost() {
        let (_temp, dir) = dir();
        segment(&dir, 0, &[record(0)]);
        segment(&dir, 5, &[record(5)]);
        let mut reader = LogReader::open(dir, 0).unwrap().unwrap();
        assert_eq!(read_all(&mut reader), (vec![record(0)], Step::Lost));
    }

    #[test]
    fn entries_written_after_the_end_and_a_roll_are_read_later() {
        let (_temp, dir) = dir();
        let mut writer = SegmentWriter::create(&dir, 0).unwrap();
        append(&mut writer, &record(0));
        writer.write_out().unwrap();
        let mut reader = LogReader::open(dir.clone(), 0).unwrap().unwrap();
        assert_eq!(read_all(&mut reader), (vec![record(0)], Step::End));
        append(&mut writer, &record(1));
        writer.write_out().unwrap();
        assert_eq!(read_all(&mut reader), (vec![record(1)], Step::End));
        append(&mut writer, &record(2));
        writer.sync().unwrap();
        segment(&dir, 3, &[record(3)]);
        assert_eq!(
            read_all(&mut reader),
            (vec![record(2), record(3)], Step::End)
        );
    }

    #[test]
    fn a_read_stops_at_the_byte_limit() {
        let (_temp, dir) = dir();
        segment(&dir, 0, &[record(0), record(1), record(2)]);
        let mut reader = LogReader::open(dir, 0).unwrap().unwrap();
        let mut out = Vec::new();
        assert_eq!(reader.read(&mut out, 100, 1).unwrap(), Step::More);
        assert_eq!(out, [record(0)]);
        assert_eq!(reader.next(), 1);
    }
}
