//! Snapshots of every live item, written to a temporary file and renamed into place.

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use super::codec::{self, Entry, FileKind, ReadOutcome, Unsupported};
use super::dir::DataDir;
use crate::oplog::{Logged, Position, Record, RunStart};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// Sequence number of the first log record the snapshot does not cover.
    pub start: u64,
    /// Flush floor when the snapshot began.
    pub floor: u64,
    pub items: u64,
    pub bytes: u64,
}

/// Writes a snapshot to a temporary file, which is removed if dropped before `finish`.
pub struct SnapshotWriter {
    file: BufWriter<File>,
    temp: PathBuf,
    path: PathBuf,
    buf: Vec<u8>,
    info: SnapshotInfo,
    renamed: bool,
}

impl SnapshotWriter {
    pub fn create(dir: &DataDir, start: u64, floor: u64) -> io::Result<Self> {
        let temp = dir.temp_snapshot_path(start);
        let mut writer = Self {
            file: BufWriter::with_capacity(1 << 20, File::create(&temp)?),
            temp,
            path: dir.snapshot_path(start),
            buf: Vec::new(),
            info: SnapshotInfo {
                start,
                floor,
                ..Default::default()
            },
            renamed: false,
        };
        codec::encode_header(FileKind::Snapshot, start, floor, &mut writer.buf);
        writer.flush_buf()?;
        Ok(writer)
    }

    /// Adds a run marker. Call before adding records.
    pub fn add_run(&mut self, run: RunStart) -> io::Result<()> {
        codec::encode_run(run, &mut self.buf);
        self.flush_buf()
    }

    /// Adds this node's position on a peer's log. Call before adding records.
    pub fn add_position(&mut self, position: Position) -> io::Result<()> {
        codec::encode_position(position, &mut self.buf);
        self.flush_buf()
    }

    /// Adds the last link record before the snapshot's start. Call before adding records.
    pub fn add_links(&mut self, up: &[u8]) -> io::Result<()> {
        codec::encode_links(up, &mut self.buf);
        self.flush_buf()
    }

    /// Adds a put (a live item) or a delete (a tombstone).
    pub fn add(&mut self, record: &Record) -> io::Result<()> {
        codec::encode_record(record, &mut self.buf);
        self.info.items += 1;
        self.flush_buf()
    }

    /// Ends the snapshot, syncs it, renames it into place, and syncs the directory.
    pub fn finish(mut self, dir: &DataDir) -> io::Result<SnapshotInfo> {
        codec::encode_end(self.info.items, &mut self.buf);
        self.flush_buf()?;
        self.file.flush()?;
        let file = self.file.get_ref();
        file.sync_all()?;
        self.info.bytes = file.metadata()?.len();
        fs::rename(&self.temp, &self.path)?;
        self.renamed = true;
        dir.sync()?;
        Ok(self.info)
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        self.file.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }
}

impl Drop for SnapshotWriter {
    fn drop(&mut self) {
        if !self.renamed {
            let _ = fs::remove_file(&self.temp);
        }
    }
}

/// What reading a snapshot found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotRead {
    Valid(SnapshotInfo),
    /// Incomplete, damaged, or its header's start does not match its name.
    Damaged,
    /// An intact header of an unsupported format.
    Unsupported(Unsupported),
}

/// Reads a snapshot whose file name gives `expected_start`, passing each entry to `apply` if
/// given. Call without `apply` first to check the whole file before loading any of it.
pub fn read_snapshot(
    path: &Path,
    expected_start: u64,
    mut apply: Option<&mut dyn FnMut(Logged)>,
) -> io::Result<SnapshotRead> {
    let bytes = fs::metadata(path)?.len();
    let mut input = BufReader::with_capacity(1 << 20, File::open(path)?);
    let mut scratch = Vec::new();
    let (start, floor) = match codec::read_entry(&mut input, &mut scratch)? {
        ReadOutcome::Entry(Entry::Header {
            kind: FileKind::Snapshot,
            start,
            floor,
            ..
        }) if start == expected_start => (start, floor),
        ReadOutcome::Entry(Entry::Unsupported(unsupported)) => {
            return Ok(SnapshotRead::Unsupported(unsupported));
        }
        _ => return Ok(SnapshotRead::Damaged),
    };
    let mut items = 0;
    loop {
        let entry = match codec::read_entry(&mut input, &mut scratch)? {
            ReadOutcome::Entry(Entry::Record(
                record @ (Record::Put { .. } | Record::Delete { .. }),
            )) => {
                items += 1;
                Logged::Record(record)
            }
            ReadOutcome::Entry(Entry::Run(run)) => Logged::Run(run),
            ReadOutcome::Entry(Entry::Position(position)) => Logged::Position(position),
            ReadOutcome::Entry(Entry::Links(up)) => Logged::Links(up),
            ReadOutcome::Entry(Entry::End { count }) if count == items => break,
            _ => return Ok(SnapshotRead::Damaged),
        };
        if let Some(apply) = apply.as_mut() {
            apply(entry);
        }
    }
    // Nothing may follow the end marker.
    if codec::read_entry(&mut input, &mut scratch)? != ReadOutcome::Eof {
        return Ok(SnapshotRead::Damaged);
    }
    Ok(SnapshotRead::Valid(SnapshotInfo {
        start,
        floor,
        items,
        bytes,
    }))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn record(index: u64) -> Record {
        Record::Put {
            key: Bytes::from(format!("k{index}")),
            version: index + 1,
            cas: index + 1,
            flags: 0,
            expiry: 0,
            stale: false,
            value: Bytes::from(format!("v{index}")),
        }
    }

    fn write(dir: &DataDir, start: u64, count: u64) -> SnapshotInfo {
        let mut writer = SnapshotWriter::create(dir, start, 7).unwrap();
        for index in 0..count {
            writer.add(&record(index)).unwrap();
        }
        writer.finish(dir).unwrap()
    }

    #[test]
    fn a_finished_snapshot_reads_back() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let info = write(&dir, 99, 3);
        assert!(!dir.temp_snapshot_path(99).exists());
        let mut seen = Vec::new();
        let SnapshotRead::Valid(read) =
            read_snapshot(&dir.snapshot_path(99), 99, Some(&mut |r| seen.push(r))).unwrap()
        else {
            panic!("not valid");
        };
        assert_eq!(read, info);
        assert_eq!((read.start, read.floor, read.items), (99, 7, 3));
        assert_eq!(
            seen,
            [
                Logged::Record(record(0)),
                Logged::Record(record(1)),
                Logged::Record(record(2))
            ]
        );
    }

    #[test]
    fn an_unfinished_snapshot_is_not_in_place() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let mut writer = SnapshotWriter::create(&dir, 5, 0).unwrap();
        writer.add(&record(0)).unwrap();
        drop(writer);
        assert!(dir.snapshots().unwrap().is_empty());
        assert!(!dir.temp_snapshot_path(5).exists());
    }

    #[test]
    fn a_snapshot_whose_header_does_not_match_its_name_reads_as_damaged() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        write(&dir, 4, 2);
        fs::rename(dir.snapshot_path(4), dir.snapshot_path(9)).unwrap();
        let path = dir.snapshot_path(9);
        assert_eq!(
            read_snapshot(&path, 9, None).unwrap(),
            SnapshotRead::Damaged
        );
        assert!(matches!(
            read_snapshot(&path, 4, None).unwrap(),
            SnapshotRead::Valid(_)
        ));
    }

    #[test]
    fn an_end_marker_with_the_wrong_count_reads_as_damaged() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let mut bytes = Vec::new();
        codec::encode_header(FileKind::Snapshot, 2, 0, &mut bytes);
        codec::encode_record(&record(0), &mut bytes);
        codec::encode_record(&record(1), &mut bytes);
        codec::encode_end(3, &mut bytes);
        let path = dir.snapshot_path(2);
        fs::write(&path, &bytes).unwrap();
        assert_eq!(
            read_snapshot(&path, 2, None).unwrap(),
            SnapshotRead::Damaged
        );
    }

    #[test]
    fn a_flush_or_loss_marker_in_a_snapshot_reads_as_damaged() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let encoders: [fn(&mut Vec<u8>); 2] = [
            |out| codec::encode_record(&Record::Flush { version: 1 }, out),
            codec::encode_loss,
        ];
        for encode in encoders {
            let mut bytes = Vec::new();
            codec::encode_header(FileKind::Snapshot, 2, 0, &mut bytes);
            codec::encode_record(&record(0), &mut bytes);
            encode(&mut bytes);
            codec::encode_end(1, &mut bytes);
            let path = dir.snapshot_path(2);
            fs::write(&path, &bytes).unwrap();
            assert_eq!(
                read_snapshot(&path, 2, None).unwrap(),
                SnapshotRead::Damaged
            );
        }
    }

    #[test]
    fn deletes_runs_positions_and_links_read_back() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let run = RunStart { id: 5, first: 0 };
        let position = Position {
            peer: 2,
            run: 8,
            next: 90,
        };
        let delete = Record::Delete {
            key: Bytes::from_static(b"gone"),
            version: 9,
        };
        let mut writer = SnapshotWriter::create(&dir, 3, 0).unwrap();
        writer.add_run(run).unwrap();
        writer.add_position(position).unwrap();
        writer.add_links(&[4]).unwrap();
        writer.add(&record(0)).unwrap();
        writer.add(&delete).unwrap();
        let info = writer.finish(&dir).unwrap();
        assert_eq!(info.items, 2);
        let mut seen = Vec::new();
        let read = read_snapshot(&dir.snapshot_path(3), 3, Some(&mut |e| seen.push(e))).unwrap();
        assert_eq!(read, SnapshotRead::Valid(info));
        assert_eq!(
            seen,
            [
                Logged::Run(run),
                Logged::Position(position),
                Logged::Links(vec![4]),
                Logged::Record(record(0)),
                Logged::Record(delete)
            ]
        );
    }

    #[test]
    fn a_snapshot_of_an_unsupported_format_reads_as_unsupported() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let mut bytes = Vec::new();
        codec::encode_header(FileKind::Snapshot, 2, 0, &mut bytes);
        bytes[10..14].copy_from_slice(&(codec::FORMAT + 1).to_le_bytes());
        let crc = crc32fast::hash(&bytes[8..]);
        bytes[4..8].copy_from_slice(&crc.to_le_bytes());
        let path = dir.snapshot_path(2);
        fs::write(&path, &bytes).unwrap();
        assert_eq!(
            read_snapshot(&path, 2, None).unwrap(),
            SnapshotRead::Unsupported(Unsupported {
                kind: 2,
                format: codec::FORMAT + 1
            })
        );
    }

    #[test]
    fn a_truncated_or_damaged_snapshot_reads_as_damaged() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let info = write(&dir, 1, 10);
        let path = dir.snapshot_path(1);
        let whole = fs::read(&path).unwrap();
        fs::write(&path, &whole[..whole.len() - 5]).unwrap();
        assert_eq!(
            read_snapshot(&path, 1, None).unwrap(),
            SnapshotRead::Damaged
        );
        let mut damaged = whole.clone();
        damaged[(info.bytes / 2) as usize] ^= 1;
        fs::write(&path, &damaged).unwrap();
        assert_eq!(
            read_snapshot(&path, 1, None).unwrap(),
            SnapshotRead::Damaged
        );
        let mut extended = whole;
        extended.extend_from_slice(b"junk");
        fs::write(&path, &extended).unwrap();
        assert_eq!(
            read_snapshot(&path, 1, None).unwrap(),
            SnapshotRead::Damaged
        );
    }
}
