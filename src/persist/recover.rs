//! Rebuilds the store from the newest valid snapshot and the log after it.

use std::fs::{self, OpenOptions};
use std::io;

use super::codec;
use super::dir::DataDir;
use super::segment::scan_segment;
use super::snapshot::{SnapshotInfo, SnapshotRead, read_snapshot};
use crate::oplog::{Logged, Position, Record, RunStart};
use crate::store::Store;

#[derive(Debug, Default)]
pub struct Recovered {
    pub snapshot: Option<SnapshotInfo>,
    /// Log records replayed after the snapshot, not counting loss markers.
    pub records: u64,
    pub next_seq: u64,
    /// Bytes in the segments from the one holding the snapshot's start onward.
    pub log_bytes: u64,
    /// The degraded marker was found: records lost to a disk error are not yet covered.
    pub degraded: bool,
    /// Runs in the snapshot and replayed log, oldest first.
    pub runs: Vec<RunStart>,
    /// The latest position on each peer's log, unless changes may be missing (`lossy`).
    pub positions: Vec<Position>,
    /// Changes may be missing (including after a crash). Positions are then discarded, since
    /// one may cover lost records, and each peer sends a full sync.
    pub lossy: bool,
    /// Positions were discarded because recovery was lossy. They are still in the files, so the
    /// writer covers them with a snapshot at once, or a clean restart could resume from them.
    pub positions_discarded: bool,
    /// The last link record in the snapshot and replayed log: the peers this node was linked
    /// with when it last served.
    pub links: Option<Vec<u8>>,
    /// A stretch of history may be missing. Unlike `lossy`, a crash alone does not set it: the
    /// log up to a crash's torn tail is whole. When set, the history-lost marker is on disk.
    pub history_lost: bool,
    /// Problems found and worked around, for the startup log.
    pub warnings: Vec<String>,
}

impl Recovered {
    /// Whether this node was the last one standing when it last served: its last link record
    /// lists no peer and no history is missing (which could hide a later link record).
    pub fn last_standing(&self) -> bool {
        !self.history_lost && self.links.as_ref().is_some_and(Vec::is_empty)
    }

    /// One line for the startup log.
    pub fn describe(&self, dir: &DataDir) -> String {
        let snapshot = match &self.snapshot {
            Some(info) => format!("a snapshot of {} items", info.items),
            None => "no snapshot".to_string(),
        };
        format!(
            "recovered {snapshot} and {} log records from {}",
            self.records,
            dir.path().display()
        )
    }
}

/// Replaces the position for `position.peer`, or adds it.
pub(crate) fn set_position(positions: &mut Vec<Position>, position: Position) {
    match positions
        .iter_mut()
        .find(|known| known.peer == position.peer)
    {
        Some(known) => *known = position,
        None => positions.push(position),
    }
}

/// Writes the history-lost marker unless `marked` says it is already on disk.
fn mark_history_lost(dir: &DataDir, marked: &mut bool) -> io::Result<()> {
    if !*marked {
        dir.write_history_lost()?;
        *marked = true;
    }
    Ok(())
}

/// Loads the newest valid snapshot into `store` and replays the log after it. Fails on any
/// unsupported file; moves damaged snapshots aside, cuts torn tails, removes empty segments.
pub fn recover(dir: &DataDir, store: &Store, now: u64) -> io::Result<Recovered> {
    let mut recovered = Recovered::default();
    let snapshots = dir.snapshots()?;
    let segments = dir.segments()?;
    // Checked before anything changes, so no unsupported file is ever discarded.
    let paths = snapshots.iter().map(|&start| dir.snapshot_path(start));
    for path in paths.chain(segments.iter().map(|&start| dir.segment_path(start))) {
        if let Some(unsupported) = codec::unsupported_format(&path) {
            return Err(unsupported.error(&path));
        }
    }
    // Taken now so a crash during this run is not mistaken for a clean stop. Without it, the
    // last writes peers received may be missing from disk, and only full syncs restore them.
    let clean = dir.take_clean()?;
    if !clean && !(snapshots.is_empty() && segments.is_empty()) {
        recovered.lossy = true;
    }
    // The history-lost marker is always written before anything erases a hole's evidence.
    let mut marked = dir.has_history_lost()?;
    if marked {
        recovered.history_lost = true;
        recovered.warnings.push(
            "an earlier start found a hole in the log and no snapshot has covered it since; this node does not count as the last one standing"
                .to_string(),
        );
    }
    if let Some(first_lost) = dir.read_degraded()? {
        recovered.degraded = true;
        recovered.lossy = true;
        recovered.history_lost = true;
        let from = first_lost.map_or(String::new(), |seq| format!(" from about sequence {seq}"));
        recovered.warnings.push(format!(
            "records{from} were lost while the disk was failing and no snapshot covers them; some recent changes may be missing or undone"
        ));
    }
    for &start in snapshots.iter().rev() {
        let path = dir.snapshot_path(start);
        let problem = match read_snapshot(&path, start, None) {
            Ok(SnapshotRead::Valid(_)) => None,
            Ok(SnapshotRead::Damaged) => Some("is incomplete or damaged".to_string()),
            Ok(SnapshotRead::Unsupported(unsupported)) => return Err(unsupported.error(&path)),
            Err(error) => Some(format!("could not be read ({error})")),
        };
        // If loading fails after the check passed, part of the snapshot is already applied.
        // Falling back is still safe: applying versioned records is idempotent.
        let problem = match problem {
            Some(problem) => problem,
            None => {
                let (mut runs, mut positions, mut links) = (Vec::new(), Vec::new(), None);
                let mut apply = |entry: Logged| match entry {
                    Logged::Record(record) => store.apply(&record, now),
                    Logged::Run(run) => runs.push(run),
                    Logged::Position(position) => set_position(&mut positions, position),
                    Logged::Links(up) => links = Some(up),
                    Logged::Loss => {}
                };
                match read_snapshot(&path, start, Some(&mut apply)) {
                    Ok(SnapshotRead::Valid(info)) => {
                        store.apply(
                            &Record::Flush {
                                version: info.floor,
                            },
                            now,
                        );
                        recovered.snapshot = Some(info);
                        recovered.runs = runs;
                        recovered.positions = positions;
                        recovered.links = links;
                        break;
                    }
                    Ok(SnapshotRead::Unsupported(unsupported)) => {
                        return Err(unsupported.error(&path));
                    }
                    Ok(SnapshotRead::Damaged) => "changed while loading".to_string(),
                    Err(error) => format!("could not be loaded ({error})"),
                }
            }
        };
        // An older snapshot replays from further back, across any hole a newer snapshot had
        // made safe to forget.
        recovered.history_lost = true;
        mark_history_lost(dir, &mut marked)?;
        // Moved aside so pruning cannot keep it in place of a good one.
        dir.quarantine_snapshot(start)?;
        recovered.warnings.push(format!(
            "snapshot {} {problem}; moved it to {} and trying an older one",
            path.display(),
            dir.damaged_snapshot_path(start).display()
        ));
    }
    let from = recovered.snapshot.map_or(0, |info| info.start);
    // Numbering never goes back past any file seen, so no new file takes an old one's name.
    recovered.next_seq = snapshots.last().map_or(from, |&newest| newest.max(from));
    // The segment that holds record `from`.
    let first = segments
        .iter()
        .rposition(|&start| start <= from)
        .unwrap_or(0);
    let mut end = from;
    for (index, &start) in segments.iter().enumerate().skip(first) {
        let path = dir.segment_path(start);
        let mut replayed = 0;
        let scan = scan_segment(&path, start, |seq, record| {
            if seq >= from {
                store.apply(&record, now);
                replayed += 1;
            }
        })?;
        recovered.records += replayed;
        for run in &scan.runs {
            // A marker before `from` is already in the snapshot; merging it again could put it
            // after newer runs.
            if run.first >= from && !recovered.runs.iter().any(|known| known.id == run.id) {
                recovered.runs.push(*run);
            }
        }
        for &(seq, position) in &scan.positions {
            if seq >= from {
                set_position(&mut recovered.positions, position);
            }
        }
        for (seq, up) in &scan.links {
            if *seq >= from {
                recovered.links = Some(up.clone());
            }
        }
        recovered.next_seq = recovered.next_seq.max(start);
        // An empty segment holds nothing to replay past.
        if start > end && (index == first || scan.entries > 0) {
            recovered.lossy = true;
            recovered.history_lost = true;
            recovered.warnings.push(if index == first {
                match recovered.snapshot {
                    Some(_) => format!(
                        "the log resumes at record {start}, after the snapshot's end at record {from}; changes in between are lost"
                    ),
                    None => format!(
                        "the log begins at record {start} and no snapshot covers the records before it; those changes are lost"
                    ),
                }
            } else {
                format!(
                    "records {end} to {} are missing from the log before {}; replaying past the gap, so changes in it are lost",
                    start - 1,
                    path.display()
                )
            });
        }
        end = end.max(start + scan.entries);
        // The snapshot covers any loss before `from`.
        for &seq in scan.losses.iter().filter(|&&seq| seq >= from) {
            recovered.lossy = true;
            recovered.history_lost = true;
            recovered.warnings.push(format!(
                "records were lost at about sequence {seq} and no snapshot covering them was completed; some recent changes may be missing or undone"
            ));
        }
        if scan.entries == 0 {
            // No longer than a header: created but never written.
            if fs::metadata(&path)?.len() <= codec::HEADER_LEN {
                fs::remove_file(&path)?;
                continue;
            }
            // Unreadable data: keep it for inspection, and number past it so no new segment
            // takes its name.
            recovered.lossy = true;
            recovered.history_lost = true;
            recovered.warnings.push(format!(
                "{} has no readable records; keeping it, and any records in it are lost",
                path.display()
            ));
            recovered.next_seq = recovered.next_seq.max(start + 1);
            continue;
        }
        if scan.torn {
            // What is cut may include entries peers already hold.
            recovered.lossy = true;
            // A crash tears only the end of the newest segment; anything else is damage.
            if clean || index + 1 < segments.len() {
                recovered.history_lost = true;
                // Cutting the entry leaves a whole-looking log, so mark the hole first.
                mark_history_lost(dir, &mut marked)?;
            }
            recovered.warnings.push(format!(
                "{} ends in a torn or damaged record after {} entries; the rest was discarded",
                path.display(),
                scan.entries
            ));
            OpenOptions::new()
                .write(true)
                .open(&path)?
                .set_len(scan.valid_len)?;
        }
        recovered.next_seq = recovered.next_seq.max(start + scan.entries);
        recovered.log_bytes += scan.valid_len;
    }
    if recovered.history_lost {
        mark_history_lost(dir, &mut marked)?;
    }
    if recovered.lossy && !recovered.positions.is_empty() {
        recovered.positions.clear();
        recovered.positions_discarded = true;
        recovered.warnings.push(
            "changes may be missing, so the replication positions are discarded and every peer will send a full sync"
                .to_string(),
        );
    }
    dir.sync()?;
    Ok(recovered)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::clock::Clock;
    use crate::persist::segment::SegmentWriter;
    use crate::persist::snapshot::SnapshotWriter;
    use crate::store::{GetArgs, StoreConfig};

    const NOW: u64 = 1_000_000;

    fn store() -> Store {
        let config = StoreConfig {
            memory_limit: 1 << 24,
            item_max: 1 << 20,
            shards: 4,
        };
        Store::new(config, Arc::new(Clock::new(1)))
    }

    fn put(key: &str, value: &str, version: u64) -> Record {
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

    fn get(store: &Store, key: &str) -> Option<String> {
        store
            .get(key.as_bytes(), NOW, &GetArgs::default())
            .map(|hit| String::from_utf8(hit.value.to_vec()).unwrap())
    }

    fn segment(dir: &DataDir, start: u64, records: &[Record]) {
        let mut segment = SegmentWriter::create(dir, start).unwrap();
        for record in records {
            segment.append(record).unwrap();
        }
        segment.sync().unwrap();
    }

    fn snapshot(dir: &DataDir, start: u64, records: &[Record]) {
        let mut writer = SnapshotWriter::create(dir, start, 0).unwrap();
        for record in records {
            writer.add(record).unwrap();
        }
        writer.finish(dir).unwrap();
    }

    #[test]
    fn an_empty_directory_recovers_nothing() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert_eq!((recovered.records, recovered.next_seq), (0, 0));
        assert!(recovered.snapshot.is_none() && recovered.warnings.is_empty());
    }

    #[test]
    fn a_degraded_marker_is_reported() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10)]);
        dir.write_degraded(1).unwrap();
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert!(recovered.degraded);
        assert_eq!(recovered.warnings.len(), 1, "{:?}", recovered.warnings);
        assert!(
            recovered.warnings[0]
                .contains("from about sequence 1 were lost while the disk was failing"),
            "{:?}",
            recovered.warnings
        );
        assert_eq!(get(&store, "a").as_deref(), Some("1"));
    }

    #[test]
    fn segments_replay_in_order() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10), put("b", "1", 11)]);
        segment(
            &dir,
            2,
            &[
                put("a", "2", 12),
                Record::Delete {
                    key: Bytes::from_static(b"b"),
                    version: 13,
                },
            ],
        );
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert_eq!((recovered.records, recovered.next_seq), (4, 4));
        assert_eq!(get(&store, "a").as_deref(), Some("2"));
        assert_eq!(get(&store, "b"), None);
    }

    #[test]
    fn a_snapshot_skips_the_log_before_its_start() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(
            &dir,
            0,
            &[put("a", "old", 10), put("b", "x", 11), put("a", "new", 12)],
        );
        snapshot(&dir, 2, &[put("a", "snap", 11), put("b", "x", 11)]);
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert_eq!(recovered.records, 1);
        assert_eq!(recovered.next_seq, 3);
        assert_eq!(get(&store, "a").as_deref(), Some("new"));
        assert_eq!(get(&store, "b").as_deref(), Some("x"));
    }

    #[test]
    fn a_damaged_newest_snapshot_falls_back_to_the_previous_one() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10), put("b", "1", 11)]);
        snapshot(&dir, 1, &[put("a", "1", 10)]);
        snapshot(&dir, 2, &[put("a", "1", 10), put("b", "1", 11)]);
        let newest = dir.snapshot_path(2);
        let bytes = fs::read(&newest).unwrap();
        fs::write(&newest, &bytes[..bytes.len() - 3]).unwrap();
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert_eq!(recovered.snapshot.unwrap().start, 1);
        assert_eq!(recovered.warnings.len(), 1);
        assert_eq!(get(&store, "b").as_deref(), Some("1"));
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_newest_snapshot_falls_back_to_the_previous_one() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10), put("b", "1", 11)]);
        snapshot(&dir, 1, &[put("a", "1", 10)]);
        // A directory in place of the newest snapshot opens but fails to read.
        fs::create_dir(dir.snapshot_path(2)).unwrap();
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert_eq!(recovered.snapshot.unwrap().start, 1);
        assert_eq!(recovered.warnings.len(), 1, "{:?}", recovered.warnings);
        assert!(recovered.warnings[0].contains("could not be read"));
        assert_eq!(get(&store, "b").as_deref(), Some("1"));
    }

    #[test]
    fn a_torn_tail_is_cut_and_an_empty_segment_removed() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10), put("b", "1", 11)]);
        let path = dir.segment_path(0);
        let len = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 4)
            .unwrap();
        segment(&dir, 2, &[]);
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert_eq!((recovered.records, recovered.next_seq), (1, 2));
        assert_eq!(recovered.warnings.len(), 1);
        assert_eq!(dir.segments().unwrap(), [0]);
        // The log is now whole, but the marker keeps the hole known.
        let again = recover(&dir, &self::store(), NOW).unwrap();
        assert_eq!(again.warnings.len(), 1, "{:?}", again.warnings);
        assert!(again.history_lost);
        assert_eq!(again.next_seq, 1);
    }

    /// Rewrites the header format of the file at `path` to an unsupported one.
    fn make_unsupported(path: &std::path::Path) {
        let mut bytes = fs::read(path).unwrap();
        bytes[10..14].copy_from_slice(&(codec::FORMAT + 1).to_le_bytes());
        let len = codec::HEADER_LEN as usize;
        let crc = crc32fast::hash(&bytes[8..len]);
        bytes[4..8].copy_from_slice(&crc.to_le_bytes());
        fs::write(path, &bytes).unwrap();
    }

    #[test]
    fn a_segment_of_an_unsupported_format_refuses_recovery_and_is_kept() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10)]);
        segment(&dir, 1, &[]);
        make_unsupported(&dir.segment_path(1));
        let before = fs::read(dir.segment_path(1)).unwrap();
        let error = recover(&dir, &store(), NOW).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("log-00000000000000000001.seg"),
            "{message}"
        );
        assert!(
            message.contains(&format!("format {}", codec::FORMAT + 1)),
            "{message}"
        );
        // Nothing changed, not even the header-only segment.
        assert_eq!(dir.segments().unwrap(), [0, 1]);
        assert_eq!(fs::read(dir.segment_path(1)).unwrap(), before);
    }

    #[test]
    fn a_snapshot_of_an_unsupported_format_refuses_recovery_and_is_kept() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10), put("b", "1", 11)]);
        // An unsupported snapshot behind a newer valid one still refuses.
        snapshot(&dir, 1, &[put("a", "1", 10)]);
        snapshot(&dir, 2, &[put("a", "1", 10), put("b", "1", 11)]);
        make_unsupported(&dir.snapshot_path(1));
        let message = recover(&dir, &store(), NOW).unwrap_err().to_string();
        assert!(
            message.contains("snap-00000000000000000001.snap"),
            "{message}"
        );
        assert_eq!(dir.snapshots().unwrap(), [1, 2]);
    }

    #[test]
    fn a_segment_with_an_unreadable_header_and_data_after_it_is_kept() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10)]);
        segment(&dir, 1, &[put("b", "1", 11), put("c", "1", 12)]);
        let path = dir.segment_path(1);
        let mut bytes = fs::read(&path).unwrap();
        bytes[12] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert!(
            recovered
                .warnings
                .iter()
                .any(|w| w.contains("no readable records; keeping it")),
            "{:?}",
            recovered.warnings
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(dir.segments().unwrap(), [0, 1]);
        // The next segment cannot take its name.
        assert_eq!(recovered.next_seq, 2);
        assert_eq!(get(&store, "a").as_deref(), Some("1"));
    }

    #[test]
    fn a_segment_no_longer_than_a_header_is_removed() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10)]);
        segment(&dir, 1, &[]);
        let path = dir.segment_path(1);
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, &bytes[..10]).unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.warnings.is_empty(), "{:?}", recovered.warnings);
        assert_eq!(dir.segments().unwrap(), [0]);
        assert_eq!(recovered.next_seq, 1);
    }

    #[test]
    fn a_damaged_snapshot_is_moved_aside_and_numbering_passes_it() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10), put("b", "1", 11)]);
        snapshot(&dir, 1, &[put("a", "1", 10)]);
        snapshot(&dir, 9, &[put("a", "1", 10), put("b", "1", 11)]);
        let path = dir.snapshot_path(9);
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert_eq!(recovered.snapshot.unwrap().start, 1);
        assert_eq!(recovered.next_seq, 9);
        assert_eq!(dir.snapshots().unwrap(), [1]);
        assert!(dir.damaged_snapshot_path(9).exists());
        assert_eq!(get(&store, "b").as_deref(), Some("1"));
    }

    /// Writes a segment at `start` with a loss marker before `records`.
    fn segment_after_loss(dir: &DataDir, start: u64, records: &[Record]) {
        let mut segment = SegmentWriter::create(dir, start).unwrap();
        segment.append_loss();
        for record in records {
            segment.append(record).unwrap();
        }
        segment.sync().unwrap();
    }

    #[test]
    fn a_gap_between_segments_is_reported_and_replayed_past() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10), put("b", "1", 11)]);
        segment(&dir, 5, &[put("c", "1", 15)]);
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert_eq!(recovered.warnings.len(), 1, "{:?}", recovered.warnings);
        assert!(
            recovered.warnings[0].contains("records 2 to 4 are missing"),
            "{:?}",
            recovered.warnings
        );
        assert_eq!((recovered.records, recovered.next_seq), (3, 6));
        assert_eq!(get(&store, "c").as_deref(), Some("1"));
    }

    #[test]
    fn a_loss_marker_without_a_covering_snapshot_is_reported() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment(&dir, 0, &[put("a", "1", 10)]);
        snapshot(&dir, 1, &[put("a", "1", 10)]);
        // The snapshot at 1 does not cover the marker at 1.
        segment_after_loss(&dir, 1, &[put("b", "1", 11)]);
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert_eq!(recovered.warnings.len(), 1, "{:?}", recovered.warnings);
        assert!(
            recovered.warnings[0].contains("records were lost at about sequence 1"),
            "{:?}",
            recovered.warnings
        );
        // The marker takes a sequence number but is not a replayed record.
        assert_eq!((recovered.records, recovered.next_seq), (1, 3));
        assert_eq!(get(&store, "b").as_deref(), Some("1"));
    }

    #[test]
    fn a_loss_marker_before_the_snapshot_is_covered_and_not_reported() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        segment_after_loss(&dir, 0, &[put("a", "1", 10)]);
        snapshot(&dir, 2, &[put("a", "1", 10)]);
        segment(&dir, 2, &[put("b", "1", 11)]);
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert!(recovered.warnings.is_empty(), "{:?}", recovered.warnings);
        assert_eq!((recovered.records, recovered.next_seq), (1, 3));
    }

    #[test]
    fn a_damaged_covering_snapshot_reports_the_missing_log() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        snapshot(&dir, 7, &[put("a", "1", 10)]);
        segment(&dir, 7, &[put("b", "1", 11)]);
        let path = dir.snapshot_path(7);
        let bytes = fs::read(&path).unwrap();
        fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
        let store = store();
        let recovered = recover(&dir, &store, NOW).unwrap();
        assert!(recovered.snapshot.is_none());
        assert_eq!(recovered.warnings.len(), 2, "{:?}", recovered.warnings);
        assert!(
            recovered.warnings[1].contains("the log begins at record 7"),
            "{:?}",
            recovered.warnings
        );
        assert_eq!(get(&store, "b").as_deref(), Some("1"));
    }

    /// Writes a segment at `start` holding `entries`.
    fn log(dir: &DataDir, start: u64, entries: &[Logged]) {
        let mut segment = SegmentWriter::create(dir, start).unwrap();
        for entry in entries {
            segment.append_logged(entry).unwrap();
        }
        segment.sync().unwrap();
    }

    #[test]
    fn the_last_link_record_comes_back_and_says_whether_the_node_was_last_standing() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(&dir, 0, &[Logged::Record(put("a", "1", 10))]);
        let mut writer = SnapshotWriter::create(&dir, 1, 0).unwrap();
        writer.add_links(&[2]).unwrap();
        writer.add(&put("a", "1", 10)).unwrap();
        writer.finish(&dir).unwrap();
        dir.write_clean().unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert_eq!(recovered.links, Some(vec![2]));
        assert!(!recovered.last_standing());
        log(
            &dir,
            1,
            &[Logged::Links(Vec::new()), Logged::Record(put("b", "1", 11))],
        );
        dir.write_clean().unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert_eq!(recovered.links, Some(Vec::new()));
        assert!(recovered.last_standing());
    }

    #[test]
    fn a_crash_can_still_be_last_standing_but_a_hole_in_the_history_cannot() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(
            &dir,
            0,
            &[Logged::Links(Vec::new()), Logged::Record(put("a", "1", 10))],
        );
        // A crash: no clean marker and a torn tail on the newest segment.
        let path = dir.segment_path(0);
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend_from_slice(&[1, 2, 3]);
        fs::write(&path, &bytes).unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.lossy, "{:?}", recovered.warnings);
        assert!(!recovered.history_lost, "{:?}", recovered.warnings);
        assert!(recovered.last_standing());
        log(&dir, 9, &[Logged::Record(put("b", "1", 11))]);
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.history_lost);
        assert!(!recovered.last_standing());
    }

    #[test]
    fn a_history_without_a_link_record_is_not_last_standing() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        assert!(!recover(&dir, &store(), NOW).unwrap().last_standing());
        log(&dir, 0, &[Logged::Record(put("a", "1", 10))]);
        dir.write_clean().unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert_eq!(recovered.links, None);
        assert!(!recovered.last_standing());
    }

    #[test]
    fn an_uncovered_loss_or_a_degraded_marker_is_lost_history() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(&dir, 0, &[Logged::Links(Vec::new()), Logged::Loss]);
        dir.write_clean().unwrap();
        assert!(recover(&dir, &store(), NOW).unwrap().history_lost);
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(&dir, 0, &[Logged::Links(Vec::new())]);
        dir.write_degraded(1).unwrap();
        dir.write_clean().unwrap();
        assert!(recover(&dir, &store(), NOW).unwrap().history_lost);
    }

    #[test]
    fn a_degraded_marker_alone_is_lost_history() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(&dir, 0, &[Logged::Links(Vec::new())]);
        dir.write_clean().unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(!recovered.history_lost, "{:?}", recovered.warnings);
        assert!(recovered.last_standing());
        dir.write_degraded(1).unwrap();
        dir.write_clean().unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.history_lost);
        assert!(!recovered.last_standing());
    }

    /// Cuts the last `bytes` bytes off the file at `path`, tearing its last entry.
    fn tear(path: &std::path::Path, bytes: u64) {
        let len = fs::metadata(path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(len - bytes)
            .unwrap();
    }

    #[test]
    fn a_torn_entry_after_a_clean_stop_is_lost_history_and_stays_so() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(
            &dir,
            0,
            &[
                Logged::Links(Vec::new()),
                Logged::Record(put("a", "1", 10)),
                Logged::Links(vec![2]),
            ],
        );
        tear(&dir.segment_path(0), 2);
        dir.write_clean().unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.history_lost, "{:?}", recovered.warnings);
        assert_eq!(recovered.links, Some(Vec::new()));
        assert!(!recovered.last_standing());
        assert!(dir.history_lost_path().exists());
        dir.write_clean().unwrap();
        let again = recover(&dir, &store(), NOW).unwrap();
        assert!(again.history_lost, "{:?}", again.warnings);
        assert!(!again.last_standing());
        assert!(
            again.warnings.iter().any(|w| w.contains("hole")),
            "{:?}",
            again.warnings
        );
    }

    #[test]
    fn a_torn_entry_in_an_older_segment_is_lost_history() {
        for clean in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let dir = DataDir::open(temp.path()).unwrap();
            log(
                &dir,
                0,
                &[Logged::Links(Vec::new()), Logged::Record(put("a", "1", 10))],
            );
            tear(&dir.segment_path(0), 2);
            log(&dir, 2, &[Logged::Record(put("b", "1", 11))]);
            if clean {
                dir.write_clean().unwrap();
            }
            let recovered = recover(&dir, &store(), NOW).unwrap();
            assert!(recovered.history_lost, "{:?}", recovered.warnings);
            assert!(!recovered.last_standing());
        }
    }

    #[test]
    fn a_segment_with_no_readable_records_is_lost_history() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(&dir, 0, &[Logged::Links(Vec::new())]);
        log(&dir, 1, &[Logged::Record(put("b", "1", 11))]);
        let path = dir.segment_path(1);
        let mut bytes = fs::read(&path).unwrap();
        bytes[12] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        dir.write_clean().unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.history_lost, "{:?}", recovered.warnings);
        assert!(!recovered.last_standing());
    }

    #[test]
    fn falling_back_past_a_damaged_snapshot_is_lost_history() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(&dir, 0, &[Logged::Links(Vec::new())]);
        let mut writer = SnapshotWriter::create(&dir, 1, 0).unwrap();
        writer.add_links(&[]).unwrap();
        writer.finish(&dir).unwrap();
        snapshot(&dir, 2, &[]);
        let newest = dir.snapshot_path(2);
        let bytes = fs::read(&newest).unwrap();
        fs::write(&newest, &bytes[..bytes.len() - 3]).unwrap();
        dir.write_clean().unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert_eq!(recovered.snapshot.unwrap().start, 1);
        assert!(recovered.history_lost);
        assert!(!recovered.last_standing());
        assert!(dir.history_lost_path().exists());
    }

    #[test]
    fn a_whole_history_leaves_no_hole_marker() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(&dir, 0, &[Logged::Links(Vec::new())]);
        dir.write_clean().unwrap();
        assert!(!recover(&dir, &store(), NOW).unwrap().history_lost);
        assert!(!dir.history_lost_path().exists());
    }

    fn position(peer: u8, run: u64, next: u64) -> Position {
        Position { peer, run, next }
    }

    #[test]
    fn runs_and_positions_come_back_from_the_snapshot_and_the_log() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        let first_run = RunStart { id: 1, first: 0 };
        let second_run = RunStart { id: 3, first: 2 };
        log(
            &dir,
            0,
            &[Logged::Run(first_run), Logged::Record(put("a", "1", 10))],
        );
        let mut writer = SnapshotWriter::create(&dir, 2, 0).unwrap();
        writer.add_run(first_run).unwrap();
        writer.add_position(position(2, 1, 50)).unwrap();
        writer.add(&put("a", "1", 10)).unwrap();
        writer.finish(&dir).unwrap();
        log(
            &dir,
            2,
            &[
                Logged::Run(second_run),
                Logged::Position(position(2, 1, 60)),
                Logged::Position(position(3, 4, 10)),
            ],
        );
        dir.write_clean().unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(!recovered.lossy, "{:?}", recovered.warnings);
        assert_eq!(recovered.runs, [first_run, second_run]);
        assert_eq!(
            recovered.positions,
            [position(2, 1, 60), position(3, 4, 10)]
        );
    }

    #[test]
    fn a_gap_discards_every_position() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(&dir, 0, &[Logged::Position(position(2, 1, 5))]);
        log(&dir, 5, &[Logged::Record(put("c", "1", 15))]);
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.lossy);
        assert!(recovered.positions.is_empty());
        assert!(
            recovered.warnings.iter().any(|w| w.contains("full sync")),
            "{:?}",
            recovered.warnings
        );
    }

    #[test]
    fn an_uncovered_loss_marker_discards_every_position() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(
            &dir,
            0,
            &[Logged::Position(position(2, 1, 5)), Logged::Loss],
        );
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.lossy);
        assert!(recovered.positions.is_empty());
    }

    #[test]
    fn a_degraded_marker_discards_every_position() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(&dir, 0, &[Logged::Position(position(2, 1, 5))]);
        dir.write_degraded(1).unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.lossy);
        assert!(recovered.positions.is_empty());
    }

    #[test]
    fn a_crash_discards_every_position_and_a_clean_stop_keeps_them() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(
            &dir,
            0,
            &[
                Logged::Position(position(2, 1, 5)),
                Logged::Record(put("a", "1", 10)),
            ],
        );
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(recovered.lossy);
        assert!(recovered.positions.is_empty());
        dir.write_clean().unwrap();
        let recovered = recover(&dir, &store(), NOW).unwrap();
        assert!(!recovered.lossy, "{:?}", recovered.warnings);
        assert_eq!(recovered.positions, [position(2, 1, 5)]);
        // The marker was taken, so a later crash is not mistaken for a clean stop.
        assert!(!dir.take_clean().unwrap());
    }

    #[test]
    fn an_empty_directory_without_the_marker_is_not_lossy() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        assert!(!recover(&dir, &store(), NOW).unwrap().lossy);
    }

    #[test]
    fn deletes_in_the_log_come_back_as_tombstones() {
        let version_at = |secs: u64| ((secs * 1000) << 22) | 1;
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        log(
            &dir,
            0,
            &[
                Logged::Record(put("k", "v", version_at(NOW))),
                Logged::Record(Record::Delete {
                    key: Bytes::from_static(b"k"),
                    version: version_at(NOW + 2),
                }),
            ],
        );
        let store = store();
        store.enable_tombstones(3600, usize::MAX);
        recover(&dir, &store, NOW).unwrap();
        assert_eq!(store.tombstones(), 1);
        store.apply(&put("k", "late", version_at(NOW + 1)), NOW);
        assert_eq!(get(&store, "k"), None);
    }
}
