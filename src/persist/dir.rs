//! The data directory: its lock, file names, listing, pruning and directory fsync.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub struct DataDir {
    path: PathBuf,
    /// Held for the process's life so a second ferrited cannot use the directory.
    _lock: File,
}

impl DataDir {
    /// Creates the directory if needed and locks it. Errors name the directory.
    pub fn open(path: &Path) -> io::Result<Self> {
        let cannot_open = |error: io::Error| {
            io::Error::new(
                error.kind(),
                format!("cannot open data directory {}: {error}", path.display()),
            )
        };
        fs::create_dir_all(path).map_err(cannot_open)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.join("LOCK"))
            .map_err(cannot_open)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    format!("{} is in use by another ferrited", path.display()),
                ));
            }
            Err(TryLockError::Error(error)) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("cannot lock {}: {error}", path.display()),
                ));
            }
        }
        Ok(Self {
            path: path.to_path_buf(),
            _lock: lock,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn segment_path(&self, start: u64) -> PathBuf {
        self.path.join(format!("log-{start:020}.seg"))
    }

    pub fn snapshot_path(&self, start: u64) -> PathBuf {
        self.path.join(format!("snap-{start:020}.snap"))
    }

    /// Where a damaged snapshot is moved, out of the listing, so pruning never keeps it.
    pub fn damaged_snapshot_path(&self, start: u64) -> PathBuf {
        self.path.join(format!("snap-{start:020}.snap.damaged"))
    }

    /// Moves a damaged snapshot aside and makes the move durable.
    pub fn quarantine_snapshot(&self, start: u64) -> io::Result<()> {
        fs::rename(self.snapshot_path(start), self.damaged_snapshot_path(start))?;
        self.sync()
    }

    pub fn temp_snapshot_path(&self, start: u64) -> PathBuf {
        self.path.join(format!("snap-{start:020}.tmp"))
    }

    /// The file that marks records lost to a disk error and not yet covered by a snapshot.
    pub fn degraded_path(&self) -> PathBuf {
        self.path.join("DEGRADED")
    }

    /// Writes the degraded marker, holding the sequence number of the first record that may be
    /// lost, and makes it durable.
    pub fn write_degraded(&self, first_lost: u64) -> io::Result<()> {
        let mut file = File::create(self.degraded_path())?;
        file.write_all(format!("{first_lost}\n").as_bytes())?;
        file.sync_all()?;
        self.sync()
    }

    /// Reads the degraded marker: `None` if there is none, otherwise the first lost sequence
    /// number if the file holds one.
    pub fn read_degraded(&self) -> io::Result<Option<Option<u64>>> {
        match fs::read_to_string(self.degraded_path()) {
            Ok(text) => Ok(Some(text.trim().parse().ok())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Removes the degraded marker, if any, and makes the removal durable.
    pub fn remove_degraded(&self) -> io::Result<()> {
        match fs::remove_file(self.degraded_path()) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
            Ok(()) => self.sync(),
        }
    }

    /// Marks a history hole (`Recovered::history_lost`) no snapshot has covered yet, so later
    /// starts still know of it after recovery erased the evidence (e.g. by cutting a torn entry).
    pub fn history_lost_path(&self) -> PathBuf {
        self.path.join("HISTORY_LOST")
    }

    /// Writes the history-lost marker and makes it durable.
    pub fn write_history_lost(&self) -> io::Result<()> {
        File::create(self.history_lost_path())?.sync_all()?;
        self.sync()
    }

    pub fn has_history_lost(&self) -> io::Result<bool> {
        self.history_lost_path().try_exists()
    }

    /// Removes the history-lost marker, if any, and makes the removal durable.
    pub fn remove_history_lost(&self) -> io::Result<()> {
        match fs::remove_file(self.history_lost_path()) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
            Ok(()) => self.sync(),
        }
    }

    /// Marks that the log thread stopped cleanly, with everything synced.
    pub fn clean_path(&self) -> PathBuf {
        self.path.join("CLEAN")
    }

    /// Writes the clean-shutdown marker and makes it durable.
    pub fn write_clean(&self) -> io::Result<()> {
        File::create(self.clean_path())?.sync_all()?;
        self.sync()
    }

    /// Removes the clean-shutdown marker, durably, and returns whether there was one.
    pub fn take_clean(&self) -> io::Result<bool> {
        match fs::remove_file(self.clean_path()) {
            Ok(()) => self.sync().map(|()| true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Start positions of the log segments, oldest first.
    pub fn segments(&self) -> io::Result<Vec<u64>> {
        self.list("log-", ".seg")
    }

    /// Start positions of the snapshots, oldest first.
    pub fn snapshots(&self) -> io::Result<Vec<u64>> {
        self.list("snap-", ".snap")
    }

    /// Removes snapshots that were being written when the process stopped.
    pub fn remove_temporaries(&self) -> io::Result<()> {
        for start in self.list("snap-", ".tmp")? {
            fs::remove_file(self.temp_snapshot_path(start))?;
        }
        Ok(())
    }

    /// Makes file creations, renames and removals in the directory durable.
    pub fn sync(&self) -> io::Result<()> {
        File::open(&self.path)?.sync_all()
    }

    /// Keeps the `keep` newest snapshots (at least one) and the segments needed to replay after
    /// the oldest of them; removes everything else.
    pub fn prune(&self, keep: usize) -> io::Result<()> {
        let snapshots = self.snapshots()?;
        let keep = snapshots.len().saturating_sub(keep.max(1));
        for &start in &snapshots[..keep] {
            fs::remove_file(self.snapshot_path(start))?;
        }
        let Some(&oldest_kept) = snapshots.get(keep) else {
            return Ok(());
        };
        let segments = self.segments()?;
        for pair in segments.windows(2) {
            if pair[1] <= oldest_kept {
                fs::remove_file(self.segment_path(pair[0]))?;
            }
        }
        self.sync()
    }

    fn list(&self, prefix: &str, suffix: &str) -> io::Result<Vec<u64>> {
        let mut starts = Vec::new();
        for entry in fs::read_dir(&self.path)? {
            let name = entry?.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(start) = name
                .strip_prefix(prefix)
                .and_then(|rest| rest.strip_suffix(suffix))
                .and_then(|digits| digits.parse().ok())
            {
                starts.push(start);
            }
        }
        starts.sort_unstable();
        Ok(starts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(path: PathBuf) {
        File::create(path).unwrap();
    }

    #[test]
    fn lists_files_in_order_and_ignores_others() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        for start in [30, 0, 1000] {
            touch(dir.segment_path(start));
        }
        touch(dir.snapshot_path(500));
        touch(dir.temp_snapshot_path(900));
        touch(dir.damaged_snapshot_path(700));
        touch(temp.path().join("notes.txt"));
        assert_eq!(dir.segments().unwrap(), [0, 30, 1000]);
        assert_eq!(dir.snapshots().unwrap(), [500]);
        dir.remove_temporaries().unwrap();
        assert!(!dir.temp_snapshot_path(900).exists());
    }

    #[test]
    fn the_degraded_marker_round_trips() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        assert_eq!(dir.read_degraded().unwrap(), None);
        dir.write_degraded(42).unwrap();
        assert_eq!(dir.read_degraded().unwrap(), Some(Some(42)));
        fs::write(dir.degraded_path(), "").unwrap();
        assert_eq!(dir.read_degraded().unwrap(), Some(None));
        dir.remove_degraded().unwrap();
        dir.remove_degraded().unwrap();
        assert_eq!(dir.read_degraded().unwrap(), None);
    }

    #[test]
    fn the_clean_marker_is_taken_once() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        assert!(!dir.take_clean().unwrap());
        dir.write_clean().unwrap();
        assert!(dir.take_clean().unwrap());
        assert!(!dir.take_clean().unwrap());
    }

    #[test]
    fn a_second_open_of_the_same_directory_fails() {
        let temp = tempfile::tempdir().unwrap();
        let _first = DataDir::open(temp.path()).unwrap();
        let error = DataDir::open(temp.path()).err().unwrap();
        assert!(error.to_string().contains("in use"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn an_error_opening_the_directory_names_it() {
        use std::os::unix::fs::PermissionsExt;

        // Root ignores directory permissions.
        if unsafe { libc::geteuid() } == 0 {
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o500)).unwrap();
        let path = temp.path().join("data");
        let error = DataDir::open(&path).err().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let message = error.to_string();
        let expected = format!("cannot open data directory {}: ", path.display());
        assert!(message.starts_with(&expected), "{message}");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn the_directory_can_be_reopened_after_the_first_is_dropped() {
        let temp = tempfile::tempdir().unwrap();
        drop(DataDir::open(temp.path()).unwrap());
        DataDir::open(temp.path()).unwrap();
    }

    #[test]
    fn prune_without_snapshots_keeps_every_segment() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        for start in [0, 100, 200] {
            touch(dir.segment_path(start));
        }
        dir.prune(2).unwrap();
        assert_eq!(dir.segments().unwrap(), [0, 100, 200]);
    }

    #[test]
    fn prune_with_one_snapshot_keeps_it_and_the_log_after_it() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        for start in [0, 100, 200] {
            touch(dir.segment_path(start));
        }
        touch(dir.snapshot_path(150));
        dir.prune(2).unwrap();
        assert_eq!(dir.snapshots().unwrap(), [150]);
        assert_eq!(dir.segments().unwrap(), [100, 200]);
    }

    #[test]
    fn prune_removes_a_segment_that_ends_exactly_at_the_oldest_kept_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        for start in [0, 100, 200] {
            touch(dir.segment_path(start));
        }
        for start in [50, 100, 200] {
            touch(dir.snapshot_path(start));
        }
        dir.prune(2).unwrap();
        assert_eq!(dir.snapshots().unwrap(), [100, 200]);
        assert_eq!(dir.segments().unwrap(), [100, 200]);
    }

    #[test]
    fn prune_keeps_two_snapshots_and_the_log_after_the_older() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        for start in [0, 100, 200, 300, 400] {
            touch(dir.segment_path(start));
        }
        for start in [100, 250, 400] {
            touch(dir.snapshot_path(start));
        }
        dir.prune(2).unwrap();
        assert_eq!(dir.snapshots().unwrap(), [250, 400]);
        // The snapshot at 250 still needs segment 200.
        assert_eq!(dir.segments().unwrap(), [200, 300, 400]);
    }

    #[test]
    fn prune_keeping_one_snapshot_removes_the_older_ones_and_the_log_before_it() {
        let temp = tempfile::tempdir().unwrap();
        let dir = DataDir::open(temp.path()).unwrap();
        for start in [0, 100, 200, 300, 400] {
            touch(dir.segment_path(start));
        }
        for start in [100, 250, 400] {
            touch(dir.snapshot_path(start));
        }
        dir.prune(1).unwrap();
        assert_eq!(dir.snapshots().unwrap(), [400]);
        assert_eq!(dir.segments().unwrap(), [400]);
    }
}
