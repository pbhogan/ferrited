//! Where a sender resumes a peer that holds a position on its log.

use crate::oplog::RunStart;

/// The sequence number to stream from, given the peer's `position` as `(run, next)`; `None`
/// means a full sync. For an earlier run, resumes no later than the next run's first entry:
/// past that run's end, the peer holds entries the sender lost in a crash. The caller checks
/// that the log still holds every entry from that point.
pub fn resume_point(runs: &[RunStart], current: u64, position: Option<(u64, u64)>) -> Option<u64> {
    let (run, next) = position?;
    if run == current {
        return Some(next);
    }
    let index = runs.iter().position(|known| known.id == run)?;
    let following = runs.get(index + 1)?;
    Some(next.min(following.first))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIRST: RunStart = RunStart { id: 11, first: 0 };
    const SECOND: RunStart = RunStart { id: 22, first: 100 };
    const THIRD: RunStart = RunStart { id: 33, first: 250 };

    #[test]
    fn a_peer_without_a_position_gets_a_full_sync() {
        assert_eq!(resume_point(&[FIRST], 11, None), None);
    }

    #[test]
    fn the_current_run_resumes_at_the_position() {
        assert_eq!(
            resume_point(&[FIRST, SECOND], 22, Some((22, 150))),
            Some(150)
        );
    }

    #[test]
    fn an_earlier_run_resumes_at_the_position_or_the_next_runs_start() {
        let runs = [FIRST, SECOND, THIRD];
        assert_eq!(resume_point(&runs, 33, Some((11, 80))), Some(80));
        // The second run reused 100 to 119, which the sender lost from the first run.
        assert_eq!(resume_point(&runs, 33, Some((11, 120))), Some(100));
        assert_eq!(resume_point(&runs, 33, Some((22, 300))), Some(250));
    }

    #[test]
    fn an_unknown_run_gets_a_full_sync() {
        assert_eq!(resume_point(&[FIRST, SECOND], 22, Some((99, 5))), None);
    }
}
