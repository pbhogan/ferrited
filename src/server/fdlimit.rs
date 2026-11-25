//! Reconciles the connection limit with the process's file-descriptor limit.

/// Descriptors kept back for the listeners, the runtime, the log, and the standard streams.
pub(crate) const MARGIN: usize = 32;
/// Descriptors each client thread may hold: its poll and its waker.
const PER_THREAD: usize = 2;
/// Descriptors each peer may hold: a link each way and a segment its sender reads.
const PER_PEER: usize = 3;
/// Most metrics connections served at once.
pub(crate) const METRICS_CONNECTIONS: usize = 8;
/// The metrics listener, its connections, and one accepted only to turn away.
const METRICS: usize = 1 + METRICS_CONNECTIONS + 1;
/// Descriptors persistence may hold: the directory, a segment, and two snapshots.
const PERSISTENCE: usize = 4;

/// Descriptors kept back from client connections. Handshakes from unknown hosts are not
/// counted: the replication listener caps them separately.
pub(crate) fn reserve(threads: usize, peers: usize, persistence: bool, metrics: bool) -> usize {
    MARGIN
        + PER_THREAD * (threads + 1)
        + PER_PEER * peers
        + if persistence { PERSISTENCE } else { 0 }
        + if metrics { METRICS } else { 0 }
}

/// The most connections, up to `wanted`, that fit in `descriptors` less `reserved`.
pub(crate) fn fit(wanted: usize, descriptors: u64, reserved: usize) -> usize {
    let room = usize::try_from(descriptors)
        .unwrap_or(usize::MAX)
        .saturating_sub(reserved);
    wanted.min(room).max(1)
}

/// The soft limit to ask for to make `needed` descriptors available, or `None` if `soft`
/// already covers it. `cap` is a platform ceiling on the soft limit.
fn target(needed: u64, soft: u64, hard: u64, cap: Option<u64>) -> Option<u64> {
    let ceiling = hard.min(cap.unwrap_or(u64::MAX));
    let wanted = needed.min(ceiling);
    (wanted > soft).then_some(wanted)
}

/// macOS refuses a soft limit above `OPEN_MAX`, whatever the hard limit says.
#[cfg(target_os = "macos")]
const SOFT_CAP: Option<u64> = Some(10_240);
#[cfg(all(unix, not(target_os = "macos")))]
const SOFT_CAP: Option<u64> = None;

/// Raises the soft `RLIMIT_NOFILE` toward `needed`; returns the resulting soft limit, or `None`
/// if it cannot be read.
#[cfg(unix)]
fn raise_descriptor_limit(needed: u64) -> Option<u64> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `getrlimit` only writes to `limit`, a valid exclusive reference.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
        return None;
    }
    // `rlim_t` is not `u64` on every Unix.
    #[allow(clippy::unnecessary_cast)]
    let (soft, hard) = (limit.rlim_cur as u64, limit.rlim_max as u64);
    let Some(wanted) = target(needed, soft, hard, SOFT_CAP) else {
        return Some(soft);
    };
    let raised = libc::rlimit {
        rlim_cur: wanted as libc::rlim_t,
        rlim_max: limit.rlim_max,
    };
    // SAFETY: `setrlimit` only reads `raised`, which outlives the call.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } != 0 {
        // The kernel may enforce a lower ceiling than the hard limit reports.
        return Some(soft);
    }
    Some(wanted)
}

/// Raises the descriptor limit to fit `wanted` connections plus `reserved`; if it cannot,
/// warns and returns the lower connection limit that fits.
pub(crate) fn reconcile(wanted: usize, reserved: usize) -> usize {
    #[cfg(unix)]
    {
        let needed = wanted.saturating_add(reserved) as u64;
        let Some(soft) = raise_descriptor_limit(needed) else {
            return wanted;
        };
        let fitted = fit(wanted, soft, reserved);
        if fitted < wanted {
            eprintln!(
                "warning: the file-descriptor limit is {soft}; \
                 serving at most {fitted} connections instead of {wanted}"
            );
        }
        fitted
    }
    #[cfg(not(unix))]
    {
        let _ = reserved;
        wanted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_limit_fits_under_the_descriptor_limit_with_a_margin() {
        assert_eq!(fit(1024, 256, 32), 224);
        assert_eq!(fit(1024, 1056, 32), 1024);
        assert_eq!(fit(1024, u64::MAX, 32), 1024);
        assert_eq!(fit(10, 20, 32), 1);
    }

    #[test]
    fn the_reserve_grows_with_threads_peers_persistence_and_metrics() {
        assert_eq!(reserve(1, 0, false, false), MARGIN + 4);
        assert_eq!(reserve(2, 0, false, false), 38);
        assert_eq!(reserve(64, 0, false, false), 162);
        assert_eq!(reserve(2, 2, false, false), 44);
        assert_eq!(reserve(2, 0, true, false), 42);
        assert_eq!(reserve(4, 4, true, false), 32 + 10 + 12 + 4);
        assert_eq!(reserve(2, 0, false, true), 38 + 10);
        assert_eq!(reserve(4, 4, true, true), 32 + 10 + 12 + 4 + 10);
    }

    #[test]
    fn soft_limit_target_respects_the_hard_limit_and_the_platform_cap() {
        assert_eq!(target(1056, 256, u64::MAX, None), Some(1056));
        assert_eq!(target(1056, 256, 512, None), Some(512));
        assert_eq!(target(100_000, 256, u64::MAX, Some(10_240)), Some(10_240));
        assert_eq!(target(1056, 4096, u64::MAX, None), None);
        assert_eq!(target(1056, 512, 512, None), None);
    }

    #[cfg(unix)]
    #[test]
    fn raising_never_lowers_the_limit_and_covers_a_modest_request() {
        let before = raise_descriptor_limit(0).unwrap();
        let after = raise_descriptor_limit(before.saturating_add(1)).unwrap();
        assert!(after >= before);
        assert!(raise_descriptor_limit(64).unwrap() >= 64);
    }
}
