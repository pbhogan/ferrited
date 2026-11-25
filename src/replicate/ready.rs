//! Startup readiness. A node with peers serves clients only once caught up with every linked
//! peer (at least one) after each configured peer has been tried, or as the last node standing
//! with no peer answering, or after `--sync-timeout` with none reachable, or at once with
//! `--force-standalone`. Once serving, it logs the set of linked peers whenever it changes,
//! which tells a later start whether this node was the last one standing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::handshake::HANDSHAKE_TIMEOUT;
use super::sender::CONNECT_TIMEOUT;
use crate::oplog::Oplog;

/// How long the last node standing tries its peers before serving alone.
pub const GRACE: Duration = Duration::from_secs(2);
/// How long a peer stays reachable after its link ends or it says it is recovering: longer than
/// the longest backoff plus a connect, so a peer between attempts still counts. Also how long a
/// peer linked only from this node is waited for to link back.
pub const REACHABLE_HOLD: Duration = Duration::from_secs(10);
/// How long after recovery to wait for each sender's first attempt before readiness can ignore
/// that peer: a connect plus one handshake step.
pub const FIRST_ATTEMPT: Duration = CONNECT_TIMEOUT.saturating_add(HANDSHAKE_TIMEOUT);
/// Pause between readiness checks.
const POLL: Duration = Duration::from_millis(100);
/// Least time between log lines giving the same reason for waiting.
const REPORT_INTERVAL: Duration = Duration::from_secs(10);
/// Least time between log lines when the reason changes.
const REPORT_CHANGE: Duration = Duration::from_secs(1);

/// Why a node became ready.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadyBy {
    /// Caught up with every peer it is linked with.
    CaughtUp,
    /// The last node standing, with no peer reachable after the grace period.
    LastStanding,
    /// No peer was reachable for `--sync-timeout`.
    SyncTimeout,
    /// `--force-standalone`.
    Forced,
}

impl ReadyBy {
    pub const ALL: [ReadyBy; 4] = [
        ReadyBy::CaughtUp,
        ReadyBy::LastStanding,
        ReadyBy::SyncTimeout,
        ReadyBy::Forced,
    ];

    pub fn name(self) -> &'static str {
        match self {
            ReadyBy::CaughtUp => "caught_up",
            ReadyBy::LastStanding => "last_standing",
            ReadyBy::SyncTimeout => "sync_timeout",
            ReadyBy::Forced => "force_standalone",
        }
    }
}

/// Why a node is not ready yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Waiting {
    /// The last node standing, trying its peers before it serves alone.
    GracePeriod,
    /// No peer is reachable; the sync timeout is running.
    NoPeerReachable,
    /// A peer answered, but it is still loading its own data.
    PeerRecovering,
    /// A peer is reachable but has not linked to this node.
    NoInboundLink,
    /// Applying what the linked peers had when they linked.
    CatchingUp,
    /// Caught up, but a sender's first attempt is unfinished, so a live peer may not have
    /// linked yet.
    ContactingPeers,
}

impl Waiting {
    pub const ALL: [Waiting; 6] = [
        Waiting::GracePeriod,
        Waiting::NoPeerReachable,
        Waiting::PeerRecovering,
        Waiting::NoInboundLink,
        Waiting::CatchingUp,
        Waiting::ContactingPeers,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Waiting::GracePeriod => "grace_period",
            Waiting::NoPeerReachable => "no_peer_reachable",
            Waiting::PeerRecovering => "peer_recovering",
            Waiting::NoInboundLink => "no_inbound_link",
            Waiting::CatchingUp => "catching_up",
            Waiting::ContactingPeers => "contacting_peers",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Loading local data.
    Recovering,
    Waiting(Waiting),
    Ready(ReadyBy),
}

/// The status command's values that are not per peer, for the metrics endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub phase: Phase,
    pub last_standing: bool,
    /// `None` waits forever.
    pub sync_timeout: Option<Duration>,
    /// Time since recovery with no peer reachable, as of the last check.
    pub unreachable: Duration,
    pub reachable: bool,
    /// Peers with a live link in either direction.
    pub linked_peers: usize,
}

/// Catching up with one peer over its inbound link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Progress {
    /// The peer's log head when it linked: caught up once every entry below it is applied.
    target: u64,
    applied: Option<u64>,
}

impl Progress {
    fn caught_up(&self) -> bool {
        self.applied.is_some_and(|applied| applied >= self.target)
    }
}

pub struct Readiness {
    state: Mutex<State>,
}

struct State {
    sync_timeout: Option<Duration>,
    force_standalone: bool,
    last_standing: bool,
    phase: Phase,
    /// Configured peers, each with its own sender.
    peers: usize,
    /// Peer indexes whose sender has finished its first attempt.
    attempted: BTreeSet<usize>,
    recovered_at: Option<Instant>,
    polled_at: Option<Instant>,
    /// No peer was reachable at the last poll (or at recovery, before the first).
    polled_unreachable: bool,
    /// Sum of poll intervals that found no peer reachable at both ends.
    unreachable: Duration,
    /// Live links by peer node ID, counting each direction.
    links: BTreeMap<u8, u8>,
    ended_at: Option<Instant>,
    /// When a peer last answered that it is still recovering.
    recovering_at: Option<Instant>,
    inbound: BTreeMap<u8, Progress>,
    /// Counted, since two configured addresses can reach the same node.
    outbound: BTreeMap<u8, u8>,
    /// When each peer linked only from this node began to be waited for. Kept across outbound
    /// reconnects, so a flapping link does not restart the wait; cleared when it links back.
    pending_since: BTreeMap<u8, Instant>,
    /// Peers no longer waited for to link back, already reported. Kept across outbound
    /// reconnects until the peer links back.
    abandoned: BTreeSet<u8>,
    /// Set when ready by the sync timeout or `--force-standalone`: no link record is logged
    /// until an inbound link catches up, so the next start does not take this node for the last
    /// one standing.
    hold_links: bool,
    /// Where link records go, once local recovery has finished.
    oplog: Option<Arc<Oplog>>,
    /// The last set of linked peers handed to the oplog.
    logged: Option<Vec<u8>>,
    /// Links ended during shutdown are not peers going down.
    stopping: bool,
    /// Log lines, printed once the lock is released.
    notes: Vec<String>,
}

impl Readiness {
    /// A node that has not finished local recovery. A `sync_timeout` of `None` waits forever.
    pub fn new(sync_timeout: Option<Duration>, force_standalone: bool, peers: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                sync_timeout,
                force_standalone,
                last_standing: false,
                phase: Phase::Recovering,
                peers,
                attempted: BTreeSet::new(),
                recovered_at: None,
                polled_at: None,
                polled_unreachable: true,
                unreachable: Duration::ZERO,
                links: BTreeMap::new(),
                ended_at: None,
                recovering_at: None,
                inbound: BTreeMap::new(),
                outbound: BTreeMap::new(),
                pending_since: BTreeMap::new(),
                abandoned: BTreeSet::new(),
                hold_links: false,
                oplog: None,
                logged: None,
                stopping: false,
                notes: Vec::new(),
            }),
        })
    }

    /// Local recovery finished. `last_standing` says whether the recovered log shows every peer
    /// down when this node last served. Only the first call counts.
    pub fn recovered(&self, oplog: Arc<Oplog>, last_standing: bool, now: Instant) {
        let mut state = self.state.lock();
        if state.phase != Phase::Recovering {
            return;
        }
        state.oplog = Some(oplog);
        state.last_standing = last_standing;
        state.recovered_at = Some(now);
        state.polled_at = Some(now);
        state.phase = Phase::Waiting(Waiting::NoPeerReachable);
    }

    /// This node's link to `peer` finished its handshake.
    pub fn outbound_up(&self, peer: u8, now: Instant) {
        let mut state = self.state.lock();
        *state.outbound.entry(peer).or_default() += 1;
        if !state.inbound.contains_key(&peer) {
            state.pending_since.entry(peer).or_insert(now);
        }
        state.add_link(peer);
    }

    /// This node's link to `peer` ended.
    pub fn outbound_down(&self, peer: u8, now: Instant) {
        let mut state = self.state.lock();
        if let Some(count) = state.outbound.get_mut(&peer) {
            *count -= 1;
            if *count == 0 {
                state.outbound.remove(&peer);
            }
        }
        // An abandoned peer does not become reachable again each time this link ends.
        let ended = !state.abandoned.contains(&peer);
        state.remove_link(peer, ended.then_some(now));
    }

    /// `peer` linked to this node with its log head at `target`.
    pub fn inbound_up(&self, peer: u8, target: u64) {
        let mut state = self.state.lock();
        let progress = Progress {
            target,
            applied: None,
        };
        state.inbound.insert(peer, progress);
        state.pending_since.remove(&peer);
        state.abandoned.remove(&peer);
        state.add_link(peer);
    }

    /// The link from `peer` ended.
    pub fn inbound_down(&self, peer: u8, now: Instant) {
        let mut state = self.state.lock();
        state.inbound.remove(&peer);
        // Still linked to it: wait for it to link back, from now.
        if state.outbound.contains_key(&peer) {
            state.pending_since.insert(peer, now);
        }
        state.remove_link(peer, Some(now));
    }

    /// The sender to peer `index` finished its first attempt. Later calls change nothing.
    pub fn attempted(&self, index: usize) {
        self.state.lock().attempted.insert(index);
    }

    /// Every entry of `peer`'s log below `next` is applied. Also called after ready, for the
    /// status command.
    pub fn applied(&self, peer: u8, next: u64) {
        let mut state = self.state.lock();
        if let Some(progress) = state.inbound.get_mut(&peer) {
            progress.applied = Some(next);
            if state.hold_links {
                state.log_links();
            }
        }
    }

    /// A peer answered that it is still recovering.
    pub fn peer_recovering(&self, now: Instant) {
        self.state.lock().recovering_at = Some(now);
    }

    /// Shutdown has begun, so links that end are no longer logged.
    pub fn stopping(&self) {
        self.state.lock().stopping = true;
    }

    /// Decides whether the node is ready. Once ready it stays ready.
    pub fn poll(&self, now: Instant) -> Phase {
        let (phase, notes) = {
            let mut state = self.state.lock();
            let phase = state.poll(now);
            (phase, std::mem::take(&mut state.notes))
        };
        for note in notes {
            eprintln!("{note}");
        }
        phase
    }

    /// The status command's lines, as (name, value).
    pub fn lines(&self, now: Instant) -> Vec<(String, String)> {
        self.state.lock().lines(now)
    }

    /// The values the metrics endpoint reports.
    pub fn snapshot(&self, now: Instant) -> Snapshot {
        let state = self.state.lock();
        Snapshot {
            phase: state.phase,
            last_standing: state.last_standing,
            sync_timeout: state.sync_timeout,
            unreachable: state.unreachable,
            reachable: state.reachable(now),
            linked_peers: state.links.len(),
        }
    }

    /// Why the node is waiting, as a phrase for the log.
    pub fn describe(&self) -> String {
        self.state.lock().describe()
    }

    /// Polls until ready. Logs why it waits at the first check, on a change of reason (at most
    /// every `REPORT_CHANGE`) and every `REPORT_INTERVAL`, also as the service manager's status.
    pub async fn wait(&self) -> ReadyBy {
        let mut tick = tokio::time::interval(POLL);
        let mut reported: Option<(Waiting, Instant)> = None;
        loop {
            tick.tick().await;
            let now = Instant::now();
            match self.poll(now) {
                Phase::Ready(by) => {
                    let line = self.state.lock().ready_line(by);
                    eprintln!("{line}");
                    return by;
                }
                Phase::Waiting(reason) => {
                    let due = reported.is_none_or(|(last, at)| {
                        let since = now.saturating_duration_since(at);
                        since >= REPORT_INTERVAL || (last != reason && since >= REPORT_CHANGE)
                    });
                    if due {
                        let status = format!("not serving clients yet: {}", self.describe());
                        eprintln!("ferrited: {status}");
                        crate::notify::global().status(&status);
                        reported = Some((reason, now));
                    }
                }
                Phase::Recovering => {}
            }
        }
    }
}

impl State {
    fn add_link(&mut self, peer: u8) {
        *self.links.entry(peer).or_default() += 1;
        self.log_links();
    }

    /// `ended` is when the link ended, if that makes the peer reachable for a while.
    fn remove_link(&mut self, peer: u8, ended: Option<Instant>) {
        if let Some(count) = self.links.get_mut(&peer) {
            *count -= 1;
            if *count == 0 {
                self.links.remove(&peer);
            }
        }
        if ended.is_some() {
            self.ended_at = ended;
        }
        self.log_links();
    }

    /// Logs the set of linked peers if it changed, while serving and not stopping.
    fn log_links(&mut self) {
        if !matches!(self.phase, Phase::Ready(_)) || self.stopping {
            return;
        }
        // Only an inbound link that has caught up ends the hold. Any other peer has not brought
        // the writes this node lacks, and an empty set logged after it would make the next
        // start last standing without them.
        if self.hold_links {
            if !self.inbound.values().any(Progress::caught_up) {
                return;
            }
            self.hold_links = false;
        }
        let Some(oplog) = &self.oplog else {
            return;
        };
        let up: Vec<u8> = self.links.keys().copied().collect();
        if self.logged.as_ref() != Some(&up) {
            oplog.set_links(up.clone());
            self.logged = Some(up);
        }
    }

    fn recent(at: Option<Instant>, now: Instant) -> bool {
        at.is_some_and(|at| now.saturating_duration_since(at) < REACHABLE_HOLD)
    }

    /// Peers linked only from this node for less than `REACHABLE_HOLD`. They are up and may
    /// hold writes this node lacks, so it waits for them.
    fn awaited(&self, now: Instant) -> Vec<u8> {
        self.outbound
            .keys()
            .filter(|peer| {
                !self.inbound.contains_key(peer)
                    && Self::recent(self.pending_since.get(peer).copied(), now)
            })
            .copied()
            .collect()
    }

    /// Stops waiting for peers linked only from this node for `REACHABLE_HOLD`. They no longer
    /// count as reachable, so a peer that never links back cannot hold off the sync timeout.
    fn abandon(&mut self, now: Instant) {
        let expired: Vec<u8> = self
            .outbound
            .keys()
            .filter(|peer| {
                !self.inbound.contains_key(peer)
                    && !Self::recent(self.pending_since.get(peer).copied(), now)
                    && !self.abandoned.contains(peer)
            })
            .copied()
            .collect();
        for peer in expired {
            self.notes.push(format!(
                "ferrited: node {peer} is linked from this node but has not linked back within {}s; not waiting for it",
                REACHABLE_HOLD.as_secs()
            ));
            self.abandoned.insert(peer);
        }
    }

    /// Every sender has finished its first attempt, or `FIRST_ATTEMPT` has passed.
    fn contacted(&self, now: Instant, since: Instant) -> bool {
        self.attempted.len() >= self.peers || now.saturating_duration_since(since) >= FIRST_ATTEMPT
    }

    fn reachable(&self, now: Instant) -> bool {
        !self.inbound.is_empty()
            || !self.awaited(now).is_empty()
            || Self::recent(self.ended_at, now)
            || Self::recent(self.recovering_at, now)
    }

    fn poll(&mut self, now: Instant) -> Phase {
        let (Phase::Waiting(_), Some(since)) = (self.phase, self.recovered_at) else {
            return self.phase;
        };
        self.abandon(now);
        let reachable = self.reachable(now);
        // Only intervals unreachable at both ends count; one ending a reachable stretch may
        // have had a peer reachable for most of it.
        if !reachable && self.polled_unreachable {
            let last = self.polled_at.unwrap_or(since);
            self.unreachable += now.saturating_duration_since(last);
        }
        self.polled_unreachable = !reachable;
        self.polled_at = Some(now);
        let in_grace = now.saturating_duration_since(since) < GRACE;
        // Every inbound link caught up, no peer still awaited to link back, and every
        // configured peer tried once, so the set is not just whichever peers linked first.
        let caught_up = !self.inbound.is_empty()
            && self.inbound.values().all(Progress::caught_up)
            && self.awaited(now).is_empty()
            && self.contacted(now, since);
        let ready = if self.force_standalone {
            Some(ReadyBy::Forced)
        } else if caught_up {
            Some(ReadyBy::CaughtUp)
        } else if reachable {
            None
        } else if self.last_standing {
            (!in_grace).then_some(ReadyBy::LastStanding)
        } else if self
            .sync_timeout
            .is_some_and(|timeout| self.unreachable >= timeout)
        {
            Some(ReadyBy::SyncTimeout)
        } else {
            None
        };
        self.phase = match ready {
            Some(by) => Phase::Ready(by),
            None => Phase::Waiting(self.reason(now, since, reachable, in_grace)),
        };
        if matches!(ready, Some(ReadyBy::SyncTimeout | ReadyBy::Forced)) {
            self.hold_links = true;
        }
        if ready.is_some() {
            self.log_links();
        }
        self.phase
    }

    fn reason(&self, now: Instant, since: Instant, reachable: bool, in_grace: bool) -> Waiting {
        if !reachable {
            if self.last_standing && in_grace {
                Waiting::GracePeriod
            } else {
                Waiting::NoPeerReachable
            }
        } else if self.inbound.values().any(|progress| !progress.caught_up()) {
            Waiting::CatchingUp
        } else if !self.awaited(now).is_empty() {
            Waiting::NoInboundLink
        } else if !self.inbound.is_empty() && !self.contacted(now, since) {
            Waiting::ContactingPeers
        } else if self.links.is_empty() && Self::recent(self.recovering_at, now) {
            Waiting::PeerRecovering
        } else {
            Waiting::NoInboundLink
        }
    }

    fn lines(&self, now: Instant) -> Vec<(String, String)> {
        let mut lines = Vec::new();
        let mut add = |name: &str, value: String| lines.push((name.to_string(), value));
        match self.phase {
            Phase::Recovering => add("state", "recovering".into()),
            Phase::Waiting(reason) => {
                add("state", "waiting".into());
                add("reason", reason.name().into());
            }
            Phase::Ready(by) => {
                add("state", "ready".into());
                add("ready_by", by.name().into());
            }
        }
        add("last_standing", u8::from(self.last_standing).to_string());
        let timeout = self.sync_timeout.map_or(0, |timeout| timeout.as_secs());
        add("sync_timeout", timeout.to_string());
        add("unreachable_secs", self.unreachable.as_secs().to_string());
        add("reachable", u8::from(self.reachable(now)).to_string());
        add("linked_peers", self.links.len().to_string());
        for (peer, progress) in &self.inbound {
            add(
                &format!("peer:node-{peer}:target"),
                progress.target.to_string(),
            );
            let applied = progress
                .applied
                .map_or("none".into(), |next| next.to_string());
            add(&format!("peer:node-{peer}:applied"), applied);
            let caught_up = u8::from(progress.caught_up()).to_string();
            add(&format!("peer:node-{peer}:caught_up"), caught_up);
        }
        lines
    }

    fn describe(&self) -> String {
        let reason = match self.phase {
            Phase::Waiting(reason) => reason,
            Phase::Recovering => return "loading local data".into(),
            Phase::Ready(by) => return format!("ready ({})", by.name()),
        };
        match reason {
            Waiting::GracePeriod => format!(
                "this node was the last one standing; trying its peers for {}s first",
                GRACE.as_secs()
            ),
            Waiting::NoPeerReachable => match self.sync_timeout {
                Some(timeout) => format!(
                    "no peer is reachable; serving with local data alone in {}s (--sync-timeout {})",
                    timeout.saturating_sub(self.unreachable).as_secs(),
                    timeout.as_secs()
                ),
                None => {
                    "no peer is reachable; waiting for one indefinitely (--sync-timeout 0)".into()
                }
            },
            Waiting::PeerRecovering => "a peer is still loading its data".into(),
            Waiting::NoInboundLink => {
                let awaited: Vec<String> = self
                    .outbound
                    .keys()
                    .filter(|peer| {
                        !self.inbound.contains_key(peer) && !self.abandoned.contains(peer)
                    })
                    .map(|peer| format!("node {peer}"))
                    .collect();
                if awaited.is_empty() {
                    "a peer is reachable but has not linked to this node yet".into()
                } else {
                    format!(
                        "{} is linked from this node but has not linked back yet",
                        awaited.join(", ")
                    )
                }
            }
            Waiting::CatchingUp => format!("catching up with {}", self.progress()),
            Waiting::ContactingPeers => format!(
                "caught up with {}, but {} of {} configured peers have not been tried yet",
                self.progress(),
                self.peers.saturating_sub(self.attempted.len()),
                self.peers
            ),
        }
    }

    /// Each inbound link's progress, as `node 2 (applied 10 of 50)`.
    fn progress(&self) -> String {
        let peers: Vec<String> = self
            .inbound
            .iter()
            .map(|(peer, progress)| match progress.applied {
                _ if progress.caught_up() => format!("node {peer} (caught up)"),
                Some(applied) => format!("node {peer} (applied {applied} of {})", progress.target),
                None => format!("node {peer} (starting)"),
            })
            .collect();
        peers.join(", ")
    }

    fn ready_line(&self, by: ReadyBy) -> String {
        match by {
            ReadyBy::CaughtUp => {
                let peers: Vec<String> = self
                    .inbound
                    .keys()
                    .map(|peer| format!("node {peer}"))
                    .collect();
                format!(
                    "ferrited: caught up with {}; serving clients",
                    peers.join(", ")
                )
            }
            ReadyBy::LastStanding => format!(
                "ferrited: this node was the last one standing and no peer answered within {}s; serving clients",
                GRACE.as_secs()
            ),
            ReadyBy::SyncTimeout => format!(
                "ferrited: warning: no peer was reachable for {}s (--sync-timeout); serving clients with local data, which may lack writes peers accepted while this node was down",
                self.sync_timeout.unwrap_or_default().as_secs()
            ),
            ReadyBy::Forced => {
                "ferrited: warning: --force-standalone: serving clients without waiting for peers"
                    .into()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(start: Instant, seconds: u64) -> Instant {
        start + Duration::from_secs(seconds)
    }

    fn millis(start: Instant, millis: u64) -> Instant {
        start + Duration::from_millis(millis)
    }

    /// A recovered node with no first attempts to wait for.
    fn recovered(
        last_standing: bool,
        timeout: Option<u64>,
        force: bool,
    ) -> (Arc<Readiness>, Arc<Oplog>, Instant) {
        recovered_with_peers(last_standing, timeout, force, 0)
    }

    fn recovered_with_peers(
        last_standing: bool,
        timeout: Option<u64>,
        force: bool,
        peers: usize,
    ) -> (Arc<Readiness>, Arc<Oplog>, Instant) {
        let readiness = Readiness::new(timeout.map(Duration::from_secs), force, peers);
        let oplog = Arc::new(Oplog::new(16, u64::MAX));
        let start = Instant::now();
        readiness.recovered(oplog.clone(), last_standing, start);
        (readiness, oplog, start)
    }

    fn line(readiness: &Readiness, now: Instant, name: &str) -> Option<String> {
        readiness
            .lines(now)
            .into_iter()
            .find(|(line, _)| line == name)
            .map(|(_, value)| value)
    }

    #[test]
    fn a_node_still_recovering_is_not_ready() {
        let readiness = Readiness::new(Some(Duration::from_secs(1)), true, 0);
        let now = Instant::now();
        assert_eq!(readiness.poll(secs(now, 100)), Phase::Recovering);
        assert_eq!(
            line(&readiness, now, "state").as_deref(),
            Some("recovering")
        );
    }

    #[test]
    fn force_standalone_is_ready_at_the_first_check() {
        let (readiness, _, start) = recovered(false, Some(60), true);
        assert_eq!(readiness.poll(start), Phase::Ready(ReadyBy::Forced));
    }

    #[test]
    fn a_node_is_ready_once_caught_up_with_every_linked_peer() {
        let (readiness, _, start) = recovered(false, Some(60), false);
        readiness.inbound_up(2, 100);
        readiness.inbound_up(3, 50);
        assert_eq!(readiness.poll(start), Phase::Waiting(Waiting::CatchingUp));
        readiness.applied(2, 100);
        readiness.applied(3, 49);
        assert_eq!(readiness.poll(start), Phase::Waiting(Waiting::CatchingUp));
        assert!(readiness.describe().contains("node 3 (applied 49 of 50)"));
        readiness.applied(3, 50);
        assert_eq!(readiness.poll(start), Phase::Ready(ReadyBy::CaughtUp));
        // Stays ready.
        readiness.inbound_down(2, start);
        readiness.inbound_down(3, start);
        assert_eq!(
            readiness.poll(secs(start, 1000)),
            Phase::Ready(ReadyBy::CaughtUp)
        );
    }

    #[test]
    fn a_peer_that_drops_while_catching_up_leaves_the_set() {
        let (readiness, _, start) = recovered(false, Some(60), false);
        readiness.inbound_up(2, 100);
        readiness.inbound_up(3, 100);
        readiness.applied(2, 100);
        assert_eq!(readiness.poll(start), Phase::Waiting(Waiting::CatchingUp));
        readiness.inbound_down(3, start);
        assert_eq!(readiness.poll(start), Phase::Ready(ReadyBy::CaughtUp));
    }

    #[test]
    fn the_set_must_hold_a_peer() {
        let (readiness, _, start) = recovered(false, Some(60), false);
        readiness.inbound_up(2, 100);
        readiness.inbound_down(2, start);
        assert_eq!(
            readiness.poll(secs(start, 1)),
            Phase::Waiting(Waiting::NoInboundLink)
        );
    }

    #[test]
    fn a_peer_linked_only_from_this_node_blocks_readiness_until_it_links_back() {
        let (readiness, _, start) = recovered(false, Some(60), false);
        readiness.inbound_up(3, 10);
        readiness.applied(3, 10);
        readiness.outbound_up(2, start);
        assert_eq!(
            readiness.poll(secs(start, 1)),
            Phase::Waiting(Waiting::NoInboundLink)
        );
        assert!(
            readiness
                .describe()
                .contains("node 2 is linked from this node")
        );
        readiness.inbound_up(2, 5);
        assert_eq!(
            readiness.poll(secs(start, 2)),
            Phase::Waiting(Waiting::CatchingUp)
        );
        readiness.applied(2, 5);
        assert_eq!(
            readiness.poll(secs(start, 2)),
            Phase::Ready(ReadyBy::CaughtUp)
        );
    }

    #[test]
    fn a_peer_that_never_links_back_is_waited_for_at_most_reachable_hold() {
        let (readiness, _, start) = recovered(false, Some(60), false);
        readiness.inbound_up(3, 10);
        readiness.applied(3, 10);
        readiness.outbound_up(2, start);
        assert_eq!(
            readiness.poll(start + REACHABLE_HOLD - Duration::from_millis(1)),
            Phase::Waiting(Waiting::NoInboundLink)
        );
        assert_eq!(
            readiness.poll(start + REACHABLE_HOLD),
            Phase::Ready(ReadyBy::CaughtUp)
        );
    }

    #[test]
    fn two_outbound_links_to_one_peer_end_its_entry_only_when_both_end() {
        let (readiness, _, start) = recovered(false, Some(60), false);
        readiness.inbound_up(3, 10);
        readiness.applied(3, 10);
        // Two configured addresses reach node 2.
        readiness.outbound_up(2, start);
        readiness.outbound_up(2, start);
        readiness.outbound_down(2, secs(start, 1));
        assert_eq!(
            readiness.poll(secs(start, 2)),
            Phase::Waiting(Waiting::NoInboundLink)
        );
        assert_eq!(
            line(&readiness, start, "linked_peers").as_deref(),
            Some("2")
        );
        readiness.outbound_down(2, secs(start, 2));
        assert_eq!(
            line(&readiness, start, "linked_peers").as_deref(),
            Some("1")
        );
        assert_eq!(
            readiness.poll(secs(start, 2)),
            Phase::Ready(ReadyBy::CaughtUp)
        );
    }

    #[test]
    fn a_peer_whose_link_back_ends_is_waited_for_again() {
        let (readiness, _, start) = recovered(false, Some(60), false);
        readiness.inbound_up(3, 10);
        readiness.applied(3, 10);
        readiness.outbound_up(2, start);
        readiness.inbound_up(2, 5);
        readiness.inbound_down(2, secs(start, 20));
        assert_eq!(
            readiness.poll(secs(start, 21)),
            Phase::Waiting(Waiting::NoInboundLink)
        );
        assert_eq!(
            readiness.poll(secs(start, 30)),
            Phase::Ready(ReadyBy::CaughtUp)
        );
    }

    #[test]
    fn a_peer_that_never_links_back_does_not_hold_off_the_sync_timeout() {
        let (readiness, _, start) = recovered(false, Some(5), false);
        readiness.outbound_up(2, start);
        assert_eq!(
            readiness.poll(millis(start, 9_900)),
            Phase::Waiting(Waiting::NoInboundLink)
        );
        // Node 2 stops counting as reachable; the timeout runs from this poll.
        assert_eq!(
            readiness.poll(secs(start, 10)),
            Phase::Waiting(Waiting::NoPeerReachable)
        );
        assert_eq!(
            readiness.poll(millis(start, 14_900)),
            Phase::Waiting(Waiting::NoPeerReachable)
        );
        assert_eq!(
            readiness.poll(secs(start, 15)),
            Phase::Ready(ReadyBy::SyncTimeout)
        );
    }

    #[test]
    fn unreachable_time_adds_up_across_a_reachable_stretch() {
        let (readiness, _, start) = recovered(false, Some(10), false);
        // Unreachable for 3 s.
        readiness.poll(secs(start, 1));
        assert_eq!(
            readiness.poll(secs(start, 3)),
            Phase::Waiting(Waiting::NoPeerReachable)
        );
        // Reachable from 3.5 s to 13.5 s.
        readiness.peer_recovering(millis(start, 3_500));
        assert_eq!(
            readiness.poll(millis(start, 3_500)),
            Phase::Waiting(Waiting::PeerRecovering)
        );
        assert_eq!(
            readiness.poll(millis(start, 13_499)),
            Phase::Waiting(Waiting::PeerRecovering)
        );
        // 7 s more makes 10.
        assert_eq!(
            readiness.poll(millis(start, 13_500)),
            Phase::Waiting(Waiting::NoPeerReachable)
        );
        assert_eq!(
            readiness.poll(millis(start, 20_499)),
            Phase::Waiting(Waiting::NoPeerReachable)
        );
        assert_eq!(
            readiness.poll(millis(start, 20_500)),
            Phase::Ready(ReadyBy::SyncTimeout)
        );
    }

    #[test]
    fn the_sync_timeout_counts_only_while_no_peer_is_reachable() {
        let (readiness, _, start) = recovered(false, Some(5), false);
        for second in 0..=9 {
            readiness.peer_recovering(secs(start, second));
            assert_eq!(
                readiness.poll(secs(start, second)),
                Phase::Waiting(Waiting::PeerRecovering)
            );
        }
        // Still reachable for `REACHABLE_HOLD` after it stops answering.
        assert_eq!(
            readiness.poll(secs(start, 18)),
            Phase::Waiting(Waiting::PeerRecovering)
        );
        assert_eq!(
            readiness.poll(secs(start, 19)),
            Phase::Waiting(Waiting::NoPeerReachable)
        );
        assert_eq!(
            readiness.poll(secs(start, 23)),
            Phase::Waiting(Waiting::NoPeerReachable)
        );
        assert!(readiness.describe().contains("in 1s (--sync-timeout 5)"));
        assert_eq!(
            readiness.poll(secs(start, 24)),
            Phase::Ready(ReadyBy::SyncTimeout)
        );
    }

    #[test]
    fn a_sync_timeout_of_none_waits_forever() {
        let (readiness, _, start) = recovered(false, None, false);
        assert_eq!(
            readiness.poll(secs(start, 1_000_000)),
            Phase::Waiting(Waiting::NoPeerReachable)
        );
        assert!(readiness.describe().contains("indefinitely"));
    }

    #[test]
    fn the_last_node_standing_serves_after_the_grace_period_when_no_peer_answers() {
        let (readiness, _, start) = recovered(true, Some(60), false);
        assert_eq!(readiness.poll(start), Phase::Waiting(Waiting::GracePeriod));
        assert_eq!(
            readiness.poll(start + GRACE - Duration::from_millis(1)),
            Phase::Waiting(Waiting::GracePeriod)
        );
        assert_eq!(
            readiness.poll(start + GRACE),
            Phase::Ready(ReadyBy::LastStanding)
        );
    }

    #[test]
    fn the_last_node_standing_catches_up_with_a_peer_that_answers() {
        let (readiness, _, start) = recovered(true, Some(60), false);
        readiness.outbound_up(2, start);
        assert_eq!(
            readiness.poll(secs(start, 3)),
            Phase::Waiting(Waiting::NoInboundLink)
        );
        readiness.inbound_up(2, 10);
        readiness.applied(2, 10);
        assert_eq!(
            readiness.poll(secs(start, 3)),
            Phase::Ready(ReadyBy::CaughtUp)
        );
    }

    #[test]
    fn link_records_are_logged_only_while_serving_and_not_once_stopping() {
        let (readiness, oplog, start) = recovered(false, None, true);
        readiness.inbound_up(2, 0);
        readiness.applied(2, 0);
        assert_eq!(oplog.take_links(), None, "not ready yet");
        assert_eq!(readiness.poll(start), Phase::Ready(ReadyBy::Forced));
        assert_eq!(oplog.take_links(), Some(vec![2]));
        readiness.outbound_up(2, start);
        assert_eq!(oplog.take_links(), None);
        readiness.inbound_up(3, 0);
        assert_eq!(oplog.take_links(), Some(vec![2, 3]));
        readiness.inbound_down(3, start);
        assert_eq!(oplog.take_links(), Some(vec![2]));
        readiness.outbound_down(2, start);
        assert_eq!(oplog.take_links(), None, "node 2 still links in");
        readiness.stopping();
        readiness.inbound_down(2, start);
        assert_eq!(oplog.take_links(), None, "a node's own stop is not logged");
    }

    #[test]
    fn a_node_ready_by_the_timeout_or_the_override_logs_nothing_until_it_links() {
        for (timeout, force) in [(Some(1), false), (None, true)] {
            let (readiness, oplog, start) = recovered(false, timeout, force);
            assert!(matches!(readiness.poll(secs(start, 2)), Phase::Ready(_)));
            assert_eq!(oplog.take_links(), None);
            // A peer linked only from this node does not end the hold.
            readiness.outbound_up(2, secs(start, 3));
            assert_eq!(oplog.take_links(), None);
            readiness.outbound_down(2, secs(start, 4));
            assert_eq!(oplog.take_links(), None);
            // Nor does an inbound link that ends before catching up.
            readiness.inbound_up(3, 10);
            readiness.applied(3, 4);
            assert_eq!(oplog.take_links(), None);
            readiness.inbound_down(3, secs(start, 5));
            assert_eq!(oplog.take_links(), None);
            readiness.inbound_up(3, 10);
            assert_eq!(oplog.take_links(), None);
            readiness.applied(3, 10);
            assert_eq!(oplog.take_links(), Some(vec![3]));
            readiness.inbound_down(3, secs(start, 6));
            assert_eq!(oplog.take_links(), Some(Vec::new()));
        }
    }

    #[test]
    fn a_peer_whose_link_keeps_reconnecting_without_linking_back_is_not_waited_for_forever() {
        let (readiness, _, start) = recovered(false, Some(5), false);
        // Node 2 never links back; the link to it flaps every second.
        let mut ready = None;
        for half in 0..120u64 {
            let now = millis(start, half * 500);
            match half % 4 {
                0 => readiness.outbound_up(2, now),
                2 => readiness.outbound_down(2, now),
                _ => {}
            }
            if let Phase::Ready(by) = readiness.poll(now) {
                ready = Some((by, half * 500));
                break;
            }
        }
        // At most 10 s waiting, 10 s reachable after the last drop, then the 5 s timeout.
        let (by, at) = ready.expect("never ready");
        assert_eq!(by, ReadyBy::SyncTimeout);
        assert!(at <= 25_000, "{at}");
    }

    #[test]
    fn a_link_from_a_peer_after_ready_is_logged() {
        let (readiness, oplog, start) = recovered(false, Some(60), false);
        readiness.inbound_up(2, 10);
        readiness.applied(2, 10);
        assert_eq!(readiness.poll(start), Phase::Ready(ReadyBy::CaughtUp));
        assert_eq!(oplog.take_links(), Some(vec![2]));
        readiness.inbound_up(3, 7);
        assert_eq!(oplog.take_links(), Some(vec![2, 3]));
    }

    #[test]
    fn progress_is_still_shown_after_ready() {
        let (readiness, _, start) = recovered(false, Some(60), false);
        readiness.inbound_up(2, 10);
        readiness.applied(2, 10);
        assert_eq!(readiness.poll(start), Phase::Ready(ReadyBy::CaughtUp));
        readiness.applied(2, 25);
        assert_eq!(
            line(&readiness, start, "peer:node-2:applied").as_deref(),
            Some("25")
        );
        readiness.inbound_up(3, 7);
        readiness.applied(3, 7);
        assert_eq!(
            line(&readiness, start, "peer:node-3:caught_up").as_deref(),
            Some("1")
        );
    }

    #[test]
    fn a_peer_that_relinks_is_caught_up_with_its_new_head() {
        let (readiness, _, start) = recovered(false, Some(60), false);
        readiness.inbound_up(2, 100);
        readiness.applied(2, 60);
        readiness.inbound_down(2, start);
        // The new link's later head is the target.
        readiness.inbound_up(2, 150);
        readiness.applied(2, 100);
        assert_eq!(
            readiness.poll(secs(start, 1)),
            Phase::Waiting(Waiting::CatchingUp)
        );
        assert!(readiness.describe().contains("node 2 (applied 100 of 150)"));
        readiness.applied(2, 150);
        assert_eq!(
            readiness.poll(secs(start, 1)),
            Phase::Ready(ReadyBy::CaughtUp)
        );
    }

    #[test]
    fn a_last_standing_node_whose_peer_went_silent_while_recovering_serves_as_last_standing() {
        let (readiness, _, start) = recovered(true, Some(1), false);
        readiness.peer_recovering(start);
        assert_eq!(
            readiness.poll(secs(start, 3)),
            Phase::Waiting(Waiting::PeerRecovering)
        );
        assert_eq!(
            readiness.poll(start + REACHABLE_HOLD - Duration::from_millis(1)),
            Phase::Waiting(Waiting::PeerRecovering)
        );
        assert_eq!(
            readiness.poll(start + REACHABLE_HOLD),
            Phase::Ready(ReadyBy::LastStanding)
        );
    }

    #[test]
    fn a_node_is_not_caught_up_until_every_sender_has_tried_its_peer() {
        let (readiness, _, start) = recovered_with_peers(false, Some(60), false, 2);
        // The second sender is still connecting.
        readiness.outbound_up(2, start);
        readiness.attempted(0);
        readiness.inbound_up(2, 10);
        readiness.applied(2, 10);
        assert_eq!(
            readiness.poll(secs(start, 1)),
            Phase::Waiting(Waiting::ContactingPeers)
        );
        assert!(
            readiness.describe().contains("1 of 2 configured peers"),
            "{}",
            readiness.describe()
        );
        readiness.attempted(1);
        assert_eq!(
            readiness.poll(secs(start, 1)),
            Phase::Ready(ReadyBy::CaughtUp)
        );
    }

    #[test]
    fn a_senders_first_attempt_is_waited_for_at_most_first_attempt() {
        let (readiness, _, start) = recovered_with_peers(false, Some(60), false, 2);
        readiness.attempted(0);
        readiness.attempted(0);
        readiness.inbound_up(2, 10);
        readiness.applied(2, 10);
        assert_eq!(
            readiness.poll(start + FIRST_ATTEMPT - Duration::from_millis(1)),
            Phase::Waiting(Waiting::ContactingPeers)
        );
        assert_eq!(
            readiness.poll(start + FIRST_ATTEMPT),
            Phase::Ready(ReadyBy::CaughtUp)
        );
    }

    #[test]
    fn recovery_is_noted_once() {
        let (readiness, oplog, start) = recovered(false, Some(60), true);
        assert_eq!(readiness.poll(start), Phase::Ready(ReadyBy::Forced));
        readiness.recovered(oplog, true, secs(start, 1));
        assert_eq!(
            readiness.poll(secs(start, 1)),
            Phase::Ready(ReadyBy::Forced)
        );
        assert_eq!(
            line(&readiness, start, "last_standing").as_deref(),
            Some("0")
        );
    }

    #[test]
    fn status_lines_show_the_state_and_each_peers_progress() {
        let (readiness, _, start) = recovered(false, Some(60), false);
        readiness.inbound_up(2, 100);
        readiness.applied(2, 40);
        readiness.poll(start);
        let lines = readiness.lines(start);
        let expected: Vec<(String, String)> = [
            ("state", "waiting"),
            ("reason", "catching_up"),
            ("last_standing", "0"),
            ("sync_timeout", "60"),
            ("unreachable_secs", "0"),
            ("reachable", "1"),
            ("linked_peers", "1"),
            ("peer:node-2:target", "100"),
            ("peer:node-2:applied", "40"),
            ("peer:node-2:caught_up", "0"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
        assert_eq!(lines, expected);
    }
}
