//! Replication counters and link states, for `stats` and `stats repl`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::backlog::Backlog;
use crate::oplog::Oplog;
use crate::store::Store;

/// A full sync starting this soon after the last one to the same peer finished is a repeat: a
/// sign the backlog cannot hold what is written during one.
const REPEAT_WINDOW: Duration = Duration::from_secs(60);

/// The totals `stats` reports, in order.
pub const STAT_NAMES: [&str; 14] = [
    "repl_peers",
    "repl_records_sent",
    "repl_records_applied",
    "repl_records_stale",
    "repl_records_oversized",
    "repl_full_syncs_sent",
    "repl_full_syncs_received",
    "repl_backlog_bytes",
    "repl_tombstones",
    "repl_tombstone_bytes",
    "repl_tombstones_evicted",
    "repl_positions_dropped",
    "repl_sync_timeouts",
    "repl_bad_frames",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkState {
    Down,
    Syncing,
    Streaming,
}

impl LinkState {
    pub const ALL: [LinkState; 3] = [LinkState::Down, LinkState::Syncing, LinkState::Streaming];

    pub fn name(self) -> &'static str {
        match self {
            LinkState::Down => "down",
            LinkState::Syncing => "syncing",
            LinkState::Streaming => "streaming",
        }
    }
}

/// A configured peer, as its sender sees it.
struct Outbound {
    address: String,
    /// Learned in the first successful handshake.
    node_id: Option<u8>,
    state: LinkState,
    /// The peer's last acknowledged position on this node's log.
    acked: u64,
    /// When the last full sync to the peer finished.
    synced_at: Option<Instant>,
    /// Repeat full syncs in a row.
    repeated_syncs: u64,
    /// Repeat full syncs in all.
    repeat_syncs: u64,
}

pub struct Status {
    backlog: Arc<Backlog>,
    oplog: Arc<Oplog>,
    store: Arc<Store>,
    pub records_sent: AtomicU64,
    pub records_applied: AtomicU64,
    /// Received but not newer than what this node held, so not applied.
    pub records_stale: AtomicU64,
    /// Newer puts too large for the memory limit, applied as deletes.
    pub records_oversized: AtomicU64,
    pub full_syncs_sent: AtomicU64,
    pub full_syncs_received: AtomicU64,
    /// 1 once the node began serving because no peer was reachable for `--sync-timeout`.
    pub sync_timeouts: AtomicU64,
    /// Links dropped because a frame failed authentication, in either direction.
    pub bad_frames: AtomicU64,
    outbound: Mutex<Vec<Outbound>>,
    /// The state of each live inbound link, by the sender's node ID.
    inbound: Mutex<BTreeMap<u8, LinkState>>,
}

impl Status {
    pub fn new(
        backlog: Arc<Backlog>,
        oplog: Arc<Oplog>,
        store: Arc<Store>,
        peers: &[String],
    ) -> Self {
        let outbound = peers
            .iter()
            .map(|address| Outbound {
                address: address.clone(),
                node_id: None,
                state: LinkState::Down,
                acked: 0,
                synced_at: None,
                repeated_syncs: 0,
                repeat_syncs: 0,
            })
            .collect();
        Self {
            backlog,
            oplog,
            store,
            records_sent: AtomicU64::new(0),
            records_applied: AtomicU64::new(0),
            records_stale: AtomicU64::new(0),
            records_oversized: AtomicU64::new(0),
            full_syncs_sent: AtomicU64::new(0),
            full_syncs_received: AtomicU64::new(0),
            sync_timeouts: AtomicU64::new(0),
            bad_frames: AtomicU64::new(0),
            outbound: Mutex::new(outbound),
            inbound: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn set_outbound(&self, index: usize, state: LinkState) {
        self.outbound.lock()[index].state = state;
    }

    pub(crate) fn set_outbound_peer(&self, index: usize, node_id: u8) {
        self.outbound.lock()[index].node_id = Some(node_id);
    }

    pub(crate) fn set_acked(&self, index: usize, next: u64) {
        self.outbound.lock()[index].acked = next;
    }

    /// Notes that a full sync to peer `index` is starting. For a repeat, returns how long ago
    /// the last one finished and how many repeats in a row, this one included.
    pub(crate) fn sync_started(&self, index: usize) -> Option<(Duration, u64)> {
        let mut outbound = self.outbound.lock();
        let peer = &mut outbound[index];
        match peer.synced_at.map(|at| at.elapsed()) {
            Some(since) if since < REPEAT_WINDOW => {
                peer.repeated_syncs += 1;
                peer.repeat_syncs += 1;
                Some((since, peer.repeated_syncs))
            }
            _ => {
                peer.repeated_syncs = 0;
                None
            }
        }
    }

    pub(crate) fn sync_finished(&self, index: usize) {
        self.outbound.lock()[index].synced_at = Some(Instant::now());
    }

    pub(crate) fn set_inbound(&self, node_id: u8, state: LinkState) {
        let mut inbound = self.inbound.lock();
        if state == LinkState::Down {
            inbound.remove(&node_id);
        } else {
            inbound.insert(node_id, state);
        }
    }

    /// The totals `stats` reports.
    pub fn entries(&self) -> Vec<(&'static str, u64)> {
        let outbound = self
            .outbound
            .lock()
            .iter()
            .filter(|peer| peer.state != LinkState::Down)
            .count();
        let up = (outbound + self.inbound.lock().len()) as u64;
        let values = [
            up,
            self.records_sent.load(Relaxed),
            self.records_applied.load(Relaxed),
            self.records_stale.load(Relaxed),
            self.records_oversized.load(Relaxed),
            self.full_syncs_sent.load(Relaxed),
            self.full_syncs_received.load(Relaxed),
            self.backlog.bytes(),
            self.store.tombstones(),
            self.store.tombstone_bytes(),
            self.store.tombstones_evicted(),
            self.oplog.positions_dropped(),
            self.sync_timeouts.load(Relaxed),
            self.bad_frames.load(Relaxed),
        ];
        STAT_NAMES.into_iter().zip(values).collect()
    }

    /// Each configured peer's links, then inbound links from unconfigured nodes.
    pub fn peers(&self) -> Vec<PeerView> {
        let head = self.backlog.head();
        let inbound = self.inbound.lock();
        let mut shown = BTreeSet::new();
        let mut views = Vec::new();
        for peer in self.outbound.lock().iter() {
            let incoming = peer
                .node_id
                .and_then(|id| inbound.get(&id).copied())
                .unwrap_or(LinkState::Down);
            shown.extend(peer.node_id);
            views.push(PeerView {
                name: peer.address.clone(),
                configured: Some(Configured {
                    node_id: peer.node_id,
                    out: peer.state,
                    acked: peer.acked,
                    lag: head.saturating_sub(peer.acked),
                    repeat_syncs: peer.repeat_syncs,
                }),
                incoming,
            });
        }
        for (id, state) in inbound.iter().filter(|(id, _)| !shown.contains(*id)) {
            views.push(PeerView {
                name: format!("node-{id}"),
                configured: None,
                incoming: *state,
            });
        }
        views
    }

    /// The `stats repl` lines, as (name, value), in `peers` order.
    pub fn peer_lines(&self) -> Vec<(String, String)> {
        let mut lines = Vec::new();
        for peer in self.peers() {
            let name = &peer.name;
            let Some(configured) = &peer.configured else {
                lines.push((format!("peer:{name}:in"), peer.incoming.name().into()));
                continue;
            };
            let id = configured
                .node_id
                .map_or("unknown".into(), |id| id.to_string());
            lines.push((format!("peer:{name}:id"), id));
            lines.push((format!("peer:{name}:out"), configured.out.name().into()));
            lines.push((format!("peer:{name}:in"), peer.incoming.name().into()));
            lines.push((format!("peer:{name}:acked"), configured.acked.to_string()));
            lines.push((format!("peer:{name}:lag"), configured.lag.to_string()));
            let repeats = configured.repeat_syncs.to_string();
            lines.push((format!("peer:{name}:repeat_syncs"), repeats));
        }
        lines
    }
}

/// One peer's links, as `Status::peers` reports them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerView {
    /// The configured address, or `node-<id>` for an unconfigured node.
    pub name: String,
    /// `None` for an unconfigured node.
    pub configured: Option<Configured>,
    pub incoming: LinkState,
}

/// A configured peer, as its sender sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Configured {
    /// Learned in the first successful handshake.
    pub node_id: Option<u8>,
    pub out: LinkState,
    /// The peer's last acknowledged position on this node's log.
    pub acked: u64,
    /// Entries the peer has not acknowledged.
    pub lag: u64,
    /// Repeat full syncs to the peer (see `REPEAT_WINDOW`).
    pub repeat_syncs: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::Clock;
    use crate::store::StoreConfig;

    /// A status for one configured peer, with a backlog whose head is 10.
    fn status() -> Status {
        let backlog = Arc::new(Backlog::new(1 << 20, 10, Vec::new()));
        let oplog = Arc::new(Oplog::new(16, u64::MAX));
        let config = StoreConfig {
            memory_limit: 1 << 20,
            item_max: 1 << 10,
            shards: 2,
        };
        let store = Arc::new(Store::new(config, Arc::new(Clock::new(1))));
        Status::new(backlog, oplog, store, &["10.0.0.2:7000".to_string()])
    }

    #[test]
    fn status_describes_each_peer_and_unknown_inbound_links() {
        let status = status();
        status.set_outbound_peer(0, 2);
        status.set_outbound(0, LinkState::Streaming);
        status.set_acked(0, 7);
        status.set_inbound(2, LinkState::Syncing);
        status.set_inbound(9, LinkState::Streaming);
        let expected: Vec<(String, String)> = [
            ("peer:10.0.0.2:7000:id", "2"),
            ("peer:10.0.0.2:7000:out", "streaming"),
            ("peer:10.0.0.2:7000:in", "syncing"),
            ("peer:10.0.0.2:7000:acked", "7"),
            ("peer:10.0.0.2:7000:lag", "3"),
            ("peer:10.0.0.2:7000:repeat_syncs", "0"),
            ("peer:node-9:in", "streaming"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
        assert_eq!(status.peer_lines(), expected);
        assert_eq!(status.entries()[0], ("repl_peers", 3));
        status.set_inbound(9, LinkState::Down);
        assert_eq!(status.entries()[0], ("repl_peers", 2));
    }

    #[test]
    fn totals_count_tombstones_dropped_positions_and_oversized_records() {
        let status = status();
        status.store.enable_tombstones(3600, usize::MAX);
        status.store.apply(
            &crate::oplog::Record::Delete {
                key: bytes::Bytes::from_static(b"gone"),
                version: Clock::new(2).next(),
            },
            crate::clock::unix_secs(),
        );
        for next in 0..20 {
            status.oplog.append_position(crate::oplog::Position {
                peer: 2,
                run: 1,
                next,
            });
        }
        status.records_oversized.fetch_add(3, Relaxed);
        let entries: BTreeMap<_, _> = status.entries().into_iter().collect();
        assert_eq!(entries.len(), STAT_NAMES.len());
        assert_eq!(entries["repl_tombstones"], 1);
        assert_eq!(
            entries["repl_tombstone_bytes"],
            crate::store::cost(4, 0) as u64
        );
        assert_eq!(entries["repl_tombstones_evicted"], 0);
        assert_eq!(entries["repl_positions_dropped"], 4);
        assert_eq!(entries["repl_records_oversized"], 3);
        assert_eq!(entries["repl_records_stale"], 0);
        assert_eq!(entries["repl_sync_timeouts"], 0);
        assert_eq!(entries["repl_bad_frames"], 0);
    }

    #[test]
    fn a_full_sync_soon_after_the_last_one_is_counted_as_a_repeat() {
        let status = status();
        assert_eq!(status.sync_started(0), None);
        status.sync_finished(0);
        let (since, repeated) = status.sync_started(0).unwrap();
        assert!(since < REPEAT_WINDOW);
        assert_eq!(repeated, 1);
        status.sync_finished(0);
        assert_eq!(
            status.sync_started(0).map(|(_, repeated)| repeated),
            Some(2)
        );
        // One long after the last is not a repeat and resets the streak.
        status.outbound.lock()[0].synced_at = Instant::now().checked_sub(REPEAT_WINDOW);
        assert_eq!(status.sync_started(0), None);
        status.sync_finished(0);
        assert_eq!(
            status.sync_started(0).map(|(_, repeated)| repeated),
            Some(1)
        );
        let repeats = status
            .peer_lines()
            .into_iter()
            .find(|(name, _)| name.ends_with(":repeat_syncs"));
        assert_eq!(repeats.unwrap().1, "3");
    }
}
