//! Renders the node's metrics in the Prometheus text exposition format (version 0.0.4). Every
//! plain `stats` value maps through `STATS` or `FOLDED`; a test fails if one is unmapped.

use std::fmt::Write;
use std::time::Instant;

use crate::replicate::{Configured, LinkState, PeerView, Phase, ReadyBy, Snapshot, Waiting};
use crate::server::{Context, StatValue, general_stats};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Counter,
    Gauge,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
        }
    }
}

/// How one `stats` value becomes a metric.
#[derive(Debug)]
pub(crate) struct Mapping {
    pub stat: &'static str,
    pub metric: &'static str,
    pub kind: Kind,
    pub help: &'static str,
    /// Converts the stat's value to the metric's base unit.
    pub divisor: u64,
}

const fn counter(stat: &'static str, metric: &'static str, help: &'static str) -> Mapping {
    Mapping {
        stat,
        metric,
        kind: Kind::Counter,
        help,
        divisor: 1,
    }
}

const fn gauge(stat: &'static str, metric: &'static str, help: &'static str) -> Mapping {
    Mapping {
        stat,
        metric,
        kind: Kind::Gauge,
        help,
        divisor: 1,
    }
}

/// A gauge whose stat is in bits, as bytes.
const fn gauge_bits_as_bytes(
    stat: &'static str,
    metric: &'static str,
    help: &'static str,
) -> Mapping {
    Mapping {
        divisor: 8,
        ..gauge(stat, metric, help)
    }
}

/// The family that carries the version.
const BUILD_INFO: &str = "ferrited_build_info";

/// Every plain `stats` value as a metric. `stats reset` zeroes some counters, which `rate`
/// treats as a counter reset.
pub(crate) const STATS: &[Mapping] = &[
    gauge("pid", "ferrited_process_id", "Process ID (stats: pid)."),
    gauge(
        "uptime",
        "ferrited_uptime_seconds",
        "Seconds since the process started (stats: uptime).",
    ),
    gauge(
        "time",
        "ferrited_time_seconds",
        "The node's clock, in seconds since the Unix epoch (stats: time).",
    ),
    // Bytes, Prometheus's base unit, rather than the stat's bits.
    gauge_bits_as_bytes(
        "pointer_size",
        "ferrited_pointer_size_bytes",
        "Size of a pointer, in bytes; the stat reports bits (stats: pointer_size).",
    ),
    gauge(
        "max_connections",
        "ferrited_max_connections",
        "Most client connections served at once, -c as fitted to the descriptor limit (stats: max_connections).",
    ),
    gauge(
        "curr_connections",
        "ferrited_curr_connections",
        "Open client connections (stats: curr_connections).",
    ),
    counter(
        "total_connections",
        "ferrited_connections_total",
        "Client connections accepted (stats: total_connections).",
    ),
    counter(
        "rejected_connections",
        "ferrited_rejected_connections_total",
        "Client connections closed at once because the node was at its connection limit (stats: rejected_connections).",
    ),
    counter(
        "cmd_get",
        "ferrited_cmd_get_total",
        "Keys fetched (stats: cmd_get).",
    ),
    counter(
        "cmd_set",
        "ferrited_cmd_set_total",
        "Store commands (stats: cmd_set).",
    ),
    counter(
        "cmd_flush",
        "ferrited_cmd_flush_total",
        "flush_all commands (stats: cmd_flush).",
    ),
    counter(
        "cmd_touch",
        "ferrited_cmd_touch_total",
        "Touches (stats: cmd_touch).",
    ),
    counter(
        "get_hits",
        "ferrited_get_hits_total",
        "Keys fetched and found (stats: get_hits).",
    ),
    counter(
        "get_misses",
        "ferrited_get_misses_total",
        "Keys fetched and not found (stats: get_misses).",
    ),
    counter(
        "delete_misses",
        "ferrited_delete_misses_total",
        "Deletes of keys not found (stats: delete_misses).",
    ),
    counter(
        "delete_hits",
        "ferrited_delete_hits_total",
        "Deletes of keys found (stats: delete_hits).",
    ),
    counter(
        "incr_misses",
        "ferrited_incr_misses_total",
        "Increments of keys not found (stats: incr_misses).",
    ),
    counter(
        "incr_hits",
        "ferrited_incr_hits_total",
        "Increments of keys found (stats: incr_hits).",
    ),
    counter(
        "decr_misses",
        "ferrited_decr_misses_total",
        "Decrements of keys not found (stats: decr_misses).",
    ),
    counter(
        "decr_hits",
        "ferrited_decr_hits_total",
        "Decrements of keys found (stats: decr_hits).",
    ),
    counter(
        "cas_misses",
        "ferrited_cas_misses_total",
        "Compare-and-swaps of keys not found (stats: cas_misses).",
    ),
    counter(
        "cas_hits",
        "ferrited_cas_hits_total",
        "Compare-and-swaps that stored (stats: cas_hits).",
    ),
    counter(
        "cas_badval",
        "ferrited_cas_badval_total",
        "Compare-and-swaps refused because the item had changed (stats: cas_badval).",
    ),
    counter(
        "touch_hits",
        "ferrited_touch_hits_total",
        "Touches of keys found (stats: touch_hits).",
    ),
    counter(
        "touch_misses",
        "ferrited_touch_misses_total",
        "Touches of keys not found (stats: touch_misses).",
    ),
    counter(
        "bytes_read",
        "ferrited_read_bytes_total",
        "Bytes read from clients (stats: bytes_read).",
    ),
    counter(
        "bytes_written",
        "ferrited_written_bytes_total",
        "Bytes written to clients (stats: bytes_written).",
    ),
    gauge(
        "threads",
        "ferrited_threads",
        "Event loop threads serving clients (stats: threads).",
    ),
    gauge(
        "limit_maxbytes",
        "ferrited_memory_limit_bytes",
        "The memory limit, -m (stats: limit_maxbytes).",
    ),
    gauge(
        "bytes",
        "ferrited_memory_used_bytes",
        "Memory items use, and with peers tombstones (stats: bytes).",
    ),
    gauge(
        "curr_items",
        "ferrited_curr_items",
        "Items stored (stats: curr_items).",
    ),
    counter(
        "total_items",
        "ferrited_items_total",
        "Items stored since the start (stats: total_items).",
    ),
    counter(
        "evictions",
        "ferrited_evictions_total",
        "Items evicted for memory (stats: evictions).",
    ),
    counter(
        "reclaimed",
        "ferrited_reclaimed_total",
        "Expired items whose memory was reused (stats: reclaimed).",
    ),
    gauge(
        "persist_enabled",
        "ferrited_persist_enabled",
        "1 if persistence is on (stats: persist_enabled).",
    ),
    gauge(
        "persist_degraded",
        "ferrited_persist_degraded",
        "1 while the disk is failing and changes are not being saved (stats: persist_degraded).",
    ),
    counter(
        "persist_records",
        "ferrited_persist_records_total",
        "Changes written to the log (stats: persist_records).",
    ),
    counter(
        "persist_bytes",
        "ferrited_persist_written_bytes_total",
        "Bytes written to the log (stats: persist_bytes).",
    ),
    counter(
        "persist_fsyncs",
        "ferrited_persist_fsyncs_total",
        "Log fsyncs (stats: persist_fsyncs).",
    ),
    counter(
        "persist_errors",
        "ferrited_persist_errors_total",
        "Disk errors (stats: persist_errors).",
    ),
    counter(
        "persist_dropped",
        "ferrited_persist_dropped_total",
        "Changes the oplog had no room for (stats: persist_dropped).",
    ),
    counter(
        "persist_lost",
        "ferrited_persist_lost_total",
        "Changes discarded while degraded (stats: persist_lost).",
    ),
    counter(
        "persist_snapshots",
        "ferrited_persist_snapshots_total",
        "Snapshots written (stats: persist_snapshots).",
    ),
    gauge(
        "persist_snapshot_items",
        "ferrited_persist_snapshot_items",
        "Items in the last snapshot written (stats: persist_snapshot_items).",
    ),
    gauge(
        "persist_log_bytes",
        "ferrited_persist_log_bytes",
        "Bytes of log since the last snapshot (stats: persist_log_bytes).",
    ),
    gauge(
        "repl_peers",
        "ferrited_repl_links_up",
        "Replication links up, each direction counted (stats: repl_peers).",
    ),
    counter(
        "repl_records_sent",
        "ferrited_repl_records_sent_total",
        "Changes sent to peers (stats: repl_records_sent).",
    ),
    counter(
        "repl_records_applied",
        "ferrited_repl_records_applied_total",
        "Changes received from peers and applied (stats: repl_records_applied).",
    ),
    counter(
        "repl_records_stale",
        "ferrited_repl_records_stale_total",
        "Changes received that were not newer than what the node held (stats: repl_records_stale).",
    ),
    counter(
        "repl_records_oversized",
        "ferrited_repl_records_oversized_total",
        "Newer puts received too large for -m, applied as deletes (stats: repl_records_oversized).",
    ),
    counter(
        "repl_full_syncs_sent",
        "ferrited_repl_full_syncs_sent_total",
        "Full syncs sent to peers (stats: repl_full_syncs_sent).",
    ),
    counter(
        "repl_full_syncs_received",
        "ferrited_repl_full_syncs_received_total",
        "Full syncs received from peers (stats: repl_full_syncs_received).",
    ),
    gauge(
        "repl_backlog_bytes",
        "ferrited_repl_backlog_bytes",
        "Bytes of recent changes held for peers (stats: repl_backlog_bytes).",
    ),
    gauge(
        "repl_tombstones",
        "ferrited_repl_tombstones",
        "Tombstones held, expired ones not yet swept included (stats: repl_tombstones).",
    ),
    gauge(
        "repl_tombstone_bytes",
        "ferrited_repl_tombstone_bytes",
        "Memory charged to tombstones (stats: repl_tombstone_bytes).",
    ),
    counter(
        "repl_tombstones_evicted",
        "ferrited_repl_tombstones_evicted_total",
        "Tombstones evicted before they expired, to stay within --tombstone-memory (stats: repl_tombstones_evicted).",
    ),
    counter(
        "repl_positions_dropped",
        "ferrited_repl_positions_dropped_total",
        "Positions on peers' logs the full oplog had no room for; each is logged again later (stats: repl_positions_dropped).",
    ),
    gauge(
        "repl_sync_timeouts",
        "ferrited_repl_sync_timeouts",
        "1 if the node began serving because no peer was reachable for --sync-timeout (stats: repl_sync_timeouts).",
    ),
    counter(
        "repl_bad_frames",
        "ferrited_repl_bad_frames_total",
        "Links dropped because a frame failed authentication (stats: repl_bad_frames).",
    ),
];

/// `stats` values reported as a label of another metric: (stat, metric).
pub(crate) const FOLDED: &[(&str, &str)] = &[("version", BUILD_INFO)];

/// The node's states, one series each.
const STATES: [&str; 4] = ["recovering", "waiting", "ready", "stopping"];

/// Directions of a link, one series each.
const DIRECTIONS: [&str; 2] = ["out", "in"];

pub(crate) fn render(ctx: &Context) -> String {
    let mut out = Exposition::default();
    let stats = general_stats(ctx, (ctx.wall_clock)());
    let version = stats.iter().find_map(|(name, value)| match value {
        StatValue::Text(text) if FOLDED.contains(&(*name, BUILD_INFO)) => Some(*text),
        _ => None,
    });
    out.family(
        BUILD_INFO,
        Kind::Gauge,
        "Always 1; the labels name the build: version, and stats_version as stats reports it (stats: version).",
    );
    out.sample(
        BUILD_INFO,
        &[
            ("version", env!("CARGO_PKG_VERSION")),
            ("stats_version", version.unwrap_or_default()),
        ],
        1u64,
    );
    readiness(ctx, &mut out);
    for (name, value) in &stats {
        let StatValue::Number(value) = value else {
            continue;
        };
        // A value without a mapping is left out here; the tests catch it.
        let Some(mapping) = STATS.iter().find(|mapping| mapping.stat == *name) else {
            continue;
        };
        out.family(mapping.metric, mapping.kind, mapping.help);
        out.sample(mapping.metric, &[], *value / mapping.divisor);
    }
    if let Some(repl) = ctx.repl.get() {
        peers(&repl.peers(), &mut out);
    }
    out.text
}

/// The node's state and, with peers, what it is waiting for.
fn readiness(ctx: &Context, out: &mut Exposition) {
    let snapshot = ctx
        .readiness
        .get()
        .map(|readiness| readiness.snapshot(Instant::now()));
    let phase = snapshot.map(|snapshot| snapshot.phase);
    let state = if ctx.stopping() {
        "stopping"
    } else if ctx.serving() {
        "ready"
    } else {
        match phase {
            // Ready but not yet serving: only until the client port is bound.
            Some(Phase::Waiting(_) | Phase::Ready(_)) => "waiting",
            Some(Phase::Recovering) | None => "recovering",
        }
    };
    out.family(
        "ferrited_ready",
        Kind::Gauge,
        "1 while the node serves clients; 0 while it loads its data, waits for its peers or stops.",
    );
    out.sample("ferrited_ready", &[], u64::from(state == "ready"));
    out.family(
        "ferrited_state",
        Kind::Gauge,
        "1 for the node's current state: recovering (loading local data), waiting (for peers), ready (serving clients) or stopping.",
    );
    for each in STATES {
        out.sample(
            "ferrited_state",
            &[("state", each)],
            u64::from(each == state),
        );
    }
    let Some(snapshot) = snapshot else {
        return;
    };
    peer_readiness(&snapshot, state == "waiting", out);
}

/// What a node with peers waits for, as its replication port's status command reports it.
fn peer_readiness(snapshot: &Snapshot, waiting: bool, out: &mut Exposition) {
    let reason = match snapshot.phase {
        Phase::Waiting(reason) if waiting => Some(reason),
        _ => None,
    };
    out.family(
        "ferrited_waiting_reason",
        Kind::Gauge,
        "1 for why a node with peers is not serving yet, while it waits (status: reason).",
    );
    for each in Waiting::ALL {
        let value = u64::from(reason == Some(each));
        out.sample("ferrited_waiting_reason", &[("reason", each.name())], value);
    }
    let by = match snapshot.phase {
        Phase::Ready(by) => Some(by),
        _ => None,
    };
    out.family(
        "ferrited_ready_by",
        Kind::Gauge,
        "1 for how a node with peers became ready, once it has (status: ready_by).",
    );
    for each in ReadyBy::ALL {
        let value = u64::from(by == Some(each));
        out.sample("ferrited_ready_by", &[("by", each.name())], value);
    }
    let gauges: [(&str, &str, u64); 5] = [
        (
            "ferrited_last_standing",
            "1 if the recovered log shows this node was the last one standing (status: last_standing).",
            u64::from(snapshot.last_standing),
        ),
        (
            "ferrited_sync_timeout_seconds",
            "--sync-timeout; 0 waits forever (status: sync_timeout).",
            snapshot.sync_timeout.map_or(0, |timeout| timeout.as_secs()),
        ),
        (
            "ferrited_unreachable_seconds",
            "Seconds since recovery during which no peer was reachable, as of the last readiness check (status: unreachable_secs).",
            snapshot.unreachable.as_secs(),
        ),
        (
            "ferrited_peer_reachable",
            "1 if a peer counts as reachable: linked, or answering that it is loading its data, now or within the last 10 seconds (status: reachable).",
            u64::from(snapshot.reachable),
        ),
        (
            "ferrited_linked_peers",
            "Peers with a live link in either direction (status: linked_peers).",
            snapshot.linked_peers as u64,
        ),
    ];
    for (name, help, value) in gauges {
        out.family(name, Kind::Gauge, help);
        out.sample(name, &[], value);
    }
}

/// Each peer's links, as `stats repl` reports them, labeled with the peer's configured address
/// (or `node-<id>` for an inbound link from a node not configured).
fn peers(peers: &[PeerView], out: &mut Exposition) {
    const STATE: &str = "ferrited_repl_link_state";
    out.family(
        STATE,
        Kind::Gauge,
        "1 for the state of each link: down, syncing or streaming; direction out is this node's link to the peer, in the peer's to this node (stats repl: :out, :in).",
    );
    for peer in peers {
        let states = [peer.configured.as_ref().map(|c| c.out), Some(peer.incoming)];
        for (direction, current) in DIRECTIONS.into_iter().zip(states) {
            let Some(current) = current else {
                continue;
            };
            for state in LinkState::ALL {
                let labels = [
                    ("peer", peer.name.as_str()),
                    ("direction", direction),
                    ("state", state.name()),
                ];
                out.sample(STATE, &labels, u64::from(state == current));
            }
        }
    }
    let configured: Vec<_> = peers
        .iter()
        .filter_map(|peer| Some((peer.name.as_str(), peer.configured.as_ref()?)))
        .collect();
    const NODE_ID: &str = "ferrited_repl_peer_node_id";
    out.family(
        NODE_ID,
        Kind::Gauge,
        "A configured peer's node ID, once a handshake has told it (stats repl: :id).",
    );
    for (name, peer) in &configured {
        if let Some(id) = peer.node_id {
            out.sample(NODE_ID, &[("peer", name)], id);
        }
    }
    type Read = fn(&Configured) -> u64;
    let per_peer: [(&str, Kind, &str, Read); 3] = [
        (
            "ferrited_repl_peer_acked_position",
            Kind::Gauge,
            "The last position the peer acknowledged on this node's log (stats repl: :acked).",
            |peer| peer.acked,
        ),
        (
            "ferrited_repl_peer_lag_entries",
            Kind::Gauge,
            "Entries of this node's log the peer has not acknowledged (stats repl: :lag).",
            |peer| peer.lag,
        ),
        (
            "ferrited_repl_peer_repeat_syncs_total",
            Kind::Counter,
            "Full syncs to the peer that started within a minute of the last one finishing; above 0, --repl-backlog is too small (stats repl: :repeat_syncs).",
            |peer| peer.repeat_syncs,
        ),
    ];
    for (name, kind, help, value) in per_peer {
        out.family(name, kind, help);
        for (peer_name, peer) in &configured {
            out.sample(name, &[("peer", peer_name)], value(peer));
        }
    }
}

/// Text in the exposition format. A family's `# HELP` and `# TYPE` lines are written with its
/// first sample, so a family without samples is left out.
#[derive(Default)]
pub(crate) struct Exposition {
    text: String,
    /// The family samples go to, and whether its header is written yet.
    current: Option<(&'static str, Kind, &'static str, bool)>,
    /// Families begun so far, to catch one begun twice.
    #[cfg(debug_assertions)]
    begun: Vec<&'static str>,
}

impl Exposition {
    /// Begins the family `name`; its samples follow.
    pub(crate) fn family(&mut self, name: &'static str, kind: Kind, help: &'static str) {
        #[cfg(debug_assertions)]
        {
            assert!(!self.begun.contains(&name), "family {name} begun twice");
            self.begun.push(name);
        }
        self.current = Some((name, kind, help, false));
    }

    /// A sample of the current family, which must be `name`.
    pub(crate) fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: impl Into<u64>) {
        let (family, kind, help, written) = self.current.as_mut().expect("no family begun");
        debug_assert_eq!(*family, name, "a sample outside its family");
        if !*written {
            let _ = writeln!(self.text, "# HELP {family} {}", escape_help(help));
            let _ = writeln!(self.text, "# TYPE {family} {}", kind.name());
            *written = true;
        }
        self.text.push_str(name);
        if !labels.is_empty() {
            self.text.push('{');
            for (index, (label, value)) in labels.iter().enumerate() {
                if index > 0 {
                    self.text.push(',');
                }
                let _ = write!(self.text, "{label}=\"{}\"", escape_label(value));
            }
            self.text.push('}');
        }
        let _ = writeln!(self.text, " {}", value.into());
    }
}

/// A label value as the format writes it: backslash, double quote and line feed escaped.
fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Help text as the format writes it: backslash and line feed escaped.
fn escape_help(help: &str) -> String {
    help.replace('\\', "\\\\").replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;
    use std::sync::atomic::Ordering::Relaxed;
    use std::time::Instant;

    use clap::Parser;

    use super::*;
    use crate::backlog::Backlog;
    use crate::config::Config;
    use crate::oplog::Oplog;
    use crate::replicate::{Readiness, Status};

    /// A context with persistence and peers started; `peers` are the configured addresses.
    fn full_context(peers: &[&str]) -> Arc<Context> {
        let mut config = Config::parse_from(["ferrited", "--data-dir", "/tmp/unused"]);
        config.peers = peers.iter().map(|peer| peer.to_string()).collect();
        let ctx = Context::new(&config);
        let oplog = Arc::new(Oplog::new(16, u64::MAX));
        let persist = crate::persist::PersistStats::new(oplog.clone());
        let _ = ctx.persist.set(Arc::new(persist));
        let backlog = Arc::new(Backlog::new(1 << 20, 10, Vec::new()));
        let status = Status::new(backlog, oplog, ctx.store.clone(), &config.peers);
        let _ = ctx.repl.set(Arc::new(status));
        let _ = ctx
            .readiness
            .set(Readiness::new(None, false, config.peers.len()));
        ctx
    }

    #[test]
    fn every_stats_value_has_a_metric_and_every_metric_a_stats_value() {
        let ctx = full_context(&["10.0.0.2:7000"]);
        let names: Vec<&str> = general_stats(&ctx, 0)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        let folded: BTreeSet<&str> = FOLDED.iter().map(|(stat, _)| *stat).collect();
        for name in &names {
            let mapped = STATS.iter().filter(|mapping| mapping.stat == *name).count();
            assert_eq!(
                mapped + usize::from(folded.contains(name)),
                1,
                "stats reports {name}, which must map to exactly one metric in STATS or FOLDED"
            );
        }
        for mapping in STATS {
            assert!(
                names.contains(&mapping.stat),
                "STATS maps {}, which stats does not report",
                mapping.stat
            );
        }
        for stat in folded {
            assert!(
                names.contains(&stat),
                "FOLDED names {stat}, which stats does not report"
            );
        }
        let metrics: BTreeSet<&str> = STATS.iter().map(|mapping| mapping.metric).collect();
        assert_eq!(metrics.len(), STATS.len(), "two stats map to one metric");
    }

    #[test]
    fn a_node_without_persistence_or_peers_maps_every_value_too() {
        let ctx = Context::new(&Config::parse_from(["ferrited"]));
        for (name, _) in general_stats(&ctx, 0) {
            assert!(
                STATS.iter().any(|mapping| mapping.stat == name)
                    || FOLDED.iter().any(|(stat, _)| *stat == name),
                "{name}"
            );
        }
        let text = render(&ctx);
        assert!(text.contains("\nferrited_persist_enabled 0\n"), "{text}");
        assert!(text.contains("\nferrited_repl_links_up 0\n"), "{text}");
        assert!(!text.contains("ferrited_waiting_reason"), "{text}");
        assert!(!text.contains("ferrited_repl_link_state"), "{text}");
        validate(&text);
    }

    #[test]
    fn the_output_is_valid_exposition_format() {
        // Configured addresses are validated as HOST:PORT, but the format must hold whatever
        // they are.
        let ctx = full_context(&["10.0.0.2:7000", "we\"ird\\host\n:1"]);
        let status = ctx.repl.get().unwrap();
        status.set_outbound_peer(0, 2);
        status.set_outbound(0, LinkState::Streaming);
        status.set_acked(0, 7);
        status.set_inbound(2, LinkState::Syncing);
        status.set_inbound(9, LinkState::Streaming);
        let text = render(&ctx);
        validate(&text);
        for line in [
            "ferrited_repl_link_state{peer=\"10.0.0.2:7000\",direction=\"out\",state=\"streaming\"} 1",
            "ferrited_repl_link_state{peer=\"10.0.0.2:7000\",direction=\"out\",state=\"down\"} 0",
            "ferrited_repl_link_state{peer=\"10.0.0.2:7000\",direction=\"in\",state=\"syncing\"} 1",
            "ferrited_repl_link_state{peer=\"node-9\",direction=\"in\",state=\"streaming\"} 1",
            "ferrited_repl_link_state{peer=\"we\\\"ird\\\\host\\n:1\",direction=\"out\",state=\"down\"} 1",
            "ferrited_repl_peer_node_id{peer=\"10.0.0.2:7000\"} 2",
            "ferrited_repl_peer_acked_position{peer=\"10.0.0.2:7000\"} 7",
            "ferrited_repl_peer_lag_entries{peer=\"10.0.0.2:7000\"} 3",
            "ferrited_repl_peer_repeat_syncs_total{peer=\"10.0.0.2:7000\"} 0",
            "ferrited_persist_enabled 1",
            "ferrited_state{state=\"recovering\"} 1",
            "ferrited_ready 0",
            "ferrited_waiting_reason{reason=\"no_peer_reachable\"} 0",
        ] {
            assert!(
                text.lines().any(|each| each == line),
                "no {line} in\n{text}"
            );
        }
        // A peer whose ID is unknown has no node ID sample, and an unconfigured one no outbound
        // series.
        assert_eq!(text.matches("ferrited_repl_peer_node_id{").count(), 1);
        assert!(!text.contains("peer=\"node-9\",direction=\"out\""));
        let version = format!(
            "ferrited_build_info{{version=\"{}\",",
            env!("CARGO_PKG_VERSION")
        );
        assert!(text.contains(&version), "{text}");
    }

    #[test]
    fn the_state_follows_recovery_waiting_serving_and_stopping() {
        let ctx = full_context(&["10.0.0.2:7000"]);
        let state = |text: &str| {
            let current: Vec<&str> = STATES
                .into_iter()
                .filter(|state| text.contains(&format!("ferrited_state{{state=\"{state}\"}} 1\n")))
                .collect();
            assert_eq!(current.len(), 1, "{text}");
            current[0]
        };
        assert_eq!(state(&render(&ctx)), "recovering");
        let readiness = ctx.readiness.get().unwrap();
        readiness.recovered(Arc::new(Oplog::new(16, u64::MAX)), false, Instant::now());
        let text = render(&ctx);
        assert_eq!(state(&text), "waiting");
        assert!(text.contains("ferrited_waiting_reason{reason=\"no_peer_reachable\"} 1\n"));
        assert!(text.contains("ferrited_peer_reachable 0\n"), "{text}");
        assert!(text.contains("ferrited_ready 0\n"));
        ctx.client_port_open.store(true, Relaxed);
        let text = render(&ctx);
        assert_eq!(state(&text), "ready");
        assert!(text.contains("ferrited_ready 1\n"));
        assert!(!text.contains("ferrited_waiting_reason{reason=\"no_peer_reachable\"} 1\n"));
        ctx.begin_shutdown();
        let text = render(&ctx);
        assert_eq!(state(&text), "stopping");
        assert!(text.contains("ferrited_ready 0\n"));
    }

    #[test]
    fn before_persistence_and_replication_start_their_values_are_left_out() {
        let config = Config::parse_from([
            "ferrited",
            "--data-dir",
            "/tmp/unused",
            "--peer",
            "10.0.0.2:7000",
            "--repl-port",
            "7000",
            "--node-id",
            "1",
        ]);
        let ctx = Context::new(&config);
        let text = render(&ctx);
        validate(&text);
        assert!(!text.contains("ferrited_persist_"), "{text}");
        assert!(!text.contains("ferrited_repl_"), "{text}");
        assert!(text.contains("\nferrited_curr_items 0\n"), "{text}");
        assert!(
            text.contains("ferrited_state{state=\"recovering\"} 1\n"),
            "{text}"
        );
    }

    #[test]
    fn values_follow_the_store_and_counters() {
        let ctx = Context::new(&Config::parse_from(["ferrited"]));
        let now = (ctx.wall_clock)();
        ctx.store.set(
            b"key",
            bytes::Bytes::from_static(b"value"),
            now,
            &Default::default(),
        );
        let mut local = crate::stats::LocalStats::default();
        local.bump(crate::stats::Stat::GetHits);
        local.add(crate::stats::Stat::BytesRead, 42);
        ctx.stats.merge(&mut local);
        let text = render(&ctx);
        let pointer = format!("ferrited_pointer_size_bytes {}", usize::BITS / 8);
        for line in [
            "ferrited_curr_items 1",
            "ferrited_get_hits_total 1",
            "ferrited_read_bytes_total 42",
            "ferrited_items_total 1",
            pointer.as_str(),
        ] {
            assert!(
                text.lines().any(|each| each == line),
                "no {line} in\n{text}"
            );
        }
    }

    #[test]
    fn label_values_and_help_are_escaped() {
        assert_eq!(escape_label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
        assert_eq!(escape_help("a\\b\nc \"d\""), "a\\\\b\\nc \"d\"");
        let mut out = Exposition::default();
        out.family("x_total", Kind::Counter, "Help with \\ and\na line.");
        out.sample("x_total", &[("l", "\"")], 3u64);
        out.sample("x_total", &[("l", "b"), ("m", "c")], 4u64);
        assert_eq!(
            out.text,
            "# HELP x_total Help with \\\\ and\\na line.\n# TYPE x_total counter\n\
             x_total{l=\"\\\"\"} 3\nx_total{l=\"b\",m=\"c\"} 4\n"
        );
    }

    fn metric_name(name: &str) -> bool {
        let mut bytes = name.bytes();
        bytes
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == b'_' || first == b':')
            && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b':')
    }

    fn label_name(name: &str) -> bool {
        metric_name(name) && !name.contains(':') && !name.starts_with("__")
    }

    /// Parses `name{labels} value`, checking the labels' escapes. Returns the name and the
    /// labels as written.
    fn parse_sample(line: &str) -> (String, BTreeMap<String, String>) {
        let (series, value) = line.rsplit_once(' ').unwrap_or_else(|| panic!("{line}"));
        value
            .parse::<f64>()
            .unwrap_or_else(|_| panic!("bad value: {line}"));
        let Some((name, rest)) = series.split_once('{') else {
            return (series.to_string(), BTreeMap::new());
        };
        let mut labels = BTreeMap::new();
        let mut rest = rest
            .strip_suffix('}')
            .unwrap_or_else(|| panic!("unclosed labels: {line}"));
        while !rest.is_empty() {
            let (label, after) = rest
                .split_once("=\"")
                .unwrap_or_else(|| panic!("bad label: {line}"));
            assert!(label_name(label), "bad label name {label}: {line}");
            let mut value = String::new();
            let mut chars = after.char_indices();
            let end = loop {
                match chars.next() {
                    Some((_, '\\')) => match chars.next() {
                        Some((_, '\\')) => value.push('\\'),
                        Some((_, '"')) => value.push('"'),
                        Some((_, 'n')) => value.push('\n'),
                        other => panic!("bad escape {other:?}: {line}"),
                    },
                    Some((index, '"')) => break index,
                    Some((_, '\n')) => panic!("raw line feed: {line}"),
                    Some((_, other)) => value.push(other),
                    None => panic!("unterminated label value: {line}"),
                }
            };
            assert!(
                labels.insert(label.to_string(), value).is_none(),
                "label {label} twice: {line}"
            );
            rest = &after[end + 1..];
            rest = rest.strip_prefix(',').unwrap_or(rest);
        }
        (name.to_string(), labels)
    }

    /// Checks one `# HELP` then one `# TYPE` per family before its samples, valid names,
    /// counters ending in `_total`, escaped label values, and no duplicate series.
    fn validate(text: &str) {
        assert!(text.ends_with('\n'), "no final line feed");
        let mut helped = BTreeSet::new();
        let mut typed = BTreeMap::new();
        let mut current: Option<String> = None;
        let mut series = BTreeSet::new();
        let mut last_help: Option<String> = None;
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let (name, help) = rest.split_once(' ').unwrap_or_else(|| panic!("{line}"));
                assert!(metric_name(name), "bad name: {line}");
                assert!(!help.is_empty(), "empty help: {line}");
                assert!(helped.insert(name.to_string()), "HELP twice for {name}");
                last_help = Some(name.to_string());
                current = None;
            } else if let Some(rest) = line.strip_prefix("# TYPE ") {
                let (name, kind) = rest.split_once(' ').unwrap_or_else(|| panic!("{line}"));
                assert_eq!(
                    last_help.as_deref(),
                    Some(name),
                    "TYPE without HELP: {line}"
                );
                assert!(matches!(kind, "counter" | "gauge"), "{line}");
                assert!(
                    typed.insert(name.to_string(), kind.to_string()).is_none(),
                    "TYPE twice for {name}"
                );
                if kind == "counter" {
                    assert!(
                        name.ends_with("_total"),
                        "counter {name} must end in _total"
                    );
                } else {
                    assert!(!name.ends_with("_total"), "gauge {name} ends in _total");
                }
                current = Some(name.to_string());
                last_help = None;
            } else {
                assert!(!line.starts_with('#'), "unexpected comment: {line}");
                let (name, labels) = parse_sample(line);
                assert!(metric_name(&name), "bad name: {line}");
                assert_eq!(
                    current.as_deref(),
                    Some(name.as_str()),
                    "sample outside its family: {line}"
                );
                assert!(series.insert((name, labels)), "series twice: {line}");
            }
        }
        assert_eq!(helped.len(), typed.len());
        for name in typed.keys() {
            assert!(
                series.iter().any(|(sample, _)| sample == name),
                "family {name} without samples"
            );
        }
    }
}
