//! Randomized fault injection ("chaos") against a cluster of ferrited processes.
//!
//! Run with `cargo test --release --test chaos -- --ignored --nocapture`. Settings:
//!
//! - `FERRITED_CHAOS_SECS`: how long the workload and the faults run (default 120).
//! - `FERRITED_CHAOS_SEED`: the seed (default random; always printed). It fixes the fault
//!   schedule and each client's nodes, keys and operations, but not timing.
//! - `FERRITED_CHAOS_NODES`: 2 or 3 (default 3).
//! - `FERRITED_CHAOS_MODE`: `strict` (default) or `tiny-cap` (see below).
//! - `FERRITED_CHAOS_SECRET`: `on` or `off` runs every node with or without a replication
//!   secret (default: drawn from the seed, half of seeds).
//! - `FERRITED_CHAOS_KEEP`: if set, keep the run directory even when the run passes.
//!
//! # Setup
//!
//! - Each node persists to its own directory and peers with every other node.
//! - Every replication link goes through an in-process TCP proxy that can cut it (close its
//!   connections) or blackhole it (stall it past the 5-second silence timeout), either way.
//! - Client threads send meta and classic sets and deletes (of absent keys too) to random nodes:
//!   100,000 cold keys written about once a minute (so a lost change stays missing), 300 hot
//!   keys written several times a second (contention), and 1,200 read-modify-write keys. A
//!   refused connection counts as a down node, as Dalli treats it.
//! - Half the seeds use 64 KiB segments and a small backlog (frequent rolls, compactions and
//!   full syncs). 40% give one node a tiny oplog for bursts to overflow. Half sign every link
//!   with `--repl-secret-file`; the proxies never alter bytes, so no node may log a frame that
//!   failed authentication.
//!
//! # Faults (drawn from the seed)
//!
//! - SIGKILL, SIGTERM, or SIGTERM then SIGKILL mid-shutdown; then a restart.
//! - SIGKILL or SIGTERM, then log damage before the restart: a torn last entry, garbage
//!   appended, one bad byte in an older segment, or (after SIGKILL) a power-loss tail cut (up to
//!   32 KiB, or everything since the node's first write in the last fsync interval).
//! - SIGSTOP and SIGCONT, short or past the silence timeout.
//! - Link cuts, blackholes, and links reset every few hundred milliseconds.
//! - Rarely, SIGKILL of every node at once.
//! - A few `flush_all`s, half delayed 1 to 3 seconds.
//! - Pipelined bursts of large sets.
//!
//! The last 14 to 18 seconds have no node faults. About 3.5 seconds before, one settled node
//! takes a final power loss, or a clean stop with log damage. Then, under writes, each node in
//! turn is blackholed for half a second and reset (so links resume mid-stream), one pair's links
//! flap, a burst arrives, and the run ends with one node cut off. Finally every link heals and,
//! once all stream with no lag, every key is read from every node.
//!
//! # The oracle
//!
//! **Versions.** A version is a node's hybrid clock: milliseconds, counter and node ID as one
//! integer. The nodes read this machine's wall clock, so a version's milliseconds lie within its
//! request's measured start and end, widened by any wall-clock step back (`watch_clock`). `ms`
//! with `c` returns the version, which must lie in its interval. An unanswered request may take
//! effect later, but only while the process that received it lives.
//!
//! **Final state.** A key's state is its highest-versioned change: a put's value, or absent after
//! a delete or a `flush_all` (a version floor). With peers, every delete leaves a tombstone.
//! Applying is idempotent and order independent, every node pair is linked, and a crashed node
//! is full-synced, so in strict mode all nodes must agree once the links settle.
//!
//! **Which changes count.** A change is *certain* if acknowledged and not *maybe lost*, else
//! *optional*. It is maybe lost if its node was killed between the request's start and
//! `LOSS_WINDOW_MS` after its end: the documented fsync-interval loss, unless a peer has it.
//! SIGTERM loses nothing; SIGKILL during shutdown is a kill. A log tail cut counts as a kill at
//! that stop (and at the one before, if the node never began a segment of its own).
//!
//! **Allowed outcomes for key K.**
//!
//! - *K holds put P's value at version v*: P's value and flags match, v is in P's interval
//!   (equal to its CAS reply, if any), and no certain put, delete or `flush_all` of K is surely
//!   above v. Otherwise an acknowledged write was lost or a deleted key came back.
//! - *K is absent*: the largest lower end among certain puts of K is below the largest upper end
//!   among deletes and flushes that may have taken effect. Otherwise a write was lost.
//!
//! The intervals are independent, so these rules are exact. The read-modify-write checks are
//! judged step by step, so they are necessary but not sufficient.
//!
//! **Read-modify-writes.** 1,200 keys also take appends and prepends, increments and decrements,
//! or conditional writes (`ms` with `C`, and `cas` after `gets`, half with an older token). Each
//! wins or loses by version like a put, so an increment can lose to another by design. Values
//! record their origin: an append key holds its put's base between the applied pieces, a counter
//! carries its put's ID above the increment range, and a conditional write names its token. A
//! held value must be one change's result with its pieces in version order, and an increment's
//! reply must have grown from a number some change produced.
//!
//! **Flushes and changes.** A node versions a change and checks the flush floor in one step, so
//! a change on node N versioned above a flush N made cannot carry content from below it. Changes
//! on nodes that have not applied the flush yet are not judged: they may race its arrival.
//!
//! **Delayed flushes.** `flush_all <secs>` falls due at its read time plus the delay and takes
//! effect at the next flush-floor read (any request, or the 1-second sweeper). It is not
//! persisted until then, so it is optional if its node stops first or another `flush_all` may
//! replace it.
//!
//! **Oplog overflow.** A dropped record is stored but unlogged until a later compaction covers
//! it, and reaches peers by full sync. A thread polls `persist_dropped` and `persist_snapshots`;
//! changes made while records were dropped are maybe lost to a kill until the third snapshot
//! after the drop (two earlier ones may finish first) or the process stops.
//!
//! **Holes.** Damage inside a segment cuts it there, and every peer full-syncs the node. The
//! node's own changes from the damage to the segment's end are maybe lost; other nodes' changes
//! come back from their origins.
//!
//! **No resurrection.** Strict mode keeps tombstones for 10 hours with a high cap, and memory far
//! exceeds the data, so nothing expires or is evicted (`evictions` and `repl_tombstones_evicted`
//! must be 0). A key back after a certain delete thus fails the first rule.
//!
//! **tiny-cap mode** uses a tiny `--tombstone-memory`, so a node may forget a delete: an older
//! put can return, full syncs omit the delete, and nodes may diverge for good. Forgetting a
//! delete also forgets the put it removed. So deletes are optional, a possibly superseded put
//! does not rule out older values, and each node is checked on its own.
//!
//! **Readiness.** A node with peers serves only once caught up with every linked peer. Before a
//! restart the test reads every key from each running peer; once the node serves, it reads the
//! node and its `ready_by`. A peer's value counts if it is 200 ms older than the snapshot, came
//! from another node that served and was fault-free (with its links to the node) from a second
//! before the snapshot until the node served, and was not maybe lost. The node must then hold
//! it, something newer, or nothing if a delete or flush may be newer. Nodes ready as last
//! standing or by sync timeout are skipped. In tiny-cap mode an older value is also allowed if a
//! delete may be newer.
//!
//! **Restarts.** After the final check, every node is stopped with SIGTERM and restarted, then,
//! after an idle fsync interval, killed and restarted. Each time every node must hold what it
//! held before (in tiny-cap mode, what the rules allow).
//!
//! **Also checked:** no node exits unprompted, SIGTERM exits with 0, and every reply is one the
//! protocol allows. Reads during the run are checked only for impossible read-modify-write
//! values, not freshness: readiness has documented gaps and failover reads are not linearizable.
//!
//! On failure the test prints the seed, the schedule, the faults as run, and each offending
//! key's history and per-node state, and keeps the run directory. Every child process is killed
//! when the test ends, panics included.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ferrited::oplog::Record;
use ferrited::persist::codec::{Entry, ReadOutcome, read_entry};

/// The fsync interval every node runs with.
const FSYNC_MS: u64 = 1000;

/// How long after acking a change a killed node may lose it: the fsync interval plus a second
/// for the log thread to drain.
const LOSS_WINDOW_MS: u64 = FSYNC_MS + 1000;

/// Bits below the milliseconds in a version: 14 of counter and 8 of node ID.
const MILLIS_SHIFT: u32 = 22;

/// Keys the workload uses.
const KEYS: u32 = HOT_KEYS + COLD_KEYS + APPEND_KEYS + COUNTER_KEYS + CAS_KEYS;

/// Keys written several times a second, so they are contended.
const HOT_KEYS: u32 = 300;

/// Keys written about once a minute or two, so a lost change stays missing at the end.
const COLD_KEYS: u32 = 100_000;

/// Keys that take appends and prepends besides sets and deletes.
const APPEND_KEYS: u32 = 400;

/// Keys that take increments and decrements besides sets and deletes.
const COUNTER_KEYS: u32 = 400;

/// Keys that take conditional writes (`cas`, `ms` with `C`) besides sets and deletes.
const CAS_KEYS: u32 = 400;

/// Percent of requests that use a cold key.
const COLD_PERCENT: u64 = 62;

/// Percent of requests that use an append, counter or CAS key.
const RMW_PERCENT: u64 = 22;

/// What a key is used for, by its number.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Class {
    Hot,
    Cold,
    Append,
    Counter,
    Cas,
}

fn class_of(key: u32) -> Class {
    match key {
        key if key < HOT_KEYS => Class::Hot,
        key if key < HOT_KEYS + COLD_KEYS => Class::Cold,
        key if key < HOT_KEYS + COLD_KEYS + APPEND_KEYS => Class::Append,
        key if key < HOT_KEYS + COLD_KEYS + APPEND_KEYS + COUNTER_KEYS => Class::Counter,
        _ => Class::Cas,
    }
}

/// Client threads.
const CLIENTS: usize = 6;

/// Bytes of a segment file's header (`persist::codec::HEADER_LEN`).
const SEGMENT_HEADER: u64 = 30;

#[test]
#[ignore = "runs for minutes; see the module documentation"]
fn chaos() {
    run(Settings::from_env(120));
}

/// A short fixed-seed run in the ordinary suite: two nodes, 30 seconds, signed links. Seed 219
/// gives node 2 a tiny oplog and draws log damage, delayed flushes, a kill, a pause, a power
/// loss, a flapping link and an oplog-overflowing burst.
#[test]
fn chaos_smoke() {
    let mut settings = Settings::new(30, 219, 2, Mode::Strict);
    settings.secret = true;
    run(settings);
}

// ---------------------------------------------------------------------------------------------
// Settings and randomness

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Strict,
    TinyCap,
}

#[derive(Clone, Debug)]
struct Settings {
    secs: u64,
    seed: u64,
    nodes: usize,
    mode: Mode,
    /// Flags that make logs roll and compact often and senders outrun their backlog, or none.
    tight_args: Vec<String>,
    /// A node with a tiny oplog for bursts to overflow, and its flags.
    overflow: Option<(usize, Vec<String>)>,
    /// Whether the nodes share a replication secret (signed links).
    secret: bool,
}

impl Settings {
    fn from_env(default_secs: u64) -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let secs = var("FERRITED_CHAOS_SECS").map_or(default_secs, |text| {
            text.parse()
                .expect("FERRITED_CHAOS_SECS must be a number of seconds")
        });
        let seed = var("FERRITED_CHAOS_SEED").map_or_else(random_seed, |text| {
            text.parse()
                .expect("FERRITED_CHAOS_SEED must be a 64-bit number")
        });
        let nodes = var("FERRITED_CHAOS_NODES").map_or(3, |text| {
            text.parse().expect("FERRITED_CHAOS_NODES must be 2 or 3")
        });
        assert!(
            nodes == 2 || nodes == 3,
            "FERRITED_CHAOS_NODES must be 2 or 3"
        );
        let mode = match var("FERRITED_CHAOS_MODE").as_deref() {
            None | Some("strict") => Mode::Strict,
            Some("tiny-cap") => Mode::TinyCap,
            Some(other) => panic!("FERRITED_CHAOS_MODE must be strict or tiny-cap, not {other}"),
        };
        assert!(secs >= 10, "FERRITED_CHAOS_SECS must be at least 10");
        let mut settings = Self::new(secs, seed, nodes, mode);
        match var("FERRITED_CHAOS_SECRET").as_deref() {
            None => {}
            Some("on") => settings.secret = true,
            Some("off") => settings.secret = false,
            Some(other) => panic!("FERRITED_CHAOS_SECRET must be on or off, not {other}"),
        }
        settings
    }

    fn new(secs: u64, seed: u64, nodes: usize, mode: Mode) -> Self {
        let mut rng = Rng::derive(seed, 3);
        let tight_args = if rng.chance(50) {
            let backlog = 1 << rng.range(14, 20);
            ["-I", "1k", "--segment-size", "64k", "--repl-backlog"]
                .iter()
                .map(|arg| arg.to_string())
                .chain([backlog.to_string()])
                .collect()
        } else {
            Vec::new()
        };
        // Holds steady traffic between log thread passes, but not a pipelined burst.
        let mut rng = Rng::derive(seed, 5);
        let overflow = rng.chance(40).then(|| {
            let node = rng.below(nodes as u64) as usize;
            let capacity = 1u64 << rng.range(5, 8);
            let (item, bytes) = if tight_args.is_empty() {
                (Some("4k"), "16k")
            } else {
                (None, "4k")
            };
            let args = item
                .map(|item| vec!["-I".to_string(), item.to_string()])
                .unwrap_or_default()
                .into_iter()
                .chain([
                    "--oplog-capacity".to_string(),
                    capacity.to_string(),
                    "--oplog-bytes".to_string(),
                    bytes.to_string(),
                ])
                .collect();
            (node, args)
        });
        let secret = Rng::derive(seed, 6).chance(50);
        Self {
            secs,
            seed,
            nodes,
            mode,
            tight_args,
            overflow,
            secret,
        }
    }
}

fn random_seed() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    Rng::new(nanos ^ (u64::from(std::process::id()) << 32)).next()
}

/// SplitMix64: small and good enough for a schedule.
#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// A generator for one purpose, independent of the others drawn from the same seed.
    fn derive(seed: u64, purpose: u64) -> Self {
        let mut rng = Self(seed ^ purpose.wrapping_mul(0xA24B_AED4_963E_E407));
        rng.next();
        rng
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// A number from `low` to `high`, both included.
    fn range(&mut self, low: u64, high: u64) -> u64 {
        low + self.below(high - low + 1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

// ---------------------------------------------------------------------------------------------
// The fault schedule

#[derive(Clone, Copy, Debug)]
enum Damage {
    /// Cut this many bytes off the end of the newest segment.
    Cut(u64),
    /// Append this many bytes of garbage to the newest segment.
    Garbage(u64),
    /// Cut whole entries, at least this many bytes, off the newest segment (an unsynced tail).
    PowerLoss(u64),
    /// Cut whole entries back to the node's last write before the last fsync interval, as a
    /// power loss just before a sync would.
    PowerLossInterval,
    /// Damage one byte of an entry in an older segment, leaving a hole in the history.
    Middle,
}

#[derive(Clone, Debug)]
enum Fault {
    /// SIGKILL; restart after the hold.
    Kill { node: usize },
    /// SIGTERM; restart after the hold.
    Term { node: usize },
    /// SIGTERM, then SIGKILL `after` ms later if still running; restart after the hold.
    TermKill { node: usize, after: u64 },
    /// SIGKILL or SIGTERM, damage the newest log segment, restart after the hold.
    Damage {
        node: usize,
        term: bool,
        damage: Damage,
    },
    /// SIGSTOP; SIGCONT after the hold.
    Pause { node: usize },
    /// Cut or blackhole `from` -> `to` (and back, if `both`) for the hold. A healed blackhole
    /// keeps its connections unless `reset`.
    Cut {
        from: usize,
        to: usize,
        both: bool,
        blackhole: bool,
        reset: bool,
    },
    /// Repeatedly close the link's connections both ways for the hold, so it resumes
    /// mid-stream many times.
    Flap { from: usize, to: usize },
    /// SIGKILL every node; restart them all after the hold.
    KillAll,
    /// `flush_all` on a node, delayed by `delay` seconds if not 0.
    Flush { node: usize, delay: u64 },
    /// A pipelined burst of `count` sets of large values to cold keys on a node.
    Burst { node: usize, count: u64 },
}

#[derive(Clone, Debug)]
struct Planned {
    /// Milliseconds after the workload starts.
    at: u64,
    /// Fault duration in milliseconds.
    hold: u64,
    fault: Fault,
}

/// Draws the fault schedule from the settings' seed.
fn plan(settings: &Settings) -> Vec<Planned> {
    let mut rng = Rng::derive(settings.seed, 1);
    let nodes = settings.nodes;
    // Every run ends partitioned (below); all other faults end, with every node up, before.
    let mut last = Rng::derive(settings.seed, 4);
    let phase = last.range(14000, 18000).min(settings.secs * 1000 / 3);
    let final_at = settings.secs * 1000 - phase;
    let end = final_at.saturating_sub(500);
    // One node and its links stay fault-free for 9 seconds before its last fault (below).
    let reserved = last.below(nodes as u64) as usize;
    let quiet_from = final_at.saturating_sub(9000);
    // When each node and each directed link is free of faults again.
    let mut node_free = vec![0u64; nodes];
    let mut link_free = vec![vec![0u64; nodes]; nodes];
    let mut killed_all = false;
    let mut planned = Vec::new();
    let mut at = 2000;
    loop {
        at += rng.range(300, 2500);
        if at >= end {
            break;
        }
        let free: Vec<usize> = (0..nodes).filter(|&node| node_free[node] <= at).collect();
        let busy = nodes - free.len();
        let pick = rng.below(100);
        let node = if free.is_empty() {
            None
        } else {
            Some(free[rng.below(free.len() as u64) as usize])
        };
        // At most all but one node in a fault at once, except `KillAll`.
        let node_fault_ok = node.is_some() && busy < nodes - 1;
        let (fault, hold) = match pick {
            0..=17 if node_fault_ok => (
                Fault::Kill {
                    node: node.unwrap(),
                },
                rng.range(200, 5000),
            ),
            18..=24 if node_fault_ok => (
                Fault::Term {
                    node: node.unwrap(),
                },
                rng.range(200, 5000),
            ),
            25..=28 if node_fault_ok => (
                Fault::TermKill {
                    node: node.unwrap(),
                    after: rng.range(0, 1500),
                },
                rng.range(200, 5000),
            ),
            29..=42 if node_fault_ok => {
                let (damage, term) = match rng.below(20) {
                    0..=2 => (Damage::Cut(rng.range(1, 300)), rng.chance(30)),
                    3..=7 => (Damage::PowerLoss(rng.range(4096, 32768)), false),
                    8..=12 => (Damage::PowerLossInterval, false),
                    // Mostly after SIGTERM: after a kill, the start is lossy anyway.
                    13..=16 => (Damage::Middle, rng.chance(70)),
                    _ => (Damage::Garbage(rng.range(1, 100)), rng.chance(30)),
                };
                let fault = Fault::Damage {
                    node: node.unwrap(),
                    term,
                    damage,
                };
                (fault, rng.range(200, 4000))
            }
            // Trips the links' 5-second silence timeout.
            43..=49 if node_fault_ok => (
                Fault::Pause {
                    node: node.unwrap(),
                },
                rng.range(5500, 9000),
            ),
            50..=55 if node_fault_ok => (
                Fault::Pause {
                    node: node.unwrap(),
                },
                rng.range(100, 3000),
            ),
            56..=79 => {
                let from = rng.below(nodes as u64) as usize;
                let to = (from + 1 + rng.below(nodes as u64 - 1) as usize) % nodes;
                let both = rng.chance(50);
                if link_free[from][to] > at || (both && link_free[to][from] > at) {
                    continue;
                }
                let fault = Fault::Cut {
                    from,
                    to,
                    both,
                    blackhole: rng.chance(40),
                    reset: rng.chance(50),
                };
                (fault, rng.range(500, 12000))
            }
            80..=85 => {
                let from = rng.below(nodes as u64) as usize;
                let to = (from + 1 + rng.below(nodes as u64 - 1) as usize) % nodes;
                if link_free[from][to] > at || link_free[to][from] > at {
                    continue;
                }
                (Fault::Flap { from, to }, rng.range(2000, 6000))
            }
            86..=87 if !killed_all && busy == 0 => {
                killed_all = true;
                (Fault::KillAll, rng.range(200, 3000))
            }
            88..=93 => {
                let node = rng.below(nodes as u64) as usize;
                let delay = if rng.chance(50) { rng.range(1, 3) } else { 0 };
                (Fault::Flush { node, delay }, 0)
            }
            94..=99 => {
                // Mostly at the tiny-oplog node, if any.
                let node = match settings.overflow {
                    Some((node, _)) if rng.chance(70) => node,
                    _ => rng.below(nodes as u64) as usize,
                };
                (
                    Fault::Burst {
                        node,
                        count: rng.range(300, 2000),
                    },
                    0,
                )
            }
            _ => continue,
        };
        // Each fault, including restart and catch-up, ends before the final partition. A
        // delayed flush takes effect within its delay plus a second or two.
        let clear = match &fault {
            Fault::Flush { delay, .. } => 200 + delay * 1000 + 2500,
            Fault::Cut { .. } | Fault::Flap { .. } | Fault::Burst { .. } => 200,
            Fault::Pause { .. } => 1000,
            Fault::KillAll => 4000,
            _ => 3000,
        };
        let hold = match &fault {
            Fault::Cut { .. } => hold.min(final_at.saturating_sub(at + clear)),
            _ => hold,
        };
        if at + hold + clear > final_at || (matches!(fault, Fault::Cut { .. }) && hold < 300) {
            continue;
        }
        let touches_reserved = match &fault {
            Fault::Kill { node }
            | Fault::Term { node }
            | Fault::TermKill { node, .. }
            | Fault::Damage { node, .. }
            | Fault::Pause { node } => *node == reserved,
            Fault::Cut { from, to, .. } | Fault::Flap { from, to } => {
                *from == reserved || *to == reserved
            }
            Fault::KillAll => true,
            Fault::Flush { .. } | Fault::Burst { .. } => false,
        };
        if touches_reserved && at + hold + clear > quiet_from {
            continue;
        }
        match &fault {
            Fault::Kill { node }
            | Fault::Term { node }
            | Fault::TermKill { node, .. }
            | Fault::Damage { node, .. } => {
                // Sometimes strike again while the node recovers or catches up.
                node_free[*node] = at + hold + rng.range(0, 3000);
            }
            Fault::Pause { node } => node_free[*node] = at + hold + 500,
            Fault::Cut { from, to, both, .. } => {
                link_free[*from][*to] = at + hold + 200;
                if *both {
                    link_free[*to][*from] = at + hold + 200;
                }
            }
            Fault::Flap { from, to } => {
                link_free[*from][*to] = at + hold + 200;
                link_free[*to][*from] = at + hold + 200;
            }
            Fault::KillAll => node_free
                .iter_mut()
                .for_each(|free| *free = at + hold + 3000),
            Fault::Flush { .. } | Fault::Burst { .. } => {}
        }
        planned.push(Planned { at, hold, fault });
    }
    // End with short partitions isolating each node in turn under writes (about 20 rounds in 15
    // seconds). All but the last are blackholes reset on heal, so links resume mid-stream at
    // once (a cut would back the sender off for up to 5 seconds). No restart follows, so no
    // full sync can repair a change a link loses.
    //
    // First, the reserved node loses writes its peers hold, by a power loss or a log hole; only
    // the full syncs of its lossy start can restore them.
    let loss_at = final_at.saturating_sub(3500);
    let busy = (0..nodes)
        .filter(|&node| node != reserved && node_free[node] > loss_at)
        .count();
    let (term, damage) = if last.chance(50) {
        (false, Damage::PowerLossInterval)
    } else {
        (true, Damage::Middle)
    };
    if loss_at > 7000 && busy + 1 < nodes {
        let fault = Fault::Damage {
            node: reserved,
            term,
            damage,
        };
        planned.push(Planned {
            at: loss_at,
            hold: 300,
            fault,
        });
    }
    let first = last.below(nodes as u64) as usize;
    // One pair's links also flap, for more mid-stream resumes.
    let flapped = last.below(nodes as u64) as usize;
    planned.push(Planned {
        at: final_at,
        hold: settings.secs * 1000 - final_at,
        fault: Fault::Flap {
            from: flapped,
            to: (flapped + 1) % nodes,
        },
    });
    // An early burst, at the tiny-oplog node if any, whose dropped records need a full sync.
    let burst_node = match settings.overflow {
        Some((node, _)) => node,
        None => last.below(nodes as u64) as usize,
    };
    planned.push(Planned {
        at: final_at + 1000,
        hold: 0,
        fault: Fault::Burst {
            node: burst_node,
            count: 2000,
        },
    });
    let mut rounds = Vec::new();
    let mut at = final_at;
    while at + 750 + 3000 <= settings.secs * 1000 {
        rounds.push((at, 500, true, true));
        at += 750;
    }
    rounds.push((at, settings.secs * 1000, last.chance(50), last.chance(50)));
    for (round, (at, hold, blackhole, reset)) in rounds.into_iter().enumerate() {
        let isolated = (first + round) % nodes;
        for other in (0..nodes).filter(|&other| other != isolated) {
            let fault = Fault::Cut {
                from: isolated,
                to: other,
                both: true,
                blackhole,
                reset,
            };
            planned.push(Planned { at, hold, fault });
        }
    }
    planned
}

fn describe(fault: &Fault) -> String {
    let n = |node: &usize| node + 1;
    match fault {
        Fault::Kill { node } => format!("SIGKILL node {}", n(node)),
        Fault::Term { node } => format!("SIGTERM node {}", n(node)),
        Fault::TermKill { node, after } => {
            format!("SIGTERM node {}, SIGKILL {after} ms later", n(node))
        }
        Fault::Damage { node, term, damage } => format!(
            "{} node {} and {}",
            if *term { "SIGTERM" } else { "SIGKILL" },
            n(node),
            match damage {
                Damage::Cut(bytes) => format!("cut {bytes} bytes off its log"),
                Damage::Garbage(bytes) => format!("append {bytes} bytes of garbage to its log"),
                Damage::PowerLoss(bytes) => {
                    format!("cut whole entries, at least {bytes} bytes, off its log")
                }
                Damage::PowerLossInterval => {
                    "cut its log back to its last write before the last fsync interval".into()
                }
                Damage::Middle => "damage an entry inside an older segment".into(),
            }
        ),
        Fault::Pause { node } => format!("SIGSTOP node {}", n(node)),
        Fault::Cut {
            from,
            to,
            both,
            blackhole,
            reset,
        } => format!(
            "{} link {} {} {}{}",
            if *blackhole { "blackhole" } else { "cut" },
            n(from),
            if *both { "<->" } else { "->" },
            n(to),
            if *blackhole && *reset {
                " (reset on heal)"
            } else {
                ""
            }
        ),
        Fault::Flap { from, to } => format!("flap link {} <-> {}", n(from), n(to)),
        Fault::KillAll => "SIGKILL every node".into(),
        Fault::Flush { node, delay: 0 } => format!("flush_all on node {}", n(node)),
        Fault::Flush { node, delay } => format!("flush_all {delay} on node {}", n(node)),
        Fault::Burst { node, count } => format!("burst of {count} sets to node {}", n(node)),
    }
}

// ---------------------------------------------------------------------------------------------
// Proxies between nodes' replication ports

const LINK_UP: u8 = 0;
const LINK_CUT: u8 = 1;
const LINK_BLACKHOLE: u8 = 2;

/// A proxy carrying node `from`'s connections to node `to`'s replication port.
struct Link {
    from: usize,
    to: usize,
    port: u16,
    state: AtomicU8,
    /// Raised to close every connection the proxy carries.
    epoch: AtomicU64,
}

impl Link {
    fn set(&self, state: u8, reset: bool) {
        self.state.store(state, SeqCst);
        if reset {
            self.epoch.fetch_add(1, SeqCst);
        }
    }
}

fn run_proxy(link: Arc<Link>, listener: TcpListener, target: u16, stop: Arc<AtomicBool>) {
    listener.set_nonblocking(true).unwrap();
    while !stop.load(SeqCst) {
        match listener.accept() {
            Ok((inbound, _)) => {
                if link.state.load(SeqCst) == LINK_CUT {
                    continue;
                }
                inbound.set_nonblocking(false).unwrap();
                let address = SocketAddr::from(([127, 0, 0, 1], target));
                let Ok(outbound) = TcpStream::connect_timeout(&address, Duration::from_secs(1))
                else {
                    continue;
                };
                // A connect to a down node can cross another; see `Conn::open`.
                let local = outbound.local_addr().map_or(0, |address| address.port());
                if CLUSTER_PORTS.lock().unwrap().contains(&local) {
                    CROSSED.fetch_add(1, SeqCst);
                    continue;
                }
                let epoch = link.epoch.load(SeqCst);
                for (from, to) in [
                    (inbound.try_clone().unwrap(), outbound.try_clone().unwrap()),
                    (outbound, inbound),
                ] {
                    let (link, stop) = (link.clone(), stop.clone());
                    thread::spawn(move || pump(&link, from, to, epoch, &stop));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => thread::sleep(Duration::from_millis(5)),
        }
    }
}

/// Copies bytes while the link is up, holds them while blackholed, and closes both ends when
/// the link is cut or reset or either side closes.
fn pump(link: &Link, mut from: TcpStream, mut to: TcpStream, epoch: u64, stop: &AtomicBool) {
    from.set_read_timeout(Some(Duration::from_millis(20)))
        .unwrap();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        if stop.load(SeqCst) || link.epoch.load(SeqCst) != epoch {
            break;
        }
        match link.state.load(SeqCst) {
            LINK_CUT => break,
            LINK_BLACKHOLE => {
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            _ => {}
        }
        match from.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                if to.write_all(&buffer[..read]).is_err() {
                    break;
                }
            }
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) => {}
            Err(_) => break,
        }
    }
    let _ = from.shutdown(Shutdown::Both);
    let _ = to.shutdown(Shutdown::Both);
}

// ---------------------------------------------------------------------------------------------
// Nodes

struct Node {
    client_port: u16,
    repl_port: u16,
    data_dir: PathBuf,
    log: PathBuf,
    args: Vec<String>,
    child: Option<Child>,
    paused: bool,
    started: Instant,
    /// When this node's current process started (wall-clock milliseconds).
    started_wall: u64,
    /// When this node last stopped (wall-clock milliseconds), if it has.
    last_stop: Option<u64>,
    /// When the run before the current one stopped, if there was one.
    previous_stop: Option<u64>,
    /// When each run stopped.
    stops: Vec<u64>,
    starts: u32,
}

impl Node {
    fn spawn(&mut self, t0: u64) {
        self.starts += 1;
        let mut log = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .unwrap();
        writeln!(
            log,
            "=== chaos: start {} at t={} ms ===",
            self.starts,
            now_ms().saturating_sub(t0)
        )
        .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_ferrited"))
            .args(&self.args)
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("cannot start ferrited");
        self.child = Some(child);
        self.paused = false;
        self.started = Instant::now();
        self.started_wall = now_ms();
        self.previous_stop = self.last_stop;
    }

    fn signal(&self, signal: i32) {
        if let Some(child) = &self.child {
            send_signal(child, signal);
        }
    }

    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
            self.stopped();
        }
        self.paused = false;
    }

    /// Sends SIGTERM and waits for the exit; `None` if it took too long and was killed.
    fn terminate(&mut self) -> Option<ExitStatus> {
        let mut child = self.child.take()?;
        if self.paused {
            send_signal(&child, libc::SIGCONT);
            self.paused = false;
        }
        send_signal(&child, libc::SIGTERM);
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break Some(status);
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            thread::sleep(Duration::from_millis(10));
        };
        self.stopped();
        status
    }

    fn stopped(&mut self) {
        let now = now_ms();
        self.last_stop = Some(now);
        self.stops.push(now);
    }

    /// The last `lines` lines of this node's log.
    fn log_tail(&self, lines: usize) -> String {
        let text = fs::read_to_string(&self.log).unwrap_or_default();
        let all: Vec<&str> = text.lines().collect();
        all[all.len().saturating_sub(lines)..].join("\n")
    }
}

/// Whether SIGTERM ended the process before it wrote a line, so before its signal handlers
/// existed: a kill, not a clean stop.
fn before_handlers(node: &Node, status: ExitStatus) -> bool {
    use std::os::unix::process::ExitStatusExt;
    let tail = node.log_tail(40);
    let since_start = tail.rsplit("=== chaos: start").next().unwrap_or("");
    status.signal() == Some(libc::SIGTERM)
        && since_start
            .lines()
            .skip(1)
            .all(|line| line.starts_with("=== chaos:"))
}

fn send_signal(child: &Child, signal: i32) {
    // SAFETY: `kill` has no memory-safety preconditions; the PID is a child of this process,
    // not yet reaped.
    unsafe {
        libc::kill(child.id() as i32, signal);
    }
}

/// What the test and its threads share.
struct Shared {
    nodes: Mutex<Vec<Node>>,
    links: Vec<Arc<Link>>,
    client_ports: Vec<u16>,
    /// Each node's log file.
    logs: Vec<PathBuf>,
    stop_workload: AtomicBool,
    stop_faults: AtomicBool,
    stop_proxies: Arc<AtomicBool>,
    /// Wall-clock milliseconds when the workload started.
    t0: AtomicU64,
    /// The faults as run: (milliseconds after t0, what happened).
    events: Mutex<Vec<(u64, String)>>,
    /// Node kills that can lose acknowledged changes: (node, wall-clock milliseconds).
    crashes: Mutex<Vec<(usize, u64)>>,
    /// The `flush_all`s sent.
    flushes: Mutex<Vec<Op>>,
    /// Each fault as run, with its wall-clock start and end (`u64::MAX` while it lasts). A
    /// node exiting on its own counts as a kill.
    faults: Mutex<Vec<(Fault, u64, u64)>>,
    /// What the readiness probes saw; see `Probe`.
    probes: Mutex<Vec<Probe>>,
    /// The most the wall clock has gone back against the monotonic clock, in milliseconds.
    clock_back: AtomicU64,
    /// Things that went wrong outside the oracle: a node exiting on its own, a bad exit status.
    anomalies: Mutex<Vec<String>>,
    /// When nodes' oplogs dropped records; see `watch_drops`.
    drops: Mutex<Vec<Drops>>,
    /// Own changes lost to log damage: (node, key or `None` for a flush, version).
    holes: Mutex<Vec<(usize, Option<u32>, u64)>>,
    /// The sets the bursts sent.
    burst_ops: Mutex<Vec<Op>>,
}

/// A stretch in which a node's oplog dropped records. Until a later snapshot covers them, a
/// kill can lose them like unsynced writes.
#[derive(Clone, Copy, Debug)]
struct Drops {
    node: usize,
    /// Wall-clock milliseconds: the drops happened after `from` and before `to`.
    from: u64,
    to: u64,
    /// When a covering snapshot completed or the process stopped, else `u64::MAX`.
    covered: u64,
}

impl Shared {
    fn t0(&self) -> u64 {
        self.t0.load(SeqCst)
    }

    fn event(&self, text: String) {
        let at = now_ms().saturating_sub(self.t0());
        println!("chaos: t={at:>6} ms  {text}");
        // Also into every node's log, to line its output up with the faults.
        for log in &self.logs {
            if let Ok(mut file) = OpenOptions::new().append(true).open(log) {
                let _ = writeln!(file, "=== chaos: t={at} ms {text} ===");
            }
        }
        self.events.lock().unwrap().push((at, text));
    }

    fn anomaly(&self, text: String) {
        println!("chaos: ANOMALY: {text}");
        self.anomalies.lock().unwrap().push(text);
    }

    fn crash(&self, node: usize) {
        self.crashes.lock().unwrap().push((node, now_ms()));
    }
}

/// Records in `clock_back` the most the wall clock steps back against the monotonic clock,
/// until the proxies stop. Each wall reading is compared with the midpoint of two monotonic
/// ones; samples far apart are skipped, since descheduling (tens of ms under load or in a VM)
/// would look like a step back.
fn watch_clock(shared: &Shared) {
    /// The widest usable sample, in µs; midpoints are this accurate, so more is a step.
    const TIGHT: i128 = 100;
    let base = Instant::now();
    let mut highest = i128::MIN;
    while !shared.stop_proxies.load(SeqCst) {
        let before = base.elapsed().as_micros() as i128;
        let wall = now_micros() as i128;
        let after = base.elapsed().as_micros() as i128;
        if after - before <= TIGHT {
            let offset = wall - (before + after) / 2;
            highest = highest.max(offset);
            let back = highest - offset;
            // A sub-millisecond step can still cross a millisecond boundary: count 1 ms at least.
            if back > TIGHT {
                shared
                    .clock_back
                    .fetch_max(((back + 999) / 1000) as u64, SeqCst);
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
}

/// Polls `node`'s per-process `persist_dropped` and `persist_snapshots` every 50 ms, noting in
/// `drops` when records were dropped and when a snapshot covered them.
///
/// - Two compactions begun before the drops may still finish, so the third snapshot covers.
/// - A stop ends every stretch: a clean stop compacts, and a kill is what stretches are for.
/// - A kill over a second after the last answered poll counts as a stretch: drops may be unseen.
fn watch_drops(shared: &Shared, node: usize) {
    let port = shared.client_ports[node];
    let mut run = 0;
    let mut conn: Option<Conn> = None;
    // Per process: the last answered poll (send time, counts), and open stretches with the
    // snapshot count that covers each.
    let mut last_ok: Option<(u64, u64, u64)> = None;
    let mut open: Vec<(usize, u64)> = Vec::new();
    while !shared.stop_proxies.load(SeqCst) {
        let (starts, running, last_stop, started_at) = {
            let nodes = shared.nodes.lock().unwrap();
            let one = &nodes[node];
            (
                one.starts,
                one.child.is_some() && !one.paused,
                one.last_stop,
                one.started_wall,
            )
        };
        if starts != run {
            // The watched process stopped.
            let stop = last_stop.unwrap_or_else(now_ms);
            let mut drops = shared.drops.lock().unwrap();
            for (index, _) in open.drain(..) {
                drops[index].covered = drops[index].covered.min(stop);
            }
            if run != 0 {
                let seen = last_ok.map_or(0, |(sent, _, _)| sent);
                if stop > seen + 1000 {
                    drops.push(Drops {
                        node,
                        from: seen,
                        to: stop,
                        covered: stop,
                    });
                }
            }
            drop(drops);
            run = starts;
            last_ok = Some((started_at, 0, 0));
            conn = None;
        }
        if !running {
            thread::sleep(Duration::from_millis(50));
            continue;
        }
        let sent = now_ms();
        let counts = conn
            .take()
            .or_else(|| Conn::open_with_timeout(port, Duration::from_millis(300)))
            .and_then(|mut one| {
                let lines = one.lines("stats\r\n", "END").ok()?;
                let stat = |name: &str| {
                    lines.iter().find_map(|line| {
                        line.strip_prefix("STAT ")?
                            .strip_prefix(name)?
                            .strip_prefix(' ')?
                            .parse::<u64>()
                            .ok()
                    })
                };
                let counts = (stat("persist_dropped")?, stat("persist_snapshots")?);
                conn = Some(one);
                Some(counts)
            });
        let replied = now_ms();
        if let Some((dropped, snapshots)) = counts
            && shared.nodes.lock().unwrap()[node].starts == run
        {
            let (seen, before, _) = last_ok.unwrap_or((sent, 0, 0));
            let mut drops = shared.drops.lock().unwrap();
            if dropped > before {
                drops.push(Drops {
                    node,
                    from: seen,
                    to: replied,
                    covered: u64::MAX,
                });
                open.push((drops.len() - 1, snapshots + 3));
            }
            open.retain(|&(index, needed)| {
                if snapshots >= needed {
                    drops[index].covered = replied;
                    false
                } else {
                    true
                }
            });
            last_ok = Some((sent, dropped, snapshots));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// Kills every node when the test ends, however it ends.
struct Guard(Arc<Shared>);

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.stop_workload.store(true, SeqCst);
        self.0.stop_faults.store(true, SeqCst);
        self.0.stop_proxies.store(true, SeqCst);
        let mut nodes = match self.0.nodes.lock() {
            Ok(nodes) => nodes,
            Err(poisoned) => poisoned.into_inner(),
        };
        for node in nodes.iter_mut() {
            node.kill();
        }
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Builds the cluster's nodes and proxies, without starting the nodes.
fn build(settings: &Settings, run_dir: &Path) -> Arc<Shared> {
    let count = settings.nodes;
    let stop_proxies = Arc::new(AtomicBool::new(false));
    let mut taken = Vec::new();
    let mut port = || loop {
        let port = free_port();
        if port != 11211 && !taken.contains(&port) {
            taken.push(port);
            return port;
        }
    };
    let ports: Vec<(u16, u16)> = (0..count).map(|_| (port(), port())).collect();
    let mut links = Vec::new();
    let mut listeners = Vec::new();
    for from in 0..count {
        for (to, &(_, target)) in ports.iter().enumerate() {
            if from == to {
                continue;
            }
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let link = Arc::new(Link {
                from,
                to,
                port: listener.local_addr().unwrap().port(),
                state: AtomicU8::new(LINK_UP),
                epoch: AtomicU64::new(0),
            });
            listeners.push((link.clone(), listener, target));
            links.push(link);
        }
    }
    {
        let mut reserved = CLUSTER_PORTS.lock().unwrap();
        reserved.extend(ports.iter().flat_map(|&(client, repl)| [client, repl]));
        reserved.extend(links.iter().map(|link| link.port));
    }
    for (link, listener, target) in listeners {
        let stop = stop_proxies.clone();
        thread::spawn(move || run_proxy(link, listener, target, stop));
    }
    let tombstone_memory = match settings.mode {
        Mode::Strict => "64m",
        Mode::TinyCap => "2k",
    };
    let secret = run_dir.join("secret");
    if settings.secret {
        fs::write(&secret, "chaos secret\n").unwrap();
    }
    let nodes = (0..count)
        .map(|index| {
            let (client_port, repl_port) = ports[index];
            let data_dir = run_dir.join(format!("node{}", index + 1));
            fs::create_dir_all(&data_dir).unwrap();
            let mut args: Vec<String> = [
                "-p",
                &client_port.to_string(),
                "-t",
                "2",
                "-m",
                "256",
                "--node-id",
                &(index + 1).to_string(),
                "--repl-port",
                &repl_port.to_string(),
                "--data-dir",
                data_dir.to_str().unwrap(),
                "--fsync-interval-ms",
                &FSYNC_MS.to_string(),
                "--tombstone-ttl",
                "36000",
                "--tombstone-memory",
                tombstone_memory,
            ]
            .iter()
            .map(|arg| arg.to_string())
            .collect();
            args.extend(settings.tight_args.iter().cloned());
            if settings.secret {
                args.push("--repl-secret-file".into());
                args.push(secret.to_str().unwrap().into());
            }
            if let Some((node, extra)) = &settings.overflow
                && *node == index
            {
                args.extend(extra.iter().cloned());
            }
            for link in links.iter().filter(|link| link.from == index) {
                args.push("--peer".into());
                args.push(format!("127.0.0.1:{}", link.port));
            }
            Node {
                client_port,
                repl_port,
                log: run_dir.join(format!("node{}.log", index + 1)),
                data_dir,
                args,
                child: None,
                paused: false,
                started: Instant::now(),
                started_wall: 0,
                last_stop: None,
                previous_stop: None,
                stops: Vec::new(),
                starts: 0,
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Shared {
        client_ports: nodes.iter().map(|node| node.client_port).collect(),
        logs: nodes.iter().map(|node| node.log.clone()).collect(),
        nodes: Mutex::new(nodes),
        links,
        stop_workload: AtomicBool::new(false),
        stop_faults: AtomicBool::new(false),
        stop_proxies,
        t0: AtomicU64::new(now_ms()),
        events: Mutex::new(Vec::new()),
        crashes: Mutex::new(Vec::new()),
        flushes: Mutex::new(Vec::new()),
        faults: Mutex::new(Vec::new()),
        probes: Mutex::new(Vec::new()),
        clock_back: AtomicU64::new(0),
        anomalies: Mutex::new(Vec::new()),
        drops: Mutex::new(Vec::new()),
        holes: Mutex::new(Vec::new()),
        burst_ops: Mutex::new(Vec::new()),
    })
}

/// Restarts any node that exited on its own, as an anomaly unless its port was taken.
fn check_exits(shared: &Shared) {
    let t0 = shared.t0();
    let exited: Vec<(usize, ExitStatus, String)> = {
        let mut nodes = shared.nodes.lock().unwrap();
        nodes
            .iter_mut()
            .enumerate()
            .filter_map(|(index, node)| {
                let status = node.child.as_mut()?.try_wait().unwrap()?;
                node.child = None;
                node.stopped();
                Some((index, status, node.log_tail(40)))
            })
            .collect()
    };
    for (index, status, tail) in exited {
        let last_start = tail.rsplit("=== chaos: start").next().unwrap_or("");
        if last_start.contains("Address already in use") {
            shared.event(format!(
                "node {} found a port taken; restarting it",
                index + 1
            ));
            thread::sleep(Duration::from_millis(200));
        } else {
            // It may have lost what it acknowledged, like a kill.
            shared.crash(index);
            let now = now_ms();
            let kill = Fault::Kill { node: index };
            shared.faults.lock().unwrap().push((kill, now, now));
            shared.anomaly(format!(
                "node {} exited on its own ({status}); its log ends:\n{tail}",
                index + 1
            ));
        }
        shared.nodes.lock().unwrap()[index].spawn(t0);
    }
}

// ---------------------------------------------------------------------------------------------
// Running the schedule

/// Runs `planned` in order until done or the workload stops, then ends any fault still active.
fn run_faults(shared: &Arc<Shared>, planned: &[Planned], seed: u64) {
    let mut rng = Rng::derive(seed, 2);
    // (time, index into planned, whether it is the fault's end)
    let mut steps: Vec<(u64, usize, bool)> = Vec::new();
    for (index, fault) in planned.iter().enumerate() {
        steps.push((fault.at, index, false));
        if !instant(&fault.fault) {
            steps.push((fault.at + fault.hold, index, true));
        }
    }
    steps.sort();
    let mut started = vec![false; planned.len()];
    let mut ended = vec![false; planned.len()];
    for &(at, index, end) in &steps {
        while now_ms() < shared.t0() + at && !shared.stop_faults.load(SeqCst) {
            check_exits(shared);
            thread::sleep(Duration::from_millis(20));
        }
        if shared.stop_faults.load(SeqCst) {
            break;
        }
        check_exits(shared);
        if end {
            if started[index] {
                end_fault(shared, &planned[index].fault);
                ended[index] = true;
            }
        } else {
            start_fault(shared, &planned[index].fault, &mut rng);
            started[index] = true;
        }
    }
    for (index, fault) in planned.iter().enumerate() {
        if started[index] && !ended[index] && !instant(&fault.fault) {
            end_fault(shared, &fault.fault);
        }
    }
}

/// Whether a fault has no end step.
fn instant(fault: &Fault) -> bool {
    matches!(fault, Fault::Flush { .. } | Fault::Burst { .. })
}

fn start_fault(shared: &Arc<Shared>, fault: &Fault, rng: &mut Rng) {
    shared.event(describe(fault));
    if !instant(fault) {
        let now = now_ms();
        shared
            .faults
            .lock()
            .unwrap()
            .push((fault.clone(), now, u64::MAX));
    }
    match fault {
        Fault::Kill { node } => {
            shared.nodes.lock().unwrap()[*node].kill();
            shared.crash(*node);
        }
        Fault::Term { node } => terminate(shared, *node),
        Fault::TermKill { node, after } => {
            let mut nodes = shared.nodes.lock().unwrap();
            let node_ref = &mut nodes[*node];
            if let Some(child) = node_ref.child.as_mut() {
                send_signal(child, libc::SIGTERM);
                let deadline = Instant::now() + Duration::from_millis(*after);
                while Instant::now() < deadline && child.try_wait().unwrap().is_none() {
                    thread::sleep(Duration::from_millis(5));
                }
                let exited = child.try_wait().unwrap();
                let early = exited.is_some_and(|status| before_handlers(node_ref, status));
                node_ref.kill();
                drop(nodes);
                match exited {
                    Some(_) if early => {
                        shared.crash(*node);
                        shared.event(format!(
                            "node {} died of SIGTERM before it could handle it",
                            node + 1
                        ));
                    }
                    Some(status) if !status.success() => shared.anomaly(format!(
                        "node {} exited with {status} after SIGTERM",
                        node + 1
                    )),
                    Some(_) => shared.event(format!("node {} had exited", node + 1)),
                    None => shared.crash(*node),
                }
            }
        }
        Fault::Damage { node, term, damage } => {
            if *term {
                terminate(shared, *node);
            } else {
                shared.nodes.lock().unwrap()[*node].kill();
                shared.crash(*node);
            }
            damage_log(shared, *node, *damage, *term, rng);
        }
        Fault::Pause { node } => {
            let mut nodes = shared.nodes.lock().unwrap();
            let node = &mut nodes[*node];
            if node.child.is_some() {
                node.signal(libc::SIGSTOP);
                node.paused = true;
            }
        }
        Fault::Cut {
            from,
            to,
            both,
            blackhole,
            ..
        } => {
            let state = if *blackhole { LINK_BLACKHOLE } else { LINK_CUT };
            for link in &shared.links {
                if (link.from == *from && link.to == *to)
                    || (*both && link.from == *to && link.to == *from)
                {
                    link.set(state, !*blackhole);
                }
            }
        }
        Fault::KillAll => {
            let mut nodes = shared.nodes.lock().unwrap();
            for (index, node) in nodes.iter_mut().enumerate() {
                node.kill();
                shared.crash(index);
            }
        }
        Fault::Flap { from, to } => {
            // Every 150 to 400 ms; each time the sender reconnects and resumes.
            let (shared, from, to, seed) = (shared.clone(), *from, *to, rng.next());
            let name = describe(fault);
            thread::spawn(move || {
                let mut rng = Rng::new(seed);
                loop {
                    thread::sleep(Duration::from_millis(rng.range(150, 400)));
                    let over = shared.stop_faults.load(SeqCst)
                        || shared
                            .faults
                            .lock()
                            .unwrap()
                            .iter()
                            .rev()
                            .find(|(other, _, _)| describe(other) == name)
                            .is_none_or(|(_, _, end)| *end != u64::MAX);
                    if over {
                        break;
                    }
                    // A link cut or blackholed meanwhile stays so.
                    for link in &shared.links {
                        if ((link.from == from && link.to == to)
                            || (link.from == to && link.to == from))
                            && link.state.load(SeqCst) == LINK_UP
                        {
                            link.epoch.fetch_add(1, SeqCst);
                        }
                    }
                }
            });
        }
        Fault::Flush { node, delay } => flush(shared, *node, *delay),
        Fault::Burst { node, count } => {
            let (shared, node, count, seed) = (shared.clone(), *node, *count, rng.next());
            thread::spawn(move || burst(&shared, node, count, seed));
        }
    }
}

fn end_fault(shared: &Arc<Shared>, fault: &Fault) {
    let t0 = shared.t0();
    {
        let name = describe(fault);
        let now = now_ms();
        let mut faults = shared.faults.lock().unwrap();
        if let Some(entry) = faults
            .iter_mut()
            .rev()
            .find(|(other, _, end)| *end == u64::MAX && describe(other) == name)
        {
            entry.2 = now;
        }
    }
    match fault {
        Fault::Kill { node }
        | Fault::Term { node }
        | Fault::TermKill { node, .. }
        | Fault::Damage { node, .. } => {
            shared.event(format!("restart node {}", node + 1));
            let snapshot = snapshot_peers(shared, *node);
            let mut nodes = shared.nodes.lock().unwrap();
            if nodes[*node].child.is_none() {
                nodes[*node].spawn(t0);
                let starts = nodes[*node].starts;
                drop(nodes);
                spawn_probe(shared, *node, starts, snapshot);
            }
        }
        Fault::Pause { node } => {
            shared.event(format!("SIGCONT node {}", node + 1));
            let mut nodes = shared.nodes.lock().unwrap();
            let node = &mut nodes[*node];
            if node.paused {
                node.signal(libc::SIGCONT);
                node.paused = false;
            }
        }
        Fault::Cut {
            from,
            to,
            both,
            blackhole,
            reset,
        } => {
            shared.event(format!(
                "heal link {} {} {}",
                from + 1,
                if *both { "<->" } else { "->" },
                to + 1
            ));
            for link in &shared.links {
                if (link.from == *from && link.to == *to)
                    || (*both && link.from == *to && link.to == *from)
                {
                    link.set(LINK_UP, *blackhole && *reset);
                }
            }
        }
        Fault::Flap { from, to } => {
            shared.event(format!("stop flapping link {} <-> {}", from + 1, to + 1));
        }
        Fault::KillAll => {
            shared.event("restart every node".into());
            let mut nodes = shared.nodes.lock().unwrap();
            for node in nodes.iter_mut().filter(|node| node.child.is_none()) {
                node.spawn(t0);
            }
        }
        Fault::Flush { .. } | Fault::Burst { .. } => {}
    }
}

fn terminate(shared: &Shared, index: usize) {
    let mut nodes = shared.nodes.lock().unwrap();
    if nodes[index].child.is_none() {
        return;
    }
    match nodes[index].terminate() {
        Some(status) if status.success() => {}
        Some(status) if before_handlers(&nodes[index], status) => {
            drop(nodes);
            shared.crash(index);
            shared.event(format!(
                "node {} died of SIGTERM before it could handle it",
                index + 1
            ));
        }
        Some(status) => {
            let tail = nodes[index].log_tail(40);
            drop(nodes);
            shared.anomaly(format!(
                "node {} exited with {status} after SIGTERM; its log ends:\n{tail}",
                index + 1
            ));
        }
        None => {
            drop(nodes);
            shared.crash(index);
            shared.anomaly(format!(
                "node {} did not exit within 30 s of SIGTERM",
                index + 1
            ));
        }
    }
}

/// Damages a stopped node's newest segment. A cut counts as a kill at this stop, and at the
/// previous one if the newest segment may be from the previous run.
fn damage_log(shared: &Shared, index: usize, damage: Damage, term: bool, rng: &mut Rng) {
    let nodes = shared.nodes.lock().unwrap();
    let node = &nodes[index];
    let mut segments: Vec<PathBuf> = fs::read_dir(&node.data_dir)
        .unwrap()
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            name.starts_with("log-") && name.ends_with(".seg")
        })
        .collect();
    segments.sort();
    let Some(newest) = segments.last() else {
        drop(nodes);
        shared.event(format!("node {} has no segment to damage", index + 1));
        return;
    };
    let len = fs::metadata(newest).unwrap().len();
    let file = OpenOptions::new().write(true).open(newest).unwrap();
    let (text, lossy) = match damage {
        Damage::Cut(bytes) => {
            let bytes = bytes.min(len.saturating_sub(SEGMENT_HEADER + 1));
            if bytes == 0 {
                (format!("{} is too short to cut", newest.display()), false)
            } else {
                file.set_len(len - bytes).unwrap();
                (
                    format!("cut {bytes} of {len} bytes off {}", newest.display()),
                    true,
                )
            }
        }
        Damage::PowerLoss(bytes) => {
            let contents = fs::read(newest).unwrap();
            let target = len.saturating_sub(bytes);
            let cut_at = entry_ends(&contents)
                .into_iter()
                .filter(|&end| end >= SEGMENT_HEADER && end <= target)
                .max()
                .unwrap_or(len);
            if cut_at >= len {
                (format!("{} is too short to cut", newest.display()), false)
            } else {
                file.set_len(cut_at).unwrap();
                (
                    format!(
                        "cut whole entries, {} of {len} bytes, off {}",
                        len - cut_at,
                        newest.display()
                    ),
                    true,
                )
            }
        }
        Damage::PowerLossInterval => {
            // What the next sync would have made durable: from the node's first own write in
            // the last fsync interval, with a margin as versions can run ahead of the clock.
            let contents = fs::read(newest).unwrap();
            let since = now_ms().saturating_sub(FSYNC_MS - 300);
            let origin = index as u64 + 1;
            let entries = decode_entries(&contents);
            let cut_at = entries
                .iter()
                .find(|(_, _, record)| {
                    record.as_ref().is_some_and(|record| {
                        record.version() & 0xff == origin
                            && record.version() >> MILLIS_SHIFT >= since
                    })
                })
                .map_or(len, |&(start, _, _)| start);
            if cut_at >= len {
                (format!("{} has nothing to cut", newest.display()), false)
            } else {
                file.set_len(cut_at).unwrap();
                (
                    format!(
                        "cut {} entries, {} of {len} bytes, off {}",
                        entries
                            .iter()
                            .filter(|&&(start, _, _)| start >= cut_at)
                            .count(),
                        len - cut_at,
                        newest.display()
                    ),
                    true,
                )
            }
        }
        Damage::Middle => {
            drop(file);
            let text = damage_middle(shared, index, &node.data_dir, &segments, rng);
            (text, false)
        }
        Damage::Garbage(bytes) => {
            let garbage: Vec<u8> = (0..bytes).map(|_| rng.next() as u8).collect();
            let mut file = file;
            file.write_all(&garbage).unwrap();
            (
                format!("appended {bytes} bytes to {}", newest.display()),
                false,
            )
        }
    };
    // A run this short may not have begun a segment of its own before it stopped.
    let previous_stop = (node.started.elapsed() < Duration::from_secs(5))
        .then_some(node.previous_stop)
        .flatten();
    drop(nodes);
    if lossy {
        if term {
            shared.crash(index);
        }
        if let Some(stop) = previous_stop {
            shared.crashes.lock().unwrap().push((index, stop));
        }
    }
    shared.event(format!("node {}: {text}", index + 1));
}

/// Every entry of a segment file up to the first torn one: start, end, and record if any.
fn decode_entries(contents: &[u8]) -> Vec<(u64, u64, Option<Record>)> {
    let mut entries = Vec::new();
    let mut input = contents;
    let mut scratch = Vec::new();
    let mut at = 0u64;
    while let Ok(ReadOutcome::Entry(entry)) = read_entry(&mut input, &mut scratch) {
        let end = (contents.len() - input.len()) as u64;
        let record = match entry {
            Entry::Record(record) => Some(record),
            _ => None,
        };
        entries.push((at, end, record));
        at = end;
    }
    entries
}

/// The key number of a workload key, from its name.
fn key_number(name: &[u8]) -> Option<u32> {
    std::str::from_utf8(name)
        .ok()?
        .strip_prefix("chaos:")?
        .parse()
        .ok()
        .filter(|&key| key < KEYS)
}

/// Damages one byte of a replayed entry in an older segment of a stopped node. Recovery cuts
/// the segment there and every peer full-syncs the node. The node's own changes in the hole
/// are noted in `holes` as maybe lost; other nodes' come back from their origins.
fn damage_middle(
    shared: &Shared,
    index: usize,
    dir: &Path,
    segments: &[PathBuf],
    rng: &mut Rng,
) -> String {
    let number = |path: &Path, prefix: &str, suffix: &str| -> Option<u64> {
        path.file_name()?
            .to_str()?
            .strip_prefix(prefix)?
            .strip_suffix(suffix)?
            .parse()
            .ok()
    };
    let replayed_from = fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| number(&entry.ok()?.path(), "snap-", ".snap"))
        .max()
        .unwrap_or(0);
    let starts: Vec<u64> = segments
        .iter()
        .map(|path| number(path, "log-", ".seg").unwrap())
        .collect();
    // Older segments holding replayed entries, else the newest (a hole too after a clean stop).
    let mut candidates: Vec<usize> = (0..segments.len().saturating_sub(1))
        .filter(|&at| starts[at + 1] > replayed_from)
        .collect();
    if candidates.is_empty() && !segments.is_empty() {
        candidates.push(segments.len() - 1);
    }
    if candidates.is_empty() {
        return "has no segment".into();
    }
    let at = candidates[rng.below(candidates.len() as u64) as usize];
    let path = &segments[at];
    let mut contents = fs::read(path).unwrap();
    let entries = decode_entries(&contents);
    // The header is the first entry; the entry after it has sequence number `starts[at]`.
    let eligible: Vec<usize> = (1..entries.len())
        .filter(|&entry| starts[at] + entry as u64 > replayed_from)
        .collect();
    if eligible.is_empty() {
        return format!("{} has no entry to damage", path.display());
    }
    // In the first four fifths, so the hole usually holds many entries.
    let pick = eligible[rng.below((eligible.len() as u64 * 4).div_ceil(5)) as usize];
    let (start, end, _) = entries[pick];
    // A byte of the payload, past the 8-byte frame header, fails the entry's checksum.
    let byte = start + 8 + rng.below(end - start - 8);
    contents[byte as usize] ^= 0x5a;
    fs::write(path, &contents).unwrap();
    let origin = index as u64 + 1;
    let mut lost = 0;
    let mut holes = shared.holes.lock().unwrap();
    for (_, _, record) in &entries[pick..] {
        let Some(record) = record else { continue };
        if record.version() & 0xff != origin {
            continue;
        }
        let key = match record {
            Record::Put { key, .. } | Record::Delete { key, .. } => match key_number(key) {
                Some(key) => Some(key),
                None => continue,
            },
            Record::Flush { .. } => None,
        };
        holes.push((index, key, record.version()));
        lost += 1;
    }
    format!(
        "damaged byte {byte} of {} (entry {} of {}); {lost} of its own changes follow it",
        path.display(),
        pick,
        entries.len() - 1
    )
}

/// What a restarted node held once serving, beside what its peers held just before the
/// restart. See `check_probes`.
struct Probe {
    node: usize,
    /// When the peers were read (wall-clock milliseconds).
    snapshot_at: u64,
    /// Each running peer's state then.
    peers: Vec<PeerState>,
    /// When the node was found serving, and its state then.
    ready_at: u64,
    state: Vec<Option<Held>>,
    /// `ready_by` from its status command.
    ready_by: String,
}

/// A node, by index, and what it held for each key.
type PeerState = (usize, Vec<Option<Held>>);

/// Reads every running peer of `node` that is not paused, before `node` restarts.
fn snapshot_peers(shared: &Shared, node: usize) -> (u64, Vec<PeerState>) {
    let running: Vec<usize> = {
        let nodes = shared.nodes.lock().unwrap();
        (0..nodes.len())
            .filter(|&other| other != node && nodes[other].child.is_some() && !nodes[other].paused)
            .collect()
    };
    let at = now_ms();
    let peers = running
        .into_iter()
        .filter_map(|other| Some((other, read_all(shared.client_ports[other]).ok()?)))
        .collect();
    (at, peers)
}

/// On its own thread, waits for `node`'s run `starts` to serve, then reads its state and
/// `ready_by`. Gives up if the run ends first.
fn spawn_probe(
    shared: &Arc<Shared>,
    node: usize,
    starts: u32,
    (snapshot_at, peers): (u64, Vec<PeerState>),
) {
    let shared = shared.clone();
    thread::spawn(move || {
        let port = shared.client_ports[node];
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let current = shared.nodes.lock().unwrap()[node].starts;
            if current != starts || Instant::now() > deadline || shared.stop_proxies.load(SeqCst) {
                return;
            }
            if let Some(conn) = Conn::open(port) {
                drop(conn);
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let ready_at = now_ms();
        let Ok(state) = read_all(port) else {
            return;
        };
        let repl_port = shared.nodes.lock().unwrap()[node].repl_port;
        let status = readiness(repl_port);
        let ready_by = status
            .split("; ")
            .find_map(|line| line.strip_prefix("STAT ready_by "))
            .unwrap_or("unknown")
            .to_string();
        shared.probes.lock().unwrap().push(Probe {
            node,
            snapshot_at,
            peers,
            ready_at,
            state,
            ready_by,
        });
    });
}

/// Checks each restarted node, once serving, against its stable peers' snapshot (see the
/// module's "Readiness"). A peer's value counts if it:
///
/// - is 200 ms older than the snapshot, so it left the origin's oplog for its backlog;
/// - came from another node, which served and had no fault (nor its links to the node) from a
///   second before the snapshot until the node served; the sender's reconnect backoff (at most
///   5 seconds) is within the 10 seconds a node waits for a peer to link back;
/// - was not maybe lost.
///
/// Nodes that served as last standing or after the sync timeout are skipped.
fn check_probes(
    history: &History,
    probes: &[Probe],
    faults: &[(Fault, u64, u64)],
) -> (usize, Vec<String>) {
    let node_quiet = |node: usize, from: u64, to: u64| {
        faults.iter().all(|(fault, start, end)| {
            let touches = match fault {
                Fault::Kill { node: other }
                | Fault::Term { node: other }
                | Fault::TermKill { node: other, .. }
                | Fault::Damage { node: other, .. }
                | Fault::Pause { node: other } => *other == node,
                Fault::KillAll => true,
                _ => false,
            };
            !touches || *start > to || end.saturating_add(1000) < from
        })
    };
    let link_quiet = |a: usize, b: usize, from: u64, to: u64| {
        faults.iter().all(|(fault, start, end)| {
            let touches = match fault {
                Fault::Cut { from: x, to: y, .. } | Fault::Flap { from: x, to: y } => {
                    (*x == a && *y == b) || (*x == b && *y == a)
                }
                _ => false,
            };
            !touches || *start > to || end.saturating_add(1000) < from
        })
    };
    let mut checked = 0;
    let mut problems = Vec::new();
    for probe in probes {
        if probe.ready_by == "last_standing" || probe.ready_by == "sync_timeout" {
            continue;
        }
        for (peer, state) in &probe.peers {
            for (key, held) in state.iter().enumerate() {
                let Some(held) = held else { continue };
                let origin = (held.cas & 0xff) as usize;
                if origin == 0 || origin - 1 == probe.node {
                    continue;
                }
                let origin = origin - 1;
                let serving = probe.peers.iter().any(|(other, _)| *other == origin);
                if (held.cas >> MILLIS_SHIFT) + 200 > probe.snapshot_at
                    || !serving
                    || !node_quiet(origin, probe.snapshot_at, probe.ready_at)
                    || !link_quiet(origin, probe.node, probe.snapshot_at - 1000, probe.ready_at)
                {
                    continue;
                }
                let key = key as u32;
                let Ok(found) = history.identify(key, held) else {
                    continue;
                };
                if history.maybe_lost(found.last) {
                    continue;
                }
                checked += 1;
                // With a tiny cap a full sync can omit a newer delete, leaving an older value.
                let deleted_later = || {
                    history.mode == Mode::TinyCap
                        && history.by_key.get(&key).into_iter().flatten().any(|op| {
                            matches!(op.kind, Kind::Delete { .. })
                                && history.versions(op).1 > held.cas
                        })
                };
                let fine = match &probe.state[key as usize] {
                    Some(now) => now.cas >= held.cas || deleted_later(),
                    None => history
                        .by_key
                        .get(&key)
                        .into_iter()
                        .flatten()
                        .chain(&history.flushes)
                        .filter(|op| !op.kind.writes())
                        .any(|op| history.versions(op).1 > held.cas),
                };
                if !fine {
                    problems.push(format!(
                        "node {} served (ready_by {}) {} ms after its restart without {} = {} \
                         (version {}, from node {}), which node {} held {} ms before the restart; \
                         it held {:?}",
                        probe.node + 1,
                        probe.ready_by,
                        probe.ready_at - probe.snapshot_at,
                        key_name(key),
                        held.value,
                        held.cas,
                        origin + 1,
                        peer + 1,
                        probe.snapshot_at.saturating_sub(held.cas >> MILLIS_SHIFT),
                        probe.state[key as usize]
                    ));
                }
            }
        }
    }
    (checked, problems)
}

/// Where each whole entry of a segment ends, header included. Entries are framed as
/// `[payload length: u32 LE][CRC-32: u32 LE][payload]` (`persist::codec`).
fn entry_ends(contents: &[u8]) -> Vec<u64> {
    let mut ends = Vec::new();
    let mut at = 0usize;
    while at + 8 <= contents.len() {
        let len = u32::from_le_bytes(contents[at..at + 4].try_into().unwrap()) as usize;
        if at + 8 + len > contents.len() {
            break;
        }
        at += 8 + len;
        ends.push(at as u64);
    }
    ends
}

/// Sends `flush_all` (delayed `delay` seconds if not 0), recording it as a change if accepted.
fn flush(shared: &Shared, first: usize, delay: u64) {
    // The first node from `first` that accepts, as a failing-over client would.
    let count = shared.client_ports.len();
    let Some((index, mut conn)) = (0..count)
        .map(|offset| (first + offset) % count)
        .find_map(|index| Some((index, Conn::open(shared.client_ports[index])?)))
    else {
        shared.event("every node refused the flush_all".into());
        return;
    };
    if index != first {
        shared.event(format!(
            "node {} refused; flush_all {delay} on node {}",
            first + 1,
            index + 1
        ));
    }
    let start = now_ms();
    let reply = if delay == 0 {
        conn.line("flush_all\r\n")
    } else {
        conn.line(&format!("flush_all {delay}\r\n"))
    };
    let end = now_ms();
    let outcome = match reply.as_deref() {
        Some("OK") => Outcome::Acked(None),
        _ => Outcome::Unknown,
    };
    shared.flushes.lock().unwrap().push(Op {
        key: u32::MAX,
        node: index as u8,
        kind: Kind::Flush { delay },
        start,
        end,
        outcome,
    });
}

// ---------------------------------------------------------------------------------------------
// Clients

#[derive(Clone, Copy, Debug)]
enum Kind {
    /// A put of a whole value, `value_for(key, id, pad)`, with flags `flags_of(id)`.
    Set {
        id: u64,
        pad: u16,
        classic: bool,
    },
    Delete {
        classic: bool,
    },
    /// `flush_all`, delayed by `delay` seconds if not 0.
    Flush {
        delay: u64,
    },
    /// An append or prepend of `piece(id, prepend)` to an append key.
    Append {
        id: u64,
        prepend: bool,
        classic: bool,
    },
    /// An increment or decrement by `delta`, and the value the reply gave.
    Arith {
        id: u64,
        delta: u64,
        decr: bool,
        classic: bool,
        result: Option<u64>,
    },
    /// A put of `value_of(id, pad)` to a CAS key, if the item's CAS token is `token`.
    Cas {
        id: u64,
        pad: u16,
        token: u64,
        classic: bool,
    },
}

impl Kind {
    /// The ID of a change that stores a value (a put or a read-modify-write).
    fn id(&self) -> Option<u64> {
        match *self {
            Kind::Set { id, .. }
            | Kind::Append { id, .. }
            | Kind::Arith { id, .. }
            | Kind::Cas { id, .. } => Some(id),
            Kind::Delete { .. } | Kind::Flush { .. } => None,
        }
    }

    /// Whether the change stores a value.
    fn writes(&self) -> bool {
        self.id().is_some()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    /// Acknowledged, with the CAS token if the reply carried one.
    Acked(Option<u64>),
    /// No reply: it may or may not take effect.
    Unknown,
}

#[derive(Clone, Copy, Debug)]
struct Op {
    key: u32,
    node: u8,
    kind: Kind,
    /// Wall-clock milliseconds before sending and after the reply.
    start: u64,
    end: u64,
    outcome: Outcome,
}

fn key_name(key: u32) -> String {
    format!("chaos:{key}")
}

fn value_of(id: u64, pad: u16) -> String {
    format!("v{id:x}:{}", "x".repeat(pad as usize))
}

/// What a put stores: an append key's base, a counter's `counter_base`, else `value_of`.
fn value_for(key: u32, id: u64, pad: u16) -> String {
    match class_of(key) {
        Class::Append => format!("P{id:x}."),
        Class::Counter => counter_base(id).to_string(),
        _ => value_of(id, pad),
    }
}

/// An append or prepend's piece. A value reads: prepends (newest first), base, appends.
fn piece(id: u64, prepend: bool) -> String {
    format!("{}{id:x}.", if prepend { 'R' } else { 'A' })
}

/// Bits below a counter's put ID: room for increments and decrements from the middle.
const COUNTER_SHIFT: u32 = 21;

/// Bits of a client's sequence number in a counter's put ID.
const COUNTER_SEQ_BITS: u32 = 26;

/// A counter put's value: its client and sequence number above `COUNTER_SHIFT` bits of room.
fn counter_base(id: u64) -> u64 {
    let (client, seq) = (id >> 40, id & ((1 << 40) - 1));
    assert!(
        client < 8 && seq < 1 << COUNTER_SEQ_BITS,
        "counter put ID {id:x} out of range"
    );
    (((client << COUNTER_SEQ_BITS) | seq) << COUNTER_SHIFT) | (1 << (COUNTER_SHIFT - 1))
}

/// The ID of the put a counter's value grew from.
fn counter_id(value: u64) -> u64 {
    let small = value >> COUNTER_SHIFT;
    ((small >> COUNTER_SEQ_BITS) << 40) | (small & ((1 << COUNTER_SEQ_BITS) - 1))
}

fn flags_of(id: u64) -> u32 {
    (id.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as u32
}

/// Every port the cluster's nodes and proxies listen on.
static CLUSTER_PORTS: Mutex<Vec<u16>> = Mutex::new(Vec::new());

/// Client sockets whose connects crossed; see `Conn::open`.
static CROSSED: AtomicU64 = AtomicU64::new(0);

/// A client connection that reads replies line by line.
struct Conn {
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn open(port: u16) -> Option<Self> {
        Self::open_with_timeout(port, Duration::from_secs(2))
    }

    /// Opens a connection whose reads and writes time out after `timeout`.
    fn open_with_timeout(port: u16, timeout: Duration) -> Option<Self> {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let stream = TcpStream::connect_timeout(&address, Duration::from_secs(1)).ok()?;
        // Node ports are ephemeral, so two clients connecting to down nodes can get each
        // other's target as their source port and connect to each other (TCP simultaneous
        // open). Such a socket reached no node: count it as refused.
        let local = stream.local_addr().ok()?.port();
        if CLUSTER_PORTS.lock().unwrap().contains(&local) {
            CROSSED.fetch_add(1, SeqCst);
            return None;
        }
        stream.set_read_timeout(Some(timeout)).ok()?;
        stream.set_write_timeout(Some(timeout)).ok()?;
        stream.set_nodelay(true).ok()?;
        Some(Self {
            reader: BufReader::new(stream),
        })
    }

    fn send(&mut self, request: &str) -> io::Result<()> {
        self.reader.get_mut().write_all(request.as_bytes())
    }

    fn read_line(&mut self) -> io::Result<String> {
        let mut line = String::new();
        if self.reader.read_line(&mut line)? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        if !line.ends_with("\r\n") {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        line.truncate(line.len() - 2);
        Ok(line)
    }

    /// Sends `request` and returns the first line of the reply.
    fn line(&mut self, request: &str) -> Option<String> {
        self.send(request).ok()?;
        self.read_line().ok()
    }

    /// Sends `request` and returns the reply's lines before `last`.
    fn lines(&mut self, request: &str, last: &str) -> io::Result<Vec<String>> {
        self.send(request)?;
        let mut lines = Vec::new();
        loop {
            let line = self.read_line()?;
            if line == last {
                return Ok(lines);
            }
            lines.push(line);
        }
    }
}

/// What one client thread did.
#[derive(Default)]
struct ClientLog {
    ops: Vec<Op>,
    refused: u64,
    odd_replies: Vec<String>,
    /// Values of read-modify-write keys read during the run: (key, node, when, value).
    reads: Vec<(u32, u8, u64, Held)>,
}

fn run_client(shared: &Shared, index: usize, seed: u64) -> ClientLog {
    let mut rng = Rng::derive(seed, 100 + index as u64);
    let nodes = shared.client_ports.len();
    let mut conns: Vec<Option<Conn>> = (0..nodes).map(|_| None).collect();
    let mut log = ClientLog::default();
    let mut seq = 0u64;
    // The last CAS token read per key. Half the conditional writes reuse one, possibly seconds
    // old, as an application would; a flush can fall in between.
    let mut tokens: HashMap<u32, u64> = HashMap::new();
    while !shared.stop_workload.load(SeqCst) {
        let node = rng.below(nodes as u64) as usize;
        let share = rng.below(100);
        let key = if share < COLD_PERCENT {
            HOT_KEYS + rng.below(u64::from(COLD_KEYS)) as u32
        } else if share < COLD_PERCENT + RMW_PERCENT {
            HOT_KEYS
                + COLD_KEYS
                + rng.below(u64::from(APPEND_KEYS + COUNTER_KEYS + CAS_KEYS)) as u32
        } else {
            rng.below(u64::from(HOT_KEYS)) as u32
        };
        let pick = rng.below(100);
        let pad = rng.below(48) as u16;
        let delta = rng.range(1, 1000);
        let pause = rng.range(0, 1500);
        let stale = rng.chance(50);
        seq += 1;
        let id = ((index as u64 + 1) << 40) | seq;
        if conns[node].is_none() {
            conns[node] = Conn::open(shared.client_ports[node]);
            if conns[node].is_none() {
                log.refused += 1;
                thread::sleep(Duration::from_millis(2));
                continue;
            }
        }
        // A conditional write needs the item's CAS token first.
        let token = match (class_of(key), pick) {
            (Class::Cas, 33..=89) if stale && tokens.contains_key(&key) => {
                tokens.get(&key).copied()
            }
            (Class::Cas, 33..=89) => {
                match read_token(conns[node].as_mut().unwrap(), key, pick < 65) {
                    Ok(Some(token)) => {
                        tokens.insert(key, token);
                        Some(token)
                    }
                    Ok(None) => continue,
                    Err(line) => {
                        if let Some(line) = line {
                            odd(&mut log, &mut conns[node], &line);
                        } else {
                            conns[node] = None;
                        }
                        continue;
                    }
                }
            }
            _ => None,
        };
        let (mut kind, request) = plan_request(key, pick, id, pad, delta, token);
        let conn = conns[node].as_mut().unwrap();
        let start = now_ms();
        let reply = conn
            .send(&request)
            .and_then(|()| conn.read_line())
            .and_then(|line| {
                let value = if line.starts_with("VA ") {
                    Some(conn.read_line()?)
                } else {
                    None
                };
                Ok((line, value))
            });
        let end = now_ms();
        let outcome = match reply {
            Err(_) => {
                conns[node] = None;
                Some(Outcome::Unknown)
            }
            Ok((line, value)) => {
                // Read-modify-write values are checked later, with the final state.
                if kind.is_none()
                    && !matches!(class_of(key), Class::Hot | Class::Cold)
                    && let Some(held) = value.as_ref().and_then(|value| parse_held(&line, value))
                {
                    log.reads.push((key, node as u8, end, held));
                }
                match judge_reply(&mut kind, &line, value.as_deref()) {
                    Ok(outcome) => outcome,
                    Err(()) => Some(odd(&mut log, &mut conns[node], &line)),
                }
            }
        };
        if let (Some(kind), Some(outcome)) = (kind, outcome) {
            log.ops.push(Op {
                key,
                node: node as u8,
                kind,
                start,
                end,
                outcome,
            });
        }
        if pause > 0 {
            thread::sleep(Duration::from_micros(pause));
        }
    }
    log
}

/// Reads a CAS key's token with `mg` if `meta`, else `gets`: `None` if absent, and on an error
/// the reply, if any.
fn read_token(conn: &mut Conn, key: u32, meta: bool) -> Result<Option<u64>, Option<String>> {
    let name = key_name(key);
    if meta {
        let line = conn.line(&format!("mg {name} c\r\n")).ok_or(None)?;
        if line == "EN" {
            return Ok(None);
        }
        return match line.strip_prefix("HD c").and_then(|cas| cas.parse().ok()) {
            Some(cas) => Ok(Some(cas)),
            None => Err(Some(line)),
        };
    }
    let line = conn.line(&format!("gets {name}\r\n")).ok_or(None)?;
    if line == "END" {
        return Ok(None);
    }
    let cas = line
        .strip_prefix(&format!("VALUE {name} "))
        .and_then(|rest| rest.split(' ').nth(2))
        .and_then(|cas| cas.parse().ok())
        .ok_or(Some(line.clone()))?;
    conn.read_line().map_err(|_| None)?;
    match conn.read_line() {
        Ok(end) if end == "END" => Ok(Some(cas)),
        Ok(end) => Err(Some(end)),
        Err(_) => Err(None),
    }
}

/// The change a request makes, if any, and the request; `pick` (0 to 99) chooses it.
fn plan_request(
    key: u32,
    pick: u64,
    id: u64,
    pad: u16,
    delta: u64,
    token: Option<u64>,
) -> (Option<Kind>, String) {
    let name = key_name(key);
    let class = class_of(key);
    let set = |classic: bool| {
        let value = value_for(key, id, pad);
        let request = if classic {
            format!(
                "set {name} {} 0 {}\r\n{value}\r\n",
                flags_of(id),
                value.len()
            )
        } else {
            format!(
                "ms {name} {} c F{}\r\n{value}\r\n",
                value.len(),
                flags_of(id)
            )
        };
        (Some(Kind::Set { id, pad, classic }), request)
    };
    let delete = |classic: bool| {
        let request = if classic {
            format!("delete {name}\r\n")
        } else {
            format!("md {name}\r\n")
        };
        (Some(Kind::Delete { classic }), request)
    };
    let read = (None, format!("mg {name} v f c\r\n"));
    match class {
        // Cold keys mostly get sets: a lost delete of an absent key would change nothing.
        Class::Hot | Class::Cold => {
            let pick = match (class, pick) {
                (Class::Hot, pick) => pick,
                (_, 0..=69) => 0,
                (_, 70..=84) => 45,
                (_, 85..=94) => 55,
                (_, 95..=97) => 80,
                (_, _) => 90,
            };
            match pick {
                0..=44 => set(false),
                45..=54 => set(true),
                55..=79 => delete(false),
                80..=86 => delete(true),
                _ => read,
            }
        }
        Class::Append => {
            let append = |prepend: bool, classic: bool| {
                let piece = piece(id, prepend);
                let request = match (prepend, classic) {
                    (false, true) => format!("append {name} 0 0 {}\r\n{piece}\r\n", piece.len()),
                    (true, true) => format!("prepend {name} 0 0 {}\r\n{piece}\r\n", piece.len()),
                    (false, false) => format!("ms {name} {} MA c\r\n{piece}\r\n", piece.len()),
                    (true, false) => format!("ms {name} {} MP c\r\n{piece}\r\n", piece.len()),
                };
                let kind = Kind::Append {
                    id,
                    prepend,
                    classic,
                };
                (Some(kind), request)
            };
            match pick {
                0..=14 => set(false),
                15..=24 => set(true),
                25..=31 => delete(false),
                32..=34 => delete(true),
                35..=59 => append(false, false),
                60..=69 => append(false, true),
                70..=84 => append(true, false),
                85..=88 => append(true, true),
                _ => read,
            }
        }
        Class::Counter => {
            let arith = |decr: bool, classic: bool| {
                let request = match (decr, classic) {
                    (false, true) => format!("incr {name} {delta}\r\n"),
                    (true, true) => format!("decr {name} {delta}\r\n"),
                    (false, false) => format!("ma {name} D{delta} v c\r\n"),
                    (true, false) => format!("ma {name} MD D{delta} v c\r\n"),
                };
                let kind = Kind::Arith {
                    id,
                    delta,
                    decr,
                    classic,
                    result: None,
                };
                (Some(kind), request)
            };
            match pick {
                0..=12 => set(false),
                13..=19 => set(true),
                20..=25 => delete(false),
                26..=27 => delete(true),
                28..=57 => arith(false, false),
                58..=71 => arith(true, false),
                72..=81 => arith(false, true),
                82..=88 => arith(true, true),
                _ => read,
            }
        }
        Class::Cas => match (pick, token) {
            (0..=17, _) => set(false),
            (18..=24, _) => set(true),
            (25..=30, _) => delete(false),
            (31..=32, _) => delete(true),
            (33..=89, Some(token)) => {
                let value = value_of(id, pad);
                let classic = pick >= 65;
                let request = if classic {
                    format!(
                        "cas {name} {} 0 {} {token}\r\n{value}\r\n",
                        flags_of(id),
                        value.len()
                    )
                } else {
                    format!(
                        "ms {name} {} C{token} c F{}\r\n{value}\r\n",
                        value.len(),
                        flags_of(id)
                    )
                };
                let kind = Kind::Cas {
                    id,
                    pad,
                    token,
                    classic,
                };
                (Some(kind), request)
            }
            _ => read,
        },
    }
}

/// Judges a reply to `kind` (`None` for a read): `Ok(Some(_))` for a change, `Ok(None)` for no
/// change, `Err` for a reply the protocol does not allow. Fills in an increment's result.
fn judge_reply(
    kind: &mut Option<Kind>,
    line: &str,
    value: Option<&str>,
) -> Result<Option<Outcome>, ()> {
    let meta_cas = |line: &str| -> Option<u64> { line.strip_prefix("HD c")?.parse().ok() };
    const TOO_LARGE: &str = "SERVER_ERROR object too large for cache";
    // A meta reply's code, without the flags it returns (`NS c0`).
    let code = line.split(' ').next().unwrap_or_default();
    match kind {
        None => match line {
            "EN" => Ok(None),
            _ if line.starts_with("VA ") => Ok(None),
            _ => Err(()),
        },
        Some(Kind::Set { classic: false, .. }) => meta_cas(line)
            .map(|cas| Some(Outcome::Acked(Some(cas))))
            .ok_or(()),
        Some(Kind::Set { classic: true, .. }) => match line {
            "STORED" => Ok(Some(Outcome::Acked(None))),
            _ => Err(()),
        },
        Some(Kind::Delete { classic: false }) => match line {
            "HD" | "NF" => Ok(Some(Outcome::Acked(None))),
            _ => Err(()),
        },
        Some(Kind::Delete { classic: true }) => match line {
            "DELETED" | "NOT_FOUND" => Ok(Some(Outcome::Acked(None))),
            _ => Err(()),
        },
        Some(Kind::Append { classic: false, .. }) => match code {
            "NS" => Ok(None),
            _ if line == TOO_LARGE => Ok(None),
            _ => meta_cas(line)
                .map(|cas| Some(Outcome::Acked(Some(cas))))
                .ok_or(()),
        },
        Some(Kind::Append { classic: true, .. }) => match line {
            "STORED" => Ok(Some(Outcome::Acked(None))),
            "NOT_STORED" | TOO_LARGE => Ok(None),
            _ => Err(()),
        },
        Some(Kind::Arith {
            classic: false,
            result,
            ..
        }) => {
            if code == "NF" {
                return Ok(None);
            }
            // `VA <length> c<cas>`, then the value.
            let mut tokens = line.split(' ');
            let cas = match (tokens.next(), tokens.next(), tokens.next(), tokens.next()) {
                (Some("VA"), Some(_), Some(cas), None) => {
                    cas.strip_prefix('c').and_then(|cas| cas.parse().ok())
                }
                _ => None,
            };
            let number = value.and_then(|value| value.parse().ok());
            match (cas, number) {
                (Some(cas), Some(number)) => {
                    *result = Some(number);
                    Ok(Some(Outcome::Acked(Some(cas))))
                }
                _ => Err(()),
            }
        }
        Some(Kind::Arith {
            classic: true,
            result,
            ..
        }) => {
            if line == "NOT_FOUND" {
                return Ok(None);
            }
            let number = line.parse().map_err(|_| ())?;
            *result = Some(number);
            Ok(Some(Outcome::Acked(None)))
        }
        Some(Kind::Cas { classic: false, .. }) => match code {
            "EX" | "NF" => Ok(None),
            _ => meta_cas(line)
                .map(|cas| Some(Outcome::Acked(Some(cas))))
                .ok_or(()),
        },
        Some(Kind::Cas { classic: true, .. }) => match line {
            "STORED" => Ok(Some(Outcome::Acked(None))),
            "EXISTS" | "NOT_FOUND" => Ok(None),
            _ => Err(()),
        },
        Some(Kind::Flush { .. }) => unreachable!("clients send no flush"),
    }
}

/// Sends `node` a pipelined burst of `count` large sets to cold keys, recording them as changes.
fn burst(shared: &Shared, node: usize, count: u64, seed: u64) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let mut rng = Rng::new(seed);
    let Some(mut conn) = Conn::open(shared.client_ports[node]) else {
        shared.event(format!("node {} refused the burst", node + 1));
        return;
    };
    let start = now_ms();
    let mut ops = Vec::with_capacity(count as usize);
    let mut alive = true;
    let mut odd_reply = None;
    // Chunks of 250, so the replies meanwhile fit the socket buffer.
    let mut left = count;
    while left > 0 {
        let chunk = left.min(250);
        left -= chunk;
        let mut planned = Vec::new();
        let mut request = String::new();
        for _ in 0..chunk {
            let key = HOT_KEYS + rng.below(u64::from(COLD_KEYS)) as u32;
            let id = (16 << 40) | NEXT.fetch_add(1, SeqCst);
            // Below the smallest item size limit the nodes run with.
            let pad = rng.range(300, 900) as u16;
            let value = value_of(id, pad);
            let _ = write!(
                request,
                "ms {} {} c F{}\r\n{value}\r\n",
                key_name(key),
                value.len(),
                flags_of(id)
            );
            planned.push((key, id, pad));
        }
        let chunk_start = now_ms();
        alive &= conn.send(&request).is_ok();
        for (key, id, pad) in planned {
            let outcome = if alive {
                match conn.read_line() {
                    Ok(line) => match line.strip_prefix("HD c").and_then(|cas| cas.parse().ok()) {
                        Some(cas) => Outcome::Acked(Some(cas)),
                        None => {
                            odd_reply.get_or_insert(line);
                            alive = false;
                            Outcome::Unknown
                        }
                    },
                    Err(_) => {
                        alive = false;
                        Outcome::Unknown
                    }
                }
            } else {
                Outcome::Unknown
            };
            ops.push(Op {
                key,
                node: node as u8,
                kind: Kind::Set {
                    id,
                    pad,
                    classic: false,
                },
                start: chunk_start,
                end: now_ms(),
                outcome,
            });
        }
        if !alive {
            break;
        }
    }
    let acked = ops
        .iter()
        .filter(|op| matches!(op.outcome, Outcome::Acked(_)))
        .count();
    shared.burst_ops.lock().unwrap().extend(ops);
    if let Some(line) = odd_reply {
        shared.anomaly(format!(
            "node {} answered a burst set with {line:?}",
            node + 1
        ));
    }
    shared.event(format!(
        "node {} acknowledged {acked} of {count} burst sets in {} ms",
        node + 1,
        now_ms() - start
    ));
}

/// Notes a disallowed reply and drops the connection, whose framing can't be trusted after it.
fn odd(log: &mut ClientLog, conn: &mut Option<Conn>, line: &str) -> Outcome {
    log.odd_replies.push(line.to_string());
    *conn = None;
    Outcome::Unknown
}

// ---------------------------------------------------------------------------------------------
// Healing and reading the final state

/// Waits until every node serves clients.
fn wait_serving(shared: &Shared, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    for (index, &port) in shared.client_ports.iter().enumerate() {
        while Conn::open(port).is_none() {
            check_exits(shared);
            if Instant::now() > deadline {
                let repl_port = shared.nodes.lock().unwrap()[index].repl_port;
                return Err(format!(
                    "node {} did not serve clients again; its replication status: {}",
                    index + 1,
                    readiness(repl_port)
                ));
            }
            thread::sleep(Duration::from_millis(100));
        }
    }
    Ok(())
}

/// The replication port's `status` reply, on one line.
fn readiness(port: u16) -> String {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return "unreachable".into();
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
    let mut reply = String::new();
    let _ = stream
        .write_all(b"status\r\n")
        .and_then(|()| stream.read_to_string(&mut reply));
    reply.replace("\r\n", "; ")
}

/// The `stats repl` lines of a node, as (name, value).
fn repl_lines(port: u16) -> Option<Vec<(String, String)>> {
    stat_lines(port, "stats repl\r\n")
}

fn stat_lines(port: u16, command: &str) -> Option<Vec<(String, String)>> {
    let mut conn = Conn::open(port)?;
    let lines = conn.lines(command, "END").ok()?;
    Some(
        lines
            .iter()
            .filter_map(|line| line.strip_prefix("STAT "))
            .filter_map(|line| line.split_once(' '))
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
    )
}

/// Whether every link streams both ways and every log is fully acknowledged.
fn quiet(shared: &Shared) -> bool {
    let peers = shared.client_ports.len() - 1;
    shared.client_ports.iter().all(|&port| {
        let Some(lines) = repl_lines(port) else {
            return false;
        };
        let values = |suffix: &str| -> Vec<&str> {
            lines
                .iter()
                .filter(|(name, _)| name.ends_with(suffix))
                .map(|(_, value)| value.as_str())
                .collect()
        };
        let out = values(":out");
        let incoming = values(":in");
        let lag = values(":lag");
        out.len() == peers
            && incoming.len() == peers
            && out
                .iter()
                .chain(&incoming)
                .all(|state| *state == "streaming")
            && lag.iter().all(|lag| *lag == "0")
    })
}

fn wait_quiet(shared: &Shared, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    let mut streak = 0;
    while streak < 3 {
        if Instant::now() > deadline {
            let mut text = String::from("the links did not settle; stats repl:\n");
            for (index, &port) in shared.client_ports.iter().enumerate() {
                let _ = writeln!(text, "node {}: {:?}", index + 1, repl_lines(port));
            }
            return Err(text);
        }
        check_exits(shared);
        streak = if quiet(shared) { streak + 1 } else { 0 };
        thread::sleep(Duration::from_millis(300));
    }
    Ok(())
}

/// Stops every node (SIGKILL if `kill`, else SIGTERM), restarts them, waits for the links to
/// settle, and reads every node's state.
fn restart_all(shared: &Shared, kill: bool) -> Result<Vec<Vec<Option<Held>>>, String> {
    let t0 = shared.t0();
    {
        let mut nodes = shared.nodes.lock().unwrap();
        for (index, node) in nodes.iter_mut().enumerate() {
            if kill {
                node.kill();
                continue;
            }
            match node.terminate() {
                Some(status) if status.success() => {}
                status => {
                    return Err(format!(
                        "node {} stopped with {status:?}; its log ends:\n{}",
                        index + 1,
                        node.log_tail(40)
                    ));
                }
            }
        }
        for node in nodes.iter_mut() {
            node.spawn(t0);
        }
    }
    wait_serving(shared, Duration::from_secs(150))?;
    wait_quiet(shared, Duration::from_secs(150))?;
    shared
        .client_ports
        .iter()
        .map(|&port| read_all(port))
        .collect::<io::Result<Vec<_>>>()
        .map_err(|error| format!("cannot read the state: {error}"))
}

/// What a node holds for a key.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Held {
    value: String,
    flags: u32,
    cas: u64,
}

/// A held value from an `mg ... v f c` hit: its `VA` line and the value.
fn parse_held(line: &str, value: &str) -> Option<Held> {
    let mut tokens = line.split(' ');
    if tokens.next() != Some("VA") {
        return None;
    }
    tokens.next();
    let (mut flags, mut cas) = (None, None);
    for token in tokens.filter(|token| !token.is_empty()) {
        match token.split_at(1) {
            ("f", number) => flags = number.parse().ok(),
            ("c", number) => cas = number.parse().ok(),
            _ => {}
        }
    }
    Some(Held {
        value: value.to_string(),
        flags: flags?,
        cas: cas?,
    })
}

fn read_all(port: u16) -> io::Result<Vec<Option<Held>>> {
    let mut conn = Conn::open(port).ok_or(io::ErrorKind::ConnectionRefused)?;
    conn.reader
        .get_ref()
        .set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut held = Vec::with_capacity(KEYS as usize);
    for chunk in (0..KEYS).collect::<Vec<_>>().chunks(500) {
        let mut request = String::new();
        for &key in chunk {
            let _ = write!(request, "mg {} v f c\r\n", key_name(key));
        }
        request.push_str("mn\r\n");
        conn.send(&request)?;
        for _ in chunk {
            let line = conn.read_line()?;
            if line == "EN" {
                held.push(None);
                continue;
            }
            let mut tokens = line.split(' ');
            if tokens.next() != Some("VA") {
                return Err(io::Error::other(format!("unexpected reply {line:?}")));
            }
            tokens.next();
            let (mut flags, mut cas) = (None, None);
            for token in tokens.filter(|token| !token.is_empty()) {
                match token.split_at(1) {
                    ("f", number) => flags = number.parse().ok(),
                    ("c", number) => cas = number.parse().ok(),
                    _ => {}
                }
            }
            let value = conn.read_line()?;
            match (flags, cas) {
                (Some(flags), Some(cas)) => held.push(Some(Held { value, flags, cas })),
                _ => return Err(io::Error::other(format!("unexpected reply {line:?}"))),
            }
        }
        let end = conn.read_line()?;
        if end != "MN" {
            return Err(io::Error::other(format!("expected MN, not {end:?}")));
        }
    }
    Ok(held)
}

// ---------------------------------------------------------------------------------------------
// The oracle

/// Every change, indexed for the oracle.
struct History {
    /// Each key's changes.
    by_key: HashMap<u32, Vec<Op>>,
    flushes: Vec<Op>,
    /// Each change that stores a value, by its ID.
    puts: HashMap<u64, Op>,
    crashes: Vec<(usize, u64)>,
    /// When each node's processes stopped, in order (wall-clock milliseconds).
    stops: Vec<Vec<u64>>,
    /// When each node was paused: (start, end), wall-clock milliseconds.
    pauses: Vec<Vec<(u64, u64)>>,
    /// When oplogs dropped records.
    drops: Vec<Drops>,
    /// Nodes' own changes lost to damage inside their logs.
    holes: Vec<(usize, Option<u32>, u64)>,
    /// When the final state was read.
    end: u64,
    /// When the workload started, for showing times.
    t0: u64,
    /// The most the wall clock went back, in milliseconds: the hybrid clock may run this far
    /// ahead, so versions are judged with this slack.
    slack: u64,
    mode: Mode,
}

/// When a delayed `flush_all` took effect, if it did.
struct Delayed {
    /// Wall-clock milliseconds: not before `low`, and if at all, by `high`.
    low: u64,
    high: u64,
    /// Acknowledged, its node ran until due, and no later `flush_all` could replace it.
    sure: bool,
}

/// A held value's origin: the change that produced it, and the changes whose content it
/// carries, oldest first, in each possible application order.
struct Found<'a> {
    last: &'a Op,
    chains: Vec<Vec<&'a Op>>,
}

impl History {
    /// The range of versions a change can have. An unanswered one ran, if at all, by its
    /// node's next stop or the final read.
    fn versions(&self, op: &Op) -> (u64, u64) {
        let low = |millis: u64| millis.saturating_sub(self.slack) << MILLIS_SHIFT;
        let by = |millis: u64| ((millis + self.slack + 1) << MILLIS_SHIFT) - 1;
        if let Kind::Flush { delay } = op.kind
            && delay > 0
        {
            let delayed = self.delayed(op);
            return (low(delayed.low), by(delayed.high));
        }
        match op.outcome {
            Outcome::Acked(Some(cas)) => (cas, cas),
            Outcome::Acked(None) => (low(op.start), by(op.end)),
            Outcome::Unknown => (low(op.start), by(self.next_stop(op).min(self.end))),
        }
    }

    /// When the process that received `op` stopped, or the final read.
    fn next_stop(&self, op: &Op) -> u64 {
        self.stops
            .get(op.node as usize)
            .and_then(|stops| stops.iter().find(|&&stop| stop >= op.start).copied())
            .unwrap_or(self.end)
    }

    /// When a delayed `flush_all` took effect (see the module's "Delayed flushes").
    fn delayed(&self, op: &Op) -> Delayed {
        let Kind::Flush { delay } = op.kind else {
            unreachable!("not a flush")
        };
        let node = op.node as usize;
        let low = (op.start / 1000 + delay) * 1000;
        let mut due = (op.end / 1000 + delay + 1) * 1000 + 1500;
        if op.outcome == Outcome::Unknown {
            // A paused node may read the request any time after it resumes.
            due = self.next_stop(op);
        }
        loop {
            let resumed = self
                .pauses
                .get(node)
                .into_iter()
                .flatten()
                .find(|&&(start, end)| start <= due && end + 1500 > due && end >= op.start);
            match resumed {
                Some(&(_, end)) => due = end + 1500,
                None => break,
            }
        }
        let stop = self.next_stop(op);
        let replaced = self
            .flushes
            .iter()
            .any(|other| other.node == op.node && other.start > op.start && other.start <= due);
        Delayed {
            low,
            high: due.min(stop).min(self.end),
            sure: matches!(op.outcome, Outcome::Acked(_)) && stop > due && !replaced,
        }
    }

    /// Whether an acknowledged change may be lost: its node was killed during or soon after
    /// it, or while it was dropped and uncovered, or damage in its log took it.
    fn maybe_lost(&self, op: &Op) -> bool {
        if !matches!(op.outcome, Outcome::Acked(_)) {
            return false;
        }
        let node = op.node as usize;
        let ran_by = match op.kind {
            Kind::Flush { delay } if delay > 0 => self.delayed(op).high,
            _ => op.end,
        };
        let crashed = |from: u64, to: u64| {
            self.crashes
                .iter()
                .any(|&(at_node, at)| at_node == node && from <= at && at <= to)
        };
        if crashed(op.start, ran_by + LOSS_WINDOW_MS) {
            return true;
        }
        let dropped = self.drops.iter().any(|drops| {
            drops.node == node
                && op.start <= drops.to
                && ran_by >= drops.from
                && crashed(op.start, drops.covered)
        });
        if dropped {
            return true;
        }
        let key = match op.kind {
            Kind::Flush { .. } => None,
            _ => Some(op.key),
        };
        let (low, high) = self.versions(op);
        self.holes.iter().any(|&(at_node, at_key, version)| {
            at_node == node && at_key == key && (low..=high).contains(&version)
        })
    }

    /// Whether a change must have taken effect and survived.
    fn certain(&self, op: &Op) -> bool {
        match op.kind {
            Kind::Delete { .. } if self.mode == Mode::TinyCap => false,
            Kind::Flush { delay } if delay > 0 => self.delayed(op).sure && !self.maybe_lost(op),
            _ => matches!(op.outcome, Outcome::Acked(_)) && !self.maybe_lost(op),
        }
    }

    /// A `flush_all` made on `change`'s node, surely before it, versioned between `above` and
    /// `change`. A node versions a change and checks the flush floor in one step, so `change`
    /// cannot carry content from below such a flush. Flushes from peers are not judged.
    fn flushed_between(&self, above: u64, change: &Op) -> Option<&Op> {
        let (low, _) = self.versions(change);
        self.flushes.iter().find(|flush| {
            let (flush_low, flush_high) = self.versions(flush);
            flush.node == change.node
                && flush_low > above
                && flush_high < low
                && self.certain(flush)
        })
    }

    /// Finds where a held value came from, or says why no change can have produced it.
    fn identify(&self, key: u32, held: &Held) -> Result<Found<'_>, String> {
        let put = |id: u64| -> Result<&Op, String> {
            self.puts
                .get(&id)
                .filter(|op| op.key == key)
                .ok_or_else(|| format!("holds a value no client wrote to it: {held:?}"))
        };
        match class_of(key) {
            Class::Hot | Class::Cold | Class::Cas => {
                let id = parse_id(&held.value)
                    .ok_or_else(|| format!("holds a value no client wrote: {held:?}"))?;
                let op = put(id)?;
                let pad = match op.kind {
                    Kind::Set { pad, .. } | Kind::Cas { pad, .. } => pad,
                    _ => {
                        return Err(format!(
                            "holds the ID of a change of another kind: {held:?}"
                        ));
                    }
                };
                if held.value != value_of(id, pad) || held.flags != flags_of(id) {
                    return Err(format!("holds a damaged value: {held:?}"));
                }
                Ok(Found {
                    last: op,
                    chains: Vec::new(),
                })
            }
            Class::Append => self.identify_pieces(held, put),
            Class::Counter => self.identify_counter(key, held, put),
        }
    }

    /// An append key's value: prepends (newest first), a put's base, appends (oldest first).
    fn identify_pieces<'a>(
        &'a self,
        held: &Held,
        put: impl Fn(u64) -> Result<&'a Op, String>,
    ) -> Result<Found<'a>, String> {
        let damaged = || format!("holds a damaged value: {held:?}");
        let mut before = Vec::new();
        let mut base = None;
        let mut after = Vec::new();
        let Some(body) = held.value.strip_suffix('.') else {
            return Err(damaged());
        };
        for piece in body.split('.') {
            let (tag, hex) = piece.split_at_checked(1).ok_or_else(damaged)?;
            let id = u64::from_str_radix(hex, 16).map_err(|_| damaged())?;
            let op = put(id)?;
            let fits = match (tag, op.kind) {
                ("P", Kind::Set { .. }) => base.is_none() && after.is_empty(),
                ("R", Kind::Append { prepend: true, .. }) => base.is_none(),
                ("A", Kind::Append { prepend: false, .. }) => base.is_some(),
                _ => false,
            };
            if !fits {
                return Err(damaged());
            }
            match tag {
                "P" => base = Some(op),
                "R" => before.push(op),
                _ => after.push(op),
            }
        }
        let base = base.ok_or_else(damaged)?;
        let Kind::Set { id, .. } = base.kind else {
            unreachable!()
        };
        if held.flags != flags_of(id) {
            return Err(damaged());
        }
        let pieces: Vec<u64> = before
            .iter()
            .chain(&after)
            .filter_map(|op| op.kind.id())
            .collect();
        if pieces
            .iter()
            .enumerate()
            .any(|(at, id)| pieces[..at].contains(id))
        {
            return Err(format!("holds a piece twice: {held:?}"));
        }
        // The newest prepend is first; the newest append is last.
        before.reverse();
        let ends: Vec<&Op> = [before.last(), after.last()]
            .into_iter()
            .flatten()
            .copied()
            .collect();
        let last = match ends.as_slice() {
            [] => base,
            [one] => one,
            [first, second] => {
                let holds = |op: &Op| {
                    let (low, high) = self.versions(op);
                    (low..=high).contains(&held.cas)
                };
                if holds(second) { second } else { first }
            }
            _ => unreachable!(),
        };
        let chains = [before, after]
            .into_iter()
            .filter(|pieces| !pieces.is_empty())
            .map(|pieces| std::iter::once(base).chain(pieces).collect())
            .collect();
        Ok(Found { last, chains })
    }

    /// A counter's value is its put's number moved by later increments and decrements.
    fn identify_counter<'a>(
        &'a self,
        key: u32,
        held: &Held,
        put: impl Fn(u64) -> Result<&'a Op, String>,
    ) -> Result<Found<'a>, String> {
        let number: u64 = held
            .value
            .parse()
            .map_err(|_| format!("holds a counter that is not a number: {held:?}"))?;
        let base = put(counter_id(number))?;
        let Kind::Set { id, .. } = base.kind else {
            return Err(format!("holds a counter no put began: {held:?}"));
        };
        if held.flags != flags_of(id) {
            return Err(format!("holds a damaged counter: {held:?}"));
        }
        let mut candidates: Vec<&Op> = self
            .by_key
            .get(&key)
            .into_iter()
            .flatten()
            .filter(|op| {
                let (low, high) = self.versions(op);
                (low..=high).contains(&held.cas)
            })
            .collect();
        if candidates.is_empty() {
            return Err(format!(
                "holds a counter with a version no change to it can have: {held:?}"
            ));
        }
        let first = candidates[0];
        let produced = |op: &&Op| match op.kind {
            Kind::Set { id: set, .. } => set == id && number == counter_base(id),
            Kind::Arith { result, .. } => result.is_none_or(|result| result == number),
            _ => false,
        };
        candidates.retain(produced);
        // A change whose reply gave this very version comes first.
        candidates.sort_by_key(|op| op.outcome != Outcome::Acked(Some(held.cas)));
        let Some(&last) = candidates.first() else {
            return Err(format!(
                "holds a counter that none of the changes with its version produced: {held:?}, such as {}",
                show_op(first, self.t0)
            ));
        };
        // `check_changes` judges what an increment was applied to, from its reply.
        Ok(Found {
            last,
            chains: Vec::new(),
        })
    }

    /// Checks `key`'s acknowledged read-modify-writes: a conditional write found its token's
    /// item, an increment grew from a produced value, and neither from an item its node's
    /// flush had removed.
    fn check_changes(&self, key: u32) -> Vec<String> {
        let empty = Vec::new();
        let ops = self.by_key.get(&key).unwrap_or(&empty);
        let mut problems = Vec::new();
        for op in ops {
            if !matches!(op.outcome, Outcome::Acked(_)) {
                continue;
            }
            match op.kind {
                Kind::Cas { token, .. } => {
                    if self.versions(op).1 <= token {
                        problems.push(format!(
                            "{} is not newer than the item its token {token} names",
                            show_op(op, self.t0)
                        ));
                    }
                    if let Some(flush) = self.flushed_between(token, op) {
                        problems.push(format!(
                            "{} succeeded against the item with version {token}, which {} on its node had removed",
                            show_op(op, self.t0),
                            show_op(flush, self.t0)
                        ));
                    }
                }
                Kind::Arith {
                    delta,
                    decr,
                    result: Some(result),
                    ..
                } => {
                    let Some(base) = self
                        .puts
                        .get(&counter_id(result))
                        .filter(|base| base.key == key && matches!(base.kind, Kind::Set { .. }))
                    else {
                        problems.push(format!(
                            "{} returned {result}, which no put to the key began",
                            show_op(op, self.t0)
                        ));
                        continue;
                    };
                    let Some(previous) = (if decr {
                        result.checked_add(delta)
                    } else {
                        result.checked_sub(delta)
                    }) else {
                        problems.push(format!("{} returned {result}", show_op(op, self.t0)));
                        continue;
                    };
                    let before = self.versions(op).1;
                    let from = |other: &Op| {
                        self.versions(other).0 < before
                            && match other.kind {
                                Kind::Set { id, .. } => counter_base(id) == previous,
                                Kind::Arith { result, .. } => {
                                    result.is_none_or(|result| result == previous)
                                }
                                _ => false,
                            }
                    };
                    // Only this step is judged against flushes; see "Flushes and changes".
                    let applied_to = ops
                        .iter()
                        .filter(|other| from(other))
                        .map(|other| self.versions(other).1)
                        .max();
                    let Some(applied_to) = applied_to else {
                        problems.push(format!(
                            "{} returned {result} from {previous}, which no change to the key produced",
                            show_op(op, self.t0)
                        ));
                        continue;
                    };
                    if let Some(flush) = self.flushed_between(applied_to, op) {
                        problems.push(format!(
                            "{} was applied to {previous} (version at most {applied_to}), from {}, past {}, which its node had applied",
                            show_op(op, self.t0),
                            show_op(base, self.t0),
                            show_op(flush, self.t0)
                        ));
                    }
                }
                _ => {}
            }
        }
        problems
    }

    /// Checks that a held value is one `key`'s changes can have produced: one change's result
    /// with its version, built in version order, no step crossing its node's flush. Returns
    /// that change.
    fn check_value(&self, key: u32, held: &Held) -> Result<&Op, String> {
        let found = self.identify(key, held)?;
        let last = found.last;
        let (low, high) = self.versions(last);
        if held.cas < low || held.cas > high {
            return Err(format!(
                "holds the result of {} with version {} outside its range {low}..={high}",
                show_op(last, self.t0),
                held.cas
            ));
        }
        // Each change in a chain is newer than the one before. The item it was applied to is
        // versioned at most as high as any earlier piece can be; only that step is judged
        // against flushes.
        let pieces: Vec<&Op> = found.chains.iter().flatten().copied().collect();
        for chain in &found.chains {
            for (at, change) in chain.iter().enumerate().skip(1) {
                let before = chain[at - 1];
                if self.versions(change).1 <= self.versions(before).0 {
                    return Err(format!(
                        "holds {} applied on top of {}, which is not older",
                        show_op(change, self.t0),
                        show_op(before, self.t0)
                    ));
                }
                let applied_to = pieces
                    .iter()
                    .filter(|other| {
                        other.kind.id() != change.kind.id()
                            && self.versions(other).0 < self.versions(change).1
                    })
                    .map(|other| self.versions(other).1)
                    .max()
                    .unwrap_or(0);
                if let Some(flush) = self.flushed_between(applied_to, change) {
                    return Err(format!(
                        "a flushed value came back: {} was applied to a value no newer than {} (version at most {applied_to}), past {}, which its node had applied",
                        show_op(change, self.t0),
                        show_op(before, self.t0),
                        show_op(flush, self.t0)
                    ));
                }
            }
        }
        Ok(found.last)
    }

    /// Checks one node's `key`. Returns what is wrong, if anything, and how many changes the
    /// outcome needs lost to a kill.
    fn check(&self, key: u32, held: &Option<Held>) -> (Option<String>, usize) {
        let empty = Vec::new();
        let ops = self.by_key.get(&key).unwrap_or(&empty);
        let all = ops.iter().chain(&self.flushes);
        match held {
            Some(held) => {
                let last = match self.check_value(key, held) {
                    Ok(last) => last,
                    Err(problem) => return (Some(problem), 0),
                };
                // With a tiny cap, a node can forget a put along with the delete that removed
                // it, so a possibly superseded put does not rule out older values.
                let forgotten = |put: &Op| {
                    self.mode == Mode::TinyCap
                        && put.kind.writes()
                        && ops.iter().any(|op| {
                            matches!(op.kind, Kind::Delete { .. })
                                && self.versions(op).1 > self.versions(put).0
                        })
                };
                let newer: Vec<&Op> = all
                    .clone()
                    .filter(|op| {
                        self.certain(op) && self.versions(op).0 > held.cas && !forgotten(op)
                    })
                    .collect();
                if let Some(op) = newer.first() {
                    let what = match op.kind {
                        Kind::Delete { .. } => "a deleted key came back",
                        Kind::Flush { .. } => "a flushed key came back",
                        _ => "an acknowledged later write was lost",
                    };
                    return (
                        Some(format!(
                            "{what}: holds the result of {} (version {}), but {} is certainly newer",
                            show_op(last, self.t0),
                            held.cas,
                            show_op(op, self.t0)
                        )),
                        0,
                    );
                }
                let lost = all
                    .filter(|op| self.maybe_lost(op) && self.versions(op).0 > held.cas)
                    .count();
                (None, lost)
            }
            None => {
                let newest_put = ops
                    .iter()
                    .filter(|op| op.kind.writes() && self.certain(op))
                    .max_by_key(|op| self.versions(op).0);
                let Some(newest_put) = newest_put else {
                    return (None, 0);
                };
                let latest_removal = all
                    .filter(|op| !op.kind.writes())
                    .map(|op| self.versions(op).1)
                    .max();
                if latest_removal.is_some_and(|high| high > self.versions(newest_put).0) {
                    (None, 0)
                } else {
                    (
                        Some(format!(
                            "an acknowledged write was lost: absent, but {} is newer than any delete or flush",
                            show_op(newest_put, self.t0)
                        )),
                        0,
                    )
                }
            }
        }
    }
}

fn parse_id(value: &str) -> Option<u64> {
    let hex = value.strip_prefix('v')?.split(':').next()?;
    u64::from_str_radix(hex, 16).ok()
}

fn show_op(op: &Op, t0: u64) -> String {
    let what = match op.kind {
        Kind::Set { id, classic, .. } => {
            format!("{} {id:x}", if classic { "set" } else { "ms" })
        }
        Kind::Delete { classic } => (if classic { "delete" } else { "md" }).to_string(),
        Kind::Flush { delay: 0 } => "flush_all".to_string(),
        Kind::Flush { delay } => format!("flush_all {delay}"),
        Kind::Append {
            id,
            prepend,
            classic,
        } => format!(
            "{} {id:x}",
            match (prepend, classic) {
                (false, true) => "append",
                (true, true) => "prepend",
                (false, false) => "ms MA",
                (true, false) => "ms MP",
            }
        ),
        Kind::Arith {
            id,
            delta,
            decr,
            classic,
            result,
        } => format!(
            "{} {id:x} by {delta}{}",
            match (decr, classic) {
                (false, true) => "incr",
                (true, true) => "decr",
                (false, false) => "ma",
                (true, false) => "ma MD",
            },
            result.map_or(String::new(), |result| format!(" = {result}"))
        ),
        Kind::Cas {
            id, token, classic, ..
        } => format!("{} {id:x} on {token}", if classic { "cas" } else { "ms C" }),
    };
    let outcome = match op.outcome {
        Outcome::Acked(Some(cas)) => format!("acked cas {cas} (ms {})", cas >> MILLIS_SHIFT),
        Outcome::Acked(None) => "acked".to_string(),
        Outcome::Unknown => "no reply".to_string(),
    };
    format!(
        "[t={}..{} ms node {}] {what} {outcome}",
        op.start as i64 - t0 as i64,
        op.end as i64 - t0 as i64,
        op.node + 1
    )
}

// ---------------------------------------------------------------------------------------------
// The run

fn run(settings: Settings) {
    println!(
        "chaos: seed {} ({} nodes, {} s, {:?}, {}); rerun with FERRITED_CHAOS_SEED={} \
         FERRITED_CHAOS_NODES={} FERRITED_CHAOS_SECS={} FERRITED_CHAOS_SECRET={}{}",
        settings.seed,
        settings.nodes,
        settings.secs,
        settings.mode,
        if settings.secret {
            "signed links"
        } else {
            "no secret"
        },
        settings.seed,
        settings.nodes,
        settings.secs,
        if settings.secret { "on" } else { "off" },
        if settings.mode == Mode::TinyCap {
            " FERRITED_CHAOS_MODE=tiny-cap"
        } else {
            ""
        }
    );
    let run_dir = std::env::temp_dir().join(format!(
        "ferrited-chaos-{}-{}",
        settings.seed,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&run_dir);
    fs::create_dir_all(&run_dir).unwrap();
    println!("chaos: run directory {}", run_dir.display());
    println!(
        "chaos: node flags drawn from the seed: {:?}{}",
        settings.tight_args,
        settings
            .overflow
            .as_ref()
            .map_or(String::new(), |(node, args)| format!(
                ", and on node {}: {args:?}",
                node + 1
            ))
    );
    let planned = plan(&settings);
    let mut schedule = String::new();
    for fault in &planned {
        let _ = writeln!(
            schedule,
            "  t={:>6} ms  hold {:>5} ms  {}",
            fault.at,
            fault.hold,
            describe(&fault.fault)
        );
    }
    println!("chaos: schedule:\n{schedule}");

    let shared = build(&settings, &run_dir);
    let guard = Guard(shared.clone());
    {
        let shared = shared.clone();
        thread::spawn(move || watch_clock(&shared));
    }
    for node in 0..settings.nodes {
        let shared = shared.clone();
        thread::spawn(move || watch_drops(&shared, node));
    }
    {
        let t0 = now_ms();
        let mut nodes = shared.nodes.lock().unwrap();
        for node in nodes.iter_mut() {
            node.spawn(t0);
        }
    }
    let mut problems: Vec<String> = Vec::new();
    if let Err(error) = wait_serving(&shared, Duration::from_secs(60))
        .and_then(|()| wait_quiet(&shared, Duration::from_secs(60)))
    {
        panic!("the cluster did not start: {error}");
    }

    shared.t0.store(now_ms(), SeqCst);
    let clients: Vec<JoinHandle<ClientLog>> = (0..CLIENTS)
        .map(|index| {
            let shared = shared.clone();
            let seed = settings.seed;
            thread::spawn(move || run_client(&shared, index, seed))
        })
        .collect();
    let faults = {
        let shared = shared.clone();
        let (planned, seed) = (planned.clone(), settings.seed);
        thread::spawn(move || run_faults(&shared, &planned, seed))
    };
    let deadline = Instant::now() + Duration::from_secs(settings.secs);
    while Instant::now() < deadline && !faults.is_finished() {
        thread::sleep(Duration::from_millis(100));
    }
    while Instant::now() < deadline {
        check_exits(&shared);
        thread::sleep(Duration::from_millis(100));
    }
    shared.stop_workload.store(true, SeqCst);
    let logs: Vec<ClientLog> = clients
        .into_iter()
        .map(|client| client.join().unwrap())
        .collect();
    shared.stop_faults.store(true, SeqCst);
    faults.join().unwrap();
    let workload_end = now_ms();
    shared.event("workload and faults stopped; healing".into());

    // Heal: every link up, every node running and resumed.
    for link in &shared.links {
        link.set(LINK_UP, false);
    }
    {
        let t0 = shared.t0();
        let mut nodes = shared.nodes.lock().unwrap();
        for node in nodes.iter_mut() {
            if node.paused {
                node.signal(libc::SIGCONT);
                node.paused = false;
            }
            if node.child.is_none() {
                node.spawn(t0);
            }
        }
    }
    let settled = wait_serving(&shared, Duration::from_secs(150))
        .and_then(|()| wait_quiet(&shared, Duration::from_secs(150)));
    println!(
        "chaos: healed {} ms after the workload stopped",
        now_ms() - workload_end
    );

    let mut ops: Vec<Op> = Vec::new();
    let (mut refused, mut odd_replies, mut reads) = (0, Vec::new(), Vec::new());
    for log in logs {
        ops.extend(log.ops);
        refused += log.refused;
        odd_replies.extend(log.odd_replies);
        reads.extend(log.reads);
    }
    ops.extend(shared.burst_ops.lock().unwrap().iter().copied());
    let flushes = shared.flushes.lock().unwrap().clone();
    let crashes = shared.crashes.lock().unwrap().clone();
    let t0 = shared.t0();
    let acked = ops
        .iter()
        .filter(|op| matches!(op.outcome, Outcome::Acked(_)))
        .count();
    println!(
        "chaos: {} changes ({} acknowledged, {} without a reply), {} refused connections, \
         {} flushes, {} kills, {} crossed connects",
        ops.len(),
        acked,
        ops.len() - acked,
        refused,
        flushes.len(),
        crashes.len(),
        CROSSED.load(SeqCst)
    );
    if !odd_replies.is_empty() {
        problems.push(format!(
            "{} replies the protocol does not allow, such as {:?}",
            odd_replies.len(),
            &odd_replies[..odd_replies.len().min(5)]
        ));
    }
    // A reply timed before its request saw a step back that `watch_clock` may have missed.
    let stepped = ops
        .iter()
        .chain(&flushes)
        .map(|op| op.start.saturating_sub(op.end))
        .max()
        .unwrap_or(0);
    shared.clock_back.fetch_max(stepped, SeqCst);
    let slack = shared.clock_back.load(SeqCst);
    if slack > 0 {
        println!("chaos: the wall clock went back by up to {slack} ms; versions get that slack");
    }
    let mut clock_misses = Vec::new();
    for op in &ops {
        if let Outcome::Acked(Some(cas)) = op.outcome {
            let millis = cas >> MILLIS_SHIFT;
            if millis + slack < op.start || millis > op.end + slack {
                clock_misses.push(show_op(op, t0));
            }
        }
    }
    if !clock_misses.is_empty() {
        problems.push(format!(
            "{} versions outside their requests' times, such as {:?}",
            clock_misses.len(),
            &clock_misses[..clock_misses.len().min(5)]
        ));
    }

    let mut by_key: HashMap<u32, Vec<Op>> = HashMap::new();
    let mut puts = HashMap::new();
    for op in &ops {
        by_key.entry(op.key).or_default().push(*op);
        if let Some(id) = op.kind.id() {
            puts.insert(id, *op);
        }
    }
    let mut history = History {
        by_key,
        flushes: flushes.clone(),
        puts,
        crashes: crashes.clone(),
        stops: Vec::new(),
        pauses: Vec::new(),
        drops: Vec::new(),
        holes: Vec::new(),
        end: u64::MAX,
        t0,
        slack: shared.clock_back.load(SeqCst),
        mode: settings.mode,
    };

    // Reread while strict mode finds nodes apart, in case changes are still in flight.
    let mut states: Vec<Vec<Option<Held>>> = Vec::new();
    let mut offending: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    if let Err(error) = &settled {
        problems.push(error.clone());
    }
    if settled.is_ok() {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            states = match shared
                .client_ports
                .iter()
                .map(|&port| read_all(port))
                .collect::<io::Result<Vec<_>>>()
            {
                Ok(states) => states,
                Err(error) => {
                    problems.push(format!("cannot read the final state: {error}"));
                    break;
                }
            };
            let agree = states.iter().all(|state| *state == states[0]);
            if agree || settings.mode == Mode::TinyCap || Instant::now() > deadline {
                break;
            }
            thread::sleep(Duration::from_secs(1));
        }
    }
    history.end = now_ms();
    history.stops = shared
        .nodes
        .lock()
        .unwrap()
        .iter()
        .map(|node| node.stops.clone())
        .collect();
    history.pauses = (0..settings.nodes)
        .map(|node| {
            shared
                .faults
                .lock()
                .unwrap()
                .iter()
                .filter(|(fault, _, _)| matches!(fault, Fault::Pause { node: paused } if *paused == node))
                .map(|&(_, start, end)| (start, end.min(history.end)))
                .collect()
        })
        .collect();
    history.drops = shared.drops.lock().unwrap().clone();
    history.holes = shared.holes.lock().unwrap().clone();
    println!(
        "chaos: oplogs dropped records in {} stretches ({} never seen covered); damage in the \
         middle of logs took {} of the nodes' own changes",
        history.drops.len(),
        history
            .drops
            .iter()
            .filter(|drops| drops.covered == u64::MAX)
            .count(),
        history.holes.len()
    );
    // Read-modify-write values read during the run must be producible, like final values.
    for (key, node, at, held) in &reads {
        if let Err(problem) = history.check_value(*key, held) {
            offending.entry(*key).or_default().push(format!(
                "read from node {} at t={} ms: {problem}",
                node + 1,
                *at as i64 - t0 as i64
            ));
        }
    }
    println!("chaos: checked {} values read during the run", reads.len());
    let mut lost_to_kills = 0;
    let mut present = 0;
    if !states.is_empty() {
        for key in 0..KEYS {
            let held: Vec<&Option<Held>> =
                states.iter().map(|state| &state[key as usize]).collect();
            present += usize::from(held[0].is_some());
            for problem in history.check_changes(key) {
                offending.entry(key).or_default().push(problem);
            }
            if settings.mode == Mode::Strict && held.iter().any(|one| *one != held[0]) {
                offending
                    .entry(key)
                    .or_default()
                    .push("the nodes disagree".into());
            }
            for (index, one) in held.iter().enumerate() {
                let (problem, lost) = history.check(key, one);
                if index == 0 {
                    lost_to_kills += lost;
                }
                if let Some(problem) = problem {
                    offending
                        .entry(key)
                        .or_default()
                        .push(format!("node {}: {problem}", index + 1));
                }
            }
        }
        println!(
            "chaos: {present} of {KEYS} keys hold a value; final states need {lost_to_kills} \
             acknowledged changes lost to kills"
        );
    }

    // Nothing was evicted, and in strict mode no tombstone was.
    for (index, &port) in shared.client_ports.iter().enumerate() {
        let Some(stats) = stat_lines(port, "stats\r\n") else {
            problems.push(format!("cannot read node {}'s stats", index + 1));
            continue;
        };
        let stat = |name: &str| {
            stats
                .iter()
                .find(|(stat, _)| stat == name)
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        };
        println!(
            "chaos: node {} (its last run): full syncs sent {} received {}, stale records {}, \
             tombstones evicted {}, oplog drops {}",
            index + 1,
            stat("repl_full_syncs_sent"),
            stat("repl_full_syncs_received"),
            stat("repl_records_stale"),
            stat("repl_tombstones_evicted"),
            stat("persist_dropped")
        );
        if stat("evictions") != "0" {
            problems.push(format!(
                "node {} evicted {} items",
                index + 1,
                stat("evictions")
            ));
        }
        if settings.mode == Mode::Strict && stat("repl_tombstones_evicted") != "0" {
            problems.push(format!(
                "node {} evicted {} tombstones",
                index + 1,
                stat("repl_tombstones_evicted")
            ));
        }
    }
    {
        let probes = shared.probes.lock().unwrap();
        let faults = shared.faults.lock().unwrap();
        let (checked, stale) = check_probes(&history, &probes, &faults);
        println!(
            "chaos: {} readiness probes checked {checked} values; {} missing",
            probes.len(),
            stale.len()
        );
        if !stale.is_empty() {
            problems.push(format!(
                "{} values a restarted node lacked when it began serving, such as:\n    {}",
                stale.len(),
                stale[..stale.len().min(10)].join("\n    ")
            ));
        }
    }

    // The settled state must survive a clean stop, then a kill after an idle fsync interval.
    if problems.is_empty() && offending.is_empty() && !states.is_empty() {
        for kill in [false, true] {
            let how = if kill { "SIGKILL" } else { "SIGTERM" };
            if kill {
                thread::sleep(Duration::from_millis(2 * FSYNC_MS + 500));
            }
            shared.event(format!("{how} every node and restart them"));
            let after = match restart_all(&shared, kill) {
                Ok(after) => after,
                Err(error) => {
                    problems.push(format!("after {how} of every node: {error}"));
                    break;
                }
            };
            for key in 0..KEYS {
                for (index, state) in after.iter().enumerate() {
                    let (before, now) = (&states[index][key as usize], &state[key as usize]);
                    let problem = match settings.mode {
                        Mode::Strict if before != now => Some(format!(
                            "held {before:?}, and {now:?} after {how} of every node"
                        )),
                        Mode::Strict => None,
                        Mode::TinyCap => history
                            .check(key, now)
                            .0
                            .map(|problem| format!("after {how} of every node: {problem}")),
                    };
                    if let Some(problem) = problem {
                        offending
                            .entry(key)
                            .or_default()
                            .push(format!("node {}: {problem}", index + 1));
                    }
                }
            }
            if !offending.is_empty() {
                break;
            }
        }
    }
    // The proxies never alter bytes, so no frame may fail authentication.
    for (index, log) in shared.logs.iter().enumerate() {
        let text = fs::read_to_string(log).unwrap_or_default();
        let bad = text
            .lines()
            .filter(|line| line.contains("failed authentication"))
            .count();
        if bad > 0 {
            problems.push(format!(
                "node {} logged {bad} frame(s) that failed authentication",
                index + 1
            ));
        }
    }
    problems.extend(shared.anomalies.lock().unwrap().iter().cloned());

    if problems.is_empty() && offending.is_empty() {
        println!("chaos: seed {} passed", settings.seed);
        let mut nodes = shared.nodes.lock().unwrap();
        for node in nodes.iter_mut() {
            let _ = node.terminate();
        }
        drop(nodes);
        drop(guard);
        if std::env::var_os("FERRITED_CHAOS_KEEP").is_some() {
            println!("chaos: kept {}", run_dir.display());
        } else {
            let _ = fs::remove_dir_all(&run_dir);
        }
        return;
    }

    let mut report = String::new();
    let _ = writeln!(
        report,
        "seed {} ({} nodes, {} s, {:?}, secret {})",
        settings.seed, settings.nodes, settings.secs, settings.mode, settings.secret
    );
    let _ = writeln!(report, "\nschedule:\n{schedule}");
    let _ = writeln!(report, "faults as run:");
    for (at, text) in shared.events.lock().unwrap().iter() {
        let _ = writeln!(report, "  t={at:>6} ms  {text}");
    }
    let _ = writeln!(report, "\nkills (node, t):");
    for (node, at) in &crashes {
        let _ = writeln!(
            report,
            "  node {} at t={} ms",
            node + 1,
            *at as i64 - t0 as i64
        );
    }
    let _ = writeln!(report, "\nflushes:");
    for op in &flushes {
        let _ = writeln!(report, "  {}{}", show_op(op, t0), maybe(&history, op));
    }
    if !problems.is_empty() {
        let _ = writeln!(report, "\nproblems:");
        for problem in &problems {
            let _ = writeln!(report, "  {problem}");
        }
    }
    let _ = writeln!(report, "\n{} offending keys", offending.len());
    for (key, why) in offending.iter().take(20) {
        let _ = writeln!(report, "\nkey {}:", key_name(*key));
        for line in why {
            let _ = writeln!(report, "  {line}");
        }
        for (index, state) in states.iter().enumerate() {
            let _ = writeln!(
                report,
                "  node {} holds {:?}",
                index + 1,
                state[*key as usize]
            );
        }
        let mut ops = history.by_key.get(key).cloned().unwrap_or_default();
        ops.sort_by_key(|op| op.start);
        for op in &ops {
            let _ = writeln!(report, "    {}{}", show_op(op, t0), maybe(&history, op));
        }
    }
    let report_path = run_dir.join("report.txt");
    fs::write(&report_path, &report).unwrap();
    println!("{report}");
    drop(guard);
    panic!(
        "chaos seed {} failed; report {} and node logs and data in {}",
        settings.seed,
        report_path.display(),
        run_dir.display()
    );
}

fn maybe(history: &History, op: &Op) -> &'static str {
    if history.maybe_lost(op) {
        " (maybe lost to a kill)"
    } else {
        ""
    }
}

/// The oracle on hand-made histories, so a run that passes is known to have been able to fail.
mod oracle {
    use super::*;

    fn version(millis: u64, node: u8) -> u64 {
        (millis << MILLIS_SHIFT) | u64::from(node)
    }

    fn put(id: u64, node: u8, millis: u64) -> Op {
        Op {
            key: 0,
            node,
            kind: Kind::Set {
                id,
                pad: 0,
                classic: false,
            },
            start: millis,
            end: millis + 1,
            outcome: Outcome::Acked(Some(version(millis, node))),
        }
    }

    fn delete(node: u8, millis: u64, outcome: Outcome) -> Op {
        Op {
            key: 0,
            node,
            kind: Kind::Delete { classic: false },
            start: millis,
            end: millis + 1,
            outcome,
        }
    }

    fn history(ops: &[Op], flushes: &[Op], crashes: &[(usize, u64)], mode: Mode) -> History {
        let mut by_key: HashMap<u32, Vec<Op>> = HashMap::new();
        let mut puts = HashMap::new();
        for op in ops {
            by_key.entry(op.key).or_default().push(*op);
            if let Some(id) = op.kind.id() {
                puts.insert(id, *op);
            }
        }
        History {
            by_key,
            flushes: flushes.to_vec(),
            puts,
            crashes: crashes.to_vec(),
            stops: Vec::new(),
            pauses: Vec::new(),
            drops: Vec::new(),
            holes: Vec::new(),
            end: 1 << 40,
            t0: 0,
            slack: 0,
            mode,
        }
    }

    fn held(op: &Op) -> Option<Held> {
        let Kind::Set { id, pad, .. } = op.kind else {
            unreachable!()
        };
        let Outcome::Acked(Some(cas)) = op.outcome else {
            unreachable!()
        };
        Some(Held {
            value: value_of(id, pad),
            flags: flags_of(id),
            cas,
        })
    }

    fn fails(history: &History, held: &Option<Held>) -> bool {
        history.check(0, held).0.is_some()
    }

    #[test]
    fn a_lost_acknowledged_write_is_caught_unless_its_node_was_killed_soon_after() {
        let (older, newer) = (put(1, 0, 1000), put(2, 1, 5000));
        let strict = history(&[older, newer], &[], &[], Mode::Strict);
        assert!(fails(&strict, &held(&older)));
        assert!(!fails(&strict, &held(&newer)));
        assert!(fails(&strict, &None));
        let killed = history(
            &[older, newer],
            &[],
            &[(1, 5000 + LOSS_WINDOW_MS)],
            Mode::Strict,
        );
        assert!(!fails(&killed, &held(&older)));
        assert_eq!(killed.check(0, &held(&older)).1, 1);
        let killed_later = history(
            &[older, newer],
            &[],
            &[(1, 5002 + LOSS_WINDOW_MS)],
            Mode::Strict,
        );
        assert!(fails(&killed_later, &held(&older)));
        let other_node_killed = history(&[older, newer], &[], &[(0, 5100)], Mode::Strict);
        assert!(fails(&other_node_killed, &held(&older)));
    }

    #[test]
    fn a_deleted_key_that_comes_back_is_caught_except_with_a_tiny_cap() {
        let value = put(1, 0, 1000);
        let deleted = delete(1, 2000, Outcome::Acked(None));
        let strict = history(&[value, deleted], &[], &[], Mode::Strict);
        assert!(fails(&strict, &held(&value)));
        assert!(!fails(&strict, &None));
        let tiny = history(&[value, deleted], &[], &[], Mode::TinyCap);
        assert!(!fails(&tiny, &held(&value)));
        assert!(!fails(&tiny, &None));
    }

    #[test]
    fn concurrent_or_unanswered_changes_allow_either_outcome() {
        let value = put(1, 0, 1000);
        // A delete in the same millisecond may be older or newer.
        let same_time = delete(1, 1000, Outcome::Acked(None));
        let concurrent = history(&[value, same_time], &[], &[], Mode::Strict);
        assert!(!fails(&concurrent, &held(&value)));
        assert!(!fails(&concurrent, &None));
        // A delete with no reply may have run at any later time, or never.
        let unanswered = delete(1, 500, Outcome::Unknown);
        let unknown = history(&[value, unanswered], &[], &[], Mode::Strict);
        assert!(!fails(&unknown, &held(&value)));
        assert!(!fails(&unknown, &None));
        // An older acknowledged delete cannot remove a newer put.
        let older = delete(1, 500, Outcome::Acked(None));
        let ordered = history(&[value, older], &[], &[], Mode::Strict);
        assert!(fails(&ordered, &None));
    }

    #[test]
    fn with_a_tiny_cap_a_delete_can_make_a_node_forget_the_put_it_removed() {
        // An older put arrives after a delete removed a newer put, its tombstone evicted.
        let older = put(1, 2, 1000);
        let newer = put(2, 0, 2000);
        let deleted = delete(1, 3000, Outcome::Acked(None));
        let tiny = history(&[older, newer, deleted], &[], &[], Mode::TinyCap);
        assert!(!fails(&tiny, &held(&older)));
        let strict = history(&[older, newer, deleted], &[], &[], Mode::Strict);
        assert!(fails(&strict, &held(&older)));
        // Without a delete newer than the newer put, it still rules the older one out.
        let early = delete(1, 1500, Outcome::Acked(None));
        let tiny = history(&[older, newer, early], &[], &[], Mode::TinyCap);
        assert!(fails(&tiny, &held(&older)));
    }

    #[test]
    fn a_flush_removes_older_puts_everywhere() {
        let value = put(1, 0, 1000);
        let flush = Op {
            key: u32::MAX,
            kind: Kind::Flush { delay: 0 },
            ..delete(1, 2000, Outcome::Acked(None))
        };
        let flushed = history(&[value], &[flush], &[], Mode::Strict);
        assert!(fails(&flushed, &held(&value)));
        assert!(!fails(&flushed, &None));
        let tiny = history(&[value], &[flush], &[], Mode::TinyCap);
        assert!(fails(&tiny, &held(&value)));
    }

    #[test]
    fn a_change_without_a_reply_runs_no_later_than_its_node_stops() {
        let value = put(1, 0, 3000);
        let unanswered = delete(1, 1000, Outcome::Unknown);
        let mut stopped = history(&[value, unanswered], &[], &[], Mode::Strict);
        stopped.stops = vec![vec![], vec![500, 2000, 9000]];
        assert!(fails(&stopped, &None));
        stopped.stops = vec![vec![], vec![500, 4000]];
        assert!(!fails(&stopped, &None));
    }

    #[test]
    fn a_value_no_put_wrote_or_with_the_wrong_version_is_caught() {
        let value = put(1, 0, 1000);
        let only = history(&[value], &[], &[], Mode::Strict);
        let mut wrong_cas = held(&value).unwrap();
        wrong_cas.cas += 1;
        assert!(fails(&only, &Some(wrong_cas)));
        let mut wrong_flags = held(&value).unwrap();
        wrong_flags.flags ^= 1;
        assert!(fails(&only, &Some(wrong_flags)));
        let mut stranger = held(&value).unwrap();
        stranger.value = value_of(99, 0);
        assert!(fails(&only, &Some(stranger)));
    }

    const APPEND_KEY: u32 = HOT_KEYS + COLD_KEYS;
    const COUNTER_KEY: u32 = APPEND_KEY + APPEND_KEYS;
    const CAS_KEY: u32 = COUNTER_KEY + COUNTER_KEYS;

    fn flush(node: u8, millis: u64, delay: u64) -> Op {
        Op {
            key: u32::MAX,
            kind: Kind::Flush { delay },
            ..delete(node, millis, Outcome::Acked(None))
        }
    }

    fn on(key: u32, op: Op) -> Op {
        Op { key, ..op }
    }

    fn append(id: u64, node: u8, millis: u64, prepend: bool) -> Op {
        Op {
            key: APPEND_KEY,
            kind: Kind::Append {
                id,
                prepend,
                classic: false,
            },
            ..put(id, node, millis)
        }
    }

    fn arith(id: u64, node: u8, millis: u64, delta: u64, result: u64) -> Op {
        Op {
            key: COUNTER_KEY,
            kind: Kind::Arith {
                id,
                delta,
                decr: false,
                classic: false,
                result: Some(result),
            },
            ..put(id, node, millis)
        }
    }

    fn holds(value: String, flags_id: u64, last: &Op) -> Option<Held> {
        let Outcome::Acked(Some(cas)) = last.outcome else {
            unreachable!()
        };
        Some(Held {
            value,
            flags: flags_of(flags_id),
            cas,
        })
    }

    fn fails_on(history: &History, key: u32, held: &Option<Held>) -> bool {
        history.check(key, held).0.is_some()
    }

    #[test]
    fn a_delayed_flush_counts_from_when_it_is_due_until_its_node_stops() {
        // Sent at 10.0 s with a delay of 2: due at 12 s, applied by 14.5 s.
        let delayed = flush(0, 10_000, 2);
        let before = put(1, 1, 11_000);
        let after = put(2, 1, 15_000);
        let ran = history(&[before], &[delayed], &[], Mode::Strict);
        assert!(fails(&ran, &held(&before)));
        assert!(!fails(&ran, &None));
        let later = history(&[after], &[delayed], &[], Mode::Strict);
        assert!(!fails(&later, &held(&after)));
        // Its node stopped before it was due: it never took effect.
        let mut stopped = history(&[before], &[delayed], &[], Mode::Strict);
        stopped.stops = vec![vec![11_500]];
        assert!(!fails(&stopped, &held(&before)));
        // Another flush_all to the node before it was due may have replaced it.
        let replaced = history(
            &[before],
            &[delayed, flush(0, 11_800, 0)],
            &[],
            Mode::Strict,
        );
        assert!(replaced.delayed(&delayed).high > 0 && !replaced.delayed(&delayed).sure);
        // A pause postpones it.
        let mut paused = history(&[before], &[delayed], &[], Mode::Strict);
        paused.pauses = vec![vec![(13_000, 18_000)]];
        assert_eq!(paused.delayed(&delayed).high, 19_500);
    }

    #[test]
    fn an_append_that_carries_a_value_past_a_flush_on_its_node_is_caught() {
        let base = on(APPEND_KEY, put(1, 0, 1000));
        let flushed = flush(0, 2000, 0);
        let value = format!("{}{}", value_for(APPEND_KEY, 1, 0), piece(2, false));
        // Made on the node that applied the flush, after it: the base is gone.
        let local = append(2, 0, 3000, false);
        let caught = history(&[base, local], &[flushed], &[], Mode::Strict);
        assert!(fails_on(
            &caught,
            APPEND_KEY,
            &holds(value.clone(), 1, &local)
        ));
        // On another node, which may not have had the flush yet: last writer wins.
        let remote = append(2, 1, 3000, false);
        let allowed = history(&[base, remote], &[flushed], &[], Mode::Strict);
        assert!(!fails_on(
            &allowed,
            APPEND_KEY,
            &holds(value.clone(), 1, &remote)
        ));
        // A flush its node may have lost to a kill proves nothing.
        let lost = history(&[base, local], &[flushed], &[(0, 2500)], Mode::Strict);
        assert!(!fails_on(&lost, APPEND_KEY, &holds(value, 1, &local)));
    }

    #[test]
    fn an_append_keys_value_must_be_pieces_in_an_order_they_can_have_been_applied() {
        let base = on(APPEND_KEY, put(1, 0, 1000));
        let first = append(2, 1, 2000, false);
        let second = append(3, 0, 3000, false);
        let front = append(4, 1, 2500, true);
        let ops = [base, first, second, front];
        let history = history(&ops, &[], &[], Mode::Strict);
        let p = |id| value_for(APPEND_KEY, id, 0);
        let a = |id| piece(id, false);
        let r = |id| piece(id, true);
        let whole = format!("{}{}{}{}", r(4), p(1), a(2), a(3));
        assert!(!fails_on(&history, APPEND_KEY, &holds(whole, 1, &second)));
        // Appends in the wrong order.
        let swapped = format!("{}{}{}{}", r(4), p(1), a(3), a(2));
        assert!(fails_on(&history, APPEND_KEY, &holds(swapped, 1, &first)));
        // A piece missing between the base and the last is fine (another node's value won),
        // but the version must be the newest piece's.
        let skipped = format!("{}{}", p(1), a(3));
        assert!(!fails_on(
            &history,
            APPEND_KEY,
            &holds(skipped.clone(), 1, &second)
        ));
        assert!(fails_on(&history, APPEND_KEY, &holds(skipped, 1, &first)));
        // A piece written to another key, or a base that is not one.
        let stranger = format!("{}{}", p(1), a(9));
        assert!(fails_on(&history, APPEND_KEY, &holds(stranger, 1, &second)));
        let twice = format!("{}{}{}", p(1), a(2), a(2));
        assert!(fails_on(&history, APPEND_KEY, &holds(twice, 1, &first)));
    }

    #[test]
    fn a_counter_must_hold_what_a_change_with_its_version_produced() {
        let base = on(COUNTER_KEY, put(1, 0, 1000));
        let start = counter_base(1);
        let incr = arith(2, 0, 3000, 5, start + 5);
        let history = history(&[base, incr], &[], &[], Mode::Strict);
        // The base alone is older than an acknowledged increment.
        assert!(fails_on(
            &history,
            COUNTER_KEY,
            &holds(start.to_string(), 1, &base)
        ));
        assert!(!fails_on(
            &history,
            COUNTER_KEY,
            &holds((start + 5).to_string(), 1, &incr)
        ));
        assert!(fails_on(
            &history,
            COUNTER_KEY,
            &holds((start + 6).to_string(), 1, &incr)
        ));
        assert!(history.check_changes(COUNTER_KEY).is_empty());
        // An increment that started from a number no change produced.
        let odd = arith(3, 0, 4000, 5, start + 7);
        let history = history_of(&[base, incr, odd]);
        assert_eq!(history.check_changes(COUNTER_KEY).len(), 1);
    }

    fn history_of(ops: &[Op]) -> History {
        history(ops, &[], &[], Mode::Strict)
    }

    #[test]
    fn an_increment_or_cas_past_a_flush_on_its_node_is_caught() {
        let base = on(COUNTER_KEY, put(1, 0, 1000));
        let flushed = flush(0, 2000, 0);
        let start = counter_base(1);
        let local = arith(2, 0, 3000, 5, start + 5);
        let caught = history(&[base, local], &[flushed], &[], Mode::Strict);
        assert_eq!(caught.check_changes(COUNTER_KEY).len(), 1);
        let remote = arith(2, 1, 3000, 5, start + 5);
        let allowed = history(&[base, remote], &[flushed], &[], Mode::Strict);
        assert!(allowed.check_changes(COUNTER_KEY).is_empty());
        // A delayed flush on the node, once due (2 s, applied by 4.5 s).
        let delayed = flush(0, 1200, 1);
        let late = arith(2, 0, 6000, 5, start + 5);
        let caught = history(&[base, late], &[delayed], &[], Mode::Strict);
        assert_eq!(caught.check_changes(COUNTER_KEY).len(), 1);

        let item = on(CAS_KEY, put(1, 0, 1000));
        let Outcome::Acked(Some(token)) = item.outcome else {
            unreachable!()
        };
        let cas = |node| Op {
            key: CAS_KEY,
            kind: Kind::Cas {
                id: 2,
                pad: 0,
                token,
                classic: false,
            },
            ..put(2, node, 3000)
        };
        let caught = history(&[item, cas(0)], &[flushed], &[], Mode::Strict);
        assert_eq!(caught.check_changes(CAS_KEY).len(), 1);
        let allowed = history(&[item, cas(1)], &[flushed], &[], Mode::Strict);
        assert!(allowed.check_changes(CAS_KEY).is_empty());
    }

    #[test]
    fn a_change_made_on_top_of_one_that_carried_a_value_past_a_flush_elsewhere_is_fine() {
        // Node 0 increments before it has node 1's flush; node 1 then increments that result.
        let base = on(COUNTER_KEY, put(1, 0, 1000));
        let flushed = flush(1, 2000, 0);
        let start = counter_base(1);
        let elsewhere = arith(2, 0, 2500, 5, start + 5);
        let on_top = arith(3, 1, 3000, 7, start + 12);
        let carried = history(&[base, elsewhere, on_top], &[flushed], &[], Mode::Strict);
        assert!(carried.check_changes(COUNTER_KEY).is_empty());
        // Applied to the base itself, it is caught.
        let direct = arith(3, 1, 3000, 7, start + 7);
        let caught = history(&[base, direct], &[flushed], &[], Mode::Strict);
        assert_eq!(caught.check_changes(COUNTER_KEY).len(), 1);

        let base = on(APPEND_KEY, put(1, 0, 1000));
        let elsewhere = append(2, 0, 2500, false);
        let on_top = append(3, 1, 3000, false);
        let carried = history(&[base, elsewhere, on_top], &[flushed], &[], Mode::Strict);
        let p = value_for(APPEND_KEY, 1, 0);
        let whole = format!("{p}{}{}", piece(2, false), piece(3, false));
        assert!(!fails_on(&carried, APPEND_KEY, &holds(whole, 1, &on_top)));
        let skipped = format!("{p}{}", piece(3, false));
        assert!(fails_on(&carried, APPEND_KEY, &holds(skipped, 1, &on_top)));
    }

    #[test]
    fn records_an_oplog_dropped_or_damage_took_can_be_lost_until_covered() {
        let older = put(1, 1, 1000);
        let newer = put(2, 0, 2000);
        let mut dropped = history(&[older, newer], &[], &[(0, 9000)], Mode::Strict);
        dropped.drops = vec![Drops {
            node: 0,
            from: 1900,
            to: 2100,
            covered: 10_000,
        }];
        assert!(!fails(&dropped, &held(&older)));
        dropped.drops[0].covered = 8000;
        assert!(fails(&dropped, &held(&older)));
        // Another node's drops say nothing about this one.
        dropped.drops[0] = Drops {
            node: 1,
            covered: 10_000,
            ..dropped.drops[0]
        };
        assert!(fails(&dropped, &held(&older)));

        let mut damaged = history(&[older, newer], &[], &[], Mode::Strict);
        damaged.holes = vec![(0, Some(0), version(2000, 0))];
        assert!(!fails(&damaged, &held(&older)));
        damaged.holes = vec![(0, Some(1), version(2000, 0))];
        assert!(fails(&damaged, &held(&older)));
    }
}
