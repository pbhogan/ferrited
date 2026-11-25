//! Executes parsed commands against the store and encodes classic replies.

use std::io::Write;

use bytes::Bytes;

use super::meta;
use super::reply::{self, Reply};
use super::{Context, Flow, VERSION};
use crate::protocol::{Command, StoreMode};
use crate::stats::{LocalStats, Stat};
use crate::store::{
    ArithArgs, ArithOutcome, DeleteArgs, DeleteOutcome, GetArgs, SetArgs, SetMode, SetOutcome,
};

/// Converts a protocol exptime to an absolute expiry: 0 is never, negative is already expired,
/// up to 30 days is relative to now, and anything larger is an absolute Unix time.
pub(super) fn expiry(exptime: i64, now: u64) -> u64 {
    const RELATIVE_MAX: i64 = 60 * 60 * 24 * 30;
    match exptime {
        0 => 0,
        negative if negative < 0 => 1,
        relative if relative <= RELATIVE_MAX => now + relative as u64,
        absolute => absolute as u64,
    }
}

/// Executes one command. `now` is the Unix time of the command's batch.
pub(super) fn execute(
    ctx: &Context,
    cmd: Command<'_>,
    now: u64,
    out: &mut Reply,
    stats: &mut LocalStats,
) -> Flow {
    match cmd {
        Command::Get { keys, cas } => retrieve(ctx, keys, cas, None, now, out, stats),
        Command::Gat { exptime, keys, cas } => {
            retrieve(ctx, keys, cas, Some(expiry(exptime, now)), now, out, stats)
        }
        Command::Store {
            mode,
            key,
            flags,
            exptime,
            cas,
            noreply,
            data,
        } => {
            stats.bump(Stat::CmdSet);
            let mode = match mode {
                StoreMode::Set => SetMode::Set,
                StoreMode::Add => SetMode::Add,
                StoreMode::Replace => SetMode::Replace,
                StoreMode::Append => SetMode::Append,
                StoreMode::Prepend => SetMode::Prepend,
            };
            let args = SetArgs {
                mode,
                flags,
                expiry: expiry(exptime, now),
                cas,
                ..Default::default()
            };
            let outcome = ctx.store.set(key, Bytes::copy_from_slice(data), now, &args);
            count_cas(&args, &outcome, stats);
            let reply: &[u8] = match outcome {
                SetOutcome::Stored { .. } => b"STORED\r\n",
                SetOutcome::NotStored => b"NOT_STORED\r\n",
                SetOutcome::Exists => b"EXISTS\r\n",
                SetOutcome::NotFound => b"NOT_FOUND\r\n",
                SetOutcome::TooLarge => b"SERVER_ERROR object too large for cache\r\n",
            };
            if !noreply {
                out.extend_from_slice(reply);
            }
        }
        Command::Delete { key, noreply } => {
            let reply: &[u8] = match ctx.store.delete(key, now, &DeleteArgs::default()) {
                DeleteOutcome::Deleted => {
                    stats.bump(Stat::DeleteHits);
                    b"DELETED\r\n"
                }
                _ => {
                    stats.bump(Stat::DeleteMisses);
                    b"NOT_FOUND\r\n"
                }
            };
            if !noreply {
                out.extend_from_slice(reply);
            }
        }
        Command::Arith {
            key,
            delta,
            decr,
            noreply,
        } => {
            let outcome = ctx.store.arith(
                key,
                now,
                &ArithArgs {
                    decr,
                    delta,
                    ..Default::default()
                },
            );
            let found = matches!(outcome, ArithOutcome::Value { .. });
            stats.bump(match (decr, found) {
                (false, true) => Stat::IncrHits,
                (false, false) => Stat::IncrMisses,
                (true, true) => Stat::DecrHits,
                (true, false) => Stat::DecrMisses,
            });
            if !noreply {
                match outcome {
                    ArithOutcome::Value { value, .. } => {
                        out.number(value);
                        out.extend_from_slice(b"\r\n");
                    }
                    ArithOutcome::NonNumeric => out.extend_from_slice(
                        b"CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
                    ),
                    ArithOutcome::NotFound | ArithOutcome::Exists => {
                        out.extend_from_slice(b"NOT_FOUND\r\n")
                    }
                }
            }
        }
        Command::Touch {
            key,
            exptime,
            noreply,
        } => {
            stats.bump(Stat::CmdTouch);
            let reply: &[u8] = if ctx.store.touch(key, now, expiry(exptime, now)) {
                stats.bump(Stat::TouchHits);
                b"TOUCHED\r\n"
            } else {
                stats.bump(Stat::TouchMisses);
                b"NOT_FOUND\r\n"
            };
            if !noreply {
                out.extend_from_slice(reply);
            }
        }
        Command::FlushAll { delay, noreply } => {
            stats.bump(Stat::CmdFlush);
            ctx.store.flush_all(now, delay);
            if !noreply {
                out.extend_from_slice(b"OK\r\n");
            }
        }
        Command::Stats { arg } => {
            // Fold in this connection's counts so the reply covers earlier commands in the batch.
            ctx.stats.merge(stats);
            stats_reply(ctx, arg, now, out);
        }
        Command::Version => {
            let _ = write!(out, "VERSION {VERSION}\r\n");
        }
        Command::Accepted { noreply } => {
            if !noreply {
                out.extend_from_slice(b"OK\r\n");
            }
        }
        Command::Quit => return Flow::Close,
        Command::Shutdown { valid, noreply } => {
            let reply: &[u8] = if !ctx.enable_shutdown {
                b"SERVER_ERROR: shutdown not enabled\r\n"
            } else if valid {
                return Flow::Shutdown;
            } else {
                b"CLIENT_ERROR invalid shutdown mode\r\n"
            };
            if !noreply {
                out.extend_from_slice(reply);
            }
        }
        Command::MetaNoop => out.extend_from_slice(b"MN\r\n"),
        Command::MetaGet(opts) => meta::get(ctx, opts, now, out, stats),
        Command::MetaSet { opts, data } => meta::set(ctx, opts, data, now, out, stats),
        Command::MetaDelete(opts) => meta::delete(ctx, opts, now, out, stats),
        Command::MetaArith(opts) => meta::arith(ctx, opts, now, out, stats),
        Command::MetaDebug(opts) => meta::debug(ctx, opts, now, out),
    }
    Flow::Continue
}

/// Counts a CAS outcome; as in memcached, only a set or replace with a token counts.
pub(super) fn count_cas(args: &SetArgs, outcome: &SetOutcome, stats: &mut LocalStats) {
    if args.cas.is_none() || !matches!(args.mode, SetMode::Set | SetMode::Replace) {
        return;
    }
    match outcome {
        SetOutcome::Stored { .. } => stats.bump(Stat::CasHits),
        SetOutcome::Exists => stats.bump(Stat::CasBadval),
        SetOutcome::NotFound => stats.bump(Stat::CasMisses),
        _ => {}
    }
}

fn retrieve(
    ctx: &Context,
    keys: &[u8],
    with_cas: bool,
    touch: Option<u64>,
    now: u64,
    out: &mut Reply,
    stats: &mut LocalStats,
) {
    let args = GetArgs {
        touch,
        ..Default::default()
    };
    for key in keys
        .split(|byte| *byte == b' ')
        .filter(|key| !key.is_empty())
    {
        // Like memcached, `gat` counts only as a touch and `get` only as a get.
        let hit = ctx.store.get(key, now, &args);
        let (command, hits, misses) = match touch {
            Some(_) => (Stat::CmdTouch, Stat::TouchHits, Stat::TouchMisses),
            None => (Stat::CmdGet, Stat::GetHits, Stat::GetMisses),
        };
        stats.bump(command);
        let Some(hit) = hit else {
            stats.bump(misses);
            continue;
        };
        stats.bump(hits);
        // The header's numbers take at most 52 bytes; a small value is copied inline.
        let inline_value = match hit.value.len() < reply::SHARE_THRESHOLD {
            true => hit.value.len(),
            false => 0,
        };
        out.reserve(key.len() + 64 + inline_value);
        out.extend_from_slice(b"VALUE ");
        out.extend_from_slice(key);
        out.push(b' ');
        out.number(hit.flags.into());
        out.push(b' ');
        out.number(hit.value.len() as u64);
        if with_cas {
            out.push(b' ');
            out.number(hit.cas);
        }
        out.extend_from_slice(b"\r\n");
        out.value(&hit.value);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(b"END\r\n");
}

/// A value a plain `stats` reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StatValue {
    Number(u64),
    Text(&'static str),
}

impl std::fmt::Display for StatValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatValue::Number(number) => number.fmt(f),
            StatValue::Text(text) => text.fmt(f),
        }
    }
}

/// Every line of a plain `stats`, in order; shared with the metrics endpoint. On a node
/// configured for persistence or replication, their lines are omitted until they start, rather
/// than reported as off or zero.
pub(crate) fn general_stats(ctx: &Context, now: u64) -> Vec<(&'static str, StatValue)> {
    use StatValue::{Number, Text};
    let load = |counter: &std::sync::atomic::AtomicU64| {
        Number(counter.load(std::sync::atomic::Ordering::Relaxed))
    };
    let store = ctx.store.stats();
    let mut lines = vec![
        ("pid", Number(std::process::id().into())),
        ("uptime", Number(ctx.started.elapsed().as_secs())),
        ("time", Number(now)),
        ("version", Text(VERSION)),
        ("pointer_size", Number(usize::BITS.into())),
        ("max_connections", Number(ctx.max_connections as u64)),
        ("curr_connections", load(&ctx.stats.curr_connections)),
        ("total_connections", load(&ctx.stats.total_connections)),
        (
            "rejected_connections",
            load(&ctx.stats.rejected_connections),
        ),
    ];
    for (name, value) in ctx.stats.snapshot() {
        lines.push((name, Number(value)));
    }
    lines.extend([
        ("threads", Number(ctx.threads as u64)),
        ("limit_maxbytes", Number(store.limit_maxbytes)),
        ("bytes", Number(store.bytes)),
        ("curr_items", Number(store.curr_items)),
        ("total_items", Number(store.total_items)),
        ("evictions", Number(store.evictions)),
        ("reclaimed", Number(store.reclaimed)),
    ]);
    match ctx.persist.get() {
        Some(persist) => {
            for (name, value) in persist.entries() {
                lines.push((name, Number(value)));
            }
        }
        None if ctx.persist_configured => {}
        None => lines.push(("persist_enabled", Number(0))),
    }
    match ctx.repl.get() {
        Some(repl) => {
            for (name, value) in repl.entries() {
                lines.push((name, Number(value)));
            }
        }
        None if ctx.repl_configured => {}
        None => {
            for name in crate::replicate::STAT_NAMES {
                lines.push((name, Number(0)));
            }
        }
    }
    lines
}

fn stats_reply(ctx: &Context, arg: &[u8], now: u64, out: &mut Reply) {
    let mut line = |name: &str, value: &dyn std::fmt::Display| {
        let _ = write!(out, "STAT {name} {value}\r\n");
    };
    match arg {
        b"" => {
            for (name, value) in general_stats(ctx, now) {
                line(name, &value);
            }
        }
        b"settings" => {
            line("maxbytes", &ctx.store.stats().limit_maxbytes);
            line("maxconns", &ctx.max_connections);
            line("tcpport", &ctx.port);
            line("item_size_max", &ctx.item_max);
            line("num_threads", &ctx.threads);
        }
        b"reset" => {
            ctx.stats.reset();
            ctx.store.reset_stats();
            out.extend_from_slice(b"RESET\r\n");
            return;
        }
        b"repl" => {
            if let Some(repl) = ctx.repl.get() {
                for (name, value) in repl.peer_lines() {
                    line(&name, &value);
                }
            }
        }
        b"slabs" | b"items" | b"sizes" | b"conns" => {}
        _ => {
            out.extend_from_slice(b"ERROR\r\n");
            return;
        }
    }
    out.extend_from_slice(b"END\r\n");
}
