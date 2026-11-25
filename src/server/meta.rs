//! Executes meta commands and encodes their replies.

use std::io::Write;

use bytes::Bytes;

use super::Context;
use super::exec::{count_cas, expiry};
use super::reply::Reply;
use crate::protocol::MetaOpts;
use crate::stats::{LocalStats, Stat};
use crate::store::{
    ArithArgs, ArithOutcome, DeleteArgs, DeleteOutcome, GetArgs, SetArgs, SetMode, SetOutcome,
};

/// Item details available for return flags. Absent details are skipped.
#[derive(Default)]
struct Details {
    cas: Option<u64>,
    flags: Option<u32>,
    hit: Option<bool>,
    idle: Option<u64>,
    size: Option<usize>,
    ttl: Option<i64>,
}

/// Appends one numeric return flag, if it has a value.
fn flag(out: &mut Reply, name: u8, value: Option<u64>) {
    if let Some(value) = value {
        out.extend_from_slice(&[b' ', name]);
        out.number(value);
    }
}

/// Appends the return flags the client asked for, in the order it asked.
fn return_flags(out: &mut Reply, opts: &MetaOpts<'_>, details: &Details) {
    for &letter in opts.returns.letters() {
        match letter {
            b'O' => {
                out.push(b' ');
                out.extend_from_slice(opts.returns.opaque);
            }
            b'k' => {
                out.extend_from_slice(b" k");
                out.extend_from_slice(opts.raw_key);
                if opts.base64 {
                    out.extend_from_slice(b" b");
                }
            }
            b'c' => flag(out, b'c', details.cas),
            b'f' => flag(out, b'f', details.flags.map(u64::from)),
            b'h' => flag(out, b'h', details.hit.map(u64::from)),
            b'l' => flag(out, b'l', details.idle),
            b's' => flag(out, b's', details.size.map(|size| size as u64)),
            b't' => {
                if let Some(ttl) = details.ttl {
                    out.extend_from_slice(b" t");
                    out.signed(ttl);
                }
            }
            _ => {}
        }
    }
}

/// Remaining TTL in seconds, or -1 for an item that never expires.
fn ttl(expiry: u64, now: u64) -> i64 {
    if expiry == 0 {
        -1
    } else {
        expiry.saturating_sub(now) as i64
    }
}

/// Writes a reply that carries only a status code and return flags.
fn status(out: &mut Reply, code: &[u8], opts: &MetaOpts<'_>, details: &Details) {
    out.extend_from_slice(code);
    return_flags(out, opts, details);
    out.extend_from_slice(b"\r\n");
}

pub(super) fn get(
    ctx: &Context,
    opts: MetaOpts<'_>,
    now: u64,
    out: &mut Reply,
    stats: &mut LocalStats,
) {
    let args = GetArgs {
        touch: opts.ttl.map(|seconds| expiry(seconds, now)),
        no_bump: opts.no_bump,
        vivify: opts.vivify.map(|seconds| expiry(seconds, now)),
        recache: opts.recache.map(|seconds| seconds.max(0) as u64),
        cas_override: opts.new_cas,
    };
    // Like memcached, a touching hit counts as a touch only and any miss as a get only.
    let touch = args.touch.is_some();
    let Some(hit) = ctx.store.get(&opts.key, now, &args) else {
        stats.bump(Stat::CmdGet);
        stats.bump(Stat::GetMisses);
        if !opts.quiet {
            status(out, b"EN", &opts, &Details::default());
        }
        return;
    };
    if touch {
        stats.bump(Stat::CmdTouch);
        stats.bump(Stat::TouchHits);
    } else {
        stats.bump(Stat::CmdGet);
        stats.bump(Stat::GetHits);
    }
    // A matching `C` token tells the server the client already has this value.
    let value = opts.value && opts.cas != Some(hit.cas);
    if value {
        out.extend_from_slice(b"VA ");
        out.number(hit.value.len() as u64);
    } else {
        out.extend_from_slice(b"HD");
    }
    let details = Details {
        cas: Some(hit.cas),
        flags: Some(hit.flags),
        hit: Some(hit.fetched),
        idle: Some(now.saturating_sub(hit.last_access)),
        size: Some(hit.value.len()),
        ttl: Some(ttl(hit.expiry, now)),
    };
    return_flags(out, &opts, &details);
    if hit.lost {
        out.extend_from_slice(b" Z");
    }
    if hit.stale {
        out.extend_from_slice(b" X");
    }
    if hit.win {
        out.extend_from_slice(b" W");
    }
    out.extend_from_slice(b"\r\n");
    if value {
        out.value(&hit.value);
        out.extend_from_slice(b"\r\n");
    }
}

pub(super) fn set(
    ctx: &Context,
    opts: MetaOpts<'_>,
    data: &[u8],
    now: u64,
    out: &mut Reply,
    stats: &mut LocalStats,
) {
    stats.bump(Stat::CmdSet);
    let mode = match opts.mode.map(|mode| mode.to_ascii_uppercase()) {
        Some(b'E') => SetMode::Add,
        Some(b'A') => SetMode::Append,
        Some(b'P') => SetMode::Prepend,
        Some(b'R') => SetMode::Replace,
        _ => SetMode::Set,
    };
    let args = SetArgs {
        mode,
        flags: opts.client_flags.unwrap_or(0),
        expiry: opts.ttl.map_or(0, |seconds| expiry(seconds, now)),
        cas: opts.cas,
        cas_override: opts.new_cas,
        invalidate: opts.invalidate,
        vivify: opts.vivify.map(|seconds| expiry(seconds, now)),
    };
    let outcome = ctx
        .store
        .set(&opts.key, Bytes::copy_from_slice(data), now, &args);
    count_cas(&args, &outcome, stats);
    // When nothing was stored, `c` reports 0 and `s` the size of the rejected value.
    let (code, cas, size): (&[u8], u64, usize) = match outcome {
        SetOutcome::Stored { cas, size } => {
            if opts.quiet {
                return;
            }
            (b"HD", cas, size)
        }
        SetOutcome::NotStored => (b"NS", 0, data.len()),
        SetOutcome::Exists => (b"EX", 0, data.len()),
        SetOutcome::NotFound => (b"NF", 0, data.len()),
        SetOutcome::TooLarge => {
            out.extend_from_slice(b"SERVER_ERROR object too large for cache\r\n");
            return;
        }
    };
    let details = Details {
        cas: Some(cas),
        size: Some(size),
        ..Default::default()
    };
    status(out, code, &opts, &details);
}

pub(super) fn delete(
    ctx: &Context,
    opts: MetaOpts<'_>,
    now: u64,
    out: &mut Reply,
    stats: &mut LocalStats,
) {
    let args = DeleteArgs {
        cas: opts.cas,
        invalidate: opts.invalidate,
        expiry: opts.ttl.map(|seconds| expiry(seconds, now)),
        keep_empty: opts.remove_value,
        flags: opts.client_flags.unwrap_or(0),
        cas_override: opts.new_cas,
    };
    let code: &[u8] = match ctx.store.delete(&opts.key, now, &args) {
        DeleteOutcome::Deleted => {
            stats.bump(Stat::DeleteHits);
            if opts.quiet {
                return;
            }
            b"HD"
        }
        DeleteOutcome::NotFound => {
            stats.bump(Stat::DeleteMisses);
            b"NF"
        }
        DeleteOutcome::Exists => b"EX",
    };
    status(out, code, &opts, &Details::default());
}

pub(super) fn arith(
    ctx: &Context,
    opts: MetaOpts<'_>,
    now: u64,
    out: &mut Reply,
    stats: &mut LocalStats,
) {
    let decr = matches!(opts.mode, Some(b'D' | b'-'));
    // A new item takes its TTL from whichever of `N` and `T` comes last on the line.
    let ttl_last = opts
        .flags
        .split(|byte| *byte == b' ')
        .rev()
        .find_map(|token| match token.first() {
            Some(b'T') => Some(true),
            Some(b'N') => Some(false),
            _ => None,
        })
        == Some(true);
    let created_ttl = if ttl_last { opts.ttl } else { opts.vivify };
    let args = ArithArgs {
        decr,
        delta: opts.delta.unwrap_or(1),
        cas: opts.cas,
        vivify: opts
            .vivify
            .and(created_ttl)
            .map(|seconds| (expiry(seconds, now), opts.initial.unwrap_or(0))),
        touch: opts.ttl.map(|seconds| expiry(seconds, now)),
        cas_override: opts.new_cas,
    };
    let outcome = ctx.store.arith(&opts.key, now, &args);
    let found = matches!(outcome, ArithOutcome::Value { .. });
    stats.bump(match (decr, found) {
        (false, true) => Stat::IncrHits,
        (false, false) => Stat::IncrMisses,
        (true, true) => Stat::DecrHits,
        (true, false) => Stat::DecrMisses,
    });
    match outcome {
        ArithOutcome::Value { value, cas, expiry } => {
            if opts.quiet {
                return;
            }
            let details = Details {
                cas: Some(cas),
                ttl: Some(ttl(expiry, now)),
                ..Default::default()
            };
            if opts.value {
                let digits = value.to_string();
                let _ = write!(out, "VA {}", digits.len());
                return_flags(out, &opts, &details);
                let _ = write!(out, "\r\n{digits}\r\n");
            } else {
                status(out, b"HD", &opts, &details);
            }
        }
        ArithOutcome::NotFound => status(out, b"NF", &opts, &Details::default()),
        ArithOutcome::Exists => status(out, b"EX", &opts, &Details::default()),
        ArithOutcome::NonNumeric => {
            out.extend_from_slice(
                b"CLIENT_ERROR cannot increment or decrement non-numeric value\r\n",
            );
        }
    }
}

pub(super) fn debug(ctx: &Context, opts: MetaOpts<'_>, now: u64, out: &mut Reply) {
    let Some(meta) = ctx.store.peek(&opts.key, now) else {
        out.extend_from_slice(b"EN\r\n");
        return;
    };
    out.extend_from_slice(b"ME ");
    out.extend_from_slice(opts.raw_key);
    let _ = write!(
        out,
        " exp={} la={} cas={} fetch={} cls=1 size={}\r\n",
        ttl(meta.expiry, now),
        now.saturating_sub(meta.last_access),
        meta.cas,
        if meta.fetched { "yes" } else { "no" },
        meta.size,
    );
}
