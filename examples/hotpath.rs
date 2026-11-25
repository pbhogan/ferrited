//! Measures ferrited's request handling alone, without the network, kernel or event loop.
//!
//! Feeds pipelined classic requests (1 set to 10 gets over a preloaded key space, 16 per
//! batch, like `scripts/bench.sh`'s `pipe16`) through a `server::Session`, one batch per read,
//! and reports nanoseconds per request.
//!
//! - `meta`: sends Dalli 5's commands instead: `mg <key> v f` and `ms <key> <len> c F0 T0 MS`.
//! - `threads` above 1: one session per thread on a shared store, to expose contention; the
//!   result is wall time per request across all threads.
//!
//! Run: `cargo run --release --example hotpath [rounds] [classic|meta] [threads]`.

use std::sync::Barrier;
use std::time::Instant;

use clap::Parser;
use ferrited::config::Config;
use ferrited::server::{Context, Outcome, Session};
use ferrited::stats::LocalStats;

const KEYS: u64 = 100_000;
const VALUE: usize = 512;
const PIPELINE: usize = 16;
const BATCHES: usize = 4096;

fn main() {
    let rounds: usize = std::env::args()
        .nth(1)
        .map_or(20, |arg| arg.parse().expect("rounds"));
    let meta = match std::env::args().nth(2).as_deref() {
        None | Some("classic") => false,
        Some("meta") => true,
        Some(other) => panic!("unknown protocol {other}"),
    };
    let threads: usize = std::env::args()
        .nth(3)
        .map_or(1, |arg| arg.parse().expect("threads"));
    let config = Config::parse_from(["ferrited", "-m", "1024"]);
    let ctx = Context::new(&config);

    let value = vec![b'x'; VALUE];
    let set = |out: &mut Vec<u8>, key: u64| {
        let line = match meta {
            true => format!("ms memtier-{key} {VALUE} c F0 T0 MS\r\n"),
            false => format!("set memtier-{key} 0 0 {VALUE}\r\n"),
        };
        out.extend_from_slice(line.as_bytes());
        out.extend_from_slice(&value);
        out.extend_from_slice(b"\r\n");
    };
    let mut preload = vec![Vec::new()];
    for key in 1..=KEYS {
        if preload.last().unwrap().len() > 8 * 1024 {
            preload.push(Vec::new());
        }
        set(preload.last_mut().unwrap(), key);
    }
    run(&ctx, &preload, false);

    // Fixed pseudo-random sequences, so every run sends the same requests.
    let workloads: Vec<Vec<Vec<u8>>> = (0..threads)
        .map(|thread| {
            let mut seed = 0x2545_f491_4f6c_dd1du64.wrapping_mul(thread as u64 + 1);
            let mut random = move || {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed % KEYS + 1
            };
            let mut batches = Vec::with_capacity(BATCHES);
            for batch_index in 0..BATCHES {
                let mut batch = Vec::new();
                for request in 0..PIPELINE {
                    let key = random();
                    match (batch_index * PIPELINE + request) % 11 {
                        0 => set(&mut batch, key),
                        _ => {
                            let line = match meta {
                                true => format!("mg memtier-{key} v f\r\n"),
                                false => format!("get memtier-{key}\r\n"),
                            };
                            batch.extend_from_slice(line.as_bytes());
                        }
                    }
                }
                batches.push(batch);
            }
            batches
        })
        .collect();

    // Every request must succeed, or the timing would describe error handling.
    let replies = run(&ctx, &workloads[0], true);
    let hit = match meta {
        true => &b"VA "[..],
        false => &b"VALUE "[..],
    };
    let hits = memchr::memmem::find_iter(&replies, hit).count();
    let expected = (0..BATCHES * PIPELINE)
        .filter(|index| index % 11 != 0)
        .count();
    assert_eq!(hits, expected, "not every get hit");
    assert!(
        memchr::memmem::find(&replies, b"ERROR").is_none(),
        "a request failed"
    );

    let requests = (threads * BATCHES * PIPELINE) as f64;
    let mut best = f64::MAX;
    for _ in 0..rounds {
        let start = Barrier::new(threads + 1);
        let elapsed = std::thread::scope(|scope| {
            for batches in &workloads {
                let (ctx, start) = (&ctx, &start);
                scope.spawn(move || {
                    start.wait();
                    run(ctx, batches, false);
                });
            }
            start.wait();
            Instant::now()
        });
        best = best.min(elapsed.elapsed().as_nanos() as f64 / requests);
    }
    let protocol = if meta { "meta" } else { "classic" };
    println!("{protocol}, {threads} threads, best of {rounds} rounds: {best:.1} ns/request");
}

/// Serves `batches` on one session, one batch per read; returns the replies if `capture`.
fn run(ctx: &Context, batches: &[Vec<u8>], capture: bool) -> Vec<u8> {
    let mut session = Session::new();
    let mut stats = LocalStats::default();
    let mut replies = capture.then(Vec::new);
    let mut written = 0;
    for batch in batches {
        session.feed(batch, &mut stats);
        let now = (ctx.wall_clock)();
        loop {
            let mut budget = usize::MAX;
            let outcome = session.run(ctx, now, &mut budget, &mut stats);
            written += session.take_output(replies.as_mut());
            match outcome {
                Outcome::NeedInput => break,
                Outcome::OutputFull => {}
                other => panic!("unexpected {other:?}"),
            }
        }
        ctx.stats.merge(&mut stats);
    }
    assert!(written > 0);
    replies.unwrap_or_default()
}
