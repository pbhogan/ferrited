//! Measures the store's delete path, with and without tombstones (peers configured or not).
//!
//! Each round runs one workload on a fresh store (1 GiB, 64 shards) on one thread; the best
//! round's nanoseconds per request are reported.
//!
//! - `miss`: deletes 2 million never-stored keys. With tombstones, they overflow the default
//!   tombstone memory (10% of the limit), so later deletes also evict the oldest tombstones.
//! - `hit`: sets then deletes 1 million keys, counted as two requests each.
//!
//! Run: `cargo run --release --example deletes [rounds]`. `CAP=<bytes>` sets tombstone memory.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use ferrited::clock::{Clock, unix_secs};
use ferrited::store::{DeleteArgs, SetArgs, Store, StoreConfig};

const MEMORY: usize = 1 << 30;
const MISSES: usize = 2_000_000;
const HITS: usize = 1_000_000;

fn store(tombstones: bool) -> Store {
    let config = StoreConfig {
        memory_limit: MEMORY,
        item_max: 1 << 20,
        shards: 64,
    };
    let store = Store::new(config, Arc::new(Clock::new(1)));
    if tombstones {
        let cap = std::env::var("CAP").map_or(MEMORY / 10, |cap| cap.parse().expect("CAP"));
        store.enable_tombstones(3600, cap);
    }
    store
}

fn keys(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|index| format!("views/posts/{index}").into_bytes())
        .collect()
}

/// Best nanoseconds per request over `rounds` runs of `workload`, each on a fresh store.
fn best(rounds: usize, tombstones: bool, requests: usize, workload: impl Fn(&Store)) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..rounds {
        let store = store(tombstones);
        let start = Instant::now();
        workload(&store);
        let elapsed = start.elapsed().as_nanos() as f64 / requests as f64;
        best = best.min(elapsed);
    }
    best
}

fn main() {
    let rounds: usize = std::env::args()
        .nth(1)
        .map_or(5, |arg| arg.parse().expect("rounds"));
    let now = unix_secs();
    let delete = DeleteArgs::default();
    let set = SetArgs::default();
    let value = Bytes::from_static(&[b'x'; 64]);
    let misses = keys(MISSES);
    let hits = keys(HITS);
    for tombstones in [false, true] {
        let label = if tombstones { "peers" } else { "no peers" };
        let miss = best(rounds, tombstones, MISSES, |store| {
            for key in &misses {
                store.delete(key, now, &delete);
            }
        });
        let hit = best(rounds, tombstones, 2 * HITS, |store| {
            for key in &hits {
                store.set(key, value.clone(), now, &set);
                store.delete(key, now, &delete);
            }
        });
        println!("{label:>8}: delete miss {miss:6.1} ns, set + delete {hit:6.1} ns/request");
    }
}
