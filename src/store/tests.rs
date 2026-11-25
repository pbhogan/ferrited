use std::sync::Arc;

use bytes::Bytes;

use super::*;
use crate::clock::Clock;
use crate::oplog::{Change, Oplog, Record};

const NOW: u64 = 1_000_000;

fn store(memory_limit: usize) -> Store {
    store_with_shards(memory_limit, 4)
}

fn store_with_shards(memory_limit: usize, shards: usize) -> Store {
    let config = StoreConfig {
        memory_limit,
        item_max: 1 << 20,
        shards,
    };
    Store::new(config, Arc::new(Clock::new(1)))
}

fn bytes(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

fn put(store: &Store, key: &str, value: &str) -> SetOutcome {
    store.set(key.as_bytes(), bytes(value), NOW, &SetArgs::default())
}

fn put_mode(store: &Store, key: &str, value: &str, mode: SetMode) -> SetOutcome {
    store.set(
        key.as_bytes(),
        bytes(value),
        NOW,
        &SetArgs {
            mode,
            ..Default::default()
        },
    )
}

fn value_at(store: &Store, key: &str, now: u64) -> Option<String> {
    store
        .get(key.as_bytes(), now, &GetArgs::default())
        .map(|hit| String::from_utf8(hit.value.to_vec()).unwrap())
}

fn value(store: &Store, key: &str) -> Option<String> {
    value_at(store, key, NOW)
}

fn cas_of(store: &Store, key: &str) -> u64 {
    store
        .get(key.as_bytes(), NOW, &GetArgs::default())
        .unwrap()
        .cas
}

#[test]
fn set_then_get_returns_value_and_flags() {
    let store = store(64 << 20);
    let args = SetArgs {
        flags: 7,
        ..Default::default()
    };
    assert!(matches!(
        store.set(b"k", bytes("hello"), NOW, &args),
        SetOutcome::Stored { .. }
    ));
    let hit = store.get(b"k", NOW, &GetArgs::default()).unwrap();
    assert_eq!(&hit.value[..], b"hello");
    assert_eq!(hit.flags, 7);
    assert_eq!(value(&store, "missing"), None);
}

#[test]
fn item_expires_at_its_expiry_second() {
    let store = store(64 << 20);
    let args = SetArgs {
        expiry: NOW + 10,
        ..Default::default()
    };
    store.set(b"k", bytes("v"), NOW, &args);
    assert_eq!(value_at(&store, "k", NOW + 9), Some("v".into()));
    assert_eq!(value_at(&store, "k", NOW + 10), None);
    assert_eq!(store.stats().curr_items, 0);
}

#[test]
fn add_and_replace_respect_existence() {
    let store = store(64 << 20);
    assert_eq!(
        put_mode(&store, "k", "x", SetMode::Replace),
        SetOutcome::NotStored
    );
    assert!(matches!(
        put_mode(&store, "k", "a", SetMode::Add),
        SetOutcome::Stored { .. }
    ));
    assert_eq!(
        put_mode(&store, "k", "b", SetMode::Add),
        SetOutcome::NotStored
    );
    assert!(matches!(
        put_mode(&store, "k", "c", SetMode::Replace),
        SetOutcome::Stored { .. }
    ));
    assert_eq!(value(&store, "k"), Some("c".into()));
}

#[test]
fn append_and_prepend_join_values_and_keep_flags() {
    let store = store(64 << 20);
    assert_eq!(
        put_mode(&store, "k", "x", SetMode::Append),
        SetOutcome::NotStored
    );
    store.set(
        b"k",
        bytes("mid"),
        NOW,
        &SetArgs {
            flags: 9,
            ..Default::default()
        },
    );
    put_mode(&store, "k", "-end", SetMode::Append);
    put_mode(&store, "k", "start-", SetMode::Prepend);
    let hit = store.get(b"k", NOW, &GetArgs::default()).unwrap();
    assert_eq!(&hit.value[..], b"start-mid-end");
    assert_eq!(hit.flags, 9);
}

#[test]
fn append_on_miss_with_vivify_creates_item() {
    let store = store(64 << 20);
    let args = SetArgs {
        mode: SetMode::Append,
        vivify: Some(0),
        ..Default::default()
    };
    assert!(matches!(
        store.set(b"k", bytes("x"), NOW, &args),
        SetOutcome::Stored { .. }
    ));
    assert_eq!(value(&store, "k"), Some("x".into()));
}

#[test]
fn cas_checks_token() {
    let store = store(64 << 20);
    let with_cas = |cas| SetArgs {
        cas: Some(cas),
        ..Default::default()
    };
    assert_eq!(
        store.set(b"k", bytes("x"), NOW, &with_cas(1)),
        SetOutcome::NotFound
    );
    put(&store, "k", "a");
    let token = cas_of(&store, "k");
    assert_eq!(
        store.set(b"k", bytes("b"), NOW, &with_cas(token + 1)),
        SetOutcome::Exists
    );
    assert!(matches!(
        store.set(b"k", bytes("b"), NOW, &with_cas(token)),
        SetOutcome::Stored { .. }
    ));
    assert_ne!(cas_of(&store, "k"), token);
    assert_eq!(value(&store, "k"), Some("b".into()));
}

/// Runs a set in `mode` with CAS token `cas` and returns the outcome and the value afterwards.
fn set_with_cas(
    store: &Store,
    key: &str,
    mode: SetMode,
    cas: u64,
    invalidate: bool,
) -> (SetOutcome, Option<String>) {
    let args = SetArgs {
        mode,
        cas: Some(cas),
        invalidate,
        ..Default::default()
    };
    let outcome = store.set(key.as_bytes(), bytes("x"), NOW, &args);
    (outcome, value(store, key))
}

#[test]
fn a_cas_token_on_a_missing_key_depends_on_the_mode() {
    let store = store(64 << 20);
    let outcome = |key, mode| set_with_cas(&store, key, mode, 5, false);
    assert_eq!(outcome("set", SetMode::Set), (SetOutcome::NotFound, None));
    assert_eq!(
        outcome("replace", SetMode::Replace),
        (SetOutcome::NotFound, None)
    );
    // Add ignores the token.
    let (added, stored) = outcome("add", SetMode::Add);
    assert!(matches!(added, SetOutcome::Stored { .. }));
    assert_eq!(stored, Some("x".into()));
    // Append and prepend report a miss the way they do without a token.
    assert_eq!(
        outcome("append", SetMode::Append),
        (SetOutcome::NotStored, None)
    );
    assert_eq!(
        outcome("prepend", SetMode::Prepend),
        (SetOutcome::NotStored, None)
    );
    // With vivify they create the item, token or not.
    let args = SetArgs {
        mode: SetMode::Append,
        cas: Some(5),
        vivify: Some(0),
        ..Default::default()
    };
    assert!(matches!(
        store.set(b"vivified", bytes("x"), NOW, &args),
        SetOutcome::Stored { .. }
    ));
}

#[test]
fn a_cas_token_on_an_existing_key_depends_on_the_mode() {
    let store = store(64 << 20);
    let run = |mode, offset: i64, invalidate| {
        put(&store, "k", "a");
        let token = cas_of(&store, "k").wrapping_add_signed(offset);
        set_with_cas(&store, "k", mode, token, invalidate)
    };
    let stored = |outcome: (SetOutcome, Option<String>), expected: &str| {
        assert!(
            matches!(outcome.0, SetOutcome::Stored { .. }),
            "{outcome:?}"
        );
        assert_eq!(outcome.1.as_deref(), Some(expected));
    };
    let unchanged = |result| (result, Some("a".to_string()));
    for mode in [SetMode::Set, SetMode::Replace] {
        stored(run(mode, 0, false), "x");
        assert_eq!(run(mode, -1, false), unchanged(SetOutcome::Exists));
        stored(run(mode, -1, true), "x");
        assert_eq!(run(mode, 1, true), unchanged(SetOutcome::Exists));
    }
    // Add fails on an existing key whatever the token.
    for offset in [0, -1] {
        assert_eq!(
            run(SetMode::Add, offset, false),
            unchanged(SetOutcome::NotStored)
        );
        assert_eq!(
            run(SetMode::Add, offset, true),
            unchanged(SetOutcome::NotStored)
        );
    }
    // Append and prepend check the token, and invalidate does not excuse an old one.
    stored(run(SetMode::Append, 0, false), "ax");
    stored(run(SetMode::Prepend, 0, false), "xa");
    for mode in [SetMode::Append, SetMode::Prepend] {
        assert_eq!(run(mode, -1, false), unchanged(SetOutcome::Exists));
        assert_eq!(run(mode, -1, true), unchanged(SetOutcome::Exists));
        assert!(!store.get(b"k", NOW, &GetArgs::default()).unwrap().stale);
    }
}

#[test]
fn cas_with_invalidate_accepts_older_token_and_marks_stale() {
    let store = store(64 << 20);
    put(&store, "k", "a");
    let token = cas_of(&store, "k");
    let args = SetArgs {
        cas: Some(token - 1),
        invalidate: true,
        ..Default::default()
    };
    assert!(matches!(
        store.set(b"k", bytes("b"), NOW, &args),
        SetOutcome::Stored { .. }
    ));
    assert!(store.get(b"k", NOW, &GetArgs::default()).unwrap().stale);
}

#[test]
fn cas_override_sets_the_token() {
    let store = store(64 << 20);
    let args = SetArgs {
        cas_override: Some(42),
        ..Default::default()
    };
    assert_eq!(
        store.set(b"k", bytes("a"), NOW, &args),
        SetOutcome::Stored { cas: 42, size: 1 }
    );
    assert_eq!(cas_of(&store, "k"), 42);
}

#[test]
fn oversized_value_is_rejected() {
    let store = store(64 << 20);
    let big = Bytes::from(vec![b'x'; (1 << 20) + 1]);
    assert_eq!(
        store.set(b"k", big, NOW, &SetArgs::default()),
        SetOutcome::TooLarge
    );
}

#[test]
fn delete_removes_and_reports() {
    let store = store(64 << 20);
    assert_eq!(
        store.delete(b"k", NOW, &DeleteArgs::default()),
        DeleteOutcome::NotFound
    );
    put(&store, "k", "a");
    let wrong = DeleteArgs {
        cas: Some(1),
        ..Default::default()
    };
    assert_eq!(store.delete(b"k", NOW, &wrong), DeleteOutcome::Exists);
    assert_eq!(
        store.delete(b"k", NOW, &DeleteArgs::default()),
        DeleteOutcome::Deleted
    );
    assert_eq!(value(&store, "k"), None);
    assert_eq!(store.stats().bytes, 0);
}

#[test]
fn touch_and_get_with_touch_change_expiry() {
    let store = store(64 << 20);
    assert!(!store.touch(b"k", NOW, NOW + 5));
    put(&store, "k", "a");
    assert!(store.touch(b"k", NOW, NOW + 5));
    assert_eq!(value_at(&store, "k", NOW + 5), None);
    put(&store, "k", "a");
    let args = GetArgs {
        touch: Some(NOW + 5),
        ..Default::default()
    };
    assert_eq!(store.get(b"k", NOW, &args).unwrap().expiry, NOW + 5);
    assert_eq!(value_at(&store, "k", NOW + 5), None);
}

#[test]
fn get_reports_previous_access_state() {
    let store = store(64 << 20);
    put(&store, "k", "a");
    let quiet = GetArgs {
        no_bump: true,
        ..Default::default()
    };
    assert!(!store.get(b"k", NOW + 3, &quiet).unwrap().fetched);
    let first = store.get(b"k", NOW + 3, &GetArgs::default()).unwrap();
    assert!(!first.fetched);
    assert_eq!(first.last_access, NOW);
    let second = store.get(b"k", NOW + 4, &GetArgs::default()).unwrap();
    assert!(second.fetched);
    assert_eq!(second.last_access, NOW + 3);
}

#[test]
fn stats_track_items_and_bytes() {
    let store = store(64 << 20);
    put(&store, "k", "abc");
    put(&store, "k", "abcdef");
    let stats = store.stats();
    assert_eq!(stats.curr_items, 1);
    assert_eq!(stats.total_items, 2);
    assert_eq!(stats.bytes as usize, cost(1, 6));
    assert_eq!(stats.limit_maxbytes, 64 << 20);
}

fn arith(store: &Store, key: &str, decr: bool, delta: u64) -> ArithOutcome {
    store.arith(
        key.as_bytes(),
        NOW,
        &ArithArgs {
            decr,
            delta,
            ..Default::default()
        },
    )
}

fn number(outcome: ArithOutcome) -> u64 {
    match outcome {
        ArithOutcome::Value { value, .. } => value,
        other => panic!("expected a value, got {other:?}"),
    }
}

#[test]
fn incr_and_decr_change_the_stored_number() {
    let store = store(64 << 20);
    assert_eq!(arith(&store, "n", false, 1), ArithOutcome::NotFound);
    put(&store, "n", "10");
    assert_eq!(number(arith(&store, "n", false, 5)), 15);
    assert_eq!(number(arith(&store, "n", true, 6)), 9);
    assert_eq!(value(&store, "n"), Some("9".into()));
}

#[test]
fn decr_stops_at_zero_and_incr_wraps() {
    let store = store(64 << 20);
    put(&store, "n", "3");
    assert_eq!(number(arith(&store, "n", true, 10)), 0);
    put(&store, "n", "18446744073709551615");
    assert_eq!(number(arith(&store, "n", false, 2)), 1);
}

#[test]
fn arith_rejects_non_numeric_values() {
    let store = store(64 << 20);
    put(&store, "n", "abc");
    assert_eq!(arith(&store, "n", false, 1), ArithOutcome::NonNumeric);
    put(&store, "n", "");
    assert_eq!(arith(&store, "n", false, 1), ArithOutcome::NonNumeric);
}

#[test]
fn stored_numbers_are_read_the_way_strtoull_reads_them() {
    let accepted: &[(&[u8], u64)] = &[
        (b"5", 5),
        (b" 5", 5),
        (b"5 ", 5),
        (b" 5 ", 5),
        (b"\t5", 5),
        (b"5\t", 5),
        (b"\n5\r", 5),
        (b"\x0b\x0c5\x0b", 5),
        (b"+5", 5),
        (b" +5", 5),
        (b"05", 5),
        (b"00000000000000000000005", 5),
        (b"5                      ", 5),
        (b"5 a", 5),
        (b"5 6", 5),
        (b"5\0", 5),
        (b"5\0abc", 5),
        (b"0", 0),
        (b"-0", 0),
        (b"18446744073709551615", u64::MAX),
        (b" 18446744073709551615", u64::MAX),
        (b"18446744073709551615 -", u64::MAX),
        // A negated magnitude wraps; it is accepted when nonnegative as a signed number.
        (b"-18446744073709551615", 1),
        (b"-9223372036854775809", 9223372036854775807),
    ];
    for (text, number) in accepted {
        assert_eq!(
            parse_number(text),
            Some(*number),
            "{:?}",
            text.escape_ascii().to_string()
        );
    }
    let rejected: &[&[u8]] = &[
        b"",
        b" ",
        b"  ",
        b"abc",
        b"5a",
        b"-5",
        b" -5",
        b"-9223372036854775808",
        b"-18446744073709551616",
        b"18446744073709551616",
        b"99999999999999999999",
        b"0x5",
        b"5.0",
        b"1e3",
        b"+ 5",
        b"- 5",
        b"--5",
        b"+-5",
        b"5-",
        b"-",
        b"+",
        b"\x005",
        b"5\x85",
        b"\x855",
        b"5\xa0",
    ];
    for text in rejected {
        assert_eq!(
            parse_number(text),
            None,
            "{:?}",
            text.escape_ascii().to_string()
        );
    }
}

#[test]
fn arith_accepts_a_number_surrounded_by_spaces() {
    let store = store(64 << 20);
    put(&store, "n", " 5");
    assert_eq!(number(arith(&store, "n", false, 1)), 6);
    put(&store, "n", "5 ");
    assert_eq!(number(arith(&store, "n", true, 1)), 4);
}

#[test]
fn arith_keeps_flags_and_changes_cas() {
    let store = store(64 << 20);
    store.set(
        b"n",
        bytes("1"),
        NOW,
        &SetArgs {
            flags: 5,
            ..Default::default()
        },
    );
    let before = cas_of(&store, "n");
    arith(&store, "n", false, 1);
    let hit = store.get(b"n", NOW, &GetArgs::default()).unwrap();
    assert_eq!(hit.flags, 5);
    assert_ne!(hit.cas, before);
}

#[test]
fn arith_vivifies_checks_cas_and_touches() {
    let store = store(64 << 20);
    let create = ArithArgs {
        delta: 1,
        vivify: Some((NOW + 50, 7)),
        ..Default::default()
    };
    assert_eq!(number(store.arith(b"n", NOW, &create)), 7);
    assert_eq!(number(store.arith(b"n", NOW, &create)), 8);
    let wrong = ArithArgs {
        delta: 1,
        cas: Some(1),
        ..Default::default()
    };
    assert_eq!(store.arith(b"n", NOW, &wrong), ArithOutcome::Exists);
    let touch = ArithArgs {
        delta: 1,
        touch: Some(NOW + 99),
        ..Default::default()
    };
    assert!(
        matches!(store.arith(b"n", NOW, &touch), ArithOutcome::Value { value: 9, expiry, .. } if expiry == NOW + 99)
    );
}

#[test]
fn flush_hides_existing_items_but_not_later_ones() {
    let store = store(64 << 20);
    put(&store, "old", "a");
    store.flush_all(NOW, 0);
    assert_eq!(value(&store, "old"), None);
    put(&store, "new", "b");
    assert_eq!(value(&store, "new"), Some("b".into()));
}

#[test]
fn delayed_flush_takes_effect_at_its_time() {
    let store = store(64 << 20);
    put(&store, "k", "a");
    store.flush_all(NOW, 10);
    assert_eq!(value_at(&store, "k", NOW + 9), Some("a".into()));
    assert_eq!(value_at(&store, "k", NOW + 10), None);
}

#[test]
fn sweep_removes_flushed_and_expired_items() {
    let store = store(64 << 20);
    for index in 0..100 {
        let args = SetArgs {
            expiry: NOW + 5,
            ..Default::default()
        };
        store.set(
            format!("expiring{index}").as_bytes(),
            bytes("v"),
            NOW,
            &args,
        );
    }
    put(&store, "keeper", "v");
    // Expired items are found by random sampling, so one pass may leave a few behind.
    for _ in 0..50 {
        store.sweep(NOW + 5);
    }
    assert_eq!(store.stats().curr_items, 1);

    store.flush_all(NOW + 5, 0);
    store.sweep(NOW + 5);
    let stats = store.stats();
    assert_eq!(stats.curr_items, 0);
    assert_eq!(stats.bytes, 0);
    assert_eq!(stats.reclaimed, 101);
}

#[test]
fn usage_stays_under_the_limit_and_newest_item_survives() {
    let limit = 64 * 1024;
    let store = store(limit);
    let payload = "x".repeat(1024);
    for index in 0..200 {
        put(&store, &format!("key{index}"), &payload);
        assert!(store.stats().bytes as usize <= limit);
        assert_eq!(value(&store, &format!("key{index}")), Some(payload.clone()));
    }
    let stats = store.stats();
    assert!(stats.evictions > 0);
    assert!(stats.curr_items < 200);
}

#[test]
fn eviction_prefers_expired_items() {
    let limit = 64 * 1024;
    let store = store(limit);
    let payload = "x".repeat(1024);
    for index in 0..40 {
        let args = SetArgs {
            expiry: NOW + 1,
            ..Default::default()
        };
        store.set(
            format!("old{index}").as_bytes(),
            bytes(&payload),
            NOW,
            &args,
        );
    }
    for index in 0..40 {
        store.set(
            format!("new{index}").as_bytes(),
            bytes(&payload),
            NOW + 1,
            &SetArgs::default(),
        );
    }
    // Victims are sampled at random, so a live item is sometimes evicted instead.
    let stats = store.stats();
    assert!(
        stats.reclaimed > stats.evictions,
        "reclaimed {} evictions {}",
        stats.reclaimed,
        stats.evictions
    );
}

#[test]
fn an_item_larger_than_the_limit_is_rejected_and_stores_nothing() {
    let limit = 64 * 1024;
    let store = store(limit);
    put(&store, "small", "v");
    let before = store.stats().bytes;
    let outcome = store.set(
        b"huge",
        Bytes::from(vec![b'x'; 100 * 1024]),
        NOW,
        &SetArgs::default(),
    );
    assert_eq!(outcome, SetOutcome::TooLarge);
    assert_eq!(store.stats().bytes, before);
    assert_eq!(value(&store, "huge"), None);
    assert_eq!(value(&store, "small"), Some("v".into()));
}

#[test]
fn a_large_item_survives_its_own_insert_in_a_full_cache() {
    let limit = 64 << 20;
    let store = store_with_shards(limit, 128);
    let small = "x".repeat(100);
    let mut index = 0;
    while (store.stats().bytes as usize) < limit {
        put(&store, &format!("fill{index}"), &small);
        index += 1;
    }
    // Keep filling so the cache is at its limit and evicting.
    for extra in 0..1000 {
        put(&store, &format!("more{extra}"), &small);
    }
    let big = Bytes::from(vec![b'y'; 1 << 20]);
    for index in 0..20 {
        let key = format!("big{index}");
        let outcome = store.set(key.as_bytes(), big.clone(), NOW, &SetArgs::default());
        assert!(matches!(outcome, SetOutcome::Stored { .. }), "{outcome:?}");
        let hit = store.get(key.as_bytes(), NOW, &GetArgs::default());
        assert_eq!(hit.map(|hit| hit.value.len()), Some(1 << 20), "key {key}");
        assert!(store.stats().bytes as usize <= limit);
    }
}

#[test]
fn append_that_exceeds_the_limit_is_rejected() {
    let limit = 4 * 1024;
    let store = store(limit);
    put(&store, "k", &"a".repeat(3000));
    let before = store.stats().bytes;
    assert_eq!(
        put_mode(&store, "k", &"b".repeat(3000), SetMode::Append),
        SetOutcome::TooLarge
    );
    assert_eq!(store.stats().bytes, before);
    assert_eq!(value(&store, "k").map(|text| text.len()), Some(3000));
}

#[test]
fn flush_with_a_huge_delay_does_not_panic_or_flush() {
    let store = store(64 << 20);
    put(&store, "k", "a");
    store.flush_all(NOW, u64::MAX);
    assert_eq!(value(&store, "k"), Some("a".into()));
}

#[test]
fn flush_racing_writers_leaves_consistent_accounting() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let store = Arc::new(store(64 << 20));
    let running = Arc::new(AtomicBool::new(true));
    let keys: Vec<String> = (0..8).map(|index| format!("key{index}")).collect();
    let flusher = {
        let store = store.clone();
        let running = running.clone();
        std::thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                store.flush_all(NOW, 0);
                std::thread::yield_now();
            }
        })
    };
    let writers: Vec<_> = (0..4)
        .map(|thread| {
            let store = store.clone();
            let keys = keys.clone();
            std::thread::spawn(move || {
                let started = std::time::Instant::now();
                let mut round = 0usize;
                while started.elapsed() < std::time::Duration::from_millis(300) {
                    let key = keys[(round + thread) % keys.len()].as_bytes();
                    match round % 5 {
                        0 => {
                            store.set(key, bytes("1"), NOW, &SetArgs::default());
                        }
                        1 => {
                            let args = SetArgs {
                                mode: SetMode::Append,
                                vivify: Some(0),
                                ..Default::default()
                            };
                            store.set(key, bytes("2"), NOW, &args);
                        }
                        2 => {
                            let args = ArithArgs {
                                delta: 1,
                                vivify: Some((0, 1)),
                                ..Default::default()
                            };
                            store.arith(key, NOW, &args);
                        }
                        3 => {
                            let args = DeleteArgs {
                                invalidate: true,
                                ..Default::default()
                            };
                            store.delete(key, NOW, &args);
                        }
                        _ => {
                            let args = GetArgs {
                                vivify: Some(0),
                                ..Default::default()
                            };
                            store.get(key, NOW, &args);
                        }
                    }
                    round += 1;
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    running.store(false, Ordering::Relaxed);
    flusher.join().unwrap();

    for _ in 0..50 {
        store.sweep(NOW);
    }
    let mut live = 0;
    let mut expected_bytes = 0;
    for key in &keys {
        if let Some(hit) = store.get(key.as_bytes(), NOW, &GetArgs::default()) {
            live += 1;
            expected_bytes += cost(key.len(), hit.value.len());
        }
    }
    let stats = store.stats();
    assert_eq!(stats.curr_items, live);
    assert_eq!(stats.bytes as usize, expected_bytes);
}

type Mutation = fn(&Store);

/// Waits until a flush holds back a mutation, or a second, so a store that lets mutations
/// through fails the caller's checks instead of hanging.
fn wait_for_a_stamp_retry(store: &Store) {
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    let started = Instant::now();
    while store.stamp_retries.load(Ordering::Relaxed) == 0
        && started.elapsed() < Duration::from_secs(1)
    {
        std::thread::yield_now();
    }
}

/// Runs `mutation` on another thread while a flush has taken its version but not raised the
/// floor, then finishes the flush once the mutation is waiting.
fn mutate_inside_a_flush(mutation: Mutation) -> Arc<Store> {
    let store = Arc::new(store(64 << 20));
    put(&store, "k", "1");
    let version = store.begin_flush();
    let worker = {
        let store = store.clone();
        std::thread::spawn(move || mutation(&store))
    };
    wait_for_a_stamp_retry(&store);
    store.finish_flush(version);
    worker.join().unwrap();
    store
}

#[test]
fn no_mutation_carries_content_past_a_flush_it_overlaps() {
    let mutations: [(&str, Mutation); 5] = [
        ("touch", |store| {
            store.touch(b"k", NOW, NOW + 60);
        }),
        ("touching get", |store| {
            let args = GetArgs {
                touch: Some(NOW + 60),
                ..Default::default()
            };
            store.get(b"k", NOW, &args);
        }),
        ("append", |store| {
            put_mode(store, "k", "2", SetMode::Append);
        }),
        ("incr", |store| {
            arith(store, "k", false, 1);
        }),
        ("invalidating delete", |store| {
            let args = DeleteArgs {
                invalidate: true,
                ..Default::default()
            };
            store.delete(b"k", NOW, &args);
        }),
    ];
    for (name, mutation) in mutations {
        let store = mutate_inside_a_flush(mutation);
        assert_eq!(value(&store, "k"), None, "{name} kept flushed content");
        // Not merely hidden: nothing below the floor is left behind to be found later.
        store.sweep(NOW);
        assert_eq!(store.stats().curr_items, 0, "{name}");
    }
}

#[test]
fn a_set_inside_a_flush_is_kept_or_flushed_whole() {
    let store = mutate_inside_a_flush(|store| {
        put(store, "k", "new");
    });
    // The set waited for the flush to finish, so it took a version above the floor.
    assert_eq!(value(&store, "k").as_deref(), Some("new"));
}

#[test]
fn concurrent_flushes_hold_mutations_until_both_finish() {
    let store = Arc::new(store(64 << 20));
    put(&store, "k", "1");
    // Two flushes in progress at once must not look like none in progress.
    let first = store.begin_flush();
    let second = store.begin_flush();
    let worker = {
        let store = store.clone();
        std::thread::spawn(move || store.touch(b"k", NOW, NOW + 60))
    };
    wait_for_a_stamp_retry(&store);
    store.finish_flush(second);
    // The first flush is still between its version and its floor, so the touch keeps waiting.
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(!worker.is_finished());
    store.finish_flush(first);
    assert!(!worker.join().unwrap());
    assert_eq!(value(&store, "k"), None);
}

#[test]
fn a_delayed_flush_activated_by_a_mutation_is_seen_by_it() {
    let store = store(64 << 20);
    put(&store, "k", "1");
    store.flush_all(NOW, 10);
    // The touch's own floor read activates the flush, so the touch must find nothing.
    assert!(!store.touch(b"k", NOW + 10, NOW + 60));
    assert_eq!(value_at(&store, "k", NOW + 10), None);
}

#[test]
fn vivify_creates_a_stub_and_only_the_first_reader_wins() {
    let store = store(64 << 20);
    let args = GetArgs {
        vivify: Some(NOW + 30),
        ..Default::default()
    };
    let first = store.get(b"k", NOW, &args).unwrap();
    assert!(first.win && !first.lost && first.value.is_empty());
    let second = store.get(b"k", NOW, &args).unwrap();
    assert!(!second.win && second.lost);
    put(&store, "k", "fresh");
    let third = store.get(b"k", NOW, &args).unwrap();
    assert!(!third.win && !third.lost);
    assert_eq!(&third.value[..], b"fresh");
}

#[test]
fn recache_wins_once_when_ttl_is_low() {
    let store = store(64 << 20);
    store.set(
        b"k",
        bytes("v"),
        NOW,
        &SetArgs {
            expiry: NOW + 20,
            ..Default::default()
        },
    );
    let relaxed = GetArgs {
        recache: Some(10),
        ..Default::default()
    };
    assert!(!store.get(b"k", NOW, &relaxed).unwrap().win);
    let eager = GetArgs {
        recache: Some(30),
        ..Default::default()
    };
    assert!(store.get(b"k", NOW, &eager).unwrap().win);
    let again = store.get(b"k", NOW, &eager).unwrap();
    assert!(!again.win && again.lost);
}

#[test]
fn invalidating_delete_marks_stale_and_hands_out_one_token() {
    let store = store(64 << 20);
    put(&store, "k", "v");
    let before = cas_of(&store, "k");
    let args = DeleteArgs {
        invalidate: true,
        expiry: Some(NOW + 30),
        ..Default::default()
    };
    assert_eq!(store.delete(b"k", NOW, &args), DeleteOutcome::Deleted);
    let first = store.get(b"k", NOW, &GetArgs::default()).unwrap();
    assert!(first.stale && first.win && !first.lost);
    assert_eq!(&first.value[..], b"v");
    assert_eq!(first.expiry, NOW + 30);
    assert_ne!(first.cas, before);
    let second = store.get(b"k", NOW, &GetArgs::default()).unwrap();
    assert!(second.stale && !second.win && second.lost);
}

#[test]
fn delete_with_keep_empty_leaves_an_empty_item() {
    let store = store(64 << 20);
    put(&store, "k", "value");
    let args = DeleteArgs {
        keep_empty: true,
        ..Default::default()
    };
    assert_eq!(store.delete(b"k", NOW, &args), DeleteOutcome::Deleted);
    assert_eq!(value(&store, "k"), Some(String::new()));
    assert_eq!(store.stats().bytes as usize, cost(1, 0));
}

#[test]
fn peek_reports_metadata_without_bumping() {
    let store = store(64 << 20);
    assert_eq!(store.peek(b"k", NOW), None);
    store.set(
        b"k",
        bytes("abc"),
        NOW,
        &SetArgs {
            expiry: NOW + 9,
            ..Default::default()
        },
    );
    let meta = store.peek(b"k", NOW + 2).unwrap();
    assert_eq!(
        (meta.expiry, meta.last_access, meta.fetched, meta.size),
        (NOW + 9, NOW, false, 3)
    );
    assert!(
        !store
            .get(b"k", NOW + 2, &GetArgs::default())
            .unwrap()
            .fetched
    );
}

fn logged_store() -> (Store, Arc<Oplog>) {
    let store = store(1 << 20);
    let oplog = Arc::new(Oplog::new(1024, u64::MAX));
    store.attach_oplog(oplog.clone());
    (store, oplog)
}

fn drained(oplog: &Oplog) -> Vec<Record> {
    let mut out = Vec::new();
    oplog.drain(&mut out, usize::MAX);
    out.into_iter()
        .map(|change| match change {
            Change::Record(record) => record,
            other => panic!("expected a record, got {other:?}"),
        })
        .collect()
}

fn put_record(record: &Record) -> (&[u8], u64, u64, u64, bool, &[u8]) {
    match record {
        Record::Put {
            key,
            version,
            cas,
            expiry,
            stale,
            value,
            ..
        } => (&key[..], *version, *cas, *expiry, *stale, &value[..]),
        other => panic!("expected a put, got {other:?}"),
    }
}

#[test]
fn a_set_logs_the_stored_item() {
    let (store, oplog) = logged_store();
    let args = SetArgs {
        flags: 5,
        expiry: NOW + 60,
        ..Default::default()
    };
    let SetOutcome::Stored { cas, .. } = store.set(b"k", bytes("v"), NOW, &args) else {
        panic!("not stored");
    };
    let records = drained(&oplog);
    assert_eq!(records.len(), 1);
    let (key, version, logged_cas, expiry, stale, value) = put_record(&records[0]);
    assert_eq!(
        (key, value, expiry, stale),
        (&b"k"[..], &b"v"[..], NOW + 60, false)
    );
    assert_eq!((version, logged_cas), (cas, cas));
    assert!(matches!(records[0], Record::Put { flags: 5, .. }));
}

#[test]
fn failed_mutations_and_reads_log_nothing() {
    let (store, oplog) = logged_store();
    put_mode(&store, "missing", "v", SetMode::Replace);
    store.delete(b"missing", NOW, &DeleteArgs::default());
    store.touch(b"missing", NOW, NOW + 10);
    put(&store, "k", "v");
    drained(&oplog);
    put_mode(&store, "k", "w", SetMode::Add);
    value(&store, "k");
    assert!(drained(&oplog).is_empty());
}

#[test]
fn every_failing_mutation_logs_nothing() {
    let (store, oplog) = logged_store();
    put(&store, "k", "v");
    put(&store, "n", "7");
    let expiring = SetArgs {
        expiry: NOW + 5,
        ..Default::default()
    };
    store.set(b"brief", bytes("v"), NOW, &expiring);
    let cas = cas_of(&store, "k");
    drained(&oplog);

    let mismatch = SetArgs {
        cas: Some(cas + 1),
        ..Default::default()
    };
    assert_eq!(
        store.set(b"k", bytes("w"), NOW, &mismatch),
        SetOutcome::Exists
    );
    assert_eq!(
        store.set(b"missing", bytes("w"), NOW, &mismatch),
        SetOutcome::NotFound
    );
    let delete_mismatch = DeleteArgs {
        cas: Some(cas + 1),
        ..Default::default()
    };
    assert_eq!(
        store.delete(b"k", NOW, &delete_mismatch),
        DeleteOutcome::Exists
    );
    assert_eq!(arith(&store, "k", false, 1), ArithOutcome::NonNumeric);
    let arith_mismatch = ArithArgs {
        delta: 1,
        cas: Some(cas_of(&store, "n") + 1),
        ..Default::default()
    };
    assert_eq!(
        store.arith(b"n", NOW, &arith_mismatch),
        ArithOutcome::Exists
    );
    assert!(!store.touch(b"brief", NOW + 5, NOW + 60));
    assert_eq!(
        put_mode(&store, "missing", "w", SetMode::Append),
        SetOutcome::NotStored
    );
    assert!(drained(&oplog).is_empty());
}

#[test]
fn a_delete_keeping_an_empty_item_logs_the_empty_item() {
    let (store, oplog) = logged_store();
    put(&store, "k", "value");
    let args = DeleteArgs {
        keep_empty: true,
        flags: 3,
        expiry: Some(NOW + 20),
        ..Default::default()
    };
    store.delete(b"k", NOW, &args);
    let records = drained(&oplog);
    assert_eq!(records.len(), 2);
    let (_, first_version, ..) = put_record(&records[0]);
    let (key, version, cas, expiry, stale, value) = put_record(&records[1]);
    assert!(version > first_version);
    assert_eq!(
        (key, cas, expiry, stale, value),
        (&b"k"[..], version, NOW + 20, false, &b""[..])
    );
    assert!(matches!(records[1], Record::Put { flags: 3, .. }));
}

#[test]
fn an_append_logs_the_joined_item() {
    let (store, oplog) = logged_store();
    let args = SetArgs {
        flags: 9,
        expiry: NOW + 60,
        ..Default::default()
    };
    store.set(b"k", bytes("ab"), NOW, &args);
    put_mode(&store, "k", "cd", SetMode::Append);
    let records = drained(&oplog);
    assert_eq!(records.len(), 2);
    let (_, first_version, ..) = put_record(&records[0]);
    let (key, version, cas, expiry, stale, value) = put_record(&records[1]);
    assert!(version > first_version);
    assert_eq!(
        (key, cas, expiry, stale, value),
        (&b"k"[..], version, NOW + 60, false, &b"abcd"[..])
    );
    assert!(matches!(records[1], Record::Put { flags: 9, .. }));
}

#[test]
fn a_delete_logs_a_newer_version_than_the_item() {
    let (store, oplog) = logged_store();
    put(&store, "k", "v");
    store.delete(b"k", NOW, &DeleteArgs::default());
    let records = drained(&oplog);
    assert_eq!(records.len(), 2);
    let (_, put_version, ..) = put_record(&records[0]);
    match &records[1] {
        Record::Delete { key, version } => {
            assert_eq!(&key[..], b"k");
            assert!(*version > put_version);
        }
        other => panic!("expected a delete, got {other:?}"),
    }
}

#[test]
fn a_touch_logs_a_new_version_and_keeps_the_cas_token() {
    let (store, oplog) = logged_store();
    put(&store, "k", "v");
    assert!(store.touch(b"k", NOW, NOW + 30));
    let records = drained(&oplog);
    assert_eq!(records.len(), 2);
    let (_, first_version, first_cas, ..) = put_record(&records[0]);
    let (_, version, cas, expiry, _, value) = put_record(&records[1]);
    assert!(version > first_version);
    assert_eq!((cas, expiry, value), (first_cas, NOW + 30, &b"v"[..]));
}

#[test]
fn a_touching_get_logs_and_a_plain_get_does_not() {
    let (store, oplog) = logged_store();
    put(&store, "k", "v");
    let records = drained(&oplog);
    let (_, first_version, first_cas, ..) = put_record(&records[0]);
    value(&store, "k");
    assert!(drained(&oplog).is_empty());
    let args = GetArgs {
        touch: Some(NOW + 40),
        ..Default::default()
    };
    store.get(b"k", NOW, &args).unwrap();
    let records = drained(&oplog);
    assert_eq!(records.len(), 1);
    let (_, version, cas, expiry, ..) = put_record(&records[0]);
    assert!(version > first_version);
    assert_eq!((cas, expiry), (first_cas, NOW + 40));
}

#[test]
fn a_vivifying_get_logs_the_empty_item() {
    let (store, oplog) = logged_store();
    let args = GetArgs {
        vivify: Some(NOW + 30),
        ..Default::default()
    };
    store.get(b"k", NOW, &args).unwrap();
    let records = drained(&oplog);
    assert_eq!(records.len(), 1);
    let (key, _, _, expiry, _, value) = put_record(&records[0]);
    assert_eq!((key, expiry, value), (&b"k"[..], NOW + 30, &b""[..]));
}

#[test]
fn arithmetic_logs_the_new_value() {
    let (store, oplog) = logged_store();
    put(&store, "n", "41");
    let args = ArithArgs {
        delta: 1,
        ..Default::default()
    };
    store.arith(b"n", NOW, &args);
    let records = drained(&oplog);
    assert_eq!(records.len(), 2);
    assert_eq!(put_record(&records[1]).5, b"42");
}

#[test]
fn an_invalidating_delete_logs_the_stale_item() {
    let (store, oplog) = logged_store();
    put(&store, "k", "v");
    let args = DeleteArgs {
        invalidate: true,
        ..Default::default()
    };
    store.delete(b"k", NOW, &args);
    let records = drained(&oplog);
    assert_eq!(records.len(), 2);
    let (_, _, _, _, stale, value) = put_record(&records[1]);
    assert!(stale);
    assert_eq!(value, b"v");
}

#[test]
fn flushes_log_when_they_take_effect() {
    let (store, oplog) = logged_store();
    store.flush_all(NOW, 0);
    assert!(matches!(drained(&oplog)[..], [Record::Flush { .. }]));
    store.flush_all(NOW, 10);
    assert!(drained(&oplog).is_empty());
    // The delayed flush takes effect on the first operation at or after its time.
    value_at(&store, "k", NOW + 10);
    assert!(matches!(drained(&oplog)[..], [Record::Flush { .. }]));
}

fn put_rec(key: &str, value: &str, version: u64, expiry: u64) -> Record {
    Record::Put {
        key: bytes(key),
        version,
        cas: version,
        flags: 0,
        expiry,
        stale: false,
        value: bytes(value),
    }
}

#[test]
fn apply_keeps_the_newest_version() {
    let store = store(1 << 20);
    store.apply(&put_rec("k", "new", 200, 0), NOW);
    store.apply(&put_rec("k", "old", 100, 0), NOW);
    assert_eq!(value(&store, "k").as_deref(), Some("new"));
    store.apply(&put_rec("k", "newer", 300, 0), NOW);
    assert_eq!(value(&store, "k").as_deref(), Some("newer"));
}

#[test]
fn apply_deletes_only_older_items() {
    let store = store(1 << 20);
    store.apply(&put_rec("k", "v", 200, 0), NOW);
    store.apply(
        &Record::Delete {
            key: bytes("k"),
            version: 100,
        },
        NOW,
    );
    assert!(value(&store, "k").is_some());
    store.apply(
        &Record::Delete {
            key: bytes("k"),
            version: 300,
        },
        NOW,
    );
    assert!(value(&store, "k").is_none());
}

#[test]
fn apply_drops_expired_items_and_honors_the_flush_floor() {
    let store = store(1 << 20);
    store.apply(&put_rec("k", "v", 100, 0), NOW);
    store.apply(&put_rec("k", "v", 200, NOW - 1), NOW);
    assert!(value(&store, "k").is_none());
    store.apply(&put_rec("a", "v", 300, 0), NOW);
    store.apply(&Record::Flush { version: 400 }, NOW);
    store.apply(&put_rec("b", "v", 350, 0), NOW);
    assert!(value(&store, "a").is_none());
    assert!(value(&store, "b").is_none());
    assert_eq!(store.flush_floor(), 400);
}

#[test]
fn applying_a_record_twice_is_the_same_as_once() {
    let store = store(1 << 20);
    let record = put_rec("k", "v", 200, 0);
    store.apply(&record, NOW);
    store.apply(&record, NOW);
    assert_eq!(value(&store, "k").as_deref(), Some("v"));
    assert_eq!(store.stats().curr_items, 1);
    let delete = Record::Delete {
        key: bytes("k"),
        version: 300,
    };
    store.apply(&delete, NOW);
    store.apply(&delete, NOW);
    assert!(value(&store, "k").is_none());
    assert_eq!(store.stats().curr_items, 0);
}

#[test]
fn an_expired_put_older_than_the_stored_item_leaves_it() {
    let store = store(1 << 20);
    store.apply(&put_rec("k", "new", 200, 0), NOW);
    store.apply(&put_rec("k", "old", 100, NOW - 1), NOW);
    assert_eq!(value(&store, "k").as_deref(), Some("new"));
}

#[test]
fn applying_a_delete_of_a_missing_key_does_nothing() {
    let store = store(1 << 20);
    store.apply(&put_rec("other", "v", 100, 0), NOW);
    store.apply(
        &Record::Delete {
            key: bytes("k"),
            version: 200,
        },
        NOW,
    );
    assert!(value(&store, "k").is_none());
    assert_eq!(value(&store, "other").as_deref(), Some("v"));
    assert_eq!(store.stats().curr_items, 1);
}

#[test]
fn applying_a_flush_below_the_floor_keeps_the_floor() {
    let store = store(1 << 20);
    store.apply(&Record::Flush { version: 400 }, NOW);
    store.apply(&Record::Flush { version: 300 }, NOW);
    assert_eq!(store.flush_floor(), 400);
    store.apply(&put_rec("k", "v", 350, 0), NOW);
    assert!(value(&store, "k").is_none());
}

#[test]
fn apply_moves_the_clock_past_applied_versions() {
    let store = store(1 << 20);
    // About the year 2096: far ahead of the wall clock.
    let far = crate::clock::Clock::with_wall(2, || 4_000_000_000_000).next();
    store.apply(&put_rec("k", "v", far, 0), NOW);
    let SetOutcome::Stored { cas, .. } = put(&store, "k", "w") else {
        panic!("not stored");
    };
    assert!(cas > far);
}

#[test]
fn shard_records_round_trip_through_apply() {
    let source = store(1 << 20);
    for index in 0..50 {
        put(&source, &format!("k{index}"), &format!("v{index}"));
    }
    let expired = SetArgs {
        expiry: NOW + 5,
        ..Default::default()
    };
    source.set(b"gone", bytes("v"), NOW, &expired);
    let copy = store(1 << 20);
    for shard in 0..source.shard_count() {
        for record in source.shard_records(shard, NOW + 5) {
            copy.apply(&record, NOW + 5);
        }
    }
    for index in 0..50 {
        assert_eq!(
            value_at(&copy, &format!("k{index}"), NOW + 5),
            Some(format!("v{index}"))
        );
    }
    assert!(value_at(&copy, "gone", NOW + 5).is_none());
}

#[test]
fn apply_logs_nothing() {
    let (store, oplog) = logged_store();
    store.apply(&put_rec("k", "v", 100, 0), NOW);
    assert!(drained(&oplog).is_empty());
}

const TTL: u64 = 3600;

fn tombstoned_store() -> Store {
    let store = store(1 << 20);
    store.enable_tombstones(TTL, usize::MAX);
    store
}

#[test]
fn a_delete_leaves_a_tombstone_only_when_tombstones_are_on() {
    let plain = store(1 << 20);
    put(&plain, "k", "v");
    plain.delete(b"k", NOW, &DeleteArgs::default());
    assert_eq!(plain.tombstones(), 0);

    let store = tombstoned_store();
    put(&store, "k", "v");
    let before = store.stats().bytes;
    assert_eq!(
        store.delete(b"k", NOW, &DeleteArgs::default()),
        DeleteOutcome::Deleted
    );
    assert_eq!(store.tombstones(), 1);
    assert_eq!(store.stats().curr_items, 0);
    // A tombstone is charged the key's cost: the item's cost without its one-byte value.
    assert_eq!(store.stats().bytes, before - 1);
}

#[test]
fn a_write_to_a_deleted_key_removes_its_tombstone() {
    let store = tombstoned_store();
    put(&store, "k", "v");
    store.delete(b"k", NOW, &DeleteArgs::default());
    put(&store, "k", "w");
    assert_eq!(store.tombstones(), 0);
    assert_eq!(value(&store, "k").as_deref(), Some("w"));
    assert_eq!(store.stats().bytes as usize, cost(1, 1));
}

#[test]
fn invalidating_or_emptying_deletes_leave_no_tombstone() {
    let store = tombstoned_store();
    put(&store, "a", "v");
    put(&store, "b", "v");
    let invalidate = DeleteArgs {
        invalidate: true,
        ..Default::default()
    };
    store.delete(b"a", NOW, &invalidate);
    let keep_empty = DeleteArgs {
        keep_empty: true,
        ..Default::default()
    };
    store.delete(b"b", NOW, &keep_empty);
    assert_eq!(store.tombstones(), 0);
}

#[test]
fn item_eviction_never_removes_tombstones() {
    // Room for about ten of the items below.
    let store = store(2000);
    store.enable_tombstones(TTL, usize::MAX);
    put(&store, "gone", "v");
    store.delete(b"gone", NOW, &DeleteArgs::default());
    for index in 0..100 {
        put(&store, &format!("k{index}"), &"x".repeat(100));
    }
    // The evicted items leave tombstones of their own.
    assert!(store.tombstones() > 1);
    assert!(
        store
            .shard_for(b"gone")
            .lock()
            .tombstones
            .get(b"gone")
            .is_some()
    );
    assert_eq!(store.tombstones_evicted(), 0);
}

#[test]
fn the_sweeper_removes_expired_tombstones_and_gives_back_their_memory() {
    let store = tombstoned_store();
    put(&store, "k", "v");
    // The delete's version carries the wall-clock time, so the tombstone lives until then plus
    // the lifetime.
    store.delete(b"k", NOW, &DeleteArgs::default());
    store.sweep(crate::clock::unix_secs());
    assert_eq!(store.tombstones(), 1);
    store.sweep(crate::clock::unix_secs() + TTL + 20);
    assert_eq!(store.tombstones(), 0);
    assert_eq!(store.stats().bytes, 0);
}

#[test]
fn tombstones_below_a_flush_are_swept() {
    let store = tombstoned_store();
    put(&store, "k", "v");
    store.delete(b"k", NOW, &DeleteArgs::default());
    store.flush_all(NOW, 0);
    store.sweep(NOW);
    assert_eq!(store.tombstones(), 0);
    assert_eq!(store.stats().bytes, 0);
}

#[test]
fn tombstones_are_swept_at_most_every_ten_seconds() {
    let store = tombstoned_store();
    store.sweep(NOW);
    put(&store, "k", "v");
    store.delete(b"k", NOW, &DeleteArgs::default());
    store.flush_all(NOW, 0);
    // Too soon after the last sweep of tombstones.
    store.sweep(NOW + 1);
    assert_eq!(store.tombstones(), 1);
    store.sweep(NOW + 10);
    assert_eq!(store.tombstones(), 0);
}

/// A version made by node `node` at Unix second `secs`, as its clock would make it.
fn version_at(secs: u64, node: u8) -> u64 {
    ((secs * 1000) << 22) | u64::from(node)
}

fn delete_rec(key: &str, version: u64) -> Record {
    Record::Delete {
        key: bytes(key),
        version,
    }
}

#[test]
fn apply_remote_keeps_only_newer_records_and_logs_them() {
    let (store, oplog) = logged_store();
    let old = put_rec("k", "old", version_at(NOW, 2), 0);
    let new = put_rec("k", "new", version_at(NOW + 1, 2), 0);
    assert_eq!(store.apply_remote(&new, NOW), Applied::Applied);
    assert_eq!(store.apply_remote(&old, NOW), Applied::Stale);
    assert_eq!(store.apply_remote(&new, NOW), Applied::Stale);
    assert_eq!(value(&store, "k").as_deref(), Some("new"));
    assert_eq!(drained(&oplog), [new]);
}

#[test]
fn a_tombstone_stops_an_older_write_that_arrives_late() {
    let store = tombstoned_store();
    store.apply_remote(&put_rec("k", "v", version_at(NOW, 2), 0), NOW);
    store.apply_remote(&delete_rec("k", version_at(NOW + 2, 3)), NOW);
    assert_eq!(store.tombstones(), 1);
    // Written on a third node before the delete, delivered after it.
    let late = put_rec("k", "late", version_at(NOW + 1, 4), 0);
    assert_eq!(store.apply_remote(&late, NOW + 2), Applied::Stale);
    assert_eq!(value(&store, "k"), None);
    // A newer write replaces the tombstone.
    let newer = put_rec("k", "newer", version_at(NOW + 3, 4), 0);
    assert_eq!(store.apply_remote(&newer, NOW + 3), Applied::Applied);
    assert_eq!(store.tombstones(), 0);
    assert_eq!(value_at(&store, "k", NOW + 3).as_deref(), Some("newer"));
}

#[test]
fn a_delete_of_a_key_never_seen_still_leaves_a_tombstone() {
    let store = tombstoned_store();
    store.apply_remote(&delete_rec("k", version_at(NOW + 2, 3)), NOW);
    let late = put_rec("k", "late", version_at(NOW + 1, 4), 0);
    assert_eq!(store.apply_remote(&late, NOW), Applied::Stale);
}

#[test]
fn an_expired_tombstone_no_longer_stops_older_writes() {
    let store = tombstoned_store();
    store.apply_remote(&delete_rec("k", version_at(NOW, 3)), NOW);
    let later = NOW + TTL;
    let older = put_rec("k", "v", version_at(NOW - 1, 4), 0);
    assert_eq!(store.apply_remote(&older, later), Applied::Applied);
    assert_eq!(value_at(&store, "k", later).as_deref(), Some("v"));
}

#[test]
fn an_expired_remote_put_removes_the_older_item_and_leaves_a_tombstone() {
    let store = tombstoned_store();
    store.apply_remote(&put_rec("k", "v", version_at(NOW, 2), 0), NOW);
    let expired = put_rec("k", "v", version_at(NOW + 1, 2), NOW);
    assert_eq!(store.apply_remote(&expired, NOW), Applied::Applied);
    assert_eq!(value(&store, "k"), None);
    assert_eq!(store.tombstones(), 1);
}

#[test]
fn apply_honors_tombstones_and_recreates_them_from_deletes() {
    let store = tombstoned_store();
    store.apply(&delete_rec("k", version_at(NOW + 2, 1)), NOW);
    assert_eq!(store.tombstones(), 1);
    store.apply(&put_rec("k", "old", version_at(NOW + 1, 1), 0), NOW);
    assert_eq!(value(&store, "k"), None);
}

#[test]
fn a_remote_flush_goes_through_the_flush_epoch_and_is_logged() {
    use std::sync::atomic::Ordering;

    let (store, oplog) = logged_store();
    put(&store, "local", "v");
    drained(&oplog);
    // A flush from another node, a minute ahead of this one's clock.
    let version = version_at(crate::clock::unix_secs() + 60, 2);
    let flush = Record::Flush { version };
    assert_eq!(store.apply_remote(&flush, NOW), Applied::Applied);
    assert_eq!(value(&store, "local"), None);
    assert_eq!(store.flush_floor(), version);
    assert_eq!(
        store.flush_epoch.load(Ordering::Relaxed) >> 32,
        1,
        "one flush finished"
    );
    assert_eq!(drained(&oplog), std::slice::from_ref(&flush));
    // A later local write takes a version above the flush, so it is kept.
    put(&store, "after", "v");
    assert_eq!(value(&store, "after").as_deref(), Some("v"));
    // The same flush again is stale.
    assert_eq!(store.apply_remote(&flush, NOW), Applied::Stale);
}

#[test]
fn a_mutation_stamped_inside_a_remote_flush_waits_for_it() {
    use std::sync::atomic::Ordering;

    let store = Arc::new(store(64 << 20));
    put(&store, "k", "1");
    // Hold a flush in progress, as `flush_remote` has it between its epoch step and its
    // floor, and move the clock past the flush's version as it does.
    let version = version_at(crate::clock::unix_secs() + 60, 2);
    store.flush_epoch.fetch_add(1, Ordering::AcqRel);
    store.clock.observe(version);
    let worker = {
        let store = store.clone();
        std::thread::spawn(move || put_mode(&store, "k", "2", SetMode::Append))
    };
    wait_for_a_stamp_retry(&store);
    store.finish_flush(version);
    // The append waited, then found the flushed item gone: nothing from before the flush
    // survives under a version above it.
    assert_eq!(worker.join().unwrap(), SetOutcome::NotStored);
    assert_eq!(value(&store, "k"), None);
}

#[test]
fn a_put_too_large_for_this_node_is_not_stored() {
    let store = store(1000);
    let big = put_rec("k", &"x".repeat(2000), version_at(NOW, 2), 0);
    assert_eq!(store.apply_remote(&big, NOW), Applied::Oversized);
    assert_eq!(store.stats().curr_items, 0);
}

#[test]
fn shard_records_carry_tombstones_to_another_store() {
    let source = tombstoned_store();
    source.apply_remote(&delete_rec("gone", version_at(NOW + 2, 3)), NOW);
    put(&source, "kept", "v");
    let copy = tombstoned_store();
    for shard in 0..source.shard_count() {
        for record in source.shard_records(shard, NOW) {
            copy.apply(&record, NOW);
        }
    }
    assert_eq!(copy.tombstones(), 1);
    assert_eq!(value(&copy, "kept").as_deref(), Some("v"));
    copy.apply(&put_rec("gone", "late", version_at(NOW + 1, 4), 0), NOW);
    assert_eq!(value(&copy, "gone"), None);
}

#[test]
fn a_delete_that_misses_still_leaves_and_logs_a_tombstone_with_peers() {
    let (store, oplog) = logged_store();
    store.enable_tombstones(TTL, usize::MAX);
    assert_eq!(
        store.delete(b"k", NOW, &DeleteArgs::default()),
        DeleteOutcome::NotFound
    );
    assert_eq!(store.tombstones(), 1);
    assert!(matches!(drained(&oplog)[..], [Record::Delete { .. }]));
    // A write from a peer made before the delete cannot bring the key back.
    let late = put_rec("k", "late", version_at(NOW, 2), 0);
    assert_eq!(store.apply_remote(&late, NOW), Applied::Stale);

    // Without peers a missed delete changes and logs nothing.
    let (plain, oplog) = logged_store();
    plain.delete(b"k", NOW, &DeleteArgs::default());
    assert_eq!(plain.tombstones(), 0);
    assert!(drained(&oplog).is_empty());
}

/// Whether `key` still has an item in its shard, expired or not, without touching it.
fn holds_item(store: &Store, key: &str) -> bool {
    store
        .shard_for(key.as_bytes())
        .lock()
        .map
        .contains_key(key.as_bytes())
}

#[test]
fn an_expired_item_removed_on_read_leaves_a_tombstone() {
    let store = tombstoned_store();
    store.apply_remote(&put_rec("k", "new", version_at(NOW + 1, 1), NOW + 60), NOW);
    assert_eq!(value_at(&store, "k", NOW + 60), None);
    assert!(!holds_item(&store, "k"));
    // Written on another node before the expiring write, delivered after the item expired.
    let late = put_rec("k", "old", version_at(NOW, 2), 0);
    assert_eq!(store.apply_remote(&late, NOW + 60), Applied::Stale);
    assert_eq!(value_at(&store, "k", NOW + 60), None);
}

#[test]
fn an_expired_item_removed_by_the_sweeper_leaves_a_tombstone() {
    let store = tombstoned_store();
    store.apply_remote(&put_rec("k", "new", version_at(NOW + 1, 1), NOW + 60), NOW);
    store.sweep(NOW + 60);
    assert!(!holds_item(&store, "k"));
    let late = put_rec("k", "old", version_at(NOW, 2), 0);
    assert_eq!(store.apply_remote(&late, NOW + 60), Applied::Stale);
    assert_eq!(value_at(&store, "k", NOW + 60), None);
}

#[test]
fn an_item_below_the_flush_floor_leaves_no_tombstone() {
    let store = tombstoned_store();
    put(&store, "k", "v");
    store.flush_all(NOW, 0);
    assert_eq!(value(&store, "k"), None);
    assert_eq!(store.tombstones(), 0);
}

#[test]
fn shard_records_send_an_expired_item_as_a_delete() {
    let source = tombstoned_store();
    let version = version_at(NOW + 1, 1);
    source.apply_remote(&put_rec("k", "new", version, NOW + 60), NOW);
    let records: Vec<Record> = (0..source.shard_count())
        .flat_map(|shard| source.shard_records(shard, NOW + 60))
        .collect();
    assert_eq!(records, [delete_rec("k", version)]);
    let copy = tombstoned_store();
    for record in &records {
        copy.apply(record, NOW + 60);
    }
    copy.apply(&put_rec("k", "old", version_at(NOW, 2), 0), NOW + 60);
    assert_eq!(value_at(&copy, "k", NOW + 60), None);

    // Without tombstones an expired item is simply left out.
    let plain = store(1 << 20);
    plain.apply(&put_rec("k", "new", version, NOW + 60), NOW);
    let records = (0..plain.shard_count()).flat_map(|shard| plain.shard_records(shard, NOW + 60));
    assert_eq!(records.count(), 0);
}

#[test]
fn an_evicted_item_leaves_a_tombstone_so_a_write_from_before_its_delete_stays_out() {
    // Room for about ten of the items below.
    let store = store(4000);
    store.enable_tombstones(TTL, usize::MAX);
    store.apply_remote(&delete_rec("k", version_at(NOW + 2, 3)), NOW);
    store.apply_remote(&put_rec("k", "new", version_at(NOW + 3, 3), 0), NOW);
    assert_eq!(store.tombstones(), 0);
    let mut index = 0;
    while holds_item(&store, "k") {
        assert!(index < 1000, "k was never evicted");
        put(&store, &format!("k{index}"), &"x".repeat(100));
        index += 1;
    }
    // Written on another node before the delete, delivered after the eviction.
    let late = put_rec("k", "late", version_at(NOW + 1, 4), 0);
    assert_eq!(store.apply_remote(&late, NOW), Applied::Stale);
    assert_eq!(value(&store, "k"), None);
}

#[test]
fn eviction_leaves_no_tombstone_without_peers() {
    let store = store(2000);
    for index in 0..100 {
        put(&store, &format!("k{index}"), &"x".repeat(100));
    }
    assert!(store.stats().evictions > 0);
    assert_eq!(store.tombstones(), 0);
}

#[test]
fn a_remote_put_too_large_for_this_node_removes_the_older_item_and_logs_a_delete() {
    let store = store(1000);
    let oplog = Arc::new(Oplog::new(1024, u64::MAX));
    store.attach_oplog(oplog.clone());
    store.enable_tombstones(TTL, usize::MAX);
    store.apply_remote(&put_rec("k", "old", version_at(NOW, 2), 0), NOW);
    drained(&oplog);
    let version = version_at(NOW + 1, 2);
    let big = put_rec("k", &"x".repeat(2000), version, 0);
    assert_eq!(store.apply_remote(&big, NOW), Applied::Oversized);
    assert_eq!(value(&store, "k"), None);
    assert_eq!(store.tombstones(), 1);
    assert_eq!(drained(&oplog), [delete_rec("k", version)]);
    // A put older than the one that did not fit stays out.
    let late = put_rec("k", "late", version_at(NOW, 4), 0);
    assert_eq!(store.apply_remote(&late, NOW), Applied::Stale);
    assert_eq!(value(&store, "k"), None);
}

#[test]
fn recovering_a_put_too_large_for_this_node_removes_the_older_item() {
    let store = store(1000);
    store.apply(&put_rec("k", "old", version_at(NOW, 1), 0), NOW);
    store.apply(
        &put_rec("k", &"x".repeat(2000), version_at(NOW + 1, 1), 0),
        NOW,
    );
    assert_eq!(value(&store, "k"), None);
    assert_eq!(store.tombstones(), 0);
    assert_eq!(store.stats().bytes, 0);
}

#[test]
fn a_huge_tombstone_ttl_does_not_overflow() {
    let store = store(1 << 20);
    store.enable_tombstones(u64::MAX, usize::MAX);
    assert!(!store.tombstone_expired(u64::MAX, NOW));
    assert!(!store.tombstone_expired(1 << 40, NOW));
}

/// The memory one tombstone for a two-byte key is charged.
const TOMBSTONE: usize = ITEM_OVERHEAD + 2;

/// A store with one shard, so the whole tombstone memory applies to every key, holding at most
/// `count` tombstones for two-byte keys.
fn capped_store(count: usize) -> Store {
    let store = store_with_shards(1 << 20, 1);
    store.enable_tombstones(TTL, count * TOMBSTONE);
    store
}

/// Whether a put to `key` made by node 9 at `secs`, arriving now, is kept out.
fn stops_a_put_from(store: &Store, key: &str, secs: u64) -> bool {
    let late = put_rec(key, "late", version_at(secs, 9), 0);
    store.apply_remote(&late, NOW) == Applied::Stale
}

#[test]
fn tombstone_memory_is_capped_and_the_oldest_tombstones_go_first() {
    let store = capped_store(3);
    // Out of version order, as a peer's deletes can arrive.
    for (key, secs) in [("k1", 1), ("k5", 5), ("k3", 3), ("k4", 4), ("k2", 2)] {
        store.apply_remote(&delete_rec(key, version_at(NOW + secs, 2)), NOW);
    }
    assert_eq!(store.tombstones(), 3);
    assert_eq!(store.tombstone_bytes(), 3 * TOMBSTONE as u64);
    assert_eq!(store.stats().bytes, 3 * TOMBSTONE as u64);
    assert_eq!(store.tombstones_evicted(), 2);
    // Evicted tombstones stop nothing.
    assert!(!stops_a_put_from(&store, "k1", NOW));
    assert!(!stops_a_put_from(&store, "k2", NOW));
    for key in ["k3", "k4", "k5"] {
        assert!(stops_a_put_from(&store, key, NOW), "{key}");
    }
}

#[test]
fn a_newer_delete_of_a_tombstoned_key_makes_it_newest_without_charging_twice() {
    let store = capped_store(2);
    store.apply_remote(&delete_rec("k1", version_at(NOW + 1, 2)), NOW);
    store.apply_remote(&delete_rec("k2", version_at(NOW + 2, 2)), NOW);
    store.apply_remote(&delete_rec("k1", version_at(NOW + 3, 2)), NOW);
    assert_eq!(store.tombstone_bytes(), 2 * TOMBSTONE as u64);
    assert_eq!(store.tombstones_evicted(), 0);
    // k2 is now the oldest, so it goes first.
    store.apply_remote(&delete_rec("k3", version_at(NOW + 4, 2)), NOW);
    assert_eq!(store.tombstones_evicted(), 1);
    assert!(!stops_a_put_from(&store, "k2", NOW));
    assert!(stops_a_put_from(&store, "k1", NOW + 2));
    assert!(stops_a_put_from(&store, "k3", NOW + 3));
}

#[test]
fn local_deletes_that_miss_are_capped_too() {
    let store = capped_store(4);
    for index in 0..10 {
        let outcome = store.delete(format!("m{index}").as_bytes(), NOW, &DeleteArgs::default());
        assert_eq!(outcome, DeleteOutcome::NotFound);
    }
    assert_eq!(store.tombstones(), 4);
    assert_eq!(store.tombstones_evicted(), 6);
    assert_eq!(store.stats().bytes, 4 * TOMBSTONE as u64);
    // The newest four are kept.
    let shard = store.shard_for(b"m9").lock();
    for index in 6..10 {
        assert!(
            shard
                .tombstones
                .get(format!("m{index}").as_bytes())
                .is_some()
        );
    }
}

#[test]
fn tombstone_bytes_stay_exact_through_writes_sweeps_and_flushes() {
    let store = capped_store(10);
    for key in ["k1", "k2", "k3"] {
        store.apply_remote(&delete_rec(key, version_at(NOW + 1, 2)), NOW);
    }
    store.apply_remote(&delete_rec("k4", version_at(NOW + 100, 2)), NOW);
    assert_eq!(store.tombstone_bytes(), 4 * TOMBSTONE as u64);
    // A write removes its key's tombstone.
    put(&store, "k1", "v");
    assert_eq!(store.tombstone_bytes(), 3 * TOMBSTONE as u64);
    assert_eq!(
        store.stats().bytes,
        3 * TOMBSTONE as u64 + cost(2, 1) as u64
    );
    // The sweeper removes the two that have expired and keeps the newer one.
    store.sweep(NOW + 1 + TTL);
    assert_eq!(store.tombstones(), 1);
    assert_eq!(store.tombstone_bytes(), TOMBSTONE as u64);
    assert!(stops_a_put_from(&store, "k4", NOW));
    // A flush leaves the rest below the floor, and the next sweep removes them.
    store.flush_all(NOW, 0);
    store.sweep(NOW + 1 + TTL + 10);
    assert_eq!((store.tombstones(), store.tombstone_bytes()), (0, 0));
    assert_eq!(store.stats().bytes, 0);
    // Sweeping and expiry are not evictions.
    assert_eq!(store.tombstones_evicted(), 0);
}

#[test]
fn an_expired_tombstone_pushed_out_by_the_cap_is_not_counted_as_evicted() {
    let store = capped_store(1);
    store.apply_remote(&delete_rec("k1", version_at(NOW, 2)), NOW);
    let later = NOW + TTL + 1;
    store.apply_remote(&delete_rec("k2", version_at(later, 2)), later);
    assert_eq!(store.tombstones(), 1);
    assert_eq!(store.tombstones_evicted(), 0);
}

#[test]
fn a_shard_keeps_its_newest_tombstone_even_when_the_cap_is_smaller() {
    let store = store_with_shards(1 << 20, 1);
    store.enable_tombstones(TTL, 1);
    store.apply_remote(&delete_rec("k1", version_at(NOW + 1, 2)), NOW);
    assert_eq!(store.tombstones(), 1);
    store.apply_remote(&delete_rec("k2", version_at(NOW + 2, 2)), NOW);
    assert_eq!(store.tombstones(), 1);
    assert!(stops_a_put_from(&store, "k2", NOW + 1));
}

#[test]
fn the_cap_is_shared_equally_among_shards() {
    let store = store_with_shards(1 << 20, 4);
    store.enable_tombstones(TTL, 4 * 2 * TOMBSTONE);
    for index in 0..1000 {
        store.delete(
            format!("{index:02}").as_bytes(),
            NOW,
            &DeleteArgs::default(),
        );
    }
    for shard in store.shards.iter() {
        assert!(shard.lock().tombstones.bytes() <= 2 * TOMBSTONE);
    }
    assert!(store.tombstone_bytes() <= 8 * TOMBSTONE as u64);
}

#[test]
fn recovery_respects_the_tombstone_cap() {
    let store = capped_store(3);
    for index in 0..10u64 {
        store.apply(
            &delete_rec(&format!("r{index}"), version_at(NOW + index, 1)),
            NOW,
        );
    }
    assert_eq!(store.tombstones(), 3);
    assert_eq!(store.tombstone_bytes(), 3 * TOMBSTONE as u64);
    assert!(stops_a_put_from(&store, "r7", NOW));
    assert!(!stops_a_put_from(&store, "r6", NOW));
}

#[test]
fn tombstones_left_by_evicted_items_respect_the_cap() {
    // Room for about twenty of the items below, and two tombstones.
    let store = store_with_shards(4000, 1);
    store.enable_tombstones(TTL, 2 * cost(3, 0));
    for index in 0..100 {
        put(&store, &format!("k{index:02}"), &"x".repeat(100));
    }
    assert!(store.stats().evictions > 10);
    assert_eq!(store.tombstones(), 2);
    assert_eq!(store.tombstone_bytes(), 2 * cost(3, 0) as u64);
}

#[test]
fn without_peers_nothing_is_held_or_evicted() {
    let store = store(1 << 20);
    for index in 0..100 {
        store.delete(format!("m{index}").as_bytes(), NOW, &DeleteArgs::default());
    }
    assert_eq!(store.tombstones(), 0);
    assert_eq!(store.tombstone_bytes(), 0);
    assert_eq!(store.tombstones_evicted(), 0);
}

#[test]
fn a_full_sync_after_the_cap_evicts_a_tombstone_no_longer_carries_its_delete() {
    let store = capped_store(1);
    store.apply_remote(&delete_rec("k1", version_at(NOW + 1, 2)), NOW);
    store.apply_remote(&delete_rec("k2", version_at(NOW + 2, 2)), NOW);
    let records: Vec<Record> = (0..store.shard_count())
        .flat_map(|shard| store.shard_records(shard, NOW))
        .collect();
    // k1's delete is gone with its tombstone: a receiver holding an older k1 keeps it.
    assert_eq!(records, [delete_rec("k2", version_at(NOW + 2, 2))]);
}

#[test]
fn the_sweeper_removes_exactly_the_dead_oldest_tombstones() {
    let store = capped_store(10);
    // Recorded out of version order: one that will fall below the floor, two that will have
    // expired, and two that stay.
    for (key, secs) in [("kc", 100), ("ka", 10), ("kd", 59), ("ke", 200), ("kb", 60)] {
        store.apply_remote(&delete_rec(key, version_at(NOW + secs, 2)), NOW);
    }
    store.apply(
        &Record::Flush {
            version: version_at(NOW + 50, 2),
        },
        NOW,
    );
    store.sweep(NOW + 60 + TTL);
    let shard = store.shard_for(b"kc").lock();
    let mut kept: Vec<&[u8]> = shard.tombstones.iter().map(|(key, _)| key).collect();
    kept.sort();
    assert_eq!(kept, [&b"kc"[..], b"ke"]);
    drop(shard);
    assert_eq!(store.tombstone_bytes(), 2 * TOMBSTONE as u64);
    assert_eq!(store.stats().bytes, 2 * TOMBSTONE as u64);
    assert_eq!(store.tombstones_evicted(), 0);
}

#[test]
fn a_remote_put_over_a_tombstone_gives_back_its_memory_exactly() {
    let store = capped_store(10);
    store.apply_remote(&delete_rec("k1", version_at(NOW + 1, 2)), NOW);
    store.apply_remote(&delete_rec("k2", version_at(NOW + 1, 2)), NOW);
    let put = put_rec("k1", "v", version_at(NOW + 2, 2), 0);
    assert_eq!(store.apply_remote(&put, NOW), Applied::Applied);
    assert_eq!(store.tombstones(), 1);
    assert_eq!(store.tombstone_bytes(), TOMBSTONE as u64);
    assert_eq!(store.stats().bytes, (TOMBSTONE + cost(2, 1)) as u64);
}

#[test]
fn an_older_delete_than_a_tombstone_changes_nothing() {
    let store = capped_store(10);
    let newer = version_at(NOW + 2, 2);
    store.apply_remote(&delete_rec("k1", newer), NOW);
    let older = delete_rec("k1", version_at(NOW + 1, 3));
    assert_eq!(store.apply_remote(&older, NOW), Applied::Stale);
    assert_eq!(
        store.shard_for(b"k1").lock().tombstones.get(b"k1"),
        Some(newer)
    );
    assert_eq!(store.tombstone_bytes(), TOMBSTONE as u64);

    // Past the newer tombstone's lifetime, but before a sweep, the older delete has expired
    // too: it applies, but leaves the tombstone and its charge as they were.
    let later = NOW + 2 + TTL;
    assert_eq!(store.apply_remote(&older, later), Applied::Applied);
    assert_eq!(
        store.shard_for(b"k1").lock().tombstones.get(b"k1"),
        Some(newer)
    );
    assert_eq!(store.tombstones(), 1);
    assert_eq!(store.tombstone_bytes(), TOMBSTONE as u64);
    assert_eq!(store.stats().bytes, TOMBSTONE as u64);
}

#[test]
fn a_set_is_stored_within_the_limit_with_tombstones_at_their_cap() {
    let limit = 4000;
    let store = store_with_shards(limit, 1);
    store.enable_tombstones(TTL, 4 * TOMBSTONE);
    for index in 0..10 {
        store.delete(format!("m{index}").as_bytes(), NOW, &DeleteArgs::default());
    }
    for index in 0..100 {
        put(&store, &format!("i{index:02}"), &"x".repeat(100));
    }
    assert!(store.stats().evictions > 0);
    assert!(matches!(
        put(&store, "new", &"y".repeat(100)),
        SetOutcome::Stored { .. }
    ));
    assert_eq!(value(&store, "new"), Some("y".repeat(100)));
    assert!(store.tombstone_bytes() <= 4 * TOMBSTONE as u64);
    // The store's total lags each shard's usage by at most `slack`, which is how far the
    // eviction loop can let usage run over the limit.
    assert!(store.stats().bytes as usize <= limit + store.slack);
}

// Flush races, forced at the store's test hooks. After a flush, a key holds nothing or a value
// made entirely after it; nothing carried from a flushed item has a version above the flush.

/// The CAS token of the item every race starts from, so a mutation can name it.
const TOKEN: u64 = 77;

#[derive(Clone, Copy, Debug)]
enum FlushKind {
    /// `flush_all` with no delay.
    Local,
    /// `flush_all` with a delay, falling due on a later floor read.
    Delayed,
    /// A flush from a peer a minute ahead, so a local version above it must come from
    /// observing it.
    Remote,
}

impl FlushKind {
    const ALL: [FlushKind; 3] = [FlushKind::Local, FlushKind::Delayed, FlushKind::Remote];

    /// Sets up the flush without starting it.
    fn prepare(self, store: &Store) {
        if let FlushKind::Delayed = self {
            store.flush_all(NOW, 10);
        }
    }

    /// Runs the flush to completion. Takes no shard lock, so a mutation may hold one.
    fn run(self, store: &Store) {
        match self {
            FlushKind::Local => store.flush_all(NOW, 0),
            FlushKind::Delayed => {
                store.floor(NOW + 10);
            }
            FlushKind::Remote => {
                let version = version_at(crate::clock::unix_secs() + 60, 2);
                let flush = Record::Flush { version };
                assert_eq!(store.apply_remote(&flush, NOW), Applied::Applied);
            }
        }
    }
}

/// A mutation run at a given time, returning its outcome for messages and comparisons.
type Op = fn(&Store, u64) -> String;

fn set_op(store: &Store, now: u64, mode: SetMode, cas: Option<u64>, vivify: bool) -> String {
    let args = SetArgs {
        mode,
        cas,
        vivify: vivify.then_some(0),
        ..Default::default()
    };
    format!("{:?}", store.set(b"k", bytes("new"), now, &args))
}

fn arith_op(store: &Store, now: u64, decr: bool, cas: Option<u64>) -> String {
    let args = ArithArgs {
        decr,
        delta: 1,
        cas,
        ..Default::default()
    };
    format!("{:?}", store.arith(b"k", now, &args))
}

/// Every mutation that reads the item it changes, plus plain sets, by name. Each targets "k".
const OPS: [(&str, Op); 18] = [
    ("set", |s, now| set_op(s, now, SetMode::Set, None, false)),
    ("add", |s, now| set_op(s, now, SetMode::Add, None, false)),
    ("replace", |s, now| {
        set_op(s, now, SetMode::Replace, None, false)
    }),
    ("append", |s, now| {
        set_op(s, now, SetMode::Append, None, false)
    }),
    ("prepend", |s, now| {
        set_op(s, now, SetMode::Prepend, None, false)
    }),
    ("vivifying append", |s, now| {
        set_op(s, now, SetMode::Append, None, true)
    }),
    ("cas", |s, now| {
        set_op(s, now, SetMode::Set, Some(TOKEN), false)
    }),
    ("cas replace", |s, now| {
        set_op(s, now, SetMode::Replace, Some(TOKEN), false)
    }),
    ("cas append", |s, now| {
        set_op(s, now, SetMode::Append, Some(TOKEN), false)
    }),
    ("cas prepend", |s, now| {
        set_op(s, now, SetMode::Prepend, Some(TOKEN), false)
    }),
    ("incr", |s, now| arith_op(s, now, false, None)),
    ("decr", |s, now| arith_op(s, now, true, None)),
    ("incr with cas", |s, now| {
        arith_op(s, now, false, Some(TOKEN))
    }),
    ("touch", |s, now| {
        format!("{}", s.touch(b"k", now, now + 60))
    }),
    ("touching get", |s, now| {
        let args = GetArgs {
            touch: Some(now + 60),
            ..Default::default()
        };
        format!("{:?}", s.get(b"k", now, &args).map(|hit| hit.value))
    }),
    ("invalidating delete", |s, now| {
        let args = DeleteArgs {
            invalidate: true,
            ..Default::default()
        };
        format!("{:?}", s.delete(b"k", now, &args))
    }),
    ("emptying delete", |s, now| {
        let args = DeleteArgs {
            keep_empty: true,
            ..Default::default()
        };
        format!("{:?}", s.delete(b"k", now, &args))
    }),
    ("delete with cas", |s, now| {
        let args = DeleteArgs {
            cas: Some(TOKEN),
            ..Default::default()
        };
        format!("{:?}", s.delete(b"k", now, &args))
    }),
];

/// A store holding "k" = "1" with CAS token `TOKEN`.
fn store_before_flush() -> Arc<Store> {
    let store = Arc::new(store(64 << 20));
    let args = SetArgs {
        cas_override: Some(TOKEN),
        ..Default::default()
    };
    store.set(b"k", bytes("1"), NOW, &args);
    store
}

/// The value and version held for `key`, whether or not it is below the floor.
fn raw_item(store: &Store, key: &str) -> Option<(String, u64)> {
    let shard = store.shard_for(key.as_bytes()).lock();
    shard.map.get(key.as_bytes()).map(|item| {
        let value = String::from_utf8(item.value.to_vec()).unwrap();
        (value, item.version)
    })
}

/// The outcome without its details: "Stored { cas: 5, .. }" becomes "Stored".
fn outcome_kind(outcome: &str) -> &str {
    outcome
        .split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or_default()
}

/// Checks that "k" reads as `expected` after the flush and that anything still held for it at
/// or above the floor, even unswept, is that value.
fn assert_flushed(store: &Store, expected: Option<&str>, context: &str) {
    let floor = store.flush_floor();
    assert!(floor > 0, "{context}: the flush never raised the floor");
    if let Some((held, version)) = raw_item(store, "k")
        && version >= floor
    {
        assert_eq!(
            Some(held.as_str()),
            expected,
            "{context}: {held:?} at version {version} survives a flush at {floor}"
        );
    }
    assert_eq!(value(store, "k").as_deref(), expected, "{context}");
    store.sweep(NOW);
    let left = u64::from(expected.is_some());
    assert_eq!(store.stats().curr_items, left, "{context}: left behind");
}

/// Pauses a `kind` flush between taking its version and raising the floor, runs `op` on another
/// thread until the flush holds it back (or a second), then finishes the flush.
fn mutate_while_flush_paused(kind: FlushKind, store: &Arc<Store>, op: Op) -> String {
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    kind.prepare(store);
    let (paused, wait_paused) = mpsc::channel();
    let (resume, wait_resume) = mpsc::channel::<()>();
    store.hooks.lock().flush = Some(Box::new(move || {
        paused.send(()).unwrap();
        wait_resume.recv().unwrap();
    }));
    let flusher = {
        let store = store.clone();
        std::thread::spawn(move || kind.run(&store))
    };
    wait_paused
        .recv_timeout(Duration::from_secs(5))
        .expect("the flush never paused before raising the floor");
    let retries = store.stamp_retries.load(Ordering::Relaxed);
    let worker = {
        let store = store.clone();
        std::thread::spawn(move || op(&store, NOW))
    };
    let started = Instant::now();
    while store.stamp_retries.load(Ordering::Relaxed) == retries
        && !worker.is_finished()
        && started.elapsed() < Duration::from_secs(1)
    {
        std::thread::yield_now();
    }
    resume.send(()).unwrap();
    flusher.join().unwrap();
    worker.join().unwrap()
}

/// Pauses `op` on another thread between its lookup and acting on it, and meanwhile runs a
/// `kind` flush to completion.
fn flush_while_mutation_paused(kind: FlushKind, store: &Arc<Store>, op: Op) -> String {
    use std::sync::mpsc;
    use std::time::Duration;

    kind.prepare(store);
    let (paused, wait_paused) = mpsc::channel();
    let (resume, wait_resume) = mpsc::channel::<()>();
    store.hooks.lock().find = Some(Box::new(move || {
        paused.send(()).unwrap();
        wait_resume.recv().unwrap();
    }));
    let worker = {
        let store = store.clone();
        std::thread::spawn(move || op(&store, NOW))
    };
    wait_paused
        .recv_timeout(Duration::from_secs(5))
        .expect("the mutation never looked up its item");
    kind.run(store);
    resume.send(()).unwrap();
    worker.join().unwrap()
}

#[test]
fn a_mutation_stamped_while_any_flush_is_raising_the_floor_waits_and_sees_it() {
    for kind in FlushKind::ALL {
        for (name, op) in OPS {
            let store = store_before_flush();
            let outcome = mutate_while_flush_paused(kind, &store, op);
            // The mutation waited for the floor, so it found "k" gone. Only a set, an add or a
            // vivifying append makes a value of its own, and that value is kept, above the floor.
            let expected = matches!(name, "set" | "add" | "vivifying append").then_some("new");
            assert_flushed(
                &store,
                expected,
                &format!("{kind:?} flush, {name} -> {outcome}"),
            );
        }
    }
}

#[test]
fn a_flush_between_a_mutations_read_and_its_write_removes_what_it_wrote() {
    for kind in FlushKind::ALL {
        for (name, op) in OPS {
            let store = store_before_flush();
            let outcome = flush_while_mutation_paused(kind, &store, op);
            // The mutation's version came before the flush's, so whatever it wrote from what it
            // read is below the floor.
            assert_flushed(
                &store,
                None,
                &format!("{kind:?} flush, {name} -> {outcome}"),
            );
        }
    }
}

#[test]
fn after_any_flush_every_mutation_treats_an_unswept_item_as_gone() {
    for kind in FlushKind::ALL {
        for (name, op) in OPS {
            let store = store_before_flush();
            kind.prepare(&store);
            // A delayed flush falls due on the mutation's own floor read.
            let now = match kind {
                FlushKind::Delayed => NOW + 10,
                FlushKind::Local | FlushKind::Remote => {
                    kind.run(&store);
                    NOW
                }
            };
            assert!(raw_item(&store, "k").is_some(), "the item is not swept yet");
            let context = format!("{kind:?} flush, {name}");
            let outcome = op(&store, now);
            let fresh = store_with_shards(64 << 20, 4);
            let expected = op(&fresh, now);
            assert_eq!(
                outcome_kind(&outcome),
                outcome_kind(&expected),
                "{context}: the flushed item was not treated as missing"
            );
            assert_eq!(
                value_at(&store, "k", now),
                value_at(&fresh, "k", now),
                "{context}"
            );
        }
    }
}

#[test]
fn a_cas_against_a_flushed_item_is_not_found() {
    for kind in FlushKind::ALL {
        let store = store_before_flush();
        kind.prepare(&store);
        let now = match kind {
            FlushKind::Delayed => NOW + 10,
            FlushKind::Local | FlushKind::Remote => {
                kind.run(&store);
                NOW
            }
        };
        let args = SetArgs {
            cas: Some(TOKEN),
            ..Default::default()
        };
        assert_eq!(
            store.set(b"k", bytes("x"), now, &args),
            SetOutcome::NotFound,
            "{kind:?}"
        );
        let args = ArithArgs {
            delta: 1,
            cas: Some(TOKEN),
            ..Default::default()
        };
        assert_eq!(
            store.arith(b"k", now, &args),
            ArithOutcome::NotFound,
            "{kind:?}"
        );
        let args = DeleteArgs {
            cas: Some(TOKEN),
            ..Default::default()
        };
        assert_eq!(
            store.delete(b"k", now, &args),
            DeleteOutcome::NotFound,
            "{kind:?}"
        );
    }
}

/// Between two of a peer's flushes, a key's increments in version order must count up from the
/// initial value; a count carried past a flush breaks the run.
#[test]
fn counters_restart_after_every_remote_flush_under_load() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    const KEYS: usize = 4;
    let store = Arc::new(store(64 << 20));
    let running = Arc::new(AtomicBool::new(true));
    let base = crate::clock::unix_secs() + 60;
    let flusher = {
        let store = store.clone();
        let running = running.clone();
        std::thread::spawn(move || {
            let mut versions = Vec::new();
            while running.load(Ordering::Relaxed) {
                let version = version_at(base + versions.len() as u64, 2);
                let flush = Record::Flush { version };
                assert_eq!(store.apply_remote(&flush, NOW), Applied::Applied);
                versions.push(version);
                std::thread::yield_now();
            }
            versions
        })
    };
    let workers: Vec<_> = (0..4)
        .map(|thread| {
            let store = store.clone();
            std::thread::spawn(move || {
                let args = ArithArgs {
                    delta: 1,
                    vivify: Some((0, 1)),
                    ..Default::default()
                };
                let started = Instant::now();
                let mut seen = Vec::new();
                let mut round = 0;
                while started.elapsed() < Duration::from_millis(300) {
                    let key = (round + thread) % KEYS;
                    match store.arith(format!("n{key}").as_bytes(), NOW, &args) {
                        ArithOutcome::Value { value, cas, .. } => seen.push((key, cas, value)),
                        other => panic!("{other:?}"),
                    }
                    round += 1;
                }
                seen
            })
        })
        .collect();
    let mut seen: Vec<_> = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect();
    running.store(false, Ordering::Relaxed);
    let flushes = flusher.join().unwrap();
    assert!(flushes.len() > 10, "only {} flushes", flushes.len());
    // Each increment's CAS token is its version.
    seen.sort_unstable();
    let mut expected = 1;
    for (index, &(key, version, value)) in seen.iter().enumerate() {
        let epoch = flushes.partition_point(|&flush| flush < version);
        let restarts = index == 0 || {
            let (last_key, last_version, _) = seen[index - 1];
            last_key != key || flushes.partition_point(|&flush| flush < last_version) != epoch
        };
        if restarts {
            expected = 1;
        }
        assert_eq!(
            value, expected,
            "n{key} at version {version}, after {epoch} flushes, carried a count past a flush"
        );
        expected += 1;
    }
}
