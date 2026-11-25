//! Request framing must not depend on how the bytes of a request stream are split into reads.

use clap::Parser;
use ferrited::config::Config;
use ferrited::server::{Context, Outcome, Session};
use ferrited::stats::LocalStats;

/// Values longer than this are rejected, so the script can hold an oversized one cheaply.
const ITEM_MAX: usize = 16;

fn script() -> String {
    [
        // A normal set.
        "set a 0 0 3\r\nabc\r\n",
        // A data block longer than announced; the excess reads as a bad line.
        "set b 0 0 3\r\nabcdefg\r\n",
        // An oversized value, swallowed unparsed.
        "set big 0 0 40\r\nget a\r\nset zz 0 0 1\r\nz\r\nflush_all\r\n01234\r\n",
        // A value whose bytes look like commands.
        "set c 0 0 14\r\nget a\r\nflush_a\r\n",
        "get a b c zz\r\n",
        // Errors silenced by noreply, one with a data block to discard.
        "bogus noreply\r\n",
        "incr a 1 noreply\r\n",
        "set d x 0 2 noreply\r\nmn\r\n",
        "set e 0 0 2 noreply\r\nabcd\r\n",
        // Meta commands.
        "ms m 2 F3\r\nhi\r\n",
        "mg m v f s k\r\n",
        "mg nope v q\r\n",
        "mn\r\n",
        "ms m 2 MA s\r\n!!\r\n",
        "ms toolarge 20 q\r\n01234567890123456789\r\n",
        "ma n N0 J5 v\r\n",
        "md m q\r\n",
        "mg m v\r\n",
        "ms bad 2 !\r\n",
        "get a d e\r\n",
    ]
    .concat()
}

const EXPECTED: &str = "STORED\r\n\
    CLIENT_ERROR bad data chunk\r\n\
    ERROR\r\n\
    SERVER_ERROR object too large for cache\r\n\
    STORED\r\n\
    VALUE a 0 3\r\nabc\r\nVALUE c 0 14\r\nget a\r\nflush_a\r\nEND\r\n\
    CLIENT_ERROR bad command line format\r\n\
    HD\r\n\
    VA 2 f3 s2 km\r\nhi\r\n\
    MN\r\n\
    HD s4\r\n\
    SERVER_ERROR object too large for cache\r\n\
    VA 1\r\n5\r\n\
    EN\r\n\
    CLIENT_ERROR bad command line format\r\n\
    VALUE a 0 3\r\nabc\r\nEND\r\n";

/// Feeds `pieces` to a fresh session as separate reads and returns every reply.
fn converse(pieces: &[&[u8]]) -> String {
    let mut config = Config::parse_from(["ferrited"]);
    config.max_item_size = ITEM_MAX;
    let ctx = Context::new(&config);
    let mut session = Session::new();
    let mut stats = LocalStats::default();
    let mut reply = Vec::new();
    for piece in pieces {
        session.feed(piece, &mut stats);
        loop {
            let mut budget = usize::MAX;
            let outcome = session.run(&ctx, 0, &mut budget, &mut stats);
            session.take_output(Some(&mut reply));
            match outcome {
                Outcome::NeedInput => break,
                Outcome::OutputFull => {}
                other => panic!("unexpected {other:?}"),
            }
        }
    }
    String::from_utf8(reply).unwrap()
}

#[test]
fn replies_do_not_depend_on_where_the_input_is_split() {
    let script = script();
    let bytes = script.as_bytes();
    let whole = converse(&[bytes]);
    assert_eq!(whole, EXPECTED);
    for split in 1..bytes.len() {
        let reply = converse(&[&bytes[..split], &bytes[split..]]);
        assert_eq!(reply, whole, "split at byte {split}");
    }
}

#[test]
fn replies_do_not_depend_on_the_size_of_the_pieces() {
    let script = script();
    let bytes = script.as_bytes();
    for size in [1, 2, 3, 5, 7, 16, 61] {
        let pieces: Vec<&[u8]> = bytes.chunks(size).collect();
        assert_eq!(converse(&pieces), EXPECTED, "pieces of {size} bytes");
    }
}
