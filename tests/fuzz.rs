//! Throws deterministic pseudo-random input at the request parser.

use ferrited::protocol::{Command, parse};

/// xorshift64*: small, fast, and the same on every run.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn pick<'a>(&mut self, choices: &[&'a str]) -> &'a str {
        choices[self.below(choices.len())]
    }
}

const COMMANDS: &[&str] = &[
    "get",
    "gets",
    "gat",
    "gats",
    "set",
    "add",
    "replace",
    "append",
    "prepend",
    "cas",
    "delete",
    "incr",
    "decr",
    "touch",
    "flush_all",
    "stats",
    "version",
    "quit",
    "shutdown",
    "verbosity",
    "cache_memlimit",
    "slabs",
    "lru",
    "mg",
    "ms",
    "md",
    "ma",
    "mn",
    "me",
    "bogus",
    "",
];

const WORDS: &[&str] = &[
    "k",
    "key",
    "0",
    "1",
    "5",
    "-1",
    "+1",
    "16",
    "17",
    "999999",
    "2147483647",
    "2147483648",
    "18446744073709551616",
    "noreply",
    "b",
    "q",
    "v",
    "c",
    "s",
    "t",
    "T30",
    "N0",
    "MA",
    "MX",
    "C5",
    "E7",
    "F3",
    "D2",
    "J9",
    "O123",
    "I",
    "x",
    "!",
    "Zm9v",
    "====",
    "graceful",
    "",
    " ",
];

const ENDINGS: &[&str] = &["\r\n", "\n", "\r", "", "\r\n\r\n", "\0\r\n"];

/// Bytes with no structure at all.
fn noise(random: &mut Random) -> Vec<u8> {
    let len = random.below(80);
    (0..len).map(|_| random.next() as u8).collect()
}

/// Something shaped like a request: a command, some words, a line ending, maybe a data block.
fn request_like(random: &mut Random) -> Vec<u8> {
    let mut text = String::from(random.pick(COMMANDS));
    for _ in 0..random.below(8) {
        text.push(' ');
        text.push_str(random.pick(WORDS));
    }
    text.push_str(random.pick(ENDINGS));
    let mut bytes = text.into_bytes();
    if random.below(2) == 0 {
        bytes.extend(noise(random));
        bytes.extend_from_slice(random.pick(ENDINGS).as_bytes());
    }
    bytes
}

/// A storage command whose data block has the announced length, or misses it by one.
fn storage_like(random: &mut Random) -> Vec<u8> {
    let announced = random.below(24);
    let actual = (announced + random.below(3)).saturating_sub(1);
    let mut text = match random.below(3) {
        0 => format!("ms {} {announced}", random.pick(WORDS)),
        1 => format!("cas k 0 0 {announced} {}", random.pick(WORDS)),
        _ => format!(
            "{} k 0 0 {announced}",
            random.pick(&["set", "add", "append"])
        ),
    };
    for _ in 0..random.below(3) {
        text.push(' ');
        text.push_str(random.pick(WORDS));
    }
    text.push_str("\r\n");
    let mut bytes = text.into_bytes();
    bytes.extend((0..actual).map(|_| random.next() as u8));
    bytes.extend_from_slice(random.pick(ENDINGS).as_bytes());
    bytes
}

/// A request-like input with a few bytes overwritten.
fn corrupted(random: &mut Random) -> Vec<u8> {
    let mut bytes = request_like(random);
    for _ in 0..random.below(4) {
        if !bytes.is_empty() {
            let index = random.below(bytes.len());
            bytes[index] = random.next() as u8;
        }
    }
    bytes
}

/// How many inputs ended as an incomplete request, a parsed request, and a failure.
#[derive(Default)]
struct Tally {
    incomplete: usize,
    parsed: usize,
    with_data: usize,
    failed: usize,
}

fn check(input: &[u8], item_max: usize, tally: &mut Tally) {
    match parse(input, item_max) {
        Ok(None) => tally.incomplete += 1,
        Ok(Some(request)) => {
            tally.parsed += 1;
            if matches!(request.cmd, Command::Store { .. } | Command::MetaSet { .. }) {
                tally.with_data += 1;
            }
            assert!(
                request.consumed > 0 && request.consumed <= input.len(),
                "consumed {} of {} bytes: {:?}",
                request.consumed,
                input.len(),
                input.escape_ascii().to_string()
            );
        }
        Err(failure) => {
            tally.failed += 1;
            assert!(
                failure.consumed > 0 && failure.consumed <= input.len(),
                "consumed {} of {} bytes: {:?}",
                failure.consumed,
                input.len(),
                input.escape_ascii().to_string()
            );
            assert!(!failure.error.message().is_empty());
        }
    }
}

#[test]
fn parse_never_panics_or_consumes_more_than_it_was_given() {
    let mut random = Random(0x5EED_1234_ABCD_0001);
    let mut tally = Tally::default();
    for round in 0..8_000 {
        let input = match round % 4 {
            0 => noise(&mut random),
            1 => request_like(&mut random),
            2 => storage_like(&mut random),
            _ => corrupted(&mut random),
        };
        let item_max = [0, 1, 16, 1024 * 1024][random.below(4)];
        check(&input, item_max, &mut tally);
        // And a truncated copy, as if only part has arrived.
        let cut = random.below(input.len() + 1);
        check(&input[..cut], item_max, &mut tally);
    }
    // The generators must reach every outcome, or the checks prove little.
    for count in [
        tally.incomplete,
        tally.parsed,
        tally.with_data,
        tally.failed,
    ] {
        assert!(count > 50, "an outcome was reached only {count} times");
    }
}
