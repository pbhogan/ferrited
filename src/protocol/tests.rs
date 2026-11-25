use std::borrow::Cow;

use super::*;

const ITEM_MAX: usize = 1 << 20;

fn ok(input: &str) -> (Command<'_>, usize) {
    let request = parse(input.as_bytes(), ITEM_MAX)
        .unwrap()
        .expect("complete request");
    (request.cmd, request.consumed)
}

fn err(input: &str) -> Failure<'_> {
    parse(input.as_bytes(), ITEM_MAX).unwrap_err()
}

fn incomplete(input: &str) -> bool {
    parse(input.as_bytes(), ITEM_MAX).unwrap().is_none()
}

#[test]
fn waits_for_a_full_line_and_a_full_data_block() {
    assert!(incomplete(""));
    assert!(incomplete("get a"));
    assert!(incomplete("set k 0 0 3\r\nab"));
    assert!(incomplete("set k 0 0 3\r\nabc\r"));
}

#[test]
fn parses_retrieval_commands() {
    assert_eq!(
        ok("get a\r\n"),
        (
            Command::Get {
                keys: b"a",
                cas: false
            },
            7
        )
    );
    assert_eq!(
        ok("gets a  bb\r\n").0,
        Command::Get {
            keys: b"a  bb",
            cas: true
        }
    );
    assert_eq!(
        ok("gat 30 a b\r\n").0,
        Command::Gat {
            exptime: 30,
            keys: b"a b",
            cas: false
        }
    );
    assert_eq!(
        ok("gats 30 a\r\n").0,
        Command::Gat {
            exptime: 30,
            keys: b"a",
            cas: true
        }
    );
    assert_eq!(
        ok("get a\n"),
        (
            Command::Get {
                keys: b"a",
                cas: false
            },
            6
        )
    );
}

#[test]
fn parses_storage_commands_and_leaves_following_bytes() {
    let input = "set k 5 60 3\r\nabc\r\nget k\r\n";
    let expected = Command::Store {
        mode: StoreMode::Set,
        key: b"k",
        flags: 5,
        exptime: 60,
        cas: None,
        noreply: false,
        data: b"abc",
    };
    assert_eq!(ok(input), (expected, "set k 5 60 3\r\nabc\r\n".len()));

    for (name, mode) in [
        ("add", StoreMode::Add),
        ("replace", StoreMode::Replace),
        ("append", StoreMode::Append),
        ("prepend", StoreMode::Prepend),
    ] {
        let line = format!("{name} k 0 -1 0 noreply\r\n\r\n");
        let (cmd, _) = ok(&line);
        assert_eq!(
            cmd,
            Command::Store {
                mode,
                key: b"k",
                flags: 0,
                exptime: -1,
                cas: None,
                noreply: true,
                data: b""
            }
        );
    }

    let (cmd, _) = ok("cas k 0 0 1 77\r\nx\r\n");
    assert!(matches!(
        cmd,
        Command::Store {
            mode: StoreMode::Set,
            cas: Some(77),
            ..
        }
    ));
}

#[test]
fn rejects_malformed_storage_commands() {
    // Too few tokens or an unusable length: nothing is swallowed.
    for line in [
        "set k 0 0\r\n",
        "set k 0 0 -1\r\n",
        "set k 0 0 +1\r\n",
        "set k 0 0 2147483648\r\n",
        "set k 0 0 abc noreply\r\n",
    ] {
        let failure = err(line);
        assert_eq!(failure.error, ParseError::BadFormat, "{line:?}");
        assert_eq!((failure.swallow, failure.silent), (0, false), "{line:?}");
    }
    // Any other error swallows the data block, and `noreply` silences it.
    let long_key = format!("set {} 0 0 1\r\n", "k".repeat(251));
    for line in [
        "set k x 0 1\r\n",
        "set k 0 x 1\r\n",
        "set k -1 0 1\r\n",
        "cas k 0 0 1\r\n",
        "cas k 0 0 1 -5\r\n",
        &long_key,
    ] {
        let failure = err(line);
        assert_eq!(failure.error, ParseError::BadFormat, "{line:?}");
        assert_eq!((failure.swallow, failure.silent), (3, false), "{line:?}");
    }
    let failure = err("cas k 0 0 1 noreply\r\n");
    assert_eq!(failure.error, ParseError::BadFormat);
    assert_eq!((failure.swallow, failure.silent), (3, true));

    let failure = err("set k 0 0 3\r\nabcde\r\n");
    assert_eq!(failure.error, ParseError::BadDataChunk);
    assert_eq!(failure.consumed, "set k 0 0 3\r\nabcde".len());
    assert!(!failure.silent);
    assert!(err("set k 0 0 3 noreply\r\nabcde\r\n").silent);
    assert!(!err("ms k 3 q\r\nabcde\r\n").silent);
}

#[test]
fn rejects_short_lines_and_leading_spaces() {
    for line in ["\r\n", "\n", "x\r\n", " \r\n", "mn\n"] {
        assert_eq!(err(line).error, ParseError::BadFormat, "{line:?}");
    }
    for line in ["xy\r\n", "  \r\n", " get a\r\n", " mn\r\n", " version\r\n"] {
        assert_eq!(err(line).error, ParseError::UnknownCommand, "{line:?}");
    }
    assert!(matches!(
        ok(" flush_all\r\n").0,
        Command::FlushAll { delay: 0, .. }
    ));
}

#[test]
fn get_checks_every_key_in_the_list() {
    let longest = "k".repeat(250);
    let line = format!("get a {longest}  b\r\n");
    let (cmd, _) = ok(&line);
    assert!(
        matches!(cmd, Command::Get { keys, .. } if keys == format!("a {longest}  b").as_bytes())
    );
    let line = format!("get a {}\r\n", "k".repeat(251));
    assert_eq!(err(&line).error, ParseError::BadFormat);
    for bad in [
        "get a b\x01c\r\n",
        "get a\tb\r\n",
        "get a \x7f\r\n",
        "get \r\n",
        "get   \r\n",
    ] {
        assert_eq!(err(bad).error, ParseError::BadFormat, "{bad:?}");
    }
    // `get` has no noreply form, so the word is just another key.
    assert!(matches!(
        ok("get a noreply\r\n").0,
        Command::Get {
            keys: b"a noreply",
            ..
        }
    ));
}

#[test]
fn noreply_is_the_last_token_and_silences_errors() {
    assert!(err("bogus noreply\r\n").silent);
    assert!(err("noreply\r\n").silent);
    assert!(!err("bogus noreply x\r\n").silent);
    assert!(err("incr k abc noreply\r\n").silent);
    assert!(err("touch k noreply\r\n").silent);
    assert!(err("verbosity noreply\r\n").silent);
    assert!(err("flush_all abc noreply\r\n").silent);
    // Too few tokens is reported even when the last one is `noreply`.
    assert!(!err("incr noreply\r\n").silent);
    assert_eq!(
        ok("delete noreply\r\n").0,
        Command::Delete {
            key: b"noreply",
            noreply: true
        }
    );
    assert!(matches!(
        ok("set k 0 0 1 noreply x\r\nv\r\n").0,
        Command::Store { noreply: false, .. }
    ));
    assert!(matches!(
        ok("set k 0 0 1 x noreply\r\nv\r\n").0,
        Command::Store { noreply: true, .. }
    ));
}

#[test]
fn reports_missing_and_malformed_arguments() {
    for line in [
        "get\r\n",
        "gets\r\n",
        "gat\r\n",
        "gat 1\r\n",
        "delete\r\n",
        "incr k\r\n",
        "touch k\r\n",
        "verbosity\r\n",
        "verbosity abc\r\n",
    ] {
        assert_eq!(err(line).error, ParseError::BadFormat, "{line:?}");
    }
    for line in [
        "gat abc k\r\n",
        "touch k +5\r\n",
        "touch k 4294967296\r\n",
        "flush_all abc\r\n",
        "flush_all noreply 0\r\n",
    ] {
        assert_eq!(err(line).error, ParseError::InvalidExptime, "{line:?}");
    }
    assert_eq!(err("incr k +5\r\n").error, ParseError::InvalidDelta);
    assert_eq!(err("incr k -1\r\n").error, ParseError::InvalidDelta);
}

#[test]
fn oversized_value_is_swallowed_and_unlinks_the_key() {
    let failure = parse(b"set k 0 0 10 noreply\r\n", 4).unwrap_err();
    assert_eq!(failure.error, ParseError::TooLarge);
    assert_eq!(failure.consumed, "set k 0 0 10 noreply\r\n".len());
    assert_eq!(failure.swallow, 12);
    assert!(failure.silent);
    assert_eq!(failure.unlink, Some(Cow::Borrowed(&b"k"[..])));

    let failure = parse(b"add k 0 0 10\r\n", 4).unwrap_err();
    assert_eq!(failure.unlink, None);
}

#[test]
fn parses_delete_arith_touch() {
    assert_eq!(
        ok("delete k\r\n").0,
        Command::Delete {
            key: b"k",
            noreply: false
        }
    );
    assert_eq!(
        ok("delete k noreply\r\n").0,
        Command::Delete {
            key: b"k",
            noreply: true
        }
    );
    assert_eq!(
        ok("delete k 0\r\n").0,
        Command::Delete {
            key: b"k",
            noreply: false
        }
    );
    // Anything after the key is ignored.
    assert_eq!(
        ok("delete k 5 x\r\n").0,
        Command::Delete {
            key: b"k",
            noreply: false
        }
    );
    assert_eq!(
        ok("incr n 5\r\n").0,
        Command::Arith {
            key: b"n",
            delta: 5,
            decr: false,
            noreply: false
        }
    );
    assert_eq!(
        ok("decr n 2 noreply\r\n").0,
        Command::Arith {
            key: b"n",
            delta: 2,
            decr: true,
            noreply: true
        }
    );
    assert_eq!(err("incr n abc\r\n").error, ParseError::InvalidDelta);
    assert_eq!(
        ok("touch k 10\r\n").0,
        Command::Touch {
            key: b"k",
            exptime: 10,
            noreply: false
        }
    );
    assert_eq!(err("touch k abc\r\n").error, ParseError::InvalidExptime);
}

#[test]
fn parses_admin_commands() {
    assert_eq!(
        ok("flush_all\r\n").0,
        Command::FlushAll {
            delay: 0,
            noreply: false
        }
    );
    assert_eq!(
        ok("flush_all 10 noreply\r\n").0,
        Command::FlushAll {
            delay: 10,
            noreply: true
        }
    );
    assert_eq!(
        ok("flush_all noreply\r\n").0,
        Command::FlushAll {
            delay: 0,
            noreply: true
        }
    );
    assert_eq!(ok("stats\r\n").0, Command::Stats { arg: b"" });
    assert_eq!(
        ok("stats settings\r\n").0,
        Command::Stats { arg: b"settings" }
    );
    assert_eq!(ok("version\r\n").0, Command::Version);
    assert_eq!(ok("quit\r\n").0, Command::Quit);
    let shutdown = |valid, noreply| Command::Shutdown { valid, noreply };
    assert_eq!(ok("shutdown\r\n").0, shutdown(true, false));
    assert_eq!(ok("shutdown graceful\r\n").0, shutdown(true, false));
    assert_eq!(ok(" shutdown  graceful \r\n").0, shutdown(true, false));
    assert_eq!(ok("shutdown bogus\r\n").0, shutdown(false, false));
    assert_eq!(ok("shutdown GRACEFUL\r\n").0, shutdown(false, false));
    assert_eq!(ok("shutdown graceful extra\r\n").0, shutdown(false, false));
    // `noreply` counts as a mode, and an unknown one, but still silences the error.
    assert_eq!(ok("shutdown noreply\r\n").0, shutdown(false, true));
    assert_eq!(ok("shutdown graceful noreply\r\n").0, shutdown(false, true));
    assert_eq!(
        ok("verbosity 1\r\n").0,
        Command::Accepted { noreply: false }
    );
    assert_eq!(
        ok("cache_memlimit 100 noreply\r\n").0,
        Command::Accepted { noreply: true }
    );
    assert_eq!(
        ok("slabs automove 1\r\n").0,
        Command::Accepted { noreply: false }
    );
}

#[test]
fn rejects_unknown_and_empty_commands() {
    assert_eq!(err("bogus\r\n").error, ParseError::UnknownCommand);
    assert_eq!(err("\r\n").error, ParseError::BadFormat);
    assert_eq!(err("get\r\n").error, ParseError::BadFormat);
    assert_eq!(
        err("slabs reassign 1 2\r\n").error,
        ParseError::UnknownCommand
    );
    assert_eq!(err("watch\r\n").error, ParseError::UnknownCommand);
}

#[test]
fn a_line_with_no_end_is_fatal() {
    let input = "a".repeat(MAX_LINE + 1);
    let failure = err(&input);
    assert_eq!(failure.error, ParseError::LineTooLong);
    assert!(failure.error.is_fatal());
    assert!(incomplete(&"a".repeat(MAX_LINE)));
}

#[test]
fn the_line_limit_does_not_depend_on_whether_the_newline_has_arrived() {
    // The limit counts every byte before the newline.
    let longest = format!("get k{}\r\n", " ".repeat(MAX_LINE - 6));
    assert_eq!(longest.len(), MAX_LINE + 1);
    assert!(matches!(ok(&longest), (Command::Get { .. }, consumed) if consumed == MAX_LINE + 1));
    for cut in [MAX_LINE - 1, MAX_LINE] {
        assert!(incomplete(&longest[..cut]));
    }

    let too_long = format!("get k{}\r\n", " ".repeat(MAX_LINE - 5));
    for cut in [MAX_LINE + 1, MAX_LINE + 2] {
        let failure = err(&too_long[..cut]);
        assert_eq!(failure.error, ParseError::LineTooLong, "cut at {cut}");
    }
    // A complete line that is too long is rejected even when more requests follow it.
    let followed = format!("{too_long}get k\r\n");
    assert_eq!(err(&followed).error, ParseError::LineTooLong);
}

fn meta_get(input: &str) -> MetaOpts<'_> {
    match ok(input).0 {
        Command::MetaGet(opts) => opts,
        other => panic!("expected mg, got {other:?}"),
    }
}

#[test]
fn parses_mg_flags() {
    let opts = meta_get("mg key v f t q k O123 T30 N60 R10 E9 u\r\n");
    assert_eq!(&opts.key[..], b"key");
    assert_eq!(opts.raw_key, b"key");
    assert_eq!(opts.flags, b"v f t q k O123 T30 N60 R10 E9 u");
    assert!(opts.value && opts.quiet && opts.no_bump && !opts.base64);
    assert_eq!(
        (opts.ttl, opts.vivify, opts.recache, opts.new_cas),
        (Some(30), Some(60), Some(10), Some(9))
    );
    assert_eq!(meta_get("mg key\r\n").flags, b"");
}

#[test]
fn parses_ms_with_data() {
    let input = "ms key 3 T0 F5 MA C7 I c\r\nabc\r\n";
    let (cmd, consumed) = ok(input);
    assert_eq!(consumed, input.len());
    let Command::MetaSet { opts, data } = cmd else {
        panic!("expected ms")
    };
    assert_eq!(data, b"abc");
    assert_eq!(
        (opts.ttl, opts.client_flags, opts.mode, opts.cas),
        (Some(0), Some(5), Some(b'A'), Some(7))
    );
    assert!(opts.invalidate);
    assert!(incomplete("ms key 3\r\nab"));
}

#[test]
fn parses_md_ma_me_mn() {
    let Command::MetaDelete(opts) = ok("md key q I T30 x C5\r\n").0 else {
        panic!("expected md")
    };
    assert!(opts.quiet && opts.invalidate && opts.remove_value);
    assert_eq!((opts.ttl, opts.cas), (Some(30), Some(5)));

    let Command::MetaArith(opts) = ok("ma key N0 J10 D3 MD v\r\n").0 else {
        panic!("expected ma")
    };
    assert_eq!(
        (opts.vivify, opts.initial, opts.delta, opts.mode),
        (Some(0), Some(10), Some(3), Some(b'D'))
    );
    assert!(opts.value);

    assert!(matches!(ok("me key\r\n").0, Command::MetaDebug(_)));
    assert_eq!(ok("mn\r\n"), (Command::MetaNoop, 4));
}

#[test]
fn decodes_base64_keys() {
    let opts = meta_get("mg Zm9vIGJhcg== b v\r\n");
    assert!(opts.base64);
    assert_eq!(&opts.key[..], b"foo bar");
    assert_eq!(opts.raw_key, b"Zm9vIGJhcg==");
    assert_eq!(err("mg !!! b\r\n").error, ParseError::BadKeyEncoding);
    assert_eq!(err("me !!! b\r\n").error, ParseError::BadFormat);
}

#[test]
fn ignores_proxy_tokens() {
    let opts = meta_get("mg key Pabc Ldef v\r\n");
    assert!(opts.value);
}

#[test]
fn rejects_bad_meta_requests() {
    assert_eq!(err("mg\r\n").error, ParseError::BadFormat);
    // A flag that starts outside `A` to `z` and the digits fails the whole line.
    assert_eq!(err("mg key !\r\n").error, ParseError::BadFormat);
    assert_eq!(err("mg key {\r\n").error, ParseError::BadFormat);
    assert_eq!(err("mg key Z\r\n").error, ParseError::InvalidFlag);
    assert_eq!(err("mg key 5\r\n").error, ParseError::InvalidFlag);
    assert_eq!(err("mg key _\r\n").error, ParseError::InvalidFlag);
    assert_eq!(err("mg key v v\r\n").error, ParseError::DuplicateFlag);
    assert_eq!(err("mg key Tabc\r\n").error, ParseError::BadToken);
    assert_eq!(err("mg key T+5\r\n").error, ParseError::BadToken);
    assert_eq!(err("mg key Dabc\r\n").error, ParseError::InvalidDeltaValue);
    assert_eq!(
        err("mg key Jabc\r\n").error,
        ParseError::InvalidInitialValue
    );
    assert_eq!(err("mg key Fabc\r\n").error, ParseError::BadFormat);
    let long_key = format!("mg {}\r\n", "k".repeat(251));
    assert_eq!(err(&long_key).error, ParseError::BadFormat);
    let flags = "s t f k c h l q u E1 N0 R1 T1 O1 P L v x";
    assert!(matches!(
        ok(&format!("mg key {flags}\r\n")).0,
        Command::MetaGet(_)
    ));
    assert_eq!(
        err(&format!("mg key {flags} I\r\n")).error,
        ParseError::TooManyFlags
    );
    assert_eq!(
        err(&format!("ms key 1 {flags}\r\n")).error,
        ParseError::TooManyFlags
    );
    let opaque = format!("mg key O{}\r\n", "a".repeat(32));
    assert_eq!(err(&opaque).error, ParseError::OpaqueTooLong);
    assert!(matches!(
        ok(&opaque.replacen("a", "", 1)).0,
        Command::MetaGet(_)
    ));
}

#[test]
fn every_meta_command_accepts_every_known_flag() {
    for line in [
        "mg key D5 J5 x I C5 F5 MX\r\n",
        "md key D5 J5 N5 F5 MX R5 s t v\r\n",
        "ma key F5 s f h l x u\r\n",
        "me key Z 5\r\n",
    ] {
        ok(line);
    }
    assert!(matches!(
        ok("ms key 1 D5 J5 R5 v x u h l t f\r\nz\r\n").0,
        Command::MetaSet { .. }
    ));
}

#[test]
fn the_last_flag_error_wins_unless_the_scan_stops() {
    assert_eq!(err("mg key Tabc Z\r\n").error, ParseError::InvalidFlag);
    assert_eq!(err("mg key Tabc v v\r\n").error, ParseError::DuplicateFlag);
    assert_eq!(err("mg !!! b Tabc\r\n").error, ParseError::BadToken);
    assert_eq!(err("mg !!! Tabc b\r\n").error, ParseError::BadKeyEncoding);
    assert_eq!(err("mg key Tabc Fabc\r\n").error, ParseError::BadToken);
}

#[test]
fn ma_reports_every_flag_error_the_same_way() {
    let long_key = format!("ma {}\r\n", "k".repeat(251));
    for line in [
        "ma key v v\r\n",
        "ma key Z\r\n",
        "ma key Dabc\r\n",
        "ma key Tabc\r\n",
        "ma !!! b\r\n",
        &long_key,
    ] {
        assert_eq!(err(line).error, ParseError::InvalidArithFlag, "{line:?}");
    }
    assert_eq!(err("ma key !\r\n").error, ParseError::BadFormat);
    for line in [
        "ma key MX\r\n",
        "ma key M\r\n",
        "ma key Mi\r\n",
        "ma key Md\r\n",
    ] {
        assert_eq!(err(line).error, ParseError::InvalidArithMode, "{line:?}");
    }
}

#[test]
fn ms_errors_swallow_the_data_block() {
    for line in ["ms key 3 MX\r\n", "ms key 3 M\r\n", "ms key 3 Ms\r\n"] {
        let failure = err(line);
        assert_eq!(failure.error, ParseError::InvalidSetMode, "{line:?}");
        assert_eq!(failure.swallow, 5);
    }
    let failure = err("ms key 3 Tabc\r\n");
    assert_eq!(failure.error, ParseError::BadToken);
    assert_eq!(failure.swallow, 5);

    // Without a usable length, or with a flag the tokenizer rejects, nothing is swallowed.
    for line in [
        "ms key\r\n",
        "ms key abc\r\n",
        "ms key -1\r\n",
        "ms key 3 !\r\n",
    ] {
        let failure = err(line);
        assert_eq!(failure.error, ParseError::BadFormat, "{line:?}");
        assert_eq!(failure.swallow, 0, "{line:?}");
    }

    let failure = parse(b"ms key 10 q\r\n", 4).unwrap_err();
    assert_eq!(failure.error, ParseError::TooLarge);
    assert_eq!(failure.swallow, 12);
    assert_eq!(failure.unlink, Some(Cow::Borrowed(&b"key"[..])));

    let failure = parse(b"ms key 10 ME\r\n", 4).unwrap_err();
    assert_eq!(failure.unlink, None);
}

#[test]
fn pending_len_reports_the_size_of_an_incomplete_data_block() {
    // The line, 10 bytes of data and the closing CRLF.
    assert_eq!(pending_len(b"set k 0 0 10\r\nabc", 1024), Some(14 + 12));
    assert_eq!(pending_len(b"ms k 10 T0\r\n", 1024), Some(12 + 12));
    // No data block, an incomplete line, an invalid line, or a value over the limit.
    assert_eq!(pending_len(b"get k\r\n", 1024), None);
    assert_eq!(pending_len(b"set k 0 0 10", 1024), None);
    assert_eq!(pending_len(b"set k 0 0 x\r\n", 1024), None);
    assert_eq!(pending_len(b"set k 0 0 2048\r\n", 1024), None);
}
