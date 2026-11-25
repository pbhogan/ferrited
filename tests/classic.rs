mod common;

use common::{exchange, send, start, start_with};
use ferrited::protocol::MAX_LINE;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn set_and_get() {
    let addr = start().await;
    let reply = exchange(addr, "set k 5 0 3\r\nabc\r\nget k missing\r\n").await;
    assert_eq!(reply, "STORED\r\nVALUE k 5 3\r\nabc\r\nEND\r\n");
}

#[tokio::test]
async fn data_persists_across_connections() {
    let addr = start().await;
    assert_eq!(exchange(addr, "set k 0 0 1\r\nx\r\n").await, "STORED\r\n");
    assert_eq!(
        exchange(addr, "get k\r\n").await,
        "VALUE k 0 1\r\nx\r\nEND\r\n"
    );
}

#[tokio::test]
async fn storage_modes() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "replace k 0 0 1\r\na\r\nadd k 0 0 1\r\nb\r\nadd k 0 0 1\r\nc\r\n\
         append k 0 0 1\r\nd\r\nprepend k 0 0 1\r\ne\r\nget k\r\n",
    )
    .await;
    assert_eq!(
        reply,
        "NOT_STORED\r\nSTORED\r\nNOT_STORED\r\nSTORED\r\nSTORED\r\nVALUE k 0 3\r\nebd\r\nEND\r\n"
    );
}

#[tokio::test]
async fn cas_round_trip() {
    let addr = start().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        send(&mut stream, "cas k 0 0 1 5\r\nx\r\n").await,
        "NOT_FOUND\r\n"
    );
    let reply = send(&mut stream, "set k 0 0 1\r\na\r\ngets k\r\n").await;
    let token: u64 = reply.split_whitespace().nth(5).unwrap().parse().unwrap();
    assert_eq!(
        send(&mut stream, &format!("cas k 0 0 1 {}\r\nb\r\n", token + 1)).await,
        "EXISTS\r\n"
    );
    assert_eq!(
        send(&mut stream, &format!("cas k 0 0 1 {token}\r\nb\r\n")).await,
        "STORED\r\n"
    );
    assert_eq!(
        send(&mut stream, "get k\r\n").await,
        "VALUE k 0 1\r\nb\r\nEND\r\n"
    );
}

#[tokio::test]
async fn delete_incr_decr_touch() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "delete n\r\nincr n 1\r\nset n 0 0 2\r\n10\r\nincr n 5\r\ndecr n 20\r\n\
         touch n 100\r\ntouch missing 100\r\ndelete n\r\nget n\r\n",
    )
    .await;
    assert_eq!(
        reply,
        "NOT_FOUND\r\nNOT_FOUND\r\nSTORED\r\n15\r\n0\r\nTOUCHED\r\nNOT_FOUND\r\nDELETED\r\nEND\r\n"
    );
}

#[tokio::test]
async fn incr_on_text_is_a_client_error() {
    let addr = start().await;
    let reply = exchange(addr, "set k 0 0 1\r\na\r\nincr k 1\r\n").await;
    assert_eq!(
        reply,
        "STORED\r\nCLIENT_ERROR cannot increment or decrement non-numeric value\r\n"
    );
}

#[tokio::test]
async fn negative_exptime_expires_immediately_and_gat_touches() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "set k 0 -1 1\r\na\r\nget k\r\nset j 0 0 1\r\nb\r\ngat -1 j\r\nget j\r\n",
    )
    .await;
    assert_eq!(
        reply,
        "STORED\r\nEND\r\nSTORED\r\nVALUE j 0 1\r\nb\r\nEND\r\nEND\r\n"
    );
}

#[tokio::test]
async fn noreply_suppresses_replies() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "set k 0 0 1 noreply\r\na\r\ndelete missing noreply\r\nincr k 1 noreply\r\nget k\r\n",
    )
    .await;
    assert_eq!(reply, "VALUE k 0 1\r\na\r\nEND\r\n");
}

#[tokio::test]
async fn flush_all_clears_everything() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "set k 0 0 1\r\na\r\nflush_all\r\nget k\r\nset k 0 0 1\r\nb\r\nget k\r\n",
    )
    .await;
    assert_eq!(
        reply,
        "STORED\r\nOK\r\nEND\r\nSTORED\r\nVALUE k 0 1\r\nb\r\nEND\r\n"
    );
}

#[tokio::test]
async fn errors_keep_the_connection_usable() {
    let addr = start().await;
    let reply = exchange(addr, "bogus\r\nset k 0 0 x\r\nget k\r\n").await;
    assert_eq!(
        reply,
        "ERROR\r\nCLIENT_ERROR bad command line format\r\nEND\r\n"
    );
}

#[tokio::test]
async fn a_rejected_storage_line_swallows_its_data_block() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "set k x 0 3\r\nabc\r\nset k 0 0 3 noreply\r\nabcde\r\nget k\r\n",
    )
    .await;
    // The bad chunk is silent under noreply; its leftover CRLF reads as an empty line.
    assert_eq!(
        reply,
        "CLIENT_ERROR bad command line format\r\nCLIENT_ERROR bad command line format\r\nEND\r\n"
    );
}

#[tokio::test]
async fn noreply_silences_errors_and_stats_rejects_unknown_arguments() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "bogus noreply\r\nincr k abc noreply\r\ndelete k 5\r\nstats bogus\r\n",
    )
    .await;
    assert_eq!(reply, "NOT_FOUND\r\nERROR\r\n");
}

#[tokio::test]
async fn oversized_set_is_rejected_swallowed_and_removes_the_old_value() {
    let addr = start_with(|config| config.max_item_size = 8).await;
    let input = format!(
        "set k 0 0 1\r\na\r\nset k 0 0 100\r\n{}\r\nget k\r\n",
        "x".repeat(100)
    );
    let reply = exchange(addr, &input).await;
    assert_eq!(
        reply,
        "STORED\r\nSERVER_ERROR object too large for cache\r\nEND\r\n"
    );
}

#[tokio::test]
async fn large_value_arriving_in_pieces() {
    let addr = start().await;
    let payload = "y".repeat(300_000);
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!("set big 0 0 {}\r\n{payload}\r\n", payload.len());
    for piece in request.as_bytes().chunks(7_000) {
        stream.write_all(piece).await.unwrap();
        stream.flush().await.unwrap();
    }
    assert_eq!(send(&mut stream, "").await, "STORED\r\n");
    let reply = send(&mut stream, "get big\r\n").await;
    assert_eq!(
        reply,
        format!("VALUE big 0 {}\r\n{payload}\r\nEND\r\n", payload.len())
    );
}

#[tokio::test]
async fn admin_commands() {
    let addr = start().await;
    let reply = exchange(addr, "version\r\nverbosity 1\r\nshutdown\r\n").await;
    let expected = format!(
        "VERSION 1.6.40-ferrited-{}\r\nOK\r\nSERVER_ERROR: shutdown not enabled\r\n",
        env!("CARGO_PKG_VERSION")
    );
    assert_eq!(reply, expected);
}

#[tokio::test]
async fn stats_report_counters() {
    let addr = start().await;
    let reply = exchange(addr, "set k 0 0 1\r\na\r\nget k\r\nget missing\r\n").await;
    assert!(reply.ends_with("END\r\n"));
    let stats = exchange(addr, "stats\r\n").await;
    for line in [
        "STAT cmd_get 2\r\n",
        "STAT get_hits 1\r\n",
        "STAT get_misses 1\r\n",
        "STAT cmd_set 1\r\n",
        "STAT curr_items 1\r\n",
        "STAT limit_maxbytes 67108864\r\n",
    ] {
        assert!(stats.contains(line), "missing {line:?} in {stats}");
    }
    assert!(stats.ends_with("END\r\n"));
    let settings = exchange(addr, "stats settings\r\n").await;
    assert!(settings.contains("STAT item_size_max 1048576\r\n"));
    assert_eq!(exchange(addr, "stats slabs\r\n").await, "END\r\n");
}

#[tokio::test]
async fn quit_closes_the_connection() {
    let addr = start().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    // Proves the connection works, so the EOF below comes from `quit`.
    assert_eq!(
        send(&mut stream, "set k 0 0 1\r\na\r\n").await,
        "STORED\r\n"
    );
    stream.write_all(b"quit\r\n").await.unwrap();
    let mut rest = Vec::new();
    assert_eq!(stream.read_to_end(&mut rest).await.unwrap(), 0);
    // The server and its data remain.
    assert_eq!(
        exchange(addr, "get k\r\n").await,
        "VALUE k 0 1\r\na\r\nEND\r\n"
    );
}

/// Writes `request` in `piece`-byte chunks, then reads until `until` arrives or the server closes.
async fn deliver(addr: std::net::SocketAddr, request: &[u8], piece: usize, until: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    for part in request.chunks(piece) {
        // The server may already have rejected the line and closed.
        if stream.write_all(part).await.is_err() {
            break;
        }
    }
    let mut received = Vec::new();
    let mut chunk = [0u8; 4096];
    while !received.ends_with(until.as_bytes()) {
        let read =
            tokio::time::timeout(std::time::Duration::from_secs(10), stream.read(&mut chunk))
                .await
                .expect("timed out waiting for a reply");
        match read {
            Ok(0) | Err(_) => break,
            Ok(read) => received.extend_from_slice(&chunk[..read]),
        }
    }
    String::from_utf8(received).unwrap()
}

#[tokio::test]
async fn a_long_multi_key_get_is_answered() {
    let addr = start().await;
    let key = "k".repeat(100);
    let request = format!(
        "set {key} 0 0 1\r\nx\r\nget{}\r\n",
        format!(" {key}").repeat(3_000)
    );
    assert!(request.len() > 300_000);
    let expected = format!(
        "STORED\r\n{}END\r\n",
        format!("VALUE {key} 0 1\r\nx\r\n").repeat(3_000)
    );
    for piece in [usize::MAX, 1_000] {
        let reply = deliver(addr, request.as_bytes(), piece, "END\r\n").await;
        assert!(
            reply == expected,
            "piece size {piece}: got {} bytes",
            reply.len()
        );
    }
}

#[tokio::test]
async fn the_request_line_limit_is_the_same_however_the_line_arrives() {
    let addr = start().await;
    // The longest allowed line: MAX_LINE bytes before the newline.
    let longest = format!("get k{}\r\n", " ".repeat(MAX_LINE - 6));
    let too_long = format!("get k{}\r\n", " ".repeat(MAX_LINE - 5));
    for piece in [usize::MAX, 1_000, 65_536] {
        let reply = deliver(addr, longest.as_bytes(), piece, "END\r\n").await;
        assert_eq!(reply, "END\r\n", "piece size {piece}");
        let reply = deliver(addr, too_long.as_bytes(), piece, "long\r\n").await;
        assert_eq!(
            reply, "CLIENT_ERROR line too long\r\n",
            "piece size {piece}"
        );
    }
    // Also rejected when complete and followed by another request.
    let followed = format!("{too_long}get k\r\n");
    let reply = deliver(addr, followed.as_bytes(), usize::MAX, "END\r\n").await;
    assert!(
        "CLIENT_ERROR line too long\r\n".starts_with(&reply),
        "{reply:?}"
    );
}

fn stat_value(stats: &str, name: &str) -> u64 {
    let prefix = format!("STAT {name} ");
    let line = stats.lines().find(|line| line.starts_with(&prefix));
    line.unwrap_or_else(|| panic!("no {name} in {stats}"))[prefix.len()..]
        .parse()
        .unwrap()
}

#[tokio::test]
async fn stats_pipelined_after_other_commands_includes_them() {
    let addr = start().await;
    let reply = exchange(addr, "set k 0 0 1\r\na\r\nget k missing\r\nstats\r\n").await;
    assert_eq!(stat_value(&reply, "cmd_set"), 1);
    assert_eq!(stat_value(&reply, "cmd_get"), 2);
    assert_eq!(stat_value(&reply, "get_hits"), 1);
    assert!(stat_value(&reply, "bytes_read") > 0);
}

#[tokio::test]
async fn gat_counts_as_a_touch_and_not_a_get() {
    let addr = start().await;
    let reply = exchange(addr, "set k 0 0 1\r\na\r\ngat 30 k missing\r\nstats\r\n").await;
    assert_eq!(stat_value(&reply, "cmd_touch"), 2);
    assert_eq!(stat_value(&reply, "touch_hits"), 1);
    assert_eq!(stat_value(&reply, "touch_misses"), 1);
    assert_eq!(stat_value(&reply, "cmd_get"), 0);
    assert_eq!(stat_value(&reply, "get_hits"), 0);
    assert_eq!(stat_value(&reply, "get_misses"), 0);
}

#[tokio::test]
async fn stats_reset_zeroes_the_counters_and_keeps_the_gauges() {
    let addr = start().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    send(
        &mut stream,
        "set k 0 0 1\r\na\r\nget k\r\nget missing\r\ntouch k 0\r\n",
    )
    .await;
    let before = send(&mut stream, "stats\r\n").await;
    assert_eq!(stat_value(&before, "total_items"), 1);
    assert_eq!(stat_value(&before, "total_connections"), 1);
    assert_eq!(send(&mut stream, "stats reset\r\n").await, "RESET\r\n");
    let after = send(&mut stream, "stats\r\n").await;
    for name in [
        "cmd_get",
        "cmd_set",
        "cmd_touch",
        "get_hits",
        "get_misses",
        "touch_hits",
        "total_items",
        "total_connections",
        "evictions",
        "reclaimed",
    ] {
        assert_eq!(stat_value(&after, name), 0, "{name}");
    }
    // Only the reset's tail and this request count.
    assert!(stat_value(&after, "bytes_read") <= "mn\r\nstats\r\nmn\r\n".len() as u64);
    assert!(stat_value(&after, "bytes_written") <= "RESET\r\nMN\r\n".len() as u64);
    assert_eq!(stat_value(&after, "curr_items"), 1);
    assert_eq!(stat_value(&after, "curr_connections"), 1);
    assert!(stat_value(&after, "bytes") > 0);
    assert_eq!(
        send(&mut stream, "stats reset extra\r\n").await,
        "RESET\r\n"
    );
}

#[tokio::test]
async fn shutdown_with_an_unknown_mode_is_an_error_and_does_not_stop_the_server() {
    let addr = start_with(|config| config.enable_shutdown = true).await;
    let reply = exchange(
        addr,
        "shutdown bogus\r\nshutdown graceful extra\r\nshutdown noreply\r\nshutdown GRACEFUL\r\n",
    )
    .await;
    assert_eq!(reply, "CLIENT_ERROR invalid shutdown mode\r\n".repeat(3));
    assert_eq!(exchange(addr, "version\r\n").await.len(), 31);
}

#[tokio::test]
async fn shutdown_and_shutdown_graceful_close_the_connection_when_enabled() {
    for request in ["shutdown\r\n", "shutdown graceful\r\n"] {
        // Shutdown stops the server, so each request gets its own.
        let addr = start_with(|config| config.enable_shutdown = true).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut rest = Vec::new();
        assert_eq!(
            stream.read_to_end(&mut rest).await.unwrap(),
            0,
            "{request:?}"
        );
    }
}

#[tokio::test]
async fn shutdown_when_disabled_reports_that_whatever_the_mode() {
    let addr = start().await;
    let reply = exchange(
        addr,
        "shutdown bogus\r\nshutdown graceful\r\nshutdown noreply\r\n",
    )
    .await;
    assert_eq!(reply, "SERVER_ERROR: shutdown not enabled\r\n".repeat(2));
}
