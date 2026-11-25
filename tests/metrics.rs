//! The Prometheus metrics endpoint (`--metrics-addr`), scraped from outside the process.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use common::process::{Process, free_port, refused, request};

/// How long a node may take to start, become ready or replicate.
const PATIENCE: Duration = Duration::from_secs(20);

/// The endpoint's read timeout.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// The metrics port `node` reports.
fn metrics_port(node: &Process) -> u16 {
    let address = node
        .wait_for_line("ferrited metrics listening on ", PATIENCE)
        .expect("the node did not report its metrics address");
    address.rsplit_once(':').unwrap().1.parse().unwrap()
}

/// Sends `request` to the metrics port and reads until the node closes the connection.
fn exchange(port: u16, request: &[u8]) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream.set_read_timeout(Some(PATIENCE)).unwrap();
    stream.write_all(request).unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).unwrap();
    reply
}

/// Scrapes `/metrics`, checks the response's head, and returns its body.
fn scrape(port: u16) -> String {
    let reply = exchange(port, b"GET /metrics HTTP/1.1\r\nHost: test\r\n\r\n");
    let (head, body) = reply.split_once("\r\n\r\n").expect("no end of head");
    assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
    assert!(
        head.contains("\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n"),
        "{head}"
    );
    assert!(
        head.contains(&format!("\r\nContent-Length: {}\r\n", body.len())),
        "{head}"
    );
    assert!(head.ends_with("\r\nConnection: close"), "{head}");
    body.to_string()
}

/// The value of `series` (name and labels, as written) in `body`.
fn value(body: &str, series: &str) -> Option<f64> {
    body.lines()
        .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
        .map(|value| value.parse().unwrap())
}

/// Scrapes until `series` equals `wanted`; returns that scrape.
fn scrape_until(port: u16, series: &str, wanted: f64) -> String {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let body = scrape(port);
        if value(&body, series) == Some(wanted) {
            return body;
        }
        assert!(
            Instant::now() < deadline,
            "{series} never became {wanted}; last scrape:\n{body}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The current `ferrited_state`.
fn state(body: &str) -> &'static str {
    let current: Vec<&str> = ["recovering", "waiting", "ready", "stopping"]
        .into_iter()
        .filter(|state| value(body, &format!("ferrited_state{{state=\"{state}\"}}")) == Some(1.0))
        .collect();
    assert_eq!(current.len(), 1, "{body}");
    current[0]
}

#[test]
fn a_single_node_reports_its_stats_and_they_move() {
    let node = Process::start(&["--metrics-addr", "127.0.0.1:0"]);
    let port = metrics_port(&node);
    let before = scrape(port);
    assert_eq!(value(&before, "ferrited_ready"), Some(1.0), "{before}");
    assert_eq!(state(&before), "ready");
    assert_eq!(value(&before, "ferrited_cmd_set_total"), Some(0.0));
    assert_eq!(value(&before, "ferrited_persist_enabled"), Some(0.0));
    assert!(before.contains("# TYPE ferrited_get_hits_total counter\n"));
    assert!(before.contains("# TYPE ferrited_curr_items gauge\n"));
    assert!(before.contains("ferrited_build_info{version=\""));
    let mut client = node.connect();
    let replies = request(
        &mut client,
        "set a 0 0 1\r\nx\r\nset b 0 0 2\r\nyy\r\nget a\r\nget missing\r\n",
    );
    assert!(replies.starts_with("STORED\r\nSTORED\r\n"), "{replies}");
    let after = scrape(port);
    for (series, wanted) in [
        ("ferrited_cmd_set_total", 2.0),
        ("ferrited_get_hits_total", 1.0),
        ("ferrited_get_misses_total", 1.0),
        ("ferrited_cmd_get_total", 2.0),
        ("ferrited_curr_items", 2.0),
        ("ferrited_curr_connections", 1.0),
    ] {
        assert_eq!(value(&after, series), Some(wanted), "{series}:\n{after}");
    }
    assert!(value(&after, "ferrited_read_bytes_total").unwrap() > 0.0);
    // The same values as `stats`.
    let stats = request(&mut client, "stats\r\n");
    let stat = |name: &str| -> f64 {
        let prefix = format!("STAT {name} ");
        let line = stats.lines().find_map(|line| line.strip_prefix(&prefix));
        line.unwrap().parse().unwrap()
    };
    assert_eq!(
        value(&after, "ferrited_memory_used_bytes"),
        Some(stat("bytes"))
    );
    assert_eq!(
        value(&after, "ferrited_memory_limit_bytes"),
        Some(stat("limit_maxbytes"))
    );
    assert_eq!(value(&after, "ferrited_process_id"), Some(stat("pid")));
    // HEAD has the head alone; other paths and methods are refused.
    let head = exchange(port, b"HEAD /metrics HTTP/1.1\r\n\r\n");
    assert!(
        head.starts_with("HTTP/1.1 200 OK\r\n") && head.ends_with("\r\n\r\n"),
        "{head}"
    );
    let not_found = exchange(port, b"GET / HTTP/1.1\r\n\r\n");
    assert!(not_found.starts_with("HTTP/1.1 404 "), "{not_found}");
    let not_allowed = exchange(port, b"POST /metrics HTTP/1.1\r\n\r\n");
    assert!(not_allowed.starts_with("HTTP/1.1 405 "), "{not_allowed}");
    assert!(
        not_allowed.contains("\r\nAllow: GET, HEAD\r\n"),
        "{not_allowed}"
    );
    let bad = exchange(port, b"stats\r\n\r\n");
    assert!(bad.starts_with("HTTP/1.1 400 "), "{bad}");
    drop(client);
    assert!(node.terminate().success());
}

#[test]
fn a_metrics_port_in_use_stops_startup() {
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = taken.local_addr().unwrap().to_string();
    let error = match Process::try_start(&["--metrics-addr", &address]) {
        Ok(_) => panic!("started with its metrics port taken"),
        Err(error) => error,
    };
    assert!(error.contains("cannot listen for metrics on"), "{error}");
}

#[test]
fn a_replicating_pair_reports_each_link() {
    for _ in 0..5 {
        let ports: Vec<u16> = {
            let mut ports = std::collections::BTreeSet::new();
            while ports.len() < 4 {
                ports.insert(free_port());
            }
            ports.into_iter().collect()
        };
        let (repl, client) = ([ports[0], ports[1]], [ports[2], ports[3]]);
        let args = |me: usize| -> Vec<String> {
            [
                "-p",
                &client[me].to_string(),
                "--node-id",
                &(me + 1).to_string(),
                "--repl-port",
                &repl[me].to_string(),
                "--peer",
                &format!("127.0.0.1:{}", repl[1 - me]),
                "--metrics-addr",
                "127.0.0.1:0",
            ]
            .map(String::from)
            .to_vec()
        };
        let spawn = |me: usize| {
            let args = args(me);
            Process::spawn(&args.iter().map(String::as_str).collect::<Vec<_>>(), &[])
        };
        let mut nodes = [spawn(0), spawn(1)];
        let started = nodes
            .iter_mut()
            .all(|node| match node.wait_listening(PATIENCE) {
                Ok(true) => true,
                Ok(false) => panic!("a node did not start serving"),
                Err(error) if error.contains("Address already in use") => false,
                Err(error) => panic!("a node did not start: {error}"),
            });
        if !started {
            continue;
        }
        let ports = [metrics_port(&nodes[0]), metrics_port(&nodes[1])];
        let peer = format!("127.0.0.1:{}", repl[1]);
        let streaming = |direction: &str| {
            format!(
                "ferrited_repl_link_state{{peer=\"{peer}\",direction=\"{direction}\",state=\"streaming\"}}"
            )
        };
        scrape_until(ports[0], &streaming("out"), 1.0);
        let body = scrape_until(ports[0], &streaming("in"), 1.0);
        assert_eq!(value(&body, "ferrited_repl_links_up"), Some(2.0), "{body}");
        assert_eq!(state(&body), "ready");
        assert_eq!(
            value(&body, "ferrited_ready_by{by=\"caught_up\"}"),
            Some(1.0),
            "{body}"
        );
        let down = streaming("out").replace("streaming", "down");
        assert_eq!(value(&body, &down), Some(0.0), "{body}");
        assert_eq!(
            value(
                &body,
                &format!("ferrited_repl_peer_node_id{{peer=\"{peer}\"}}")
            ),
            Some(2.0)
        );
        let mut client = nodes[0].connect();
        let mut writes = String::new();
        for index in 0..50 {
            writes.push_str(&format!("set key{index} 0 0 1\r\nv\r\n"));
        }
        request(&mut client, &writes);
        // Node 2 applies them and node 1's lag returns to 0.
        scrape_until(ports[1], "ferrited_repl_records_applied_total", 50.0);
        let lag = format!("ferrited_repl_peer_lag_entries{{peer=\"{peer}\"}}");
        let body = scrape_until(ports[0], &lag, 0.0);
        assert_eq!(
            value(&body, "ferrited_repl_records_sent_total"),
            Some(50.0),
            "{body}"
        );
        let acked = format!("ferrited_repl_peer_acked_position{{peer=\"{peer}\"}}");
        assert!(value(&body, &acked).unwrap() >= 50.0, "{body}");
        let [first, second] = nodes;
        assert!(first.terminate().success());
        assert!(second.terminate().success());
        return;
    }
    panic!("could not start the pair: its ports kept being taken");
}

#[test]
fn a_node_that_is_not_ready_can_be_scraped_while_its_client_port_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let client = free_port();
    let repl = loop {
        let repl = free_port();
        if repl != client {
            break repl;
        }
    };
    // Slowed recovery, then a wait for the sync timeout: its only peer never answers.
    let node = Process::spawn(
        &[
            "-p",
            &client.to_string(),
            "--node-id",
            "1",
            "--repl-port",
            &repl.to_string(),
            "--peer",
            "127.0.0.1:1",
            "--sync-timeout",
            "3",
            "--data-dir",
            dir.path().to_str().unwrap(),
            "--metrics-addr",
            "127.0.0.1:0",
        ],
        &[("FERRITED_TEST_RECOVERY_DELAY_MS", "2000")],
    );
    let port = metrics_port(&node);
    let body = scrape(port);
    assert_eq!(state(&body), "recovering", "{body}");
    assert_eq!(value(&body, "ferrited_ready"), Some(0.0));
    // Persistence and replication have not started yet.
    assert!(!body.contains("ferrited_persist_"), "{body}");
    assert!(!body.contains("ferrited_repl_"), "{body}");
    assert!(refused(client));
    let body = scrape_until(port, "ferrited_state{state=\"waiting\"}", 1.0);
    assert!(
        refused(client),
        "the client port accepted while the node waited"
    );
    assert_eq!(value(&body, "ferrited_ready"), Some(0.0));
    assert_eq!(
        value(
            &body,
            "ferrited_waiting_reason{reason=\"no_peer_reachable\"}"
        ),
        Some(1.0),
        "{body}"
    );
    assert_eq!(value(&body, "ferrited_sync_timeout_seconds"), Some(3.0));
    assert_eq!(value(&body, "ferrited_peer_reachable"), Some(0.0));
    assert_eq!(value(&body, "ferrited_persist_enabled"), Some(1.0));
    let peer = "ferrited_repl_link_state{peer=\"127.0.0.1:1\",direction=\"out\",state=\"down\"}";
    assert_eq!(value(&body, peer), Some(1.0), "{body}");
    let body = scrape_until(port, "ferrited_ready", 1.0);
    assert_eq!(state(&body), "ready");
    assert_eq!(
        value(&body, "ferrited_ready_by{by=\"sync_timeout\"}"),
        Some(1.0)
    );
    assert_eq!(value(&body, "ferrited_repl_sync_timeouts"), Some(1.0));
    // Ready is reported only once the client port accepts.
    assert!(!refused(client));
    assert!(node.terminate().success());
}

#[test]
fn connections_are_capped_and_idle_ones_time_out() {
    let node = Process::start(&["--metrics-addr", "127.0.0.1:0"]);
    let port = metrics_port(&node);
    // Eight silent clients fill every slot.
    let idle: Vec<TcpStream> = (0..8)
        .map(|_| {
            let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            stream.set_read_timeout(Some(PATIENCE)).unwrap();
            stream
        })
        .collect();
    let opened = Instant::now();
    std::thread::sleep(Duration::from_millis(200));
    let mut turned_away = TcpStream::connect(("127.0.0.1", port)).unwrap();
    turned_away.set_read_timeout(Some(PATIENCE)).unwrap();
    let mut reply = String::new();
    turned_away.read_to_string(&mut reply).unwrap();
    assert!(reply.starts_with("HTTP/1.1 503 "), "{reply}");
    // The idle ones are answered 408 and closed at the read timeout.
    for mut stream in idle {
        let mut reply = String::new();
        stream.read_to_string(&mut reply).unwrap();
        assert!(reply.starts_with("HTTP/1.1 408 "), "{reply}");
    }
    let waited = opened.elapsed();
    assert!(
        waited >= READ_TIMEOUT - Duration::from_millis(250),
        "{waited:?}"
    );
    assert!(waited < READ_TIMEOUT * 2, "{waited:?}");
    // With slots free, a scrape succeeds and an oversized head is refused.
    scrape(port);
    let mut oversized = b"GET /metrics HTTP/1.1\r\nX: ".to_vec();
    oversized.resize(8 * 1024, b'a');
    let reply = exchange(port, &oversized);
    assert!(reply.starts_with("HTTP/1.1 431 "), "{reply}");
    assert!(node.terminate().success());
}
