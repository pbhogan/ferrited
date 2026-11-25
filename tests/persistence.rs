mod common;

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::process::{Process, request, spawn, stat, wait_for_exit};

#[test]
fn data_survives_a_graceful_restart() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let server = Process::start(&["--data-dir", data_dir]);
    let mut stream = server.connect();
    assert_eq!(stat(&mut stream, "persist_enabled"), "1");
    let reply = request(
        &mut stream,
        "ms kept 5 T0\r\nhello\r\nms gone 1\r\nx\r\nmd gone q\r\nms touched 1 T100\r\ny\r\nmg touched T5000 q\r\nset n 0 0 2\r\n41\r\nincr n 1\r\n",
    );
    // `q` hides the `md` reply; a quiet `mg` hit still replies `HD`.
    assert_eq!(reply, "HD\r\nHD\r\nHD\r\nHD\r\nSTORED\r\n42\r\n");
    drop(stream);
    assert!(server.terminate().success());

    let server = Process::start(&["--data-dir", data_dir]);
    let mut stream = server.connect();
    let reply = request(&mut stream, "mg kept v\r\nmg gone v\r\nmg n v\r\n");
    assert_eq!(reply, "VA 5\r\nhello\r\nEN\r\nVA 2\r\n42\r\n");
    let ttl = request(&mut stream, "mg touched t\r\n");
    let seconds: u64 = ttl.trim().strip_prefix("HD t").unwrap().parse().unwrap();
    assert!(seconds > 4000, "{ttl}");
}

#[test]
fn a_server_without_a_data_dir_reports_persistence_off() {
    let server = Process::start(&[]);
    let mut stream = server.connect();
    assert_eq!(stat(&mut stream, "persist_enabled"), "0");
}

#[test]
fn a_data_dir_in_use_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let first = Process::start(&["--data-dir", data_dir]);
    assert_eq!(stat(&mut first.connect(), "persist_enabled"), "1");
    let mut second = spawn(
        std::process::Command::new(env!("CARGO_BIN_EXE_ferrited"))
            .args(["-p", "0", "--data-dir", data_dir])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped()),
    )
    .unwrap();
    let mut stderr = second.stderr.take().unwrap();
    let reading = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = stderr.read_to_string(&mut text);
        text
    });
    let status = wait_for_exit(&mut second, Duration::from_secs(10))
        .expect("the second server did not exit");
    let stderr = reading.join().unwrap();
    assert!(!status.success());
    assert!(stderr.contains("is in use by another ferrited"), "{stderr}");
}

#[test]
fn shutdown_closes_idle_connections_and_keeps_their_writes() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let server = Process::start(&["--data-dir", data_dir]);
    // Idle connections that each wrote something, open across the SIGTERM.
    let mut streams: Vec<_> = (0..5).map(|_| server.connect()).collect();
    for (index, stream) in streams.iter_mut().enumerate() {
        assert_eq!(
            request(stream, &format!("ms k{index} 1\r\nv\r\n")),
            "HD\r\n"
        );
    }
    let started = std::time::Instant::now();
    assert!(server.terminate().success());
    // Closed at once, not after the full drain timeout.
    assert!(started.elapsed() < std::time::Duration::from_secs(4));
    drop(streams);

    let server = Process::start(&["--data-dir", data_dir]);
    let mut stream = server.connect();
    for index in 0..5 {
        assert_eq!(
            request(&mut stream, &format!("mg k{index} v\r\n")),
            "VA 1\r\nv\r\n"
        );
    }
}

/// The value for key `index`, varied so a mixed-up or damaged record cannot pass.
fn expected_value(index: u64) -> String {
    let filler = (b'a' + (index % 26) as u8) as char;
    format!(
        "v{index}-{}",
        filler.to_string().repeat((index % 300) as usize)
    )
}

/// Writes keys from `first` until stopped, at `limit` or on failure; returns each ack's time.
fn write_until_stopped(
    server_port: u16,
    first: u64,
    limit: u64,
    stop: Arc<AtomicBool>,
) -> Vec<(u64, Instant)> {
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", server_port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.set_nodelay(true).unwrap();
    let mut acked = Vec::new();
    let mut reply = [0u8; 4];
    for index in first..first + limit {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let value = expected_value(index);
        let request = format!("ms k{index} {}\r\n{value}\r\n", value.len());
        if stream.write_all(request.as_bytes()).is_err() || stream.read_exact(&mut reply).is_err() {
            break;
        }
        assert_eq!(&reply, b"HD\r\n");
        acked.push((index, Instant::now()));
    }
    acked
}

/// Checks keys below `end`: `required` ones present, all values exact. Returns the count present.
fn verify(server: &Process, end: u64, required: &BTreeSet<u64>) -> u64 {
    const PER_REQUEST: u64 = 1000;
    let mut stream = server.connect();
    let mut present = 0;
    let mut start = 0;
    while start < end {
        let stop = (start + PER_REQUEST).min(end);
        let gets: String = (start..stop)
            .map(|index| format!("mg k{index} v\r\n"))
            .collect();
        let reply = request(&mut stream, &gets);
        let mut lines = reply.split("\r\n");
        for index in start..stop {
            let line = lines
                .next()
                .unwrap_or_else(|| panic!("no reply for k{index}"));
            if line == "EN" {
                assert!(
                    !required.contains(&index),
                    "acknowledged key k{index} was lost"
                );
                continue;
            }
            let value = expected_value(index);
            assert_eq!(line, format!("VA {}", value.len()), "k{index}");
            assert_eq!(lines.next(), Some(value.as_str()), "k{index}");
            present += 1;
        }
        assert_eq!(lines.next(), Some(""), "trailing reply after k{}", stop - 1);
        start = stop;
    }
    present
}

/// SIGKILLs the server mid-write, across segment rolls and compactions, and checks that writes
/// acked well before each kill survive intact.
///
/// SIGKILL keeps the page cache, so this test cannot detect a missing fsync.
#[test]
fn a_killed_server_recovers_its_acknowledged_writes_intact() {
    const FSYNC_MS: u64 = 100;
    // Margin for scheduling on a busy machine, on top of the fsync interval.
    const MARGIN: Duration = Duration::from_millis(400);
    // Low enough that nothing is evicted, since evictions are not persisted.
    const ROUND_LIMIT: u64 = 25_000;
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let args = [
        "--data-dir",
        data_dir,
        "--fsync-interval-ms",
        "100",
        "--segment-size",
        "128k",
    ];
    let mut required = BTreeSet::new();
    let mut next = 0;
    let mut snapshots = 0;
    // Write time before each kill. Some rounds hit `ROUND_LIMIT` first and are killed idle.
    let rounds = [300, 700, 450, 950, 600, 800];
    for (round, millis) in rounds.into_iter().enumerate() {
        let server = Process::start(&args);
        verify(&server, next, &required);
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (port, stop) = (server.port, stop.clone());
            std::thread::spawn(move || write_until_stopped(port, next, ROUND_LIMIT, stop))
        };
        std::thread::sleep(Duration::from_millis(millis));
        let mut stream = server.connect();
        assert_eq!(stat(&mut stream, "evictions"), "0");
        snapshots += stat(&mut stream, "persist_snapshots")
            .parse::<u64>()
            .unwrap();
        let killed_at = Instant::now();
        server.kill();
        stop.store(true, Ordering::Relaxed);
        let acked = writer.join().unwrap();
        for (index, at) in &acked {
            if killed_at.duration_since(*at) > Duration::from_millis(FSYNC_MS) + MARGIN {
                required.insert(*index);
            }
        }
        eprintln!(
            "round {round}: {} writes acknowledged, {} required so far",
            acked.len(),
            required.len()
        );
        next = acked.last().map_or(next, |(index, _)| index + 1);
    }
    // The kills must have landed across compactions too.
    assert!(snapshots > 0, "no compaction happened");
    assert!(
        required.len() > 1000,
        "too few writes to be a meaningful test: {}",
        required.len()
    );
    let server = Process::start(&args);
    let present = verify(&server, next, &required);
    eprintln!("{present} of {next} keys recovered");
}

#[test]
fn deletes_and_flushes_survive_a_kill() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let args = ["--data-dir", data_dir, "--fsync-interval-ms", "50"];
    let server = Process::start(&args);
    let mut stream = server.connect();
    request(
        &mut stream,
        "ms a 1\r\n1\r\nms b 1\r\n1\r\nflush_all\r\nms c 1\r\n1\r\nmd c q\r\nms d 1\r\n1\r\n",
    );
    std::thread::sleep(Duration::from_millis(500));
    server.kill();
    let server = Process::start(&args);
    let mut stream = server.connect();
    let reply = request(&mut stream, "mg a v\r\nmg b v\r\nmg c v\r\nmg d v\r\n");
    assert_eq!(reply, "EN\r\nEN\r\nEN\r\nVA 1\r\n1\r\n");
}

#[test]
fn a_second_signal_during_shutdown_exits_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let mut server = Process::start(&["--data-dir", data_dir]);
    // A client that never reads its replies keeps its connection from draining.
    let mut stream = server.connect();
    let value = "x".repeat(512 * 1024);
    let mut pipeline = format!("ms big {}\r\n{value}\r\n", value.len());
    pipeline.push_str(&"mg big v\r\n".repeat(200));
    stream.write_all(pipeline.as_bytes()).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    let signaled = Instant::now();
    server.signal("TERM");
    std::thread::sleep(Duration::from_millis(300));
    server.signal("INT");
    let status = server
        .wait(Duration::from_secs(10))
        .expect("the server did not exit");
    assert!(!status.success());
    // Well within the drain timeout.
    assert!(signaled.elapsed() < Duration::from_secs(3));
    drop(stream);
}

/// Pipelines writes from key 0 until the server closes, setting `started` at the first reply.
/// Returns the reply count; replies are in order, so exactly the keys below it are acked.
fn pipeline_until_closed(port: u16, started: Arc<AtomicBool>) -> u64 {
    let stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    let mut sender = stream.try_clone().unwrap();
    let sending = std::thread::spawn(move || {
        let mut next = 0u64;
        loop {
            let mut batch = String::new();
            for index in next..next + 100 {
                let value = expected_value(index);
                batch.push_str(&format!("ms k{index} {}\r\n{value}\r\n", value.len()));
            }
            next += 100;
            if sender.write_all(batch.as_bytes()).is_err() {
                return;
            }
        }
    });
    let mut receiver = stream;
    let mut acked = 0u64;
    let mut pending = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let read = match receiver.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        pending.extend_from_slice(&chunk[..read]);
        let whole = pending.len() / 4 * 4;
        for reply in pending[..whole].chunks(4) {
            assert_eq!(reply, b"HD\r\n");
        }
        acked += (whole / 4) as u64;
        pending.drain(..whole);
        if acked > 0 {
            started.store(true, Ordering::Relaxed);
        }
    }
    // The server is gone, so the sender's next write fails.
    sending.join().unwrap();
    acked
}

#[test]
fn shutdown_stops_a_client_that_keeps_sending_and_keeps_its_acknowledged_writes() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    // No eviction, even if shutdown waits out its full drain timeout.
    let args = ["--data-dir", data_dir, "-m", "2048"];
    let server = Process::start(&args);
    let started = Arc::new(AtomicBool::new(false));
    let client = {
        let (port, started) = (server.port, started.clone());
        std::thread::spawn(move || pipeline_until_closed(port, started))
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while !started.load(Ordering::Relaxed) {
        assert!(Instant::now() < deadline, "no write was acknowledged");
        std::thread::sleep(Duration::from_millis(5));
    }
    std::thread::sleep(Duration::from_millis(200));
    let signaled = Instant::now();
    assert!(server.terminate().success());
    // Stopped at its next batch, not after the full drain timeout.
    assert!(signaled.elapsed() < Duration::from_secs(4));
    let acked = client.join().unwrap();
    assert!(acked > 0);

    let server = Process::start(&args);
    let required: BTreeSet<u64> = (0..acked).collect();
    verify(&server, acked, &required);
}
