//! Replication between ferrited processes.

mod common;

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use common::process::{Process, free_port, refused, repl_status, request, stat};

/// How long a change may take to reach another node.
const PATIENCE: Duration = Duration::from_secs(20);

/// How one node of a cluster is started.
#[derive(Clone)]
struct Setup {
    id: u8,
    persist: bool,
    extra: Vec<String>,
    env: Vec<(String, String)>,
    /// A local port (such as a proxy's) to reach peers through. Only for a cluster of two.
    via: Option<u16>,
}

impl Setup {
    /// Node ID `index + 1`, no persistence, nothing extra.
    fn plain(index: usize) -> Self {
        Self {
            id: index as u8 + 1,
            persist: false,
            extra: Vec::new(),
            env: Vec::new(),
            via: None,
        }
    }

    fn persistent(index: usize) -> Self {
        Self {
            persist: true,
            ..Self::plain(index)
        }
    }
}

struct Node {
    setup: Setup,
    repl_port: u16,
    client_port: u16,
    data_dir: Option<tempfile::TempDir>,
    process: Option<Process>,
}

/// Nodes that list each other as peers. Dropping it kills every node still running.
struct Cluster {
    nodes: Vec<Node>,
}

impl Cluster {
    /// Starts `count` nodes, set up by `setup(index)`.
    fn start(count: usize, setup: impl Fn(usize) -> Setup) -> Self {
        for _ in 0..5 {
            let mut ports = HashSet::new();
            while ports.len() < 2 * count {
                ports.insert(free_port());
            }
            let ports: Vec<u16> = ports.into_iter().collect();
            let nodes = (0..count)
                .map(|index| {
                    let setup = setup(index);
                    Node {
                        data_dir: setup.persist.then(|| tempfile::tempdir().unwrap()),
                        setup,
                        repl_port: ports[2 * index],
                        client_port: ports[2 * index + 1],
                        process: None,
                    }
                })
                .collect();
            let mut cluster = Cluster { nodes };
            // A node serves only once linked with a peer, so start all before waiting.
            for index in 0..count {
                cluster.spawn(index);
            }
            if (0..count).all(|index| cluster.wait_bound(index))
                && (0..count).all(|index| cluster.wait_listening(index))
            {
                return cluster;
            }
            // A port was taken: retry with new ports.
        }
        panic!("could not start the cluster: its ports kept being taken");
    }

    /// Starts node `index` without waiting for it.
    fn spawn(&mut self, index: usize) {
        let args = self.args(index);
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let env = &self.nodes[index].setup.env;
        let vars: Vec<(&str, &str)> = env
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
            .collect();
        let process = Process::spawn(&args, &vars);
        self.nodes[index].process = Some(process);
    }

    /// Waits until node `index` has bound its ports. Returns false if one was taken.
    fn wait_bound(&mut self, index: usize) -> bool {
        let process = self.nodes[index].process.as_mut().unwrap();
        wait_bound(process, &format!("node {}", index + 1))
    }

    /// Waits until node `index` serves clients. Returns false if a port was taken.
    fn wait_listening(&mut self, index: usize) -> bool {
        let process = self.nodes[index].process.as_mut().unwrap();
        match process.wait_listening(PATIENCE) {
            Ok(true) => true,
            Ok(false) => panic!("node {} did not start serving clients", index + 1),
            Err(error) if error.contains("Address already in use") => false,
            Err(error) => panic!("node {} did not start: {error}", index + 1),
        }
    }

    /// Starts node `index` and waits until it serves. Returns false if a port was taken.
    fn try_launch(&mut self, index: usize) -> bool {
        self.spawn(index);
        self.wait_bound(index) && self.wait_listening(index)
    }

    /// Like `restart`, but waits only until the node binds its ports, not until it serves.
    fn respawn(&mut self, index: usize) {
        let deadline = Instant::now() + PATIENCE;
        loop {
            self.spawn(index);
            if self.wait_bound(index) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "node {}'s ports stayed taken",
                index + 1
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn args(&self, index: usize) -> Vec<String> {
        let node = &self.nodes[index];
        let mut args = vec![
            "-p".to_string(),
            node.client_port.to_string(),
            "--node-id".to_string(),
            node.setup.id.to_string(),
            "--repl-port".to_string(),
            node.repl_port.to_string(),
        ];
        for (other, peer) in self.nodes.iter().enumerate() {
            if other != index {
                args.push("--peer".into());
                let port = node.setup.via.unwrap_or(peer.repl_port);
                args.push(format!("127.0.0.1:{port}"));
            }
        }
        if let Some(dir) = &node.data_dir {
            args.push("--data-dir".into());
            args.push(dir.path().to_str().unwrap().into());
        }
        args.extend(node.setup.extra.iter().cloned());
        args
    }

    /// Restarts a stopped node on the same ports and data directory.
    fn restart(&mut self, index: usize) {
        let deadline = Instant::now() + PATIENCE;
        while !self.try_launch(index) {
            assert!(
                Instant::now() < deadline,
                "node {}'s ports stayed taken",
                index + 1
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Stops node `index` with SIGTERM and checks for a clean exit.
    fn stop(&mut self, index: usize) {
        let process = self.nodes[index].process.take().unwrap();
        assert!(process.terminate().success());
    }

    /// Kills node `index`, as a crash would.
    fn kill(&mut self, index: usize) {
        self.nodes[index].process.take().unwrap().kill();
    }

    fn node(&self, index: usize) -> &Process {
        self.nodes[index].process.as_ref().unwrap()
    }

    fn client(&self, index: usize) -> TcpStream {
        self.node(index).connect()
    }
}

/// Waits until `process` binds its client port, or its replication port if not yet serving
/// (bound after the client address check). Returns false if a port was taken.
fn wait_bound(process: &mut Process, name: &str) -> bool {
    let deadline = Instant::now() + PATIENCE;
    loop {
        match process.wait_listening(Duration::from_millis(20)) {
            Ok(true) => return true,
            Ok(false) if process.logged("ferrited replication listening on") => return true,
            Ok(false) => assert!(Instant::now() < deadline, "{name} did not bind its ports"),
            Err(error) if error.contains("Address already in use") => return false,
            Err(error) => panic!("{name} did not start: {error}"),
        }
    }
}

fn set(stream: &mut TcpStream, key: &str, value: &str) {
    let reply = request(stream, &format!("ms {key} {}\r\n{value}\r\n", value.len()));
    assert_eq!(reply, "HD\r\n");
}

/// Sets `key` and returns its CAS token (the item's version).
fn set_cas(stream: &mut TcpStream, key: &str, value: &str) -> u64 {
    let reply = request(
        stream,
        &format!("ms {key} {} c\r\n{value}\r\n", value.len()),
    );
    reply
        .trim_end()
        .strip_prefix("HD c")
        .unwrap_or_else(|| panic!("{reply}"))
        .parse()
        .unwrap()
}

fn get(stream: &mut TcpStream, key: &str) -> Option<String> {
    let reply = request(stream, &format!("mg {key} v\r\n"));
    if reply == "EN\r\n" {
        return None;
    }
    let (_, value) = reply
        .split_once("\r\n")
        .unwrap_or_else(|| panic!("{reply}"));
    Some(value.strip_suffix("\r\n").unwrap().to_string())
}

fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Waits until node `index` holds `expected` for `key`.
fn wait_for(cluster: &Cluster, index: usize, key: &str, expected: Option<&str>) {
    let mut stream = cluster.client(index);
    eventually(
        &format!("node {} has {key} = {expected:?}", index + 1),
        || get(&mut stream, key).as_deref() == expected,
    );
}

/// Waits until stat `name` on node `index` reads `value`.
fn wait_for_stat(cluster: &Cluster, index: usize, name: &str, value: &str) {
    let mut stream = cluster.client(index);
    eventually(&format!("node {}'s {name} is {value}", index + 1), || {
        stat(&mut stream, name) == value
    });
}

/// The values of the `stats repl` lines whose names end with `suffix`.
fn repl_values(stream: &mut TcpStream, suffix: &str) -> Vec<String> {
    request(stream, "stats repl\r\n")
        .lines()
        .filter_map(|line| line.strip_prefix("STAT "))
        .filter_map(|line| line.split_once(' '))
        .filter(|(name, _)| name.ends_with(suffix))
        .map(|(_, value)| value.to_string())
        .collect()
}

/// Waits until every peer of node `index` has acknowledged its whole log. After this,
/// `repl_full_syncs_sent` is final: peers ack only after a full sync is counted.
fn wait_until_acknowledged(cluster: &Cluster, index: usize) {
    let mut stream = cluster.client(index);
    eventually(
        &format!("node {}'s peers acknowledged its log", index + 1),
        || {
            let out = repl_values(&mut stream, ":out");
            let lags = repl_values(&mut stream, ":lag");
            !out.is_empty()
                && out.iter().all(|state| state == "streaming")
                && lags.iter().all(|lag| lag == "0")
        },
    );
}

/// Waits until every running node's links stream both ways and every log is acknowledged.
fn wait_until_linked(cluster: &Cluster) {
    for index in 0..cluster.nodes.len() {
        if cluster.nodes[index].process.is_none() {
            continue;
        }
        wait_until_acknowledged(cluster, index);
        let mut stream = cluster.client(index);
        eventually(&format!("node {}'s links stream in", index + 1), || {
            let incoming = repl_values(&mut stream, ":in");
            !incoming.is_empty() && incoming.iter().all(|state| state == "streaming")
        });
    }
}

#[test]
fn a_server_without_peers_reports_replication_off() {
    let server = Process::start(&[]);
    let mut stream = server.connect();
    assert_eq!(stat(&mut stream, "repl_peers"), "0");
    assert_eq!(request(&mut stream, "stats repl\r\n"), "END\r\n");
}

#[test]
fn two_nodes_replicate_writes_deletes_and_touches_both_ways() {
    let cluster = Cluster::start(2, Setup::plain);
    wait_for_stat(&cluster, 0, "repl_peers", "2");
    set(&mut cluster.client(0), "from-a", "1");
    set(&mut cluster.client(1), "from-b", "2");
    wait_for(&cluster, 1, "from-a", Some("1"));
    wait_for(&cluster, 0, "from-b", Some("2"));
    assert_eq!(request(&mut cluster.client(0), "md from-b\r\n"), "HD\r\n");
    wait_for(&cluster, 1, "from-b", None);
    // A touch on node 2 gives node 1's item an expiry.
    assert_eq!(
        request(&mut cluster.client(1), "mg from-a T1000\r\n"),
        "HD\r\n"
    );
    let mut a = cluster.client(0);
    eventually("the touch reached node 1", || {
        request(&mut a, "mg from-a t\r\n").trim_end() != "HD t-1"
    });
    let lines = request(&mut a, "stats repl\r\n");
    assert!(lines.contains(":out streaming"), "{lines}");
    assert!(lines.contains(":in streaming"), "{lines}");
}

#[test]
fn conflicting_writes_converge_on_the_higher_version() {
    let cluster = Cluster::start(2, Setup::plain);
    wait_until_linked(&cluster);
    for round in 0..10 {
        let key = format!("k{round}");
        let barrier = Arc::new(Barrier::new(2));
        let writers: Vec<_> = (0..2)
            .map(|index| {
                let (mut stream, barrier, key) =
                    (cluster.client(index), barrier.clone(), key.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    set_cas(&mut stream, &key, &format!("from-{index}"))
                })
            })
            .collect();
        let cas: Vec<u64> = writers
            .into_iter()
            .map(|writer| writer.join().unwrap())
            .collect();
        let winner = if cas[0] > cas[1] { "from-0" } else { "from-1" };
        wait_for(&cluster, 0, &key, Some(winner));
        wait_for(&cluster, 1, &key, Some(winner));
    }
}

#[test]
fn a_delete_stops_an_older_write_that_arrives_after_it() {
    let secrets = tempfile::tempdir().unwrap();
    let right = secrets.path().join("right");
    let wrong = secrets.path().join("wrong");
    std::fs::write(&right, "right\n").unwrap();
    std::fs::write(&wrong, "wrong\n").unwrap();
    let secret = |path: &std::path::Path| {
        vec![
            "--repl-secret-file".to_string(),
            path.to_str().unwrap().to_string(),
        ]
    };
    // Node 2 has the wrong secret until restarted. It persists, so its delete survives the
    // restart as a tombstone.
    let mut cluster = Cluster::start(2, |index| {
        let mut setup = if index == 0 {
            Setup::plain(index)
        } else {
            Setup::persistent(index)
        };
        setup.extra = secret(if index == 0 { &right } else { &wrong });
        // Serve despite the refused links.
        setup.extra.push("--force-standalone".into());
        setup
    });
    let mut b = cluster.client(1);
    set(&mut b, "k", "first");
    std::thread::sleep(Duration::from_millis(5));
    // Older than the delete below, but reaches node 2 after it.
    set(&mut cluster.client(0), "k", "older");
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(request(&mut b, "md k\r\n"), "HD\r\n");
    drop(b);
    assert!(cluster.node(1).wait_for_log("wrong secret", PATIENCE));
    cluster.stop(1);
    cluster.nodes[1].setup.extra = secret(&right);
    cluster.restart(1);
    wait_for_stat(&cluster, 0, "repl_peers", "2");
    // Node 2's tombstone stops node 1's older write; node 2's delete reaches node 1.
    wait_for(&cluster, 0, "k", None);
    let mut b = cluster.client(1);
    eventually("node 2 counted the stale write", || {
        stat(&mut b, "repl_records_stale") != "0"
    });
    assert_eq!(get(&mut b, "k"), None);
}

#[test]
fn flush_all_reaches_every_node_at_once_or_when_its_delay_ends() {
    let cluster = Cluster::start(2, Setup::plain);
    wait_for_stat(&cluster, 0, "repl_peers", "2");
    set(&mut cluster.client(0), "x", "1");
    set(&mut cluster.client(1), "y", "2");
    wait_for(&cluster, 0, "y", Some("2"));
    wait_for(&cluster, 1, "x", Some("1"));
    assert_eq!(request(&mut cluster.client(0), "flush_all\r\n"), "OK\r\n");
    wait_for(&cluster, 1, "x", None);
    wait_for(&cluster, 1, "y", None);
    // A write after the flush is kept everywhere.
    set(&mut cluster.client(1), "z", "3");
    wait_for(&cluster, 0, "z", Some("3"));
    // A delayed flush replicates when it takes effect.
    assert_eq!(request(&mut cluster.client(0), "flush_all 2\r\n"), "OK\r\n");
    wait_for(&cluster, 1, "z", None);
}

#[test]
fn three_nodes_converge_and_none_forwards_another_nodes_writes() {
    let cluster = Cluster::start(3, Setup::plain);
    // All links up and their initial full syncs done, so none can carry the writes below.
    for index in 0..3 {
        wait_for_stat(&cluster, index, "repl_peers", "4");
    }
    wait_until_linked(&cluster);
    for index in 0..3 {
        set(
            &mut cluster.client(index),
            &format!("from-{index}"),
            &index.to_string(),
        );
    }
    for reader in 0..3 {
        for writer in 0..3 {
            let expected = writer.to_string();
            wait_for(&cluster, reader, &format!("from-{writer}"), Some(&expected));
        }
    }
    // Once every log is acknowledged, a forwarded write would have arrived twice, the second
    // copy counted as stale.
    wait_until_linked(&cluster);
    for index in 0..3 {
        assert_eq!(stat(&mut cluster.client(index), "repl_records_stale"), "0");
    }
}

#[test]
fn a_peer_that_was_down_resumes_from_the_backlog() {
    // Node 2 persists, so it keeps its position on node 1's log across a restart.
    let mut cluster = Cluster::start(2, |index| {
        if index == 1 {
            Setup::persistent(index)
        } else {
            Setup::plain(index)
        }
    });
    set(&mut cluster.client(0), "before", "v");
    wait_for(&cluster, 1, "before", Some("v"));
    wait_until_acknowledged(&cluster, 0);
    // The full sync when the link first came up.
    assert_eq!(stat(&mut cluster.client(0), "repl_full_syncs_sent"), "1");
    cluster.stop(1);
    let mut a = cluster.client(0);
    for index in 0..100 {
        set(&mut a, &format!("k{index}"), "v");
    }
    cluster.restart(1);
    wait_for(&cluster, 1, "k99", Some("v"));
    let mut b = cluster.client(1);
    for index in 0..100 {
        assert_eq!(get(&mut b, &format!("k{index}")).as_deref(), Some("v"));
    }
    wait_until_acknowledged(&cluster, 0);
    assert_eq!(
        stat(&mut a, "repl_full_syncs_sent"),
        "1",
        "node 2 was not resumed"
    );
}

#[test]
fn a_restarted_sender_resumes_its_peer_from_its_log() {
    let mut cluster = Cluster::start(2, Setup::persistent);
    set(&mut cluster.client(0), "before", "v");
    wait_for(&cluster, 1, "before", Some("v"));
    wait_until_acknowledged(&cluster, 0);
    // A new run, with an empty backlog.
    cluster.stop(0);
    cluster.restart(0);
    set(&mut cluster.client(0), "after", "v");
    wait_for(&cluster, 1, "after", Some("v"));
    wait_until_acknowledged(&cluster, 0);
    // Node 1's log still covers node 2's position from the previous run.
    assert_eq!(stat(&mut cluster.client(0), "repl_full_syncs_sent"), "0");
}

#[test]
fn a_peer_too_far_behind_gets_a_full_sync() {
    // Node 1 keeps a 16 KiB backlog and no disk log, so it cannot resume node 2.
    let mut cluster = Cluster::start(2, |index| {
        if index == 0 {
            Setup {
                extra: ["-I", "1k", "--repl-backlog", "16k"]
                    .map(String::from)
                    .to_vec(),
                ..Setup::plain(index)
            }
        } else {
            Setup::persistent(index)
        }
    });
    wait_for_stat(&cluster, 0, "repl_peers", "2");
    wait_until_acknowledged(&cluster, 0);
    cluster.stop(1);
    let mut a = cluster.client(0);
    let value = "v".repeat(100);
    for index in 0..500 {
        set(&mut a, &format!("k{index}"), &value);
    }
    cluster.restart(1);
    wait_for(&cluster, 1, "k499", Some(&value));
    let mut b = cluster.client(1);
    for index in 0..500 {
        assert_eq!(get(&mut b, &format!("k{index}")), Some(value.clone()));
    }
    wait_until_acknowledged(&cluster, 0);
    assert_eq!(stat(&mut a, "repl_full_syncs_sent"), "2");
}

#[test]
fn a_persistent_node_that_crashed_gets_a_full_sync_from_its_peers() {
    let mut cluster = Cluster::start(2, Setup::persistent);
    set(&mut cluster.client(0), "from-a", "v");
    set(&mut cluster.client(1), "from-b", "v");
    wait_for(&cluster, 0, "from-b", Some("v"));
    wait_for(&cluster, 1, "from-a", Some("v"));
    wait_until_acknowledged(&cluster, 0);
    // A crash may lose node 2's last writes, which only a full sync sends back, so a crashed
    // node discards its positions.
    cluster.kill(1);
    cluster.restart(1);
    // Node 2 recovers both items from its log, so wait for the full sync itself.
    wait_for_stat(&cluster, 0, "repl_full_syncs_sent", "2");
    wait_for(&cluster, 1, "from-a", Some("v"));
    wait_for(&cluster, 1, "from-b", Some("v"));
    wait_until_acknowledged(&cluster, 0);
    // A clean stop keeps the positions: no full sync this time.
    cluster.stop(1);
    cluster.restart(1);
    set(&mut cluster.client(0), "after", "v");
    wait_for(&cluster, 1, "after", Some("v"));
    wait_until_acknowledged(&cluster, 0);
    assert_eq!(stat(&mut cluster.client(0), "repl_full_syncs_sent"), "2");
}

#[test]
fn a_crashed_node_stopped_before_its_full_sync_still_gets_one_at_the_next_start() {
    let mut cluster = Cluster::start(2, |index| Setup {
        extra: vec!["--fsync-interval-ms".into(), "50".into()],
        ..Setup::persistent(index)
    });
    set(&mut cluster.client(0), "from-a", "v");
    wait_for(&cluster, 1, "from-a", Some("v"));
    wait_until_acknowledged(&cluster, 0);
    // Past the fsync interval, so node 2's position is on disk.
    std::thread::sleep(Duration::from_millis(500));
    // After the crash, node 2's next start discards that position.
    cluster.kill(1);
    cluster.stop(0);
    // Node 2 starts with node 1 down and is stopped cleanly before any full sync.
    cluster.respawn(1);
    assert!(
        cluster
            .node(1)
            .wait_for_log("not serving clients yet", PATIENCE)
    );
    cluster.stop(1);
    // The clean stop must not restore the discarded position. Node 1 was last standing, so it
    // serves alone first.
    cluster.restart(0);
    cluster.restart(1);
    wait_for_stat(&cluster, 1, "repl_full_syncs_received", "1");
    wait_for(&cluster, 1, "from-a", Some("v"));
}

#[test]
fn a_node_without_persistence_is_refilled_by_a_full_sync() {
    let mut cluster = Cluster::start(2, Setup::plain);
    let mut a = cluster.client(0);
    for index in 0..50 {
        set(&mut a, &format!("k{index}"), "v");
    }
    wait_for(&cluster, 1, "k49", Some("v"));
    cluster.kill(1);
    cluster.restart(1);
    wait_for(&cluster, 1, "k49", Some("v"));
    let mut b = cluster.client(1);
    for index in 0..50 {
        assert_eq!(get(&mut b, &format!("k{index}")).as_deref(), Some("v"));
    }
    wait_until_acknowledged(&cluster, 0);
    assert_eq!(stat(&mut a, "repl_full_syncs_sent"), "2");
}

#[test]
fn a_node_with_the_same_id_is_refused() {
    let cluster = Cluster::start(2, |index| Setup {
        id: 1,
        extra: vec!["--force-standalone".into()],
        ..Setup::plain(index)
    });
    set(&mut cluster.client(0), "k", "v");
    assert!(cluster.node(1).wait_for_log("this node's own ID", PATIENCE));
    // Proving nothing crosses takes a quiet period.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(get(&mut cluster.client(1), "k"), None);
    assert_eq!(stat(&mut cluster.client(0), "repl_peers"), "0");
}

#[test]
fn a_node_with_the_wrong_secret_is_refused() {
    let secrets = tempfile::tempdir().unwrap();
    let paths: Vec<_> = ["one", "two"]
        .iter()
        .map(|name| {
            let path = secrets.path().join(name);
            std::fs::write(&path, name).unwrap();
            path
        })
        .collect();
    let cluster = Cluster::start(2, |index| Setup {
        extra: vec![
            "--force-standalone".into(),
            "--repl-secret-file".into(),
            paths[index].to_str().unwrap().into(),
        ],
        ..Setup::plain(index)
    });
    set(&mut cluster.client(0), "k", "v");
    // Each node refuses the other's link, and each sender hears why.
    for index in 0..2 {
        let node = cluster.node(index);
        assert!(node.wait_for_log(": refused the link: wrong secret", PATIENCE));
        assert!(node.wait_for_log("the peer refused the link: wrong secret", PATIENCE));
    }
    // Proving nothing crosses takes a quiet period.
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(get(&mut cluster.client(1), "k"), None);
    assert_eq!(stat(&mut cluster.client(0), "repl_peers"), "0");
}

#[test]
fn a_node_whose_replication_port_is_taken_leaves_its_data_dir_as_a_clean_stop_left_it() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let server = Process::start(&["--data-dir", data_dir]);
    assert!(server.terminate().success());
    assert!(dir.path().join("CLEAN").exists());
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = taken.local_addr().unwrap().port().to_string();
    let args = [
        "--node-id",
        "1",
        "--repl-port",
        &port,
        "--peer",
        "127.0.0.1:1",
        "--data-dir",
        data_dir,
    ];
    let error = match Process::try_start(&args) {
        Ok(_) => panic!("the node started on a taken replication port"),
        Err(error) => error,
    };
    assert!(error.contains("cannot listen for replication"), "{error}");
    // The directory still reads as cleanly stopped.
    assert!(dir.path().join("CLEAN").exists());
}

#[test]
fn a_single_node_may_keep_node_id_0() {
    let server = Process::start(&["--node-id", "0"]);
    let mut stream = server.connect();
    set(&mut stream, "k", "v");
    assert_eq!(get(&mut stream, "k").as_deref(), Some("v"));
}

// Startup readiness.

/// Starts a node whose only peer never answers, on fresh ports (retrying if one is taken), and
/// waits until it binds its replication port. Returns the node and its client port.
fn start_alone(data_dir: &str, extra: &[&str]) -> (Process, u16) {
    start_alone_with(data_dir, extra, &[])
}

/// Like `start_alone`, with environment variables `vars`.
fn start_alone_with(data_dir: &str, extra: &[&str], vars: &[(&str, &str)]) -> (Process, u16) {
    for _ in 0..5 {
        let (client, repl) = loop {
            let (a, b) = (free_port(), free_port());
            if a != b {
                break (a, b);
            }
        };
        let (client_text, repl_text) = (client.to_string(), repl.to_string());
        let mut args = vec![
            "-p",
            &client_text,
            "--node-id",
            "1",
            "--repl-port",
            &repl_text,
            "--peer",
            "127.0.0.1:1",
            "--data-dir",
            data_dir,
        ];
        args.extend_from_slice(extra);
        let mut node = Process::spawn(&args, vars);
        if wait_bound(&mut node, "the node") {
            return (node, client);
        }
    }
    panic!("could not start the node: its ports kept being taken");
}

#[test]
fn a_node_whose_client_port_is_taken_stops_before_it_loads_its_data() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let server = Process::start(&["--data-dir", data_dir]);
    assert!(server.terminate().success());
    let before = listing(dir.path());
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = taken.local_addr().unwrap().port().to_string();
    let error = match Process::try_start(&["-p", &port, "--data-dir", data_dir]) {
        Ok(_) => panic!("the node started on a taken client port"),
        Err(error) => error,
    };
    assert!(error.contains("cannot listen for clients on"), "{error}");
    // Recovery never ran, or it would have touched the segments and the clean-stop marker.
    assert_eq!(listing(dir.path()), before);
}

/// Every file under `dir` with its length and modification time, sorted by path.
fn listing(dir: &std::path::Path) -> Vec<(std::path::PathBuf, u64, std::time::SystemTime)> {
    let mut files = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                pending.push(entry.path());
            } else {
                files.push((entry.path(), metadata.len(), metadata.modified().unwrap()));
            }
        }
    }
    files.sort();
    files
}

#[test]
fn a_node_whose_client_port_is_taken_while_it_waits_stops_cleanly_with_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    // Not ready for at least the 2-second sync timeout after the client address check.
    let (mut node, client) = start_alone(data_dir, &["--sync-timeout", "2"]);
    assert!(refused(client));
    // Taken after the startup check, before the node binds it (or by another process).
    let _taken = std::net::TcpListener::bind(("127.0.0.1", client));
    let error = node.wait_listening(PATIENCE).unwrap_err();
    assert!(error.contains("not serving clients yet"), "{error}");
    assert!(error.contains("cannot listen for clients on"), "{error}");
    assert!(error.contains("exit status: 1"), "{error}");
    assert!(dir.path().join("CLEAN").exists());
}

#[test]
fn a_node_waiting_for_its_peers_stops_cleanly_on_sigterm() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let (node, _) = start_alone(data_dir, &["--sync-timeout", "0"]);
    assert!(node.wait_for_log(
        "no peer is reachable; waiting for one indefinitely",
        PATIENCE
    ));
    assert!(node.terminate().success());
    assert!(dir.path().join("CLEAN").exists());
}

/// A data directory a clean stop left, holding `k` = `v`.
fn stopped_data_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let server = Process::start(&["--data-dir", dir.path().to_str().unwrap()]);
    set(&mut server.connect(), "k", "v");
    assert!(server.terminate().success());
    dir
}

#[test]
fn a_node_stopped_while_it_loads_its_data_stops_cleanly_once_loaded_without_serving() {
    for peers in [false, true] {
        let dir = stopped_data_dir();
        let data_dir = dir.path().to_str().unwrap();
        let vars = [("FERRITED_TEST_RECOVERY_DELAY_MS", "3000")];
        let mut node = if peers {
            start_alone_with(data_dir, &[], &vars).0
        } else {
            Process::spawn(&["--data-dir", data_dir], &vars)
        };
        assert!(node.wait_for_log("pausing 3000 ms before recovery", PATIENCE));
        node.signal("TERM");
        // Exits once recovery ends, without listening for clients.
        let error = node.wait_listening(PATIENCE).unwrap_err();
        assert!(error.contains("exit status: 0"), "{error}");
        assert!(
            error.contains("stopping once local data has loaded"),
            "{error}"
        );
        assert!(!error.contains("ferrited listening on"), "{error}");
        assert!(dir.path().join("CLEAN").exists(), "peers: {peers}");
        let server = Process::start(&["--data-dir", data_dir]);
        assert_eq!(get(&mut server.connect(), "k").as_deref(), Some("v"));
    }
}

#[test]
fn a_second_stop_signal_while_a_node_loads_its_data_exits_at_once() {
    let dir = stopped_data_dir();
    let data_dir = dir.path().to_str().unwrap();
    let vars = [("FERRITED_TEST_RECOVERY_DELAY_MS", "60000")];
    let mut node = Process::spawn(&["--data-dir", data_dir], &vars);
    assert!(node.wait_for_log("pausing 60000 ms before recovery", PATIENCE));
    let started = Instant::now();
    node.signal("TERM");
    assert!(node.wait_for_log("stopping once local data has loaded", PATIENCE));
    node.signal("INT");
    let error = node.wait_listening(PATIENCE).unwrap_err();
    assert!(error.contains("exit status: 1"), "{error}");
    assert!(error.contains("stop interrupted local recovery"), "{error}");
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn an_unreadable_recovery_delay_is_ignored_and_logged() {
    let vars = [("FERRITED_TEST_RECOVERY_DELAY_MS", "soon")];
    let dir = tempfile::tempdir().unwrap();
    let mut node = Process::spawn(&["--data-dir", dir.path().to_str().unwrap()], &vars);
    assert_eq!(node.wait_listening(PATIENCE), Ok(true));
    assert!(node.logged("ignoring FERRITED_TEST_RECOVERY_DELAY_MS=\"soon\""));
}

#[test]
fn a_restarted_node_refuses_clients_until_it_has_caught_up() {
    let mut cluster = Cluster::start(2, Setup::persistent);
    wait_until_linked(&cluster);
    cluster.stop(1);
    let mut a = cluster.client(0);
    let value = "v".repeat(1000);
    for index in 0..2000 {
        set(&mut a, &format!("k{index}"), &value);
    }
    // Node 2 was not last standing, so it waits until caught up with node 1.
    cluster.respawn(1);
    let port = cluster.nodes[1].client_port;
    let repl = cluster.nodes[1].repl_port;
    let deadline = Instant::now() + PATIENCE;
    let mut states = HashSet::new();
    let mut b = loop {
        let connected = TcpStream::connect(("127.0.0.1", port));
        // Read after connecting: readiness is permanent, so waiting now means waiting then.
        let status = repl_status(repl);
        let waiting = !has(&status, "state", "ready");
        if waiting {
            assert!(
                connected.is_err(),
                "node 2 served while waiting: {status:?}"
            );
            let reason = status.iter().find(|(name, _)| name == "reason");
            states.insert(reason.map_or("recovering".into(), |(_, value)| value.clone()));
        }
        if let Ok(stream) = connected {
            break stream;
        }
        assert!(Instant::now() < deadline, "node 2 never served");
        std::thread::sleep(Duration::from_millis(5));
    };
    // How long node 2 waited, and why, depends on timing.
    eprintln!("node 2 refused clients while: {states:?}");
    b.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    // The first accepted connection sees every missed write, newest checked first.
    for index in (0..2000).rev() {
        let key = format!("k{index}");
        assert_eq!(get(&mut b, &key).as_deref(), Some(value.as_str()), "{key}");
    }
    assert!(cluster.node(1).logged("not serving clients yet"));
    assert!(cluster.node(1).logged("caught up with node 1"));
    // A node that serves never closes its port again, even alone.
    cluster.stop(0);
    std::thread::sleep(Duration::from_millis(500));
    assert!(TcpStream::connect(("127.0.0.1", port)).is_ok());
    assert_eq!(get(&mut b, "k0").as_deref(), Some(value.as_str()));
}

#[test]
fn the_last_node_standing_serves_after_a_short_grace_period() {
    let mut cluster = Cluster::start(2, Setup::persistent);
    wait_until_linked(&cluster);
    cluster.stop(1);
    // Node 1 must see node 2 go down to count as last standing.
    let mut a = cluster.client(0);
    eventually("node 1's links to node 2 are down", || {
        repl_values(&mut a, ":out") == ["down"] && repl_values(&mut a, ":in") == ["down"]
    });
    set(&mut a, "alone", "v");
    drop(a);
    cluster.stop(0);
    // Node 1 was last standing, so no peer can hold anything it lacks: it serves after the
    // 2-second grace period, not the 60-second sync timeout.
    let started = Instant::now();
    cluster.restart(0);
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "{:?}",
        started.elapsed()
    );
    assert!(
        cluster
            .node(0)
            .logged("this node was the last one standing")
    );
    assert_eq!(get(&mut cluster.client(0), "alone").as_deref(), Some("v"));
    let status = repl_status(cluster.nodes[0].repl_port);
    assert!(
        status.contains(&("ready_by".to_string(), "last_standing".to_string())),
        "{status:?}"
    );
    // Node 2 was not last standing, so it waits for node 1 and catches up.
    cluster.restart(1);
    assert!(cluster.node(1).logged("caught up with node 1"));
    assert_eq!(get(&mut cluster.client(1), "alone").as_deref(), Some("v"));
}

#[test]
fn two_nodes_that_crashed_together_both_become_ready_with_each_others_writes() {
    let mut cluster = Cluster::start(2, |index| Setup {
        extra: vec!["--fsync-interval-ms".into(), "50".into()],
        ..Setup::persistent(index)
    });
    set(&mut cluster.client(0), "from-a", "1");
    set(&mut cluster.client(1), "from-b", "2");
    wait_for(&cluster, 0, "from-b", Some("2"));
    wait_for(&cluster, 1, "from-a", Some("1"));
    // Well past the fsync interval, so both writes are on both disks.
    std::thread::sleep(Duration::from_millis(500));
    // As in a power loss. Node 2 may count as last standing if it saw node 1 go down first.
    cluster.kill(0);
    cluster.kill(1);
    // Node 1 first, so it answers within node 2's 2-second grace period.
    cluster.respawn(0);
    cluster.respawn(1);
    for index in 0..2 {
        assert!(cluster.wait_listening(index));
    }
    for index in 0..2 {
        let node = cluster.node(index);
        assert!(node.logged("caught up with node"), "node {}", index + 1);
        let mut stream = cluster.client(index);
        assert_eq!(get(&mut stream, "from-a").as_deref(), Some("1"));
        assert_eq!(get(&mut stream, "from-b").as_deref(), Some("2"));
    }
}

/// Whether the status lines include `name` with `value`.
fn has(status: &[(String, String)], name: &str, value: &str) -> bool {
    status.contains(&(name.to_string(), value.to_string()))
}

#[test]
fn a_lone_node_that_was_not_last_standing_serves_only_after_the_sync_timeout() {
    let mut cluster = Cluster::start(2, Setup::persistent);
    wait_until_linked(&cluster);
    // Node 1 crashes while linked, so it was not last standing; node 2 stays down.
    cluster.kill(0);
    cluster.kill(1);
    cluster.nodes[0].setup.extra = vec!["--sync-timeout".into(), "3".into()];
    let started = Instant::now();
    cluster.respawn(0);
    let (client, repl) = (cluster.nodes[0].client_port, cluster.nodes[0].repl_port);
    eventually("node 1 reports that no peer is reachable", || {
        has(&repl_status(repl), "reason", "no_peer_reachable")
    });
    assert!(refused(client));
    assert!(cluster.wait_listening(0));
    assert!(
        started.elapsed() >= Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert!(
        cluster
            .node(0)
            .logged("warning: no peer was reachable for 3s")
    );
    assert_eq!(stat(&mut cluster.client(0), "repl_sync_timeouts"), "1");
    assert!(has(&repl_status(repl), "ready_by", "sync_timeout"));
}

#[test]
fn the_sync_timeout_does_not_run_while_a_peer_is_recovering() {
    let mut cluster = Cluster::start(2, Setup::persistent);
    wait_until_linked(&cluster);
    cluster.kill(0);
    cluster.kill(1);
    cluster.nodes[0].setup.extra = vec!["--sync-timeout".into(), "3".into()];
    // Node 2 stays in recovery for 15 seconds.
    cluster.nodes[1].setup.env = vec![("FERRITED_TEST_RECOVERY_DELAY_MS".into(), "15000".into())];
    cluster.respawn(1);
    let started = Instant::now();
    cluster.respawn(0);
    let (repl_a, repl_b) = (cluster.nodes[0].repl_port, cluster.nodes[1].repl_port);
    assert!(has(&repl_status(repl_b), "state", "recovering"));
    eventually("node 1 sees node 2 recovering", || {
        has(&repl_status(repl_a), "reason", "peer_recovering")
    });
    // Longer than node 1's sync timeout, while node 2 still recovers.
    std::thread::sleep(Duration::from_secs(4));
    assert!(
        started.elapsed() < Duration::from_secs(13),
        "too slow to tell: node 2's recovery may be over"
    );
    assert!(refused(cluster.nodes[0].client_port));
    assert!(has(&repl_status(repl_a), "unreachable_secs", "0"));
    // Once node 2 recovers, both catch up and serve.
    assert!(cluster.wait_listening(1));
    assert!(cluster.wait_listening(0));
    assert!(cluster.node(0).logged("caught up with node 2"));
    assert!(!cluster.node(0).logged("warning: no peer was reachable"));
    assert_eq!(stat(&mut cluster.client(0), "repl_sync_timeouts"), "0");
}

#[test]
fn force_standalone_serves_at_once_with_a_warning() {
    let mut cluster = Cluster::start(2, Setup::persistent);
    wait_until_linked(&cluster);
    cluster.kill(0);
    cluster.kill(1);
    cluster.nodes[0].setup.extra = vec!["--force-standalone".into()];
    let started = Instant::now();
    cluster.restart(0);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    assert!(cluster.node(0).logged("warning: --force-standalone"));
    assert_eq!(stat(&mut cluster.client(0), "repl_sync_timeouts"), "0");
}

/// The batch frame type: the first sent after the handshake, from which signed frames carry
/// two tags.
const BATCH: u8 = 5;
/// Frame header bytes: payload length and type.
const HEADER_LEN: usize = 5;
/// Bytes of each signed-frame tag: one after the header, one after the payload.
const TAG_LEN: usize = 16;

/// What the tampering proxy does to the frames of a signed link from node 1 to node 2.
#[derive(Clone, Copy, Debug)]
enum Spoil {
    /// Changes a byte of the next batch.
    Change,
    /// Sends a frame of its own before the next batch.
    Insert,
    /// Leaves out the next batch.
    Drop,
    /// Raises the next batch's length near the frame limit, keeping its header tag.
    Lengthen,
    /// Closes the link, then replays the first link's first post-handshake frame on the next.
    Replay,
}

#[derive(Default)]
struct TamperState {
    /// Node 2's replication port; until set (0), connections are closed.
    target: u16,
    armed: Option<Spoil>,
    /// Node 1's first post-handshake frame on each link, in order.
    firsts: Vec<Vec<u8>>,
    /// Both ends of every carried connection, for closing.
    open: Vec<TcpStream>,
    /// How many spoils were carried out.
    spoiled: usize,
}

/// A proxy that spoils node 1's frames to node 2 on demand; node 2's pass unchanged.
struct Tamper {
    port: u16,
    state: Arc<Mutex<TamperState>>,
}

impl Tamper {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(Mutex::new(TamperState::default()));
        let shared = state.clone();
        std::thread::spawn(move || {
            for inbound in listener.incoming() {
                let Ok(inbound) = inbound else { continue };
                let target = shared.lock().unwrap().target;
                if target == 0 {
                    continue;
                }
                let Ok(outbound) = TcpStream::connect(("127.0.0.1", target)) else {
                    continue;
                };
                {
                    let mut state = shared.lock().unwrap();
                    state.open.push(inbound.try_clone().unwrap());
                    state.open.push(outbound.try_clone().unwrap());
                }
                let (mut from, mut to) =
                    (outbound.try_clone().unwrap(), inbound.try_clone().unwrap());
                std::thread::spawn(move || {
                    let _ = std::io::copy(&mut from, &mut to);
                    let _ = from.shutdown(Shutdown::Both);
                    let _ = to.shutdown(Shutdown::Both);
                });
                let state = shared.clone();
                std::thread::spawn(move || forward_frames(inbound, outbound, &state));
            }
        });
        Self { port, state }
    }

    fn set_target(&self, port: u16) {
        self.state.lock().unwrap().target = port;
    }

    fn arm(&self, spoil: Spoil) {
        let mut state = self.state.lock().unwrap();
        state.armed = Some(spoil);
        if let Spoil::Replay = spoil {
            for stream in state.open.drain(..) {
                let _ = stream.shutdown(Shutdown::Both);
            }
        }
    }

    fn spoiled(&self) -> usize {
        self.state.lock().unwrap().spoiled
    }
}

/// Carries frames from `from` to `to`, spoiling them as `state` says.
fn forward_frames(mut from: TcpStream, mut to: TcpStream, state: &Mutex<TamperState>) {
    let mut first = true;
    loop {
        let mut header = [0u8; HEADER_LEN];
        if from.read_exact(&mut header).is_err() {
            break;
        }
        let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let kind = header[4];
        let tagged = kind >= BATCH;
        let tags = if tagged { 2 * TAG_LEN } else { 0 };
        let mut frame = header.to_vec();
        frame.resize(HEADER_LEN + len + tags, 0);
        if from.read_exact(&mut frame[HEADER_LEN..]).is_err() {
            break;
        }
        let mut out = Vec::new();
        if tagged {
            let mut state = state.lock().unwrap();
            if first {
                first = false;
                state.firsts.push(frame.clone());
                if let Some(Spoil::Replay) = state.armed
                    && state.firsts.len() > 1
                {
                    frame = state.firsts[0].clone();
                    state.armed = None;
                    state.spoiled += 1;
                }
            }
            if kind == BATCH {
                let spoil = state.armed.take();
                match spoil {
                    // The first byte of the batch's position: still well formed.
                    Some(Spoil::Change) => frame[HEADER_LEN + TAG_LEN] ^= 1,
                    Some(Spoil::Insert) => {
                        // A heartbeat with made-up tags.
                        out.extend_from_slice(&[0, 0, 0, 0, 7]);
                        out.extend_from_slice(&[0; 2 * TAG_LEN]);
                    }
                    Some(Spoil::Drop) => frame.clear(),
                    // 1 GiB: must be rejected at the header tag, not waited for.
                    Some(Spoil::Lengthen) => {
                        frame[..4].copy_from_slice(&(1u32 << 30).to_le_bytes())
                    }
                    Some(Spoil::Replay) => state.armed = spoil,
                    None => {}
                }
                if !matches!(spoil, None | Some(Spoil::Replay)) {
                    state.spoiled += 1;
                }
            }
        }
        out.extend_from_slice(&frame);
        if to.write_all(&out).is_err() {
            break;
        }
    }
    let _ = from.shutdown(Shutdown::Both);
    let _ = to.shutdown(Shutdown::Both);
}

#[test]
fn a_signed_link_drops_a_tampered_frame_and_links_again() {
    let secrets = tempfile::tempdir().unwrap();
    let path = secrets.path().join("secret");
    std::fs::write(&path, "s3cret").unwrap();
    let proxy = Tamper::start();
    let cluster = Cluster::start(2, |index| Setup {
        extra: vec![
            "--force-standalone".into(),
            "--repl-secret-file".into(),
            path.to_str().unwrap().into(),
        ],
        via: (index == 0).then_some(proxy.port),
        ..Setup::plain(index)
    });
    proxy.set_target(cluster.nodes[1].repl_port);
    wait_until_linked(&cluster);
    let reason = "the link from node 1 ended: a frame failed authentication";
    let mut a = cluster.client(0);
    let spoils = [
        Spoil::Change,
        Spoil::Insert,
        Spoil::Drop,
        Spoil::Lengthen,
        Spoil::Replay,
    ];
    for (round, spoil) in spoils.into_iter().enumerate() {
        proxy.arm(spoil);
        let key = format!("k{round}");
        let started = Instant::now();
        set(&mut a, &key, "v");
        eventually(&format!("node 2 dropped the link for {spoil:?}"), || {
            cluster.node(1).log_count(reason) == round + 1
        });
        if let Spoil::Lengthen = spoil {
            // At the header tag, not after the 5-second silence limit; slack for a busy machine.
            let waited = started.elapsed();
            assert!(waited < Duration::from_secs(4), "dropped after {waited:?}");
        }
        wait_for_stat(&cluster, 1, "repl_bad_frames", &(round + 1).to_string());
        // Node 1 relinks and resumes.
        wait_for(&cluster, 1, &key, Some("v"));
    }
    assert_eq!(proxy.spoiled(), spoils.len());
    // The link from node 2 was unaffected.
    assert_eq!(stat(&mut a, "repl_bad_frames"), "0");
    set(&mut cluster.client(1), "back", "v");
    wait_for(&cluster, 0, "back", Some("v"));
    wait_until_linked(&cluster);
}
