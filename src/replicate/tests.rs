//! Two nodes in one process, linked over loopback TCP.

use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::ready::{Phase, Waiting};
use super::*;
use crate::backlog::{Log, LogConfig};
use crate::clock::{Clock, unix_secs};
use crate::oplog::Record;
use crate::persist::PersistConfig;
use crate::replicate::frame;
use crate::store::{GetArgs, SetArgs, StoreConfig};

struct Node {
    id: u8,
    store: Arc<Store>,
    log: Option<Log>,
    repl: Option<Replication>,
    secret: Option<Vec<u8>>,
}

impl Node {
    /// Starts the store and log without replication. With `persist`, segments are too large
    /// to roll, so nothing is compacted away.
    fn without_replication(id: u8, backlog_bytes: u64, persist: Option<&Path>) -> Self {
        let config = StoreConfig {
            memory_limit: 64 << 20,
            item_max: 1 << 20,
            shards: 4,
        };
        let store = Arc::new(Store::new(config, Arc::new(Clock::new(id))));
        store.enable_tombstones(3600, usize::MAX);
        let config = LogConfig {
            oplog_capacity: 65536,
            oplog_bytes: 64 << 20,
            backlog_bytes,
            persist: persist.map(|dir| PersistConfig {
                dir: dir.to_path_buf(),
                fsync_interval: Duration::from_millis(20),
                segment_bytes: 64 << 20,
            }),
        };
        let (log, _recovered) = Log::start(&config, &store, unix_secs()).unwrap();
        Node {
            id,
            store,
            log: Some(log),
            repl: None,
            secret: None,
        }
    }

    async fn start(id: u8, peers: Vec<String>) -> Self {
        Self::start_with_secret(id, peers, None).await
    }

    async fn start_with_secret(id: u8, peers: Vec<String>, secret: Option<&[u8]>) -> Self {
        let mut node = Self::without_replication(id, 1 << 20, None);
        node.secret = secret.map(<[u8]>::to_vec);
        node.start_replication(peers).await;
        node
    }

    async fn start_replication(&mut self, peers: Vec<String>) {
        self.start_replication_at(0, peers).await;
    }

    /// Starts replication listening on `port`, or any free port when 0.
    async fn start_replication_at(&mut self, port: u16, peers: Vec<String>) {
        let bound = Self::bind(self.id, port, peers, self.secret.clone()).await;
        self.start_bound(bound).await;
    }

    /// Binds the listener, which answers that the node is recovering until `start_bound`.
    async fn bind(id: u8, port: u16, peers: Vec<String>, secret: Option<Vec<u8>>) -> Bound {
        let readiness = Readiness::new(None, false, peers.len());
        let config = ReplConfig {
            node_id: id,
            peers,
            listen: "127.0.0.1".into(),
            port,
            secret,
            sync_timeout: None,
            force_standalone: false,
        };
        Replication::bind(config, readiness).await.unwrap()
    }

    /// Starts replication on a bound listener, as after local recovery.
    async fn start_bound(&mut self, bound: Bound) {
        let log = self.log.as_ref().unwrap();
        let start = Start {
            run: log.run,
            store: self.store.clone(),
            oplog: log.oplog.clone(),
            backlog: log.backlog.clone(),
            dir: log.dir.clone(),
            positions: Vec::new(),
            last_standing: false,
        };
        self.repl = Some(bound.start(start, |_| {}).await);
    }

    fn readiness(&self) -> &Readiness {
        &self.repl.as_ref().unwrap().readiness
    }

    async fn stop_replication(&mut self) {
        self.repl.take().unwrap().shutdown().await;
    }

    fn address(&self) -> String {
        self.repl.as_ref().unwrap().local_addr.to_string()
    }

    fn status(&self) -> &Status {
        &self.repl.as_ref().unwrap().status
    }

    fn set(&self, key: &str, value: &str) {
        let value = Bytes::from(value.to_string());
        self.store
            .set(key.as_bytes(), value, unix_secs(), &SetArgs::default());
    }

    fn get(&self, key: &str) -> Option<String> {
        self.store
            .get(key.as_bytes(), unix_secs(), &GetArgs::default())
            .map(|hit| String::from_utf8(hit.value.to_vec()).unwrap())
    }

    async fn stop(mut self) {
        if let Some(repl) = self.repl.take() {
            repl.shutdown().await;
        }
        let log = self.log.take().unwrap();
        tokio::task::spawn_blocking(move || log.shutdown())
            .await
            .unwrap()
            .unwrap();
    }
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sender_streams_its_own_writes_to_a_peer() {
    let b = Node::start(2, Vec::new()).await;
    let a = Node::start(1, vec![b.address()]).await;
    a.set("k", "v");
    eventually("b has k", || b.get("k").as_deref() == Some("v")).await;
    a.set("k", "w");
    eventually("b has the new k", || b.get("k").as_deref() == Some("w")).await;
    a.store.delete(b"k", unix_secs(), &Default::default());
    eventually("b lost k", || b.get("k").is_none()).await;
    eventually("b acknowledged everything", || {
        a.status()
            .peer_lines()
            .iter()
            .any(|(name, value)| name.ends_with(":lag") && value == "0")
    })
    .await;
    assert_eq!(b.status().records_applied.load(Relaxed), 3);
    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_without_a_position_gets_a_full_sync_first() {
    let b = Node::start(2, Vec::new()).await;
    b.set("gone", "old");
    tokio::time::sleep(Duration::from_millis(5)).await;
    // Written before the link exists, including a delete that leaves a tombstone.
    let mut a = Node::without_replication(1, 1 << 20, None);
    for index in 0..100 {
        a.set(&format!("k{index}"), "v");
    }
    a.set("gone", "v");
    a.store.delete(b"gone", unix_secs(), &Default::default());
    a.start_replication(vec![b.address()]).await;
    eventually("b has every key", || {
        (0..100).all(|index| b.get(&format!("k{index}")).is_some())
    })
    .await;
    eventually("the tombstone removed b's older gone", || {
        b.get("gone").is_none()
    })
    .await;
    eventually("a counted the full sync", || {
        a.status().full_syncs_sent.load(Relaxed) == 1
    })
    .await;
    assert_eq!(b.status().full_syncs_received.load(Relaxed), 1);
    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn records_from_another_node_are_not_forwarded() {
    let b = Node::start(2, Vec::new()).await;
    let a = Node::start(1, vec![b.address()]).await;
    eventually("the first full sync is done", || {
        a.status().full_syncs_sent.load(Relaxed) == 1
    })
    .await;
    // A record from node 3 is applied and logged on A, but not sent on to B.
    let version = Clock::new(3).next();
    let from_three = Record::Put {
        key: Bytes::from_static(b"from-3"),
        version,
        cas: version,
        flags: 0,
        expiry: 0,
        stale: false,
        value: Bytes::from_static(b"v"),
    };
    a.store.apply_remote(&from_three, unix_secs());
    a.set("from-1", "v");
    eventually("b has from-1", || b.get("from-1").is_some()).await;
    assert_eq!(b.get("from-3"), None);
    // The sender counts a batch after writing it, so B may have it first.
    eventually("a counted one record sent", || {
        a.status().records_sent.load(Relaxed) == 1
    })
    .await;
    a.stop().await;
    b.stop().await;
}

/// Writes on A while its replication is stopped, so B's entries leave A's one-entry backlog,
/// then restarts it. Returns A's full syncs since the restart.
async fn outage(persist: Option<&Path>, unreadable: bool) -> (Node, Node, u64) {
    let b = Node::start(2, Vec::new()).await;
    let mut a = Node::without_replication(1, 1, persist);
    a.start_replication(vec![b.address()]).await;
    a.set("first", "v");
    eventually("b has first", || b.get("first").is_some()).await;
    a.stop_replication().await;
    let backlog = a.log.as_ref().unwrap().backlog.clone();
    let head = backlog.head();
    let value = "x".repeat(1000);
    for index in 0..200 {
        a.set(&format!("k{index}"), &value);
    }
    // Wait for every write to be numbered, or a full sync could note a head that later writes
    // push out of the backlog, forcing a second one.
    eventually("a numbered every write", || backlog.head() >= head + 200).await;
    if unreadable {
        make_unreadable(persist.unwrap());
    }
    a.start_replication(vec![b.address()]).await;
    eventually("b has every key", || {
        (0..200).all(|index| b.get(&format!("k{index}")).as_deref() == Some(value.as_str()))
    })
    .await;
    // A counts a full sync before handling B's acknowledgment of it, so the count is final.
    eventually("b acknowledged everything", || {
        a.status()
            .peer_lines()
            .iter()
            .any(|(name, value)| name.ends_with(":lag") && value == "0")
    })
    .await;
    let syncs = a.status().full_syncs_sent.load(Relaxed);
    (a, b, syncs)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_behind_the_backlog_catches_up_from_the_segments() {
    let temp = tempfile::tempdir().unwrap();
    let (a, b, syncs) = outage(Some(temp.path()), false).await;
    // B's position was still in A's segments.
    assert_eq!(syncs, 0);
    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn without_persistence_a_peer_behind_the_backlog_gets_a_full_sync() {
    let (a, b, syncs) = outage(None, false).await;
    assert_eq!(syncs, 1);
    a.stop().await;
    b.stop().await;
}

/// Makes the log segments in `dir` unreadable; the log thread keeps its open segment.
#[cfg(unix)]
fn make_unreadable(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|extension| extension == "seg") {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
        }
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn segments_that_cannot_be_read_mean_a_full_sync() {
    let temp = tempfile::tempdir().unwrap();
    let (a, b, syncs) = outage(Some(temp.path()), true).await;
    assert_eq!(syncs, 1);
    a.stop().await;
    b.stop().await;
}

/// Listens as node 2, shakes hands with each sender and hands the stream to `link`.
async fn fake_receiver<F, Fut>(link: F) -> (String, tokio::task::JoinHandle<()>)
where
    F: Fn(tokio::net::TcpStream) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = bool> + Send,
{
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let me = handshake::Me {
                node_id: 2,
                run: 9,
                head: 0,
                recovering: false,
                secret: None,
            };
            if handshake::accept(&mut stream, &me, |_| Ok(()), |_| None)
                .await
                .is_ok()
                && !link(stream).await
            {
                return;
            }
        }
    });
    (address, task)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sender_waits_out_a_peer_that_heartbeats_but_does_not_read() {
    let mut a = Node::without_replication(1, 1 << 20, None);
    // Far more than the socket buffers hold, so the sender's writes block.
    let value = "x".repeat(40_000);
    for index in 0..800 {
        a.set(&format!("k{index}"), &value);
    }
    let (address, receiver) = fake_receiver(|mut stream| async move {
        let mut buf = Vec::new();
        // Heartbeat, but read nothing, for longer than the silence limit.
        let until = Instant::now() + link::SILENCE * 2;
        while Instant::now() < until {
            frame::write_frame(&mut stream, &frame::Frame::Heartbeat, &mut buf)
                .await
                .unwrap();
            tokio::time::sleep(link::HEARTBEAT).await;
        }
        loop {
            let read = frame::read_frame(&mut stream, frame::FRAME_MAX, &mut buf).await;
            if let frame::Frame::SyncEnd { .. } = read.expect("the sender kept the link") {
                return false;
            }
        }
    })
    .await;
    a.start_replication(vec![address]).await;
    tokio::time::timeout(Duration::from_secs(20), receiver)
        .await
        .expect("the full sync ended")
        .unwrap();
    assert_eq!(a.status().full_syncs_sent.load(Relaxed), 1);
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_link_that_fails_right_after_the_handshake_backs_off() {
    let handshakes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counted = handshakes.clone();
    let (address, receiver) = fake_receiver(move |stream| {
        counted.fetch_add(1, Relaxed);
        drop(stream);
        async { true }
    })
    .await;
    let a = Node::start(1, vec![address]).await;
    // Doubling from 100 ms allows about 4 attempts in 1.5 s; resetting after every handshake
    // would allow about 12.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let attempts = handshakes.load(Relaxed);
    assert!((1..=6).contains(&attempts), "{attempts} attempts");
    a.stop().await;
    receiver.abort();
}

/// A port that was free a moment ago.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_nodes_linked_both_ways_stop_writing_their_logs() {
    let (dir_a, dir_b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let ports = [free_port(), free_port()];
    let address = |port: u16| format!("127.0.0.1:{port}");
    let mut a = Node::without_replication(1, 1 << 20, Some(dir_a.path()));
    let mut b = Node::without_replication(2, 1 << 20, Some(dir_b.path()));
    a.start_replication_at(ports[0], vec![address(ports[1])])
        .await;
    b.start_replication_at(ports[1], vec![address(ports[0])])
        .await;
    a.set("from-a", "v");
    b.set("from-b", "v");
    eventually("both have both", || {
        a.get("from-b").is_some() && b.get("from-a").is_some()
    })
    .await;
    // Let positions settle (one not yet due is logged up to a tick later). After that, one
    // node's position entries must not make the other log one.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    // Bytes written include position entries, which `records` does not count.
    let written = |node: &Node| {
        let log = node.log.as_ref().unwrap();
        log.persist.as_ref().unwrap().bytes.load(Relaxed)
    };
    let before = (written(&a), written(&b));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!((written(&a), written(&b)), before);
    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_still_recovering_answers_handshakes_and_the_status_command() {
    let bound = Node::bind(2, 0, Vec::new(), None).await;
    let mut stream = TcpStream::connect(bound.local_addr).await.unwrap();
    let me = Me {
        node_id: 1,
        run: 5,
        head: 0,
        recovering: false,
        secret: None,
    };
    let accepted = handshake::connect(&mut stream, &me).await.unwrap();
    assert!(accepted.peer.recovering);
    assert_eq!(accepted.position, None);
    // It closes the connection without streaming anything.
    let mut rest = Vec::new();
    stream.read_to_end(&mut rest).await.unwrap();
    assert!(rest.is_empty());
    let mut stream = TcpStream::connect(bound.local_addr).await.unwrap();
    stream.write_all(b"status\r\n").await.unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).await.unwrap();
    assert!(reply.starts_with("STAT state recovering\r\n"), "{reply}");
    // Once started, the same listener links.
    let mut node = Node::without_replication(2, 1 << 20, None);
    let address = bound.local_addr;
    node.start_bound(bound).await;
    let mut stream = TcpStream::connect(address).await.unwrap();
    let accepted = handshake::connect(&mut stream, &me).await.unwrap();
    assert!(!accepted.peer.recovering);
    assert_eq!(
        node.readiness().lines(Instant::now())[0],
        ("state".to_string(), "waiting".to_string())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sender_waits_out_a_recovering_peer_and_links_once_it_has_recovered() {
    let bound = Node::bind(2, 0, Vec::new(), None).await;
    let a = Node::start(1, vec![bound.local_addr.to_string()]).await;
    eventually("node 1 sees node 2 recovering", || {
        a.readiness().poll(Instant::now()) == Phase::Waiting(Waiting::PeerRecovering)
    })
    .await;
    let mut b = Node::without_replication(2, 1 << 20, None);
    b.start_bound(bound).await;
    // Node 2 lists no peers, so it never links back.
    eventually("node 1 links to node 2", || {
        a.readiness().poll(Instant::now()) == Phase::Waiting(Waiting::NoInboundLink)
    })
    .await;
    let lines = a.readiness().lines(Instant::now());
    assert!(
        lines.contains(&("linked_peers".to_string(), "1".to_string())),
        "{lines:?}"
    );
    // Node 1's link ends, and its guard takes node 2 out of the count.
    b.stop_replication().await;
    let linked = |node: &Node| {
        node.readiness()
            .lines(Instant::now())
            .contains(&("linked_peers".to_string(), "0".to_string()))
    };
    eventually("node 1's link to node 2 ends", || linked(&a)).await;
    a.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_is_ready_once_caught_up_with_a_peer_that_links_to_it() {
    let a = Node::start(1, Vec::new()).await;
    let mut b = Node::without_replication(2, 1 << 20, None);
    for index in 0..50 {
        b.set(&format!("k{index}"), "v");
    }
    b.start_replication(vec![a.address()]).await;
    eventually("node 1 caught up with node 2", || {
        a.readiness().poll(Instant::now()) == Phase::Ready(ReadyBy::CaughtUp)
    })
    .await;
    for index in 0..50 {
        assert_eq!(a.get(&format!("k{index}")).as_deref(), Some("v"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn nodes_with_a_secret_replicate_both_ways_over_signed_links() {
    let mut b = Node::without_replication(2, 1 << 20, None);
    b.secret = Some(b"s".to_vec());
    b.set("before", "v");
    let port = free_port();
    let b_address = format!("127.0.0.1:{port}");
    let a = Node::start_with_secret(1, vec![b_address], Some(b"s")).await;
    b.start_replication_at(port, vec![a.address()]).await;
    eventually("a has b's write from its full sync", || {
        a.get("before").is_some()
    })
    .await;
    a.set("from-a", "v");
    b.set("from-b", "v");
    eventually("both have both writes", || {
        a.get("from-b").is_some() && b.get("from-a").is_some()
    })
    .await;
    // Idle past the silence limit: only signed heartbeats keep the links up.
    tokio::time::sleep(link::SILENCE * 2).await;
    a.set("later", "v");
    eventually("b has the later write", || b.get("later").is_some()).await;
    for node in [&a, &b] {
        assert_eq!(node.status().bad_frames.load(Relaxed), 0);
        assert_eq!(node.status().full_syncs_sent.load(Relaxed), 1);
    }
    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_sender_drops_a_link_whose_acknowledgment_was_changed_and_links_again() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let receiver = tokio::spawn(async move {
        let me = handshake::Me {
            node_id: 2,
            run: 9,
            head: 0,
            recovering: false,
            secret: Some(b"s".to_vec()),
        };
        for attempt in 0.. {
            let (mut stream, _) = listener.accept().await.unwrap();
            let Ok((_, (), keys)) = handshake::accept(&mut stream, &me, |_| Ok(()), |_| None).await
            else {
                continue;
            };
            let mut keys = keys.unwrap();
            if attempt > 0 {
                // The sender linked again.
                return;
            }
            let mut ack = Vec::new();
            frame::write_frame_with(
                &mut ack,
                &frame::Frame::Ack { next: 0 },
                &mut Vec::new(),
                Some(&mut keys.send),
            )
            .await
            .unwrap();
            // Flip a payload bit to acknowledge a position the receiver never reached.
            ack[frame::HEADER_LEN + frame::TAG_LEN] ^= 0x40;
            stream.write_all(&ack).await.unwrap();
            // The sender closes the link; drain what it sent first.
            let mut sink = Vec::new();
            let _ = stream.read_to_end(&mut sink).await;
        }
    });
    let a = Node::start_with_secret(1, vec![address], Some(b"s")).await;
    tokio::time::timeout(Duration::from_secs(10), receiver)
        .await
        .expect("the sender linked again")
        .unwrap();
    assert_eq!(a.status().bad_frames.load(Relaxed), 1);
    // The changed acknowledgment was not taken.
    assert!(
        a.status()
            .peer_lines()
            .iter()
            .any(|(name, value)| name.ends_with(":acked") && value == "0")
    );
    a.stop().await;
}
