//! Replication: a sender per peer streams this node's changes to it, and a receiver per inbound
//! link applies another node's. The listener binds before local recovery; until recovery ends
//! it only answers the status command and tells peers this node is recovering.

mod command;
pub mod frame;
pub mod handshake;
mod link;
pub mod ready;
mod receiver;
pub mod resume;
mod sender;
mod status;
#[cfg(test)]
mod tests;

pub use ready::{Phase, Readiness, ReadyBy, Snapshot, Waiting};
pub use status::{Configured, LinkState, PeerView, STAT_NAMES, Status};

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

use crate::backlog::Backlog;
use crate::oplog::{Oplog, Position};
use crate::persist::dir::DataDir;
use crate::store::Store;
use handshake::Me;

/// How long tasks get to finish on stop before they are aborted.
const STOP_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug)]
pub struct ReplConfig {
    pub node_id: u8,
    /// The other nodes' replication addresses.
    pub peers: Vec<String>,
    /// Address and port of this node's replication listener.
    pub listen: String,
    pub port: u16,
    pub secret: Option<Vec<u8>>,
    /// How long to wait with no peer reachable before serving anyway; `None` waits forever.
    pub sync_timeout: Option<Duration>,
    /// Whether to serve as soon as local recovery finishes.
    pub force_standalone: bool,
}

/// What replication needs to start once local recovery is done.
pub struct Start {
    /// This process's run ID.
    pub run: u64,
    pub store: Arc<Store>,
    pub oplog: Arc<Oplog>,
    pub backlog: Arc<Backlog>,
    /// The data directory, when persistence is on.
    pub dir: Option<Arc<DataDir>>,
    /// This node's recovered positions on its peers' logs.
    pub positions: Vec<Position>,
    /// Whether the recovered log shows this node was the last one standing.
    pub last_standing: bool,
}

pub(crate) struct Shared {
    pub config: ReplConfig,
    pub run: u64,
    pub store: Arc<Store>,
    pub oplog: Arc<Oplog>,
    pub backlog: Arc<Backlog>,
    pub dir: Option<Arc<DataDir>>,
    /// This node's position on each peer's log, by the peer's node ID.
    pub positions: Mutex<HashMap<u8, Position>>,
    /// The last position on each peer's log queued to this node's oplog. A full oplog can drop
    /// a link's final position, so the next link from that peer starts from this, not
    /// `positions`, and logs the newer one.
    pub logged: Mutex<HashMap<u8, Position>>,
    /// Node IDs with a live inbound link.
    pub inbound: Mutex<HashSet<u8>>,
    pub status: Arc<Status>,
    pub readiness: Arc<Readiness>,
    /// Becomes true when replication stops.
    pub stop: watch::Receiver<bool>,
}

impl Shared {
    /// This node as its hello introduces it.
    pub fn me(&self) -> Me {
        Me {
            node_id: self.config.node_id,
            run: self.run,
            head: self.backlog.head(),
            recovering: false,
            secret: self.config.secret.clone(),
        }
    }
}

pub struct Replication {
    stop: watch::Sender<bool>,
    tasks: JoinSet<()>,
    pub status: Arc<Status>,
    pub readiness: Arc<Readiness>,
    pub local_addr: SocketAddr,
}

/// The replication listener, bound before local recovery. Until `start`, it answers the status
/// command and tells connecting peers that this node is recovering.
pub struct Bound {
    config: ReplConfig,
    readiness: Arc<Readiness>,
    /// Set to true to take the listener back from the recovering answerer.
    done: watch::Sender<bool>,
    answering: JoinHandle<TcpListener>,
    pub local_addr: SocketAddr,
}

impl Replication {
    /// Binds the listener. Call before local recovery, so peers see this node is alive while it
    /// loads its data.
    pub async fn bind(config: ReplConfig, readiness: Arc<Readiness>) -> io::Result<Bound> {
        let (listen, port) = (config.listen.as_str(), config.port);
        let listener = TcpListener::bind((listen, port)).await.map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("cannot listen for replication on {listen}:{port}: {error}"),
            )
        })?;
        let local_addr = listener.local_addr()?;
        eprintln!("ferrited replication listening on {local_addr}");
        if config.secret.is_none() && !config.peers.is_empty() {
            eprintln!(
                "ferrited: warning: replication has no --repl-secret-file, so any host that can reach {local_addr} can link to this node, send it records and hold a node ID's place"
            );
        }
        // No run or log exists yet; the peer uses the zero run and head only to check the proof.
        let me = Me {
            node_id: config.node_id,
            run: 0,
            head: 0,
            recovering: true,
            secret: config.secret.clone(),
        };
        let (done, done_receiver) = watch::channel(false);
        let answering = tokio::spawn(receiver::answer_while_recovering(
            listener,
            me,
            readiness.clone(),
            done_receiver,
        ));
        Ok(Bound {
            config,
            readiness,
            done,
            answering,
            local_addr,
        })
    }

    /// Stops every link. Receiving links log their final positions first; tasks still running
    /// after `STOP_TIMEOUT` are aborted.
    pub async fn shutdown(mut self) {
        self.stop.send_replace(true);
        let deadline = tokio::time::sleep(STOP_TIMEOUT);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                joined = self.tasks.join_next() => {
                    if joined.is_none() {
                        return;
                    }
                }
                _ = &mut deadline => break,
            }
        }
        eprintln!("replication: stopping links that did not finish in {STOP_TIMEOUT:?}");
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }
}

impl Bound {
    /// Takes the listener back from the recovering answerer and starts the links once local
    /// recovery is done. `publish` gets the links' status before readiness leaves recovering.
    pub async fn start(self, start: Start, publish: impl FnOnce(&Arc<Status>)) -> Replication {
        self.done.send_replace(true);
        let listener = self
            .answering
            .await
            .expect("the recovering answerer panicked");
        let (stop, stop_receiver) = watch::channel(false);
        let status = Arc::new(Status::new(
            start.backlog.clone(),
            start.oplog.clone(),
            start.store.clone(),
            &self.config.peers,
        ));
        // Published before readiness moves to waiting, so anything reporting the node as
        // waiting (the metrics endpoint) also finds the peers' links.
        publish(&status);
        self.readiness
            .recovered(start.oplog.clone(), start.last_standing, Instant::now());
        let shared = Arc::new(Shared {
            positions: Mutex::new(start.positions.iter().map(|p| (p.peer, *p)).collect()),
            logged: Mutex::new(start.positions.iter().map(|p| (p.peer, *p)).collect()),
            inbound: Mutex::new(HashSet::new()),
            status: status.clone(),
            readiness: self.readiness.clone(),
            stop: stop_receiver,
            config: self.config,
            run: start.run,
            store: start.store,
            oplog: start.oplog,
            backlog: start.backlog,
            dir: start.dir,
        });
        let mut tasks = JoinSet::new();
        tasks.spawn(receiver::listen(listener, shared.clone()));
        for index in 0..shared.config.peers.len() {
            tasks.spawn(sender::run(shared.clone(), index));
        }
        Replication {
            stop,
            tasks,
            status,
            readiness: self.readiness,
            local_addr: self.local_addr,
        }
    }
}

/// Reads the shared secret: the file's bytes without trailing line breaks.
pub fn read_secret(path: &Path) -> Result<Vec<u8>, String> {
    let shown = path.display();
    let mut secret = std::fs::read(path)
        .map_err(|error| format!("cannot read the replication secret {shown}: {error}"))?;
    while secret
        .last()
        .is_some_and(|&byte| byte == b'\n' || byte == b'\r')
    {
        secret.pop();
    }
    if secret.is_empty() {
        return Err(format!("the replication secret {shown} is empty"));
    }
    Ok(secret)
}

#[cfg(test)]
pub(crate) fn test_shared(
    node_id: u8,
    store: Arc<Store>,
    oplog: Arc<Oplog>,
    backlog: Arc<Backlog>,
    stop: watch::Receiver<bool>,
    secret: Option<Vec<u8>>,
) -> Arc<Shared> {
    Arc::new(Shared {
        config: ReplConfig {
            node_id,
            peers: Vec::new(),
            listen: "127.0.0.1".into(),
            port: 0,
            secret,
            sync_timeout: None,
            force_standalone: false,
        },
        run: 1,
        status: Arc::new(Status::new(
            backlog.clone(),
            oplog.clone(),
            store.clone(),
            &[],
        )),
        readiness: Readiness::new(None, false, 0),
        store,
        oplog,
        backlog,
        dir: None,
        positions: Mutex::new(HashMap::new()),
        logged: Mutex::new(HashMap::new()),
        inbound: Mutex::new(HashSet::new()),
        stop,
    })
}

#[cfg(test)]
mod secret_tests {
    use super::read_secret;

    #[test]
    fn a_secret_loses_only_its_trailing_line_breaks() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("secret");
        std::fs::write(&path, b" two\nlines \r\n\n").unwrap();
        assert_eq!(read_secret(&path).unwrap(), b" two\nlines ");
        std::fs::write(&path, b"\r\n\n").unwrap();
        assert!(read_secret(&path).unwrap_err().contains("is empty"));
        std::fs::write(&path, b"").unwrap();
        assert!(read_secret(&path).unwrap_err().contains("is empty"));
        assert!(read_secret(&temp.path().join("missing")).is_err());
    }
}
