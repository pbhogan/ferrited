//! The client server: shared context, startup and shutdown. Clients are served by event loop
//! threads fed by an acceptor thread; everything else runs on the Tokio runtime.

mod accept;
mod clients;
mod eventloop;
mod exec;
mod fdlimit;
mod meta;
mod reply;
mod session;
mod stream;

#[cfg(test)]
mod loop_tests;
#[cfg(test)]
mod tests;

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::backlog::Log;
use crate::clock::{Clock, unix_secs};
use crate::config::Config;
use crate::persist::recover::Recovered;
use crate::replicate::{Readiness, ReadyBy, Replication, Start};
use crate::stats::Stats;
use crate::store::{Store, StoreConfig};
pub use clients::Clients;
use clients::SocketFile;
pub(crate) use exec::{StatValue, general_stats};
pub(crate) use fdlimit::METRICS_CONNECTIONS;
pub use session::{Outcome, Session};
use stream::Listener;

/// Reported by `version`; the memcached prefix keeps clients that gate features on 1.6 working.
pub const VERSION: &str = concat!("1.6.40-ferrited-", env!("CARGO_PKG_VERSION"));

/// State shared by every connection.
pub struct Context {
    pub store: Arc<Store>,
    pub stats: Stats,
    pub item_max: usize,
    pub enable_shutdown: bool,
    pub max_connections: usize,
    pub threads: usize,
    pub port: u16,
    pub started: Instant,
    /// Unix time in seconds; tests replace it.
    pub wall_clock: fn() -> u64,
    /// Stops the server when set; use `begin_shutdown`, which also wakes the client threads.
    pub shutdown: watch::Sender<bool>,
    /// Shutdown has begun: the acceptor stops, and connections finish the requests they have
    /// read and close.
    stopping: AtomicBool,
    /// Set once shutdown stops waiting: the event loops close every connection at once.
    close_now: AtomicBool,
    /// The acceptor may still hand connections to the loops, so no loop may finish yet.
    accepting: AtomicBool,
    /// Wakes each client thread so it sees `stopping`.
    wakers: parking_lot::Mutex<Vec<Arc<mio::Waker>>>,
    pub persist: std::sync::OnceLock<Arc<crate::persist::PersistStats>>,
    pub repl: std::sync::OnceLock<Arc<crate::replicate::Status>>,
    /// `persist` will be set once persistence starts.
    pub persist_configured: bool,
    /// `repl` will be set once replication starts.
    pub repl_configured: bool,
    /// Set once the replication port is bound.
    pub readiness: std::sync::OnceLock<Arc<Readiness>>,
    pub(crate) client_port_open: AtomicBool,
}

impl Context {
    pub fn new(config: &Config) -> Arc<Self> {
        let cores = std::thread::available_parallelism().map_or(4, |count| count.get());
        let store_config = StoreConfig {
            memory_limit: config.memory_bytes(),
            item_max: config.max_item_size,
            shards: cores * 4,
        };
        Arc::new(Self {
            store: Arc::new(Store::new(
                store_config,
                Arc::new(Clock::new(config.node_id())),
            )),
            stats: Stats::new(),
            item_max: config.max_item_size,
            enable_shutdown: config.enable_shutdown,
            max_connections: config.conn_limit,
            threads: config.client_threads(),
            port: config.port,
            started: Instant::now(),
            wall_clock: unix_secs,
            shutdown: watch::channel(false).0,
            stopping: AtomicBool::new(false),
            close_now: AtomicBool::new(false),
            accepting: AtomicBool::new(false),
            wakers: parking_lot::Mutex::new(Vec::new()),
            persist: std::sync::OnceLock::new(),
            repl: std::sync::OnceLock::new(),
            persist_configured: config.data_dir.is_some(),
            repl_configured: !config.peers.is_empty(),
            readiness: std::sync::OnceLock::new(),
            client_port_open: AtomicBool::new(false),
        })
    }

    /// Whether the client port accepts connections.
    pub fn serving(&self) -> bool {
        self.client_port_open.load(Relaxed) && !self.stopping()
    }

    pub fn stopping(&self) -> bool {
        self.stopping.load(Relaxed)
    }

    /// Tells every connection to stop after the requests it has read, and the server to stop.
    pub fn begin_shutdown(&self) {
        self.stopping.store(true, Relaxed);
        self.shutdown.send_replace(true);
        self.wake_all();
    }

    /// Registers a client thread's waker, waking it at once if shutdown has begun.
    /// `begin_shutdown` sets `stopping` before taking the lock, so no wake is missed.
    fn add_waker(&self, waker: Arc<mio::Waker>) {
        let mut wakers = self.wakers.lock();
        if self.stopping.load(Relaxed) {
            let _ = waker.wake();
        }
        wakers.push(waker);
    }

    fn wake_all(&self) {
        for waker in self.wakers.lock().iter() {
            let _ = waker.wake();
        }
    }
}

/// What the connection loop should do after a command.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Flow {
    Continue,
    Close,
    Shutdown,
}

/// Serves clients on `listener` with `ctx.threads` event loops until shutdown drains them.
pub fn run(listener: std::net::TcpListener, ctx: &Arc<Context>) -> io::Result<Clients> {
    Clients::start(ctx, Listener::from(listener), None)
}

/// How long shutdown waits for connections to finish the requests they have read.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Runs the server described by `config` until it is told to stop.
pub async fn start(mut config: Config) -> io::Result<()> {
    let reserved = fdlimit::reserve(
        config.client_threads(),
        config.peers.len(),
        config.data_dir.is_some(),
        config.metrics_addr.is_some(),
    );
    config.conn_limit = fdlimit::reconcile(config.conn_limit, reserved);
    // First, so a missing secret stops startup before recovery.
    let repl_config = config
        .repl()
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
    // Installed before recovery so a stop signal during startup stops the node cleanly.
    let mut signals = StopSignals::new()?;
    let notifier = crate::notify::global();
    // The client port is bound only once the node is ready; catch an address in use now.
    check_client_address(&config)?;
    let ctx = Context::new(&config);
    // Before recovery, so deletes in the log come back as tombstones.
    if !config.peers.is_empty() {
        ctx.store
            .enable_tombstones(config.tombstone_ttl as u64, config.tombstone_memory_bytes());
    }
    // Bound before recovery so a node loading data or waiting for peers can be scraped.
    let metrics = match &config.metrics_addr {
        Some(address) => Some(crate::metrics::Metrics::bind(address, ctx.clone()).await?),
        None => None,
    };
    // Bound before recovery, so peers can tell this node is alive while it loads its data.
    let bound = match repl_config {
        Some(repl_config) => {
            let readiness = Readiness::new(
                repl_config.sync_timeout,
                repl_config.force_standalone,
                repl_config.peers.len(),
            );
            let _ = ctx.readiness.set(readiness.clone());
            Some(Replication::bind(repl_config, readiness).await?)
        }
        None => None,
    };
    // A stop signal during recovery stops the node once recovery ends; a second one exits at
    // once, which is as safe as a crash.
    let mut stop_requested = false;
    let (log, recovered) = match config.log() {
        Some(log_config) => {
            notifier.status("loading local data");
            let store = ctx.store.clone();
            let delay = test_recovery_delay();
            let mut recovery = tokio::task::spawn_blocking(move || {
                if let Some(delay) = delay {
                    std::thread::sleep(delay);
                }
                Log::start(&log_config, &store, unix_secs())
            });
            let joined = loop {
                tokio::select! {
                    joined = &mut recovery => break joined,
                    _ = signals.recv() => {
                        if stop_requested {
                            eprintln!("ferrited: stop interrupted local recovery; exiting now");
                            std::process::exit(1);
                        }
                        stop_requested = true;
                        eprintln!("ferrited: stopping once local data has loaded");
                    }
                }
            };
            let (log, recovered) = joined.expect("recovery thread panicked")?;
            if let Some(stats) = &log.persist {
                let _ = ctx.persist.set(stats.clone());
            }
            (Some(log), recovered)
        }
        None => (None, Recovered::default()),
    };
    if stop_requested {
        exit_on_signal(signals);
        notifier.stopping("stopping: writing and syncing the log");
        ctx.begin_shutdown();
        if let Some(log) = log {
            stop_log(log).await?;
        }
        if let Some(metrics) = metrics {
            metrics.stop().await;
        }
        return Ok(());
    }
    let replication = match (bound, &log) {
        (Some(bound), Some(running)) => {
            let last_standing = recovered.last_standing();
            let start = Start {
                run: running.run,
                store: ctx.store.clone(),
                oplog: running.oplog.clone(),
                backlog: running.backlog.clone(),
                dir: running.dir.clone(),
                positions: recovered.positions,
                last_standing,
            };
            // Set before readiness leaves recovering, so a scrape that sees the node waiting
            // also sees its peers' links.
            let replication = bound
                .start(start, |status| {
                    let _ = ctx.repl.set(status.clone());
                })
                .await;
            Some(replication)
        }
        _ => None,
    };
    spawn_sweeper(ctx.clone());
    let mut stop = ctx.shutdown.subscribe();
    let mut clients = None;
    // A node with peers opens its client port only once it is ready.
    let serving = async {
        if let Some(replication) = &replication
            && replication.readiness.wait().await == ReadyBy::SyncTimeout
        {
            replication.status.sync_timeouts.fetch_add(1, Relaxed);
        }
        clients = Some(listen(&config, &ctx)?);
        std::future::pending::<io::Result<()>>().await
    };
    let result = tokio::select! {
        result = serving => result,
        _ = stop.changed() => Ok(()),
        _ = signals.recv() => Ok(()),
    };
    // Links ended by stopping are not peers going down, so the log keeps the peers linked while
    // serving. Must precede stopping the log thread, which reads the link mailbox once at exit.
    if let Some(replication) = &replication {
        replication.readiness.stopping();
    }
    exit_on_signal(signals);
    notifier.stopping("stopping: finishing client requests");
    ctx.begin_shutdown();
    if let Some(clients) = clients {
        clients.drain(&ctx, DRAIN_TIMEOUT).await;
    }
    // Before the log, so every record a receiver applied is logged before its final position.
    if let Some(replication) = replication {
        replication.shutdown().await;
    }
    let stopped_log = match log {
        Some(log) => {
            notifier.status("stopping: writing and syncing the log");
            stop_log(log).await
        }
        None => Ok(()),
    };
    if let Some(metrics) = metrics {
        metrics.stop().await;
    }
    // Exits non-zero if the data on disk may be missing changes.
    if let Err(error) = stopped_log {
        result?;
        return Err(error);
    }
    result
}

/// Exits non-zero on the next stop signal, which is safe: the files on disk survive a crash.
fn exit_on_signal(mut signals: StopSignals) {
    tokio::spawn(async move {
        signals.recv().await;
        eprintln!("ferrited: shutdown interrupted; exiting now");
        std::process::exit(1);
    });
}

const MAX_TEST_RECOVERY_DELAY: Duration = Duration::from_secs(60);

/// A pause before recovery from `FERRITED_TEST_RECOVERY_DELAY_MS`, for tests. A test hook, not
/// a supported setting, so it is logged whenever set.
fn test_recovery_delay() -> Option<Duration> {
    const NAME: &str = "FERRITED_TEST_RECOVERY_DELAY_MS";
    let text = std::env::var_os(NAME)?;
    let Some(millis) = text
        .to_str()
        .and_then(|text| text.trim().parse::<u64>().ok())
    else {
        eprintln!("ferrited: ignoring {NAME}={text:?}: not a number of milliseconds");
        return None;
    };
    let delay = Duration::from_millis(millis).min(MAX_TEST_RECOVERY_DELAY);
    eprintln!(
        "ferrited: {NAME} set; pausing {} ms before recovery",
        delay.as_millis()
    );
    Some(delay)
}

fn cannot_listen(address: impl std::fmt::Display, error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        format!("cannot listen for clients on {address}: {error}"),
    )
}

/// Fails if the client address is already in use, by binding it and closing it again.
fn check_client_address(config: &Config) -> io::Result<()> {
    #[cfg(unix)]
    if let Some(path) = &config.socket {
        return check_unix(path)
            .map(drop)
            .map_err(|error| cannot_listen(path.display(), error));
    }
    if config.port == 0 {
        return Ok(());
    }
    bind_without_listening(&config.listen, config.port)
        .map(drop)
        .map_err(|error| cannot_listen(format!("{}:{}", config.listen, config.port), error))
}

/// Binds as `bind_tcp` would but does not listen: a listener would accept a client's handshake
/// and then reset it on close, whereas a bound socket refuses it and the client retries.
fn bind_without_listening(host: &str, port: u16) -> io::Result<tokio::net::TcpSocket> {
    use std::net::ToSocketAddrs;

    let mut last = None;
    for address in (host, port).to_socket_addrs()? {
        let socket = match address {
            std::net::SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            std::net::SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        // As the standard library's listener does, so conflicts are judged the same way.
        socket.set_reuseaddr(true)?;
        match socket.bind(address) {
            Ok(()) => return Ok(socket),
            Err(error) => last = Some(error),
        }
    }
    Err(last.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not resolve to any addresses",
        )
    }))
}

/// Writes and syncs everything queued and stops the log thread.
async fn stop_log(log: Log) -> io::Result<()> {
    tokio::task::spawn_blocking(move || log.shutdown())
        .await
        .unwrap_or_else(|_| Err(io::Error::other("the log shutdown panicked")))
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("persistence did not finish cleanly: {error}"),
            )
        })
}

/// Starts serving clients, then tells the service manager the node is ready.
fn listen(config: &Config, ctx: &Arc<Context>) -> io::Result<Clients> {
    let (clients, shown) = start_clients(config, ctx)?;
    ctx.client_port_open.store(true, Relaxed);
    crate::notify::global().ready(&format!("serving clients on {shown}"));
    Ok(clients)
}

/// Binds the client address and starts the client threads; returns them and the address.
fn start_clients(config: &Config, ctx: &Arc<Context>) -> io::Result<(Clients, String)> {
    #[cfg(unix)]
    if let Some(path) = &config.socket {
        let listener = bind_unix(path).map_err(|error| cannot_listen(path.display(), error))?;
        // From here on the socket file is removed however the server ends.
        let socket_file = SocketFile(path.clone());
        let shown = path.display().to_string();
        eprintln!("ferrited listening on {shown}");
        let clients = Clients::start(ctx, Listener::from(listener), Some(socket_file))?;
        return Ok((clients, shown));
    }
    let listener = bind_tcp(&config.listen, config.port)
        .map_err(|error| cannot_listen(format!("{}:{}", config.listen, config.port), error))?;
    let shown = listener.local_addr()?.to_string();
    eprintln!("ferrited listening on {shown}");
    Ok((Clients::start(ctx, Listener::from(listener), None)?, shown))
}

/// Listen backlog, as memcached's default (`-b 1024`).
const LISTEN_BACKLOG: libc::c_int = 1024;

/// Binds a TCP listener for clients with `LISTEN_BACKLOG`.
pub(crate) fn bind_tcp(host: &str, port: u16) -> io::Result<std::net::TcpListener> {
    let listener = std::net::TcpListener::bind((host, port))?;
    set_backlog(&listener)?;
    Ok(listener)
}

/// Raises the backlog from the standard library's 128 by listening again, which only changes
/// the backlog.
fn set_backlog(listener: &impl std::os::fd::AsRawFd) -> io::Result<()> {
    // SAFETY: `listen` only changes the state of a socket `listener` owns and keeps open.
    if unsafe { libc::listen(listener.as_raw_fd(), LISTEN_BACKLOG) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Returns whether `path` holds a stale socket file to replace; anything else there, or a live
/// socket, is an error.
#[cfg(unix)]
fn check_unix(path: &std::path::Path) -> io::Result<bool> {
    use std::os::unix::fs::FileTypeExt;

    let shown = path.display();
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
        Ok(metadata) if !metadata.file_type().is_socket() => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{shown} exists and is not a socket"),
        )),
        Ok(_) => match std::os::unix::net::UnixStream::connect(path) {
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("{shown} is already in use"),
            )),
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => Ok(true),
            Err(error) => Err(io::Error::new(
                error.kind(),
                format!("cannot tell whether {shown} is in use: {error}"),
            )),
        },
    }
}

/// Binds a Unix socket at `path`, replacing a stale socket file.
#[cfg(unix)]
pub(crate) fn bind_unix(path: &std::path::Path) -> io::Result<std::os::unix::net::UnixListener> {
    if check_unix(path)? {
        std::fs::remove_file(path)?;
    }
    let listener = std::os::unix::net::UnixListener::bind(path)?;
    if let Err(error) = set_backlog(&listener) {
        let _ = std::fs::remove_file(path);
        return Err(error);
    }
    Ok(listener)
}

/// Removes expired and flushed items once a second.
fn spawn_sweeper(ctx: Arc<Context>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            let ctx = ctx.clone();
            let _ = tokio::task::spawn_blocking(move || ctx.store.sweep(unix_secs())).await;
        }
    });
}

/// Listens for SIGINT and SIGTERM.
struct StopSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl StopSignals {
    /// Installs the handlers; signals from here on are delivered to `recv`.
    fn new() -> io::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt())?,
                terminate: signal(SignalKind::terminate())?,
            })
        }
        #[cfg(not(unix))]
        Ok(Self {})
    }

    async fn recv(&mut self) {
        #[cfg(unix)]
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}
