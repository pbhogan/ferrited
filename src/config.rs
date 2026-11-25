//! Command-line configuration. Short flags follow memcached's where an equivalent exists.

use std::path::PathBuf;

use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};

/// Smallest maximum item size accepted.
const MIN_ITEM_SIZE: usize = 1024;
/// Largest oplog capacity accepted. The oplog's queue is allocated in full at startup.
const MAX_OPLOG_CAPACITY: usize = 1 << 24;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "ferrited",
    version,
    about = "A memcached-compatible cache server"
)]
#[command(group(
    clap::ArgGroup::new("log_consumer")
        .args(["data_dir", "peers"])
        .multiple(true)
))]
pub struct Config {
    /// TCP port to listen on
    #[arg(short = 'p', long, default_value_t = 11211)]
    pub port: u16,

    /// Address to listen on
    #[arg(short = 'l', long, default_value = "127.0.0.1")]
    pub listen: String,

    /// Unix socket path to listen on instead of TCP
    #[arg(short = 's', long)]
    pub socket: Option<PathBuf>,

    /// Memory limit in megabytes
    #[arg(short = 'm', long = "memory-limit", default_value_t = 64, value_parser = parse_memory_limit)]
    pub memory_limit: usize,

    /// Maximum simultaneous client connections
    #[arg(short = 'c', long = "conn-limit", default_value_t = 1024, value_parser = parse_positive)]
    pub conn_limit: usize,

    /// Maximum item size, in bytes or with a k or m suffix
    #[arg(short = 'I', long = "max-item-size", default_value = "1m", value_parser = parse_item_size)]
    pub max_item_size: usize,

    /// Worker threads (default: one per core)
    #[arg(short = 't', long, value_parser = parse_positive)]
    pub threads: Option<usize>,

    /// Enable the shutdown command
    #[arg(short = 'A', long = "enable-shutdown")]
    pub enable_shutdown: bool,

    /// Node ID, unique among peers; required with --peer (default: 1)
    #[arg(long = "node-id", value_parser = clap::value_parser!(u8))]
    pub node_id: Option<u8>,

    /// Directory for persistence files (default: no persistence)
    #[arg(long = "data-dir")]
    pub data_dir: Option<PathBuf>,

    /// Milliseconds between fsyncs of the persistence log
    #[arg(long = "fsync-interval-ms", default_value_t = 1000, value_parser = parse_positive, requires = "data_dir")]
    pub fsync_interval_ms: usize,

    /// Log segment size, in bytes or with a k or m suffix
    #[arg(long = "segment-size", default_value = "64m", value_parser = parse_segment_size, requires = "data_dir")]
    pub segment_size: usize,

    /// Changes queued in memory for the log and peers
    #[arg(long = "oplog-capacity", default_value_t = 65536, value_parser = parse_oplog_capacity, requires = "log_consumer")]
    pub oplog_capacity: usize,

    /// Value bytes queued in memory for the log and peers (default: the larger of 64m and 4 max items)
    #[arg(long = "oplog-bytes", value_parser = parse_size, requires = "log_consumer")]
    pub oplog_bytes: Option<usize>,

    /// Another node's replication address, HOST:PORT; repeat for each peer
    #[arg(long = "peer", value_name = "HOST:PORT", value_parser = parse_peer)]
    pub peers: Vec<String>,

    /// Replication port, on the -l address; required with --peer
    #[arg(long = "repl-port", requires = "peers")]
    pub repl_port: Option<u16>,

    /// File holding the shared secret that authenticates and signs replication
    #[arg(long = "repl-secret-file", requires = "peers")]
    pub repl_secret_file: Option<PathBuf>,

    /// Recent changes kept for peers that fall behind, in bytes or with a k or m suffix
    #[arg(long = "repl-backlog", default_value = "64m", value_parser = parse_size, requires = "peers")]
    pub repl_backlog: usize,

    /// Seconds a delete is remembered, so a late write cannot bring the key back
    #[arg(long = "tombstone-ttl", default_value_t = 3600, value_parser = parse_tombstone_ttl, requires = "peers")]
    pub tombstone_ttl: usize,

    /// Memory for remembered deletes, in bytes or with a k or m suffix (default: a tenth of -m)
    #[arg(long = "tombstone-memory", value_parser = parse_positive_size, requires = "peers")]
    pub tombstone_memory: Option<usize>,

    /// Seconds to wait for an unreachable peer at startup before serving alone; 0 waits forever
    #[arg(
        long = "sync-timeout",
        value_name = "SECONDS",
        default_value_t = 60,
        requires = "peers"
    )]
    pub sync_timeout: u64,

    /// Serve as soon as local data is loaded, without waiting for peers
    #[arg(long = "force-standalone", requires = "peers")]
    pub force_standalone: bool,

    /// Prometheus metrics address, HOST:PORT (default: off)
    #[arg(long = "metrics-addr", value_name = "HOST:PORT", value_parser = parse_listen_address)]
    pub metrics_addr: Option<String>,
}

/// The most Tokio worker threads `runtime_threads` asks for.
const MAX_RUNTIME_THREADS: usize = 4;

impl Config {
    /// Event loop threads serving clients: `-t`, or one per core.
    pub fn client_threads(&self) -> usize {
        self.threads
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |count| count.get()))
    }

    /// Tokio worker threads: `client_threads`, at most 4. Clients are served by the event loops;
    /// the runtime runs only replication, readiness, signals and timers.
    pub fn runtime_threads(&self) -> usize {
        self.client_threads().min(MAX_RUNTIME_THREADS)
    }

    /// Parses and validates a command line. The first argument is the program name.
    pub fn from_args<I, T>(args: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let config = Self::try_parse_from(args)?;
        config
            .validate()
            .map_err(|message| Self::command().error(ErrorKind::ValueValidation, message))?;
        Ok(config)
    }

    /// Checks the rules that involve more than one option.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_item_size > self.memory_bytes() {
            return Err(format!(
                "maximum item size ({} bytes) exceeds the memory limit ({} MB)",
                self.max_item_size, self.memory_limit
            ));
        }
        let logged = self.data_dir.is_some() || !self.peers.is_empty();
        if logged && self.max_item_size > crate::persist::codec::MAX_VALUE {
            return Err(format!(
                "maximum item size ({} bytes) exceeds the {} bytes the log can hold",
                self.max_item_size,
                crate::persist::codec::MAX_VALUE
            ));
        }
        if self.oplog_byte_budget() < self.max_item_size {
            return Err(format!(
                "oplog bytes ({}) must be at least the maximum item size ({} bytes)",
                self.oplog_byte_budget(),
                self.max_item_size
            ));
        }
        if !self.peers.is_empty() {
            if self.repl_port.is_none() {
                return Err("--repl-port is required with --peer".into());
            }
            // Compared as given, which is how `parse_peer` keeps them: two different spellings
            // of one address are not caught, since names resolve only at each connect.
            for (index, peer) in self.peers.iter().enumerate() {
                if self.peers[..index].contains(peer) {
                    return Err(format!("--peer {peer} is given twice"));
                }
            }
            // Both listen on `--listen`; the clash would otherwise show only once the node is
            // ready to bind its client port, which may be never.
            if self.socket.is_none() && self.port != 0 && self.repl_port == Some(self.port) {
                return Err(format!(
                    "--repl-port and -p are both {}; the replication and client ports must differ",
                    self.port
                ));
            }
            match self.node_id {
                None => {
                    return Err(
                        "--node-id is required with --peer, so that no two nodes share the default"
                            .into(),
                    );
                }
                Some(0) => return Err("--node-id must be from 1 to 255 with --peer".into()),
                Some(_) => {}
            }
            if self.tombstone_memory_bytes() > self.memory_bytes() / 2 {
                return Err(format!(
                    "tombstone memory ({} bytes) must be at most half the memory limit ({} MB)",
                    self.tombstone_memory_bytes(),
                    self.memory_limit
                ));
            }
            // Tombstones can hold their whole share of the memory limit, so an item must fit
            // in the rest.
            let for_items = self.memory_bytes() - self.tombstone_memory_bytes();
            if self.max_item_size > for_items {
                return Err(format!(
                    "maximum item size ({} bytes) exceeds the memory limit less the tombstone memory ({} bytes)",
                    self.max_item_size, for_items
                ));
            }
            if self.repl_backlog < self.max_item_size {
                return Err(format!(
                    "replication backlog ({} bytes) must be at least the maximum item size ({} bytes)",
                    self.repl_backlog, self.max_item_size
                ));
            }
        }
        if let Some(port) = self.metrics_port()
            && port != 0
        {
            // Checked whatever the hosts, as for the replication port: a wildcard address on
            // either side would clash, and the client port is bound only once the node is ready.
            if self.socket.is_none() && port == self.port {
                return Err(format!(
                    "--metrics-addr and -p both use port {port}; the metrics and client ports must differ"
                ));
            }
            if self.repl_port == Some(port) {
                return Err(format!(
                    "--metrics-addr and --repl-port both use port {port}; the metrics and replication ports must differ"
                ));
            }
        }
        Ok(())
    }

    /// The port of `--metrics-addr`, if given.
    pub fn metrics_port(&self) -> Option<u16> {
        let (_, port) = self.metrics_addr.as_deref()?.rsplit_once(':')?;
        port.parse().ok()
    }

    /// Value bytes the oplog may hold: as given, or the larger of 64 MiB and four max items.
    pub fn oplog_byte_budget(&self) -> usize {
        self.oplog_bytes
            .unwrap_or_else(|| (64 << 20).max(self.max_item_size.saturating_mul(4)))
    }

    pub fn persist(&self) -> Option<crate::persist::PersistConfig> {
        Some(crate::persist::PersistConfig {
            dir: self.data_dir.clone()?,
            fsync_interval: std::time::Duration::from_millis(self.fsync_interval_ms as u64),
            segment_bytes: self.segment_size as u64,
        })
    }

    /// Settings for the numbered log, if anything consumes it: persistence, or peers.
    pub fn log(&self) -> Option<crate::backlog::LogConfig> {
        let persist = self.persist();
        if persist.is_none() && self.peers.is_empty() {
            return None;
        }
        Some(crate::backlog::LogConfig {
            oplog_capacity: self.oplog_capacity,
            oplog_bytes: self.oplog_byte_budget() as u64,
            backlog_bytes: if self.peers.is_empty() {
                0
            } else {
                self.repl_backlog as u64
            },
            persist,
        })
    }

    /// Replication settings, if peers are configured. Reads the secret file.
    pub fn repl(&self) -> Result<Option<crate::replicate::ReplConfig>, String> {
        if self.peers.is_empty() {
            return Ok(None);
        }
        let secret = match &self.repl_secret_file {
            Some(path) => Some(crate::replicate::read_secret(path)?),
            None => None,
        };
        Ok(Some(crate::replicate::ReplConfig {
            node_id: self.node_id(),
            peers: self.peers.clone(),
            listen: self.listen.clone(),
            port: self
                .repl_port
                .expect("validated: --repl-port comes with --peer"),
            secret,
            sync_timeout: (self.sync_timeout > 0)
                .then(|| std::time::Duration::from_secs(self.sync_timeout)),
            force_standalone: self.force_standalone,
        }))
    }

    /// Memory tombstones may use: as given, or a tenth of the memory limit.
    pub fn tombstone_memory_bytes(&self) -> usize {
        self.tombstone_memory
            .unwrap_or_else(|| self.memory_bytes() / 10)
    }

    pub fn node_id(&self) -> u8 {
        self.node_id.unwrap_or(1)
    }

    /// The memory limit in bytes.
    pub fn memory_bytes(&self) -> usize {
        megabytes(self.memory_limit).unwrap_or(usize::MAX)
    }
}

fn megabytes(count: usize) -> Option<usize> {
    count.checked_mul(1024 * 1024)
}

/// Parses a count that must be at least 1.
fn parse_positive(text: &str) -> Result<usize, String> {
    match text.parse::<usize>() {
        Ok(0) => Err("must be at least 1".into()),
        Ok(count) => Ok(count),
        Err(error) => Err(error.to_string()),
    }
}

/// The longest tombstone lifetime, in seconds: 10 years.
const TOMBSTONE_TTL_MAX: usize = 315_360_000;

/// Parses a tombstone lifetime, which must be from 1 second to 10 years.
fn parse_tombstone_ttl(text: &str) -> Result<usize, String> {
    match parse_positive(text)? {
        ttl if ttl > TOMBSTONE_TTL_MAX => Err(format!(
            "must be at most {TOMBSTONE_TTL_MAX} seconds (10 years)"
        )),
        ttl => Ok(ttl),
    }
}

/// Parses an oplog capacity, which must be from 1 to `MAX_OPLOG_CAPACITY` records.
fn parse_oplog_capacity(text: &str) -> Result<usize, String> {
    match parse_positive(text)? {
        count if count > MAX_OPLOG_CAPACITY => Err(format!(
            "oplog capacity must be at most {MAX_OPLOG_CAPACITY} records"
        )),
        count => Ok(count),
    }
}

/// Parses a memory limit in megabytes: at least 1, and small enough to count in bytes.
fn parse_memory_limit(text: &str) -> Result<usize, String> {
    let megabytes_wanted = parse_positive(text)?;
    megabytes(megabytes_wanted)
        .map(|_| megabytes_wanted)
        .ok_or_else(|| format!("memory limit is too large: {text}"))
}

/// Parses a maximum item size, which must be at least 1 KiB.
fn parse_item_size(text: &str) -> Result<usize, String> {
    match parse_size(text)? {
        size if size < MIN_ITEM_SIZE => Err(format!(
            "maximum item size must be at least {MIN_ITEM_SIZE} bytes"
        )),
        size => Ok(size),
    }
}

/// Parses a log segment size, which must be at least 64 KiB.
fn parse_segment_size(text: &str) -> Result<usize, String> {
    match parse_size(text)? {
        size if size < 64 * 1024 => Err("segment size must be at least 64k".into()),
        size => Ok(size),
    }
}

/// Parses a peer's replication address, `HOST:PORT`. The host is resolved at each connect.
fn parse_peer(text: &str) -> Result<String, String> {
    let malformed = || format!("expected HOST:PORT, got {text}");
    let (host, port) = text.rsplit_once(':').ok_or_else(malformed)?;
    let bracketed = host.starts_with('[') && host.ends_with(']');
    if host.contains(':') && !bracketed {
        return Err(format!(
            "an IPv6 host must be bracketed, as [::1]:7000; got {text}"
        ));
    }
    match port.parse::<u16>() {
        Ok(port) if port > 0 && !host.is_empty() => Ok(text.to_string()),
        _ => Err(malformed()),
    }
}

/// Parses a listen address like `parse_peer`, but also allows port 0 (the system chooses).
fn parse_listen_address(text: &str) -> Result<String, String> {
    match text.rsplit_once(':') {
        Some((host, "0")) if !host.is_empty() => {
            parse_peer(&format!("{host}:1")).map(|_| text.into())
        }
        _ => parse_peer(text),
    }
}

/// Parses a byte count, as `parse_size` does, that must be at least 1.
fn parse_positive_size(text: &str) -> Result<usize, String> {
    match parse_size(text)? {
        0 => Err("must be at least 1".into()),
        size => Ok(size),
    }
}

/// Parses a byte count such as `4096`, `512k`, or `2m`.
pub fn parse_size(text: &str) -> Result<usize, String> {
    let (digits, multiplier) = match text.as_bytes().last() {
        Some(b'k' | b'K') => (&text[..text.len() - 1], 1024),
        Some(b'm' | b'M') => (&text[..text.len() - 1], 1024 * 1024),
        _ => (text, 1),
    };
    let count = digits
        .parse::<usize>()
        .map_err(|_| format!("invalid size: {text}"))?;
    count
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size is too large: {text}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn persistence_is_off_without_a_data_dir() {
        let config = Config::parse_from(["ferrited"]);
        assert!(config.persist().is_none());
        let config =
            Config::parse_from(["ferrited", "--data-dir", "/tmp/x", "--segment-size", "1m"]);
        let persist = config.persist().unwrap();
        assert_eq!(persist.segment_bytes, 1 << 20);
        assert_eq!(persist.fsync_interval, std::time::Duration::from_secs(1));
        assert!(
            Config::from_args(["ferrited", "--data-dir", "/tmp/x", "--segment-size", "1k"])
                .is_err()
        );
        let too_big = crate::persist::codec::MAX_VALUE + 1;
        let size = too_big.to_string();
        let args = ["ferrited", "-m", "4096", "-I", size.as_str()];
        assert!(Config::from_args(args).is_ok());
        let args = [
            "ferrited",
            "-m",
            "4096",
            "-I",
            size.as_str(),
            "--data-dir",
            "/tmp/x",
        ];
        assert!(Config::from_args(args).is_err());
    }

    #[test]
    fn persistence_options_require_a_data_dir() {
        // Their defaults apply without a data dir; only giving one explicitly is an error.
        let config = Config::from_args(["ferrited"]).unwrap();
        assert_eq!(config.fsync_interval_ms, 1000);
        assert_eq!(config.segment_size, 64 << 20);
        assert_eq!(config.oplog_capacity, 65536);
        for args in [
            ["--fsync-interval-ms", "50"],
            ["--segment-size", "1m"],
            ["--oplog-capacity", "1024"],
            ["--oplog-bytes", "8m"],
        ] {
            let message = rejected(&args);
            assert!(message.contains("--data-dir"), "{args:?}: {message}");
            let mut line = vec!["ferrited", "--data-dir", "/tmp/x"];
            line.extend_from_slice(&args);
            assert!(Config::from_args(line).is_ok(), "{args:?}");
        }
    }

    #[test]
    fn the_oplog_byte_budget_defaults_to_64m_or_four_items_and_holds_one_item() {
        let budget = |args: &[&str]| {
            let mut line = vec!["ferrited", "--data-dir", "/tmp/x"];
            line.extend_from_slice(args);
            Config::from_args(line).unwrap().log().unwrap().oplog_bytes
        };
        assert_eq!(budget(&[]), 64 << 20);
        assert_eq!(budget(&["-m", "1024", "-I", "32m"]), 128 << 20);
        assert_eq!(budget(&["--oplog-bytes", "2m"]), 2 << 20);
        let message = rejected(&["--data-dir", "/tmp/x", "-I", "2m", "--oplog-bytes", "1m"]);
        assert!(
            message.contains("at least the maximum item size"),
            "{message}"
        );
    }

    #[test]
    fn rejects_an_oplog_capacity_of_zero_or_above_the_cap() {
        let with_capacity = |capacity: &str| {
            Config::from_args([
                "ferrited",
                "--data-dir",
                "/tmp/x",
                "--oplog-capacity",
                capacity,
            ])
        };
        assert!(
            with_capacity("0")
                .unwrap_err()
                .to_string()
                .contains("at least 1")
        );
        let message = with_capacity(&(MAX_OPLOG_CAPACITY + 1).to_string())
            .unwrap_err()
            .to_string();
        assert!(message.contains("at most 16777216 records"), "{message}");
        let largest = with_capacity(&MAX_OPLOG_CAPACITY.to_string()).unwrap();
        assert_eq!(largest.log().unwrap().oplog_capacity, MAX_OPLOG_CAPACITY);
    }

    #[test]
    fn defaults_match_memcached() {
        let config = Config::parse_from(["ferrited"]);
        assert_eq!(config.port, 11211);
        assert_eq!(config.listen, "127.0.0.1");
        assert_eq!(config.memory_limit, 64);
        assert_eq!(config.conn_limit, 1024);
        assert_eq!(config.max_item_size, 1 << 20);
        assert_eq!(config.node_id(), 1);
        assert!(!config.enable_shutdown);
        assert!(config.socket.is_none() && config.threads.is_none());
    }

    #[test]
    fn accepts_memcached_style_flags() {
        let config = Config::parse_from([
            "ferrited", "-p", "11311", "-l", "0.0.0.0", "-m", "256", "-c", "50", "-I", "2m", "-t",
            "8", "-A",
        ]);
        assert_eq!(
            (config.port, config.memory_limit, config.conn_limit),
            (11311, 256, 50)
        );
        assert_eq!(config.listen, "0.0.0.0");
        assert_eq!(config.max_item_size, 2 << 20);
        assert_eq!(config.threads, Some(8));
        assert!(config.enable_shutdown);
    }

    #[test]
    fn parses_sizes_with_suffixes() {
        assert_eq!(parse_size("512k"), Ok(512 * 1024));
        assert_eq!(parse_size("2M"), Ok(2 * 1024 * 1024));
        assert_eq!(parse_size("4096"), Ok(4096));
        assert!(parse_size("big").is_err());
        assert!(parse_size("").is_err());
    }

    fn rejected(args: &[&str]) -> String {
        let mut line = vec!["ferrited"];
        line.extend_from_slice(args);
        Config::from_args(line).unwrap_err().to_string()
    }

    #[test]
    fn from_args_accepts_a_valid_command_line() {
        let config =
            Config::from_args(["ferrited", "-m", "1", "-I", "1m", "-c", "1", "-t", "1"]).unwrap();
        assert_eq!(config.memory_bytes(), 1 << 20);
        assert_eq!(config.max_item_size, 1 << 20);
        assert_eq!((config.conn_limit, config.threads), (1, Some(1)));
        assert_eq!(
            Config::from_args(["ferrited"]).unwrap().memory_bytes(),
            64 << 20
        );
    }

    #[test]
    fn the_runtime_gets_at_most_four_threads() {
        let threads = |args: &[&str]| {
            let mut all = vec!["ferrited"];
            all.extend_from_slice(args);
            let config = Config::from_args(all).unwrap();
            (config.client_threads(), config.runtime_threads())
        };
        assert_eq!(threads(&["-t", "1"]), (1, 1));
        assert_eq!(threads(&["-t", "4"]), (4, 4));
        assert_eq!(threads(&["-t", "16"]), (16, 4));
        let (clients, runtime) = threads(&[]);
        assert_eq!(runtime, clients.min(4));
    }

    #[test]
    fn rejects_zero_threads() {
        assert!(rejected(&["-t", "0"]).contains("at least 1"));
    }

    #[test]
    fn rejects_a_zero_connection_limit() {
        assert!(rejected(&["-c", "0"]).contains("at least 1"));
    }

    #[test]
    fn rejects_a_memory_limit_below_one_megabyte_or_too_large_to_count() {
        assert!(rejected(&["-m", "0"]).contains("at least 1"));
        let huge = usize::MAX.to_string();
        assert!(rejected(&["-m", &huge]).contains("too large"));
        let wraps_to_zero = (1u128 << (usize::BITS - 20)).to_string();
        assert!(rejected(&["-m", &wraps_to_zero]).contains("too large"));
    }

    #[test]
    fn rejects_an_item_size_below_one_kibibyte_or_too_large_to_count() {
        assert!(rejected(&["-I", "0"]).contains("at least 1024"));
        assert!(rejected(&["-I", "1023"]).contains("at least 1024"));
        assert_eq!(
            Config::from_args(["ferrited", "-I", "1k"])
                .unwrap()
                .max_item_size,
            1024
        );
        let wraps_to_zero = format!("{}m", 1u128 << (usize::BITS - 20));
        assert!(rejected(&["-I", &wraps_to_zero]).contains("too large"));
        assert!(parse_size(&format!("{}k", usize::MAX)).is_err());
    }

    #[test]
    fn rejects_an_item_size_above_the_memory_limit() {
        let message = rejected(&["-m", "1", "-I", "2m"]);
        assert!(message.contains("exceeds the memory limit"), "{message}");
        assert!(rejected(&["-I", "65m"]).contains("exceeds the memory limit"));
        assert!(Config::from_args(["ferrited", "-m", "2", "-I", "2m"]).is_ok());
    }

    fn peer_line<'a>(extra: &[&'a str]) -> Vec<&'a str> {
        let mut line = vec![
            "ferrited",
            "--peer",
            "10.0.0.2:7000",
            "--repl-port",
            "7000",
            "--node-id",
            "3",
        ];
        line.extend_from_slice(extra);
        line
    }

    #[test]
    fn the_secret_file_is_read_without_its_trailing_line_break() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("secret");
        std::fs::write(&path, "s3cret\n").unwrap();
        let path_text = path.to_str().unwrap();
        let config = Config::from_args(peer_line(&["--repl-secret-file", path_text])).unwrap();
        let repl = config.repl().unwrap().unwrap();
        assert_eq!(repl.secret.as_deref(), Some(&b"s3cret"[..]));
        assert_eq!((repl.node_id, repl.port), (3, 7000));
        std::fs::write(&path, "\n").unwrap();
        assert!(config.repl().unwrap_err().contains("is empty"));
        assert!(
            Config::from_args(["ferrited"])
                .unwrap()
                .repl()
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn peers_need_a_replication_port_and_an_explicit_node_id() {
        let message = rejected(&["--peer", "10.0.0.2:7000", "--node-id", "1"]);
        assert!(message.contains("--repl-port"), "{message}");
        let message = rejected(&["--peer", "10.0.0.2:7000", "--repl-port", "7000"]);
        assert!(message.contains("--node-id"), "{message}");
        let config = Config::from_args(peer_line(&["--peer", "b.example:7001"])).unwrap();
        assert_eq!(config.peers, ["10.0.0.2:7000", "b.example:7001"]);
        assert_eq!((config.node_id(), config.repl_port), (3, Some(7000)));
        assert_eq!(config.tombstone_ttl, 3600);
        let log = config.log().unwrap();
        assert!(log.persist.is_none());
        assert_eq!(log.backlog_bytes, 64 << 20);
    }

    #[test]
    fn the_replication_port_must_differ_from_the_client_port() {
        let message = rejected(&peer_line(&["-p", "7000"])[1..]);
        assert!(message.contains("must differ"), "{message}");
        // A client Unix socket leaves the TCP port free.
        assert!(Config::from_args(peer_line(&["-p", "7000", "-s", "/tmp/f.sock"])).is_ok());
        assert!(Config::from_args(peer_line(&["-p", "7001"])).is_ok());
    }

    #[test]
    fn the_metrics_address_is_a_host_and_a_port_apart_from_the_others() {
        assert!(
            Config::from_args(["ferrited"])
                .unwrap()
                .metrics_addr
                .is_none()
        );
        let config = Config::from_args(["ferrited", "--metrics-addr", "10.0.0.1:9150"]).unwrap();
        assert_eq!(config.metrics_addr.as_deref(), Some("10.0.0.1:9150"));
        assert_eq!(config.metrics_port(), Some(9150));
        assert!(Config::from_args(["ferrited", "--metrics-addr", "127.0.0.1:0"]).is_ok());
        assert!(Config::from_args(["ferrited", "--metrics-addr", "[::1]:9150"]).is_ok());
        for bad in ["9150", ":9150", "host:port", "::1:9150", "host:70000"] {
            let message = rejected(&["--metrics-addr", bad]);
            assert!(
                message.contains("HOST:PORT") || message.contains("bracketed"),
                "{bad}: {message}"
            );
        }
        let message = rejected(&["-p", "9150", "--metrics-addr", "10.0.0.1:9150"]);
        assert!(
            message.contains("metrics and client ports must differ"),
            "{message}"
        );
        // A client Unix socket leaves the TCP port free, and port 0 never clashes.
        let line = [
            "ferrited",
            "-p",
            "9150",
            "-s",
            "/tmp/f.sock",
            "--metrics-addr",
            "10.0.0.1:9150",
        ];
        assert!(Config::from_args(line).is_ok());
        assert!(
            Config::from_args(["ferrited", "-p", "0", "--metrics-addr", "127.0.0.1:0"]).is_ok()
        );
        let message = rejected(&peer_line(&["--metrics-addr", "10.0.0.1:7000"])[1..]);
        assert!(
            message.contains("metrics and replication ports must differ"),
            "{message}"
        );
        assert!(Config::from_args(peer_line(&["--metrics-addr", "10.0.0.1:7001"])).is_ok());
    }

    #[test]
    fn a_node_id_is_from_0_to_255_and_at_least_1_with_peers() {
        assert!(rejected(&["--node-id", "256"]).contains("256"));
        assert_eq!(
            Config::from_args(["ferrited", "--node-id", "255"])
                .unwrap()
                .node_id(),
            255
        );
        // Without peers, node ID 0 is accepted.
        assert_eq!(
            Config::from_args(["ferrited", "--node-id", "0"])
                .unwrap()
                .node_id(),
            0
        );
        let mut line = peer_line(&[]);
        *line.last_mut().unwrap() = "0";
        let message = Config::from_args(line).unwrap_err().to_string();
        assert!(message.contains("from 1 to 255 with --peer"), "{message}");
    }

    #[test]
    fn a_peer_given_twice_is_rejected() {
        let message = rejected(&peer_line(&["--peer", "10.0.0.2:7000"])[1..]);
        assert!(
            message.contains("--peer 10.0.0.2:7000 is given twice"),
            "{message}"
        );
        assert!(Config::from_args(peer_line(&["--peer", "10.0.0.2:7001"])).is_ok());
    }

    #[test]
    fn a_peer_is_a_host_and_a_port() {
        for bad in ["nohost", ":7000", "host:0", "host:port", "host:70000"] {
            let message = rejected(&["--peer", bad, "--repl-port", "7000", "--node-id", "1"]);
            assert!(message.contains("HOST:PORT"), "{bad}: {message}");
        }
        assert!(Config::from_args(peer_line(&["--peer", "[::1]:7001"])).is_ok());
        let message = rejected(&[
            "--peer",
            "::1:7000",
            "--repl-port",
            "7000",
            "--node-id",
            "1",
        ]);
        assert!(message.contains("bracketed"), "{message}");
    }

    #[test]
    fn a_tombstone_ttl_is_capped_at_ten_years() {
        let ok = peer_line(&["--tombstone-ttl", "315360000"]);
        assert_eq!(Config::from_args(ok).unwrap().tombstone_ttl, 315_360_000);
        let message = rejected(&peer_line(&["--tombstone-ttl", "315360001"])[1..]);
        assert!(message.contains("10 years"), "{message}");
        let message = rejected(&peer_line(&["--tombstone-ttl", "18446744073709551615"])[1..]);
        assert!(message.contains("10 years"), "{message}");
    }

    #[test]
    fn tombstone_memory_defaults_to_a_tenth_of_the_memory_limit() {
        let config = Config::from_args(peer_line(&["-m", "100"])).unwrap();
        assert_eq!(config.tombstone_memory_bytes(), 10 << 20);
        let config = Config::from_args(peer_line(&["--tombstone-memory", "512k"])).unwrap();
        assert_eq!(config.tombstone_memory_bytes(), 512 << 10);
    }

    #[test]
    fn tombstone_memory_is_from_1_byte_to_half_the_memory_limit() {
        let config = Config::from_args(peer_line(&["-m", "64", "--tombstone-memory", "32m"]));
        assert_eq!(config.unwrap().tombstone_memory_bytes(), 32 << 20);
        let message = rejected(&peer_line(&["-m", "64", "--tombstone-memory", "33m"])[1..]);
        assert!(message.contains("half the memory limit"), "{message}");
        let message = rejected(&peer_line(&["--tombstone-memory", "0"])[1..]);
        assert!(message.contains("at least 1"), "{message}");
    }

    #[test]
    fn an_item_must_fit_beside_the_tombstone_memory() {
        let line = peer_line(&["-m", "4", "-I", "3m", "--tombstone-memory", "1m"]);
        assert!(Config::from_args(line).is_ok());
        let line = peer_line(&["-m", "4", "-I", "3m", "--tombstone-memory", "1025k"]);
        let message = rejected(&line[1..]);
        assert!(message.contains("less the tombstone memory"), "{message}");
        // Without peers there are no tombstones to leave room for.
        assert!(Config::from_args(["ferrited", "-m", "1", "-I", "1m"]).is_ok());
    }

    #[test]
    fn replication_options_require_a_peer() {
        for args in [
            ["--repl-port", "7000"],
            ["--repl-secret-file", "/tmp/secret"],
            ["--repl-backlog", "1m"],
            ["--tombstone-ttl", "60"],
            ["--tombstone-memory", "1m"],
        ] {
            let message = rejected(&args);
            assert!(message.contains("--peer"), "{args:?}: {message}");
        }
    }

    #[test]
    fn readiness_options_require_a_peer_and_the_timeout_defaults_to_60s() {
        assert!(rejected(&["--sync-timeout", "5"]).contains("--peer"));
        assert!(rejected(&["--force-standalone"]).contains("--peer"));
        let repl = Config::from_args(peer_line(&[]))
            .unwrap()
            .repl()
            .unwrap()
            .unwrap();
        assert_eq!(repl.sync_timeout, Some(std::time::Duration::from_secs(60)));
        assert!(!repl.force_standalone);
        let config = Config::from_args(peer_line(&["--sync-timeout", "0", "--force-standalone"]));
        let repl = config.unwrap().repl().unwrap().unwrap();
        // 0 waits forever.
        assert_eq!(repl.sync_timeout, None);
        assert!(repl.force_standalone);
        assert!(rejected(&peer_line(&["--sync-timeout", "soon"])[1..]).contains("soon"));
    }

    #[test]
    fn the_backlog_must_hold_an_item_and_items_must_fit_the_log() {
        let message = rejected(&peer_line(&["-I", "2m", "--repl-backlog", "1m"])[1..]);
        assert!(message.contains("replication backlog"), "{message}");
        let too_big = (crate::persist::codec::MAX_VALUE + 1).to_string();
        let message = rejected(&peer_line(&["-m", "4096", "-I", too_big.as_str()])[1..]);
        assert!(message.contains("the log can hold"), "{message}");
    }

    #[test]
    fn the_log_runs_with_persistence_or_peers_and_the_oplog_options_need_one() {
        assert!(Config::from_args(["ferrited"]).unwrap().log().is_none());
        let persisted = Config::from_args(["ferrited", "--data-dir", "/tmp/x"]).unwrap();
        assert_eq!(persisted.log().unwrap().backlog_bytes, 0);
        assert!(Config::from_args(peer_line(&["--oplog-capacity", "1024"])).is_ok());
    }
}
