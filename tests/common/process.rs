//! Starting and stopping the `ferrited` binary, and talking to it with blocking sockets.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// How long a server may take to start or to stop.
const PATIENCE: Duration = Duration::from_secs(30);

pub struct Process {
    child: Mutex<Option<Child>>,
    /// The client port, once the server listens on it; 0 before.
    pub port: u16,
    /// Every line the server has written to stderr.
    logs: Arc<Mutex<Vec<String>>>,
    /// Receives the client port when the server reports it.
    listening: Mutex<Option<mpsc::Receiver<u16>>>,
}

impl Process {
    /// Starts the binary on a free port (unless `args` has `-p`) and waits until it listens.
    pub fn start(args: &[&str]) -> Self {
        Self::try_start(args).unwrap_or_else(|error| panic!("{error}"))
    }

    /// Like `start`, but returns the server's stderr if it exits before listening.
    ///
    /// Retries a start that macOS killed because a concurrent `cargo` was replacing the binary.
    pub fn try_start(args: &[&str]) -> Result<Self, String> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let started = Self::launch(args, &[]).and_then(|mut process| {
                match process.await_port(PATIENCE)? {
                    true => Ok(process),
                    // Dropping it kills it.
                    false => Err(Launch::Failed("server did not start".into())),
                }
            });
            match started {
                Err(Launch::Replaced(error)) if attempts < 5 => {
                    eprintln!(
                        "retrying a start that failed while the binary was replaced: {error}"
                    );
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(Launch::Replaced(error) | Launch::Failed(error)) => return Err(error),
                Ok(process) => return Ok(process),
            }
        }
    }

    /// Starts the binary with `args` and env `vars` without waiting; see `wait_listening`.
    pub fn spawn(args: &[&str], vars: &[(&str, &str)]) -> Self {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match Self::launch(args, vars) {
                Ok(process) => return process,
                Err(Launch::Replaced(error)) if attempts < 5 => {
                    eprintln!(
                        "retrying a start that failed while the binary was replaced: {error}"
                    );
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(Launch::Replaced(error) | Launch::Failed(error)) => panic!("{error}"),
            }
        }
    }

    /// Waits up to `timeout` for the client port: `Ok(false)` if not yet listening, the
    /// server's stderr if it exited.
    pub fn wait_listening(&mut self, timeout: Duration) -> Result<bool, String> {
        self.await_port(timeout)
            .map_err(|(Launch::Replaced(error) | Launch::Failed(error))| error)
    }

    fn launch(args: &[&str], vars: &[(&str, &str)]) -> Result<Self, Launch> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ferrited"));
        if !args.contains(&"-p") {
            command.args(["-p", "0"]);
        }
        let spawned = spawn(
            command
                .args(args)
                .envs(vars.iter().copied())
                .stderr(Stdio::piped()),
        );
        let mut child = match spawned {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(Launch::Replaced(format!(
                    "cannot start the server: {error}"
                )));
            }
            Err(error) => panic!("cannot start the server: {error}"),
        };
        let stderr = child.stderr.take().unwrap();
        let logs = Arc::new(Mutex::new(Vec::new()));
        let (found, listening) = mpsc::channel();
        {
            let logs = logs.clone();
            std::thread::spawn(move || {
                let mut found = Some(found);
                for line in BufReader::new(stderr).lines() {
                    let Ok(line) = line else { break };
                    let port = listening_port(&line);
                    eprintln!("{line}");
                    logs.lock().unwrap().push(line);
                    if let Some(port) = port
                        && let Some(found) = found.take()
                    {
                        let _ = found.send(port);
                    }
                }
            });
        }
        Ok(Self {
            child: Mutex::new(Some(child)),
            port: 0,
            logs,
            listening: Mutex::new(Some(listening)),
        })
    }

    /// Like `wait_listening`, keeping why a start failed.
    fn await_port(&mut self, timeout: Duration) -> Result<bool, Launch> {
        if self.port != 0 {
            return Ok(true);
        }
        let Some(listening) = self.listening.get_mut().unwrap().as_ref() else {
            return Ok(false);
        };
        let deadline = Instant::now() + timeout;
        loop {
            match listening.recv_timeout(Duration::from_millis(20)) {
                Ok(port) => {
                    self.port = port;
                    return Ok(true);
                }
                Err(RecvTimeoutError::Timeout) => {
                    if Instant::now() > deadline {
                        return Ok(false);
                    }
                }
                // Stderr closed: the server exited.
                Err(RecvTimeoutError::Disconnected) => {
                    let child = self.child.get_mut().unwrap().as_mut().unwrap();
                    let status = child.wait().unwrap();
                    let logs = self.logs.lock().unwrap();
                    let error = format!(
                        "server exited before it listened: {status}\n{}",
                        logs.join("\n")
                    );
                    // Killed before it wrote a line: see `try_start`.
                    if logs.is_empty() && status.signal() == Some(9) {
                        return Err(Launch::Replaced(error));
                    }
                    return Err(Launch::Failed(error));
                }
            }
        }
    }

    /// Whether the server has written a line containing `text` to stderr.
    pub fn logged(&self, text: &str) -> bool {
        self.logs
            .lock()
            .unwrap()
            .iter()
            .any(|line| line.contains(text))
    }

    /// How many lines containing `text` the server has written to stderr.
    pub fn log_count(&self, text: &str) -> usize {
        self.logs
            .lock()
            .unwrap()
            .iter()
            .filter(|line| line.contains(text))
            .count()
    }

    /// Waits up to `timeout` for the server to write a line containing `text` to stderr.
    pub fn wait_for_log(&self, text: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while !self.logged(text) {
            if Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        true
    }

    /// Waits up to `timeout` for a stderr line starting with `prefix`; returns the rest of it.
    pub fn wait_for_line(&self, prefix: &str, timeout: Duration) -> Option<String> {
        let deadline = Instant::now() + timeout;
        loop {
            let found = self
                .logs
                .lock()
                .unwrap()
                .iter()
                .find_map(|line| line.strip_prefix(prefix).map(str::to_string));
            if found.is_some() || Instant::now() > deadline {
                return found;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn connect(&self) -> TcpStream {
        let deadline = Instant::now() + PATIENCE;
        loop {
            match TcpStream::connect(("127.0.0.1", self.port)) {
                Ok(stream) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(10)))
                        .unwrap();
                    return stream;
                }
                Err(error) if Instant::now() > deadline => panic!("cannot connect: {error}"),
                Err(_) => {
                    if let Some(child) = self.child.lock().unwrap().as_mut()
                        && let Some(status) = child.try_wait().unwrap()
                    {
                        panic!("server exited: {status}");
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }

    fn take_child(&mut self) -> Child {
        self.child.get_mut().unwrap().take().unwrap()
    }

    /// Kills the process with SIGKILL, as a crash would.
    pub fn kill(mut self) {
        let mut child = self.take_child();
        child.kill().unwrap();
        child.wait().unwrap();
    }

    /// Sends SIGTERM and waits for a clean exit.
    pub fn terminate(mut self) -> ExitStatus {
        self.signal("TERM");
        let mut child = self.take_child();
        wait_for_exit(&mut child, PATIENCE).expect("server did not exit after SIGTERM")
    }

    /// Sends the signal `name`, such as `TERM` or `INT`, without waiting.
    pub fn signal(&mut self, name: &str) {
        let id = self.child.get_mut().unwrap().as_ref().unwrap().id();
        let status = Command::new("kill")
            .args([format!("-{name}"), id.to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    }

    /// Waits up to `timeout` for exit; on timeout kills the process and returns `None`.
    pub fn wait(mut self, timeout: Duration) -> Option<ExitStatus> {
        wait_for_exit(&mut self.take_child(), timeout)
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Ok(Some(child)) = self.child.get_mut().map(Option::as_mut) {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Why a server did not start.
enum Launch {
    /// It could not run because the binary was being replaced.
    Replaced(String),
    Failed(String),
}

/// The port in the binary's `ferrited listening on <address>:<port>` line.
fn listening_port(line: &str) -> Option<u16> {
    let address = line.strip_prefix("ferrited listening on ")?;
    address.rsplit_once(':')?.1.parse().ok()
}

/// A port that was free a moment ago; it may be taken before use, so callers must retry.
pub fn free_port() -> u16 {
    let _probing = PROBING.lock().unwrap_or_else(PoisonError::into_inner);
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Spawns `command` while no `free_port` probe is open. Use it for every child process.
pub fn spawn(command: &mut Command) -> std::io::Result<Child> {
    let _probing = PROBING.lock().unwrap_or_else(PoisonError::into_inner);
    // `spawn` returns after exec, so the child holds no copy of a probe.
    command.spawn()
}

/// Held during a `free_port` probe and a `spawn`. A child forked mid-probe keeps the listener
/// open until exec, so an early client is accepted, then reset (seen on Linux and macOS).
static PROBING: Mutex<()> = Mutex::new(());

/// Waits up to `timeout` for `child` to exit; on timeout kills it and returns `None`.
pub fn wait_for_exit(child: &mut Child, timeout: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Sends `input` followed by `mn` and returns everything before the `MN` reply.
pub fn request(stream: &mut TcpStream, input: &str) -> String {
    stream.write_all(input.as_bytes()).unwrap();
    stream.write_all(b"mn\r\n").unwrap();
    let mut received = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    while !received.ends_with(b"MN\r\n") {
        let read = stream.read(&mut chunk).unwrap();
        assert!(
            read > 0,
            "connection closed: {:?}",
            String::from_utf8_lossy(&received)
        );
        received.extend_from_slice(&chunk[..read]);
    }
    received.truncate(received.len() - 4);
    String::from_utf8(received).unwrap()
}

/// The value of one `stats` line.
pub fn stat(stream: &mut TcpStream, name: &str) -> String {
    let reply = request(stream, "stats\r\n");
    let prefix = format!("STAT {name} ");
    reply
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("no {name} in {reply}"))
        .to_string()
}

/// Whether nothing accepts connections on `port`.
pub fn refused(port: u16) -> bool {
    TcpStream::connect(("127.0.0.1", port)).is_err()
}

/// Sends `status` to replication port `port` and returns its `(name, value)` lines.
pub fn repl_status(port: u16) -> Vec<(String, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream.write_all(b"status\r\n").unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).unwrap();
    assert!(reply.ends_with("END\r\n"), "{reply}");
    reply
        .lines()
        .filter_map(|line| line.strip_prefix("STAT "))
        .filter_map(|line| line.split_once(' '))
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}
