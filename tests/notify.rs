//! Notifications to a service manager, such as systemd, through `NOTIFY_SOCKET`.

mod common;

use std::net::TcpStream;
use std::os::unix::net::UnixDatagram;
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use common::process::{Process, free_port};

/// How long a notification may take to arrive.
const PATIENCE: Duration = Duration::from_secs(30);

/// A notification, and whether the checked client port accepted a connection on arrival.
struct Notification {
    text: String,
    accepted: Option<bool>,
}

/// Stands in for the service manager's socket, probing the client port as each notification
/// arrives. A refusal proves the port was closed at send time; an acceptance proves nothing.
struct Manager {
    path: std::path::PathBuf,
    received: Receiver<Notification>,
    _dir: tempfile::TempDir,
}

impl Manager {
    /// Listens for notifications, probing `client_port`, or with `None` the port a `READY=1`
    /// status names.
    fn new(client_port: Option<u16>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notify");
        let socket = UnixDatagram::bind(&path).unwrap();
        let (sender, received) = mpsc::channel();
        std::thread::spawn(move || {
            let mut buffer = [0u8; 4096];
            while let Ok(length) = socket.recv(&mut buffer) {
                let text = String::from_utf8_lossy(&buffer[..length]).into_owned();
                let port = client_port.or_else(|| serving_port(&text));
                let accepted = port.map(|port| TcpStream::connect(("127.0.0.1", port)).is_ok());
                if sender.send(Notification { text, accepted }).is_err() {
                    break;
                }
            }
        });
        Self {
            path,
            received,
            _dir: dir,
        }
    }

    fn vars(&self) -> Vec<(&str, &str)> {
        vec![("NOTIFY_SOCKET", self.path.to_str().unwrap())]
    }

    /// Waits for the next notification.
    fn next(&self) -> Notification {
        self.received
            .recv_timeout(PATIENCE)
            .expect("no notification arrived")
    }

    /// Waits for a notification holding `line`; returns it and every notification before it.
    fn until(&self, line: &str) -> (Notification, Vec<Notification>) {
        let mut before = Vec::new();
        loop {
            let notification = self.next();
            if notification.text.lines().any(|each| each == line) {
                return (notification, before);
            }
            before.push(notification);
        }
    }
}

/// The port in a `READY=1` notification's `STATUS=serving clients on <address>:<port>`.
fn serving_port(text: &str) -> Option<u16> {
    if !text.lines().any(|line| line == "READY=1") {
        return None;
    }
    let status = text.lines().find_map(|line| line.strip_prefix("STATUS="))?;
    let address = status.strip_prefix("serving clients on ")?;
    address.rsplit_once(':')?.1.parse().ok()
}

fn is_ready(notification: &Notification) -> bool {
    notification.text.lines().any(|line| line == "READY=1")
}

fn is_waiting(notification: &Notification) -> bool {
    notification
        .text
        .starts_with("STATUS=not serving clients yet:")
}

#[test]
fn a_node_says_it_is_ready_once_it_serves_and_that_it_is_stopping_on_sigterm() {
    let manager = Manager::new(None);
    let dir = tempfile::tempdir().unwrap();
    let mut node = Process::spawn(
        &["-p", "0", "--data-dir", dir.path().to_str().unwrap()],
        &manager.vars(),
    );
    let (ready, _) = manager.until("READY=1");
    assert_eq!(ready.accepted, Some(true), "{}", ready.text);
    assert!(node.wait_listening(PATIENCE).unwrap());
    assert_eq!(serving_port(&ready.text), Some(node.port));
    node.signal("TERM");
    let (stopping, _) = manager.until("STOPPING=1");
    assert!(stopping.text.contains("STATUS="), "{}", stopping.text);
    assert!(node.wait(PATIENCE).unwrap().success());
    assert!(dir.path().join("CLEAN").exists());
}

#[test]
fn a_node_waiting_for_its_peers_reports_why_and_says_it_is_ready_only_once_it_serves() {
    let (client, repl) = loop {
        let (client, repl) = (free_port(), free_port());
        if client != repl {
            break (client, repl);
        }
    };
    let manager = Manager::new(Some(client));
    let dir = tempfile::tempdir().unwrap();
    let (client_text, repl_text) = (client.to_string(), repl.to_string());
    // Its only peer never answers, so it serves only after the sync timeout.
    let node = Process::spawn(
        &[
            "-p",
            &client_text,
            "--node-id",
            "1",
            "--repl-port",
            &repl_text,
            "--peer",
            "127.0.0.1:1",
            "--sync-timeout",
            "3",
            "--data-dir",
            dir.path().to_str().unwrap(),
        ],
        &manager.vars(),
    );
    let (ready, before) = manager.until("READY=1");
    let waiting: Vec<&Notification> = before
        .iter()
        .filter(|notification| is_waiting(notification))
        .collect();
    // The first waiting status arrives seconds before the sync timeout, so the port is surely
    // closed. Later ones may race serving, so only their order is checked.
    let first = waiting.first().expect("no waiting status before READY=1");
    assert!(
        waiting.iter().any(|notification| notification
            .text
            .starts_with("STATUS=not serving clients yet: no peer is reachable")),
        "no waiting status names the unreachable peer"
    );
    assert_eq!(
        first.accepted,
        Some(false),
        "the client port accepted while the node waited: {}",
        first.text
    );
    // The port is bound before READY=1 is sent, so this check is exact.
    assert_eq!(ready.accepted, Some(true), "{}", ready.text);
    assert!(node.logged("no peer was reachable for 3s"));
    assert!(node.terminate().success());
    let (stopping, after) = manager.until("STOPPING=1");
    assert!(!is_ready(&stopping), "{}", stopping.text);
    for notification in &after {
        assert!(
            !is_waiting(notification),
            "a waiting status after READY=1: {}",
            notification.text
        );
    }
}
