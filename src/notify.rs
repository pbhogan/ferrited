//! `sd_notify` support: each notification is one datagram of `KEY=VALUE` lines sent to the
//! Unix socket in `NOTIFY_SOCKET`, if set. Send failures are logged once and never fatal.

use std::ffi::OsStr;
use std::io;
use std::os::unix::net::{SocketAddr, UnixDatagram};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::time::Duration;

/// The environment variable naming the service manager's socket.
pub const NOTIFY_SOCKET: &str = "NOTIFY_SOCKET";

/// How long a notification may wait for room in the service manager's socket.
const SEND_TIMEOUT: Duration = Duration::from_secs(1);

/// Sends notifications to the socket in `NOTIFY_SOCKET`, if any.
pub struct Notifier {
    /// `NOTIFY_SOCKET` as given, for the log; `None` when notifications are off.
    shown: Option<String>,
    /// The socket to send from and the address to send to, or why there are none.
    target: Result<(UnixDatagram, SocketAddr), String>,
    warned: AtomicBool,
}

impl Notifier {
    /// `address` is an absolute path or, on Linux, an abstract name after `@`. `None` or an
    /// empty address turns notifications off.
    pub fn new(address: Option<&OsStr>) -> Self {
        let Some(address) = address.filter(|address| !address.is_empty()) else {
            return Self {
                shown: None,
                target: Err("notifications are off".into()),
                warned: AtomicBool::new(false),
            };
        };
        Self {
            shown: Some(address.to_string_lossy().into_owned()),
            target: connect(address).map_err(|error| error.to_string()),
            warned: AtomicBool::new(false),
        }
    }

    /// Reads but does not remove `NOTIFY_SOCKET`; ferrited starts no child that could inherit it.
    pub fn from_env() -> Self {
        Self::new(std::env::var_os(NOTIFY_SOCKET).as_deref())
    }

    /// Whether `NOTIFY_SOCKET` named a socket.
    pub fn enabled(&self) -> bool {
        self.shown.is_some()
    }

    /// Sends `READY=1` with `status`.
    pub fn ready(&self, status: &str) -> bool {
        self.send(&format!("READY=1\nSTATUS={}", one_line(status)))
    }

    pub fn status(&self, status: &str) -> bool {
        self.send(&format!("STATUS={}", one_line(status)))
    }

    /// Sends `STOPPING=1` with `status`.
    pub fn stopping(&self, status: &str) -> bool {
        self.send(&format!("STOPPING=1\nSTATUS={}", one_line(status)))
    }

    /// Sends `message` as one datagram, returning whether it was sent.
    pub fn send(&self, message: &str) -> bool {
        let Some(shown) = &self.shown else {
            return false;
        };
        let sent = match &self.target {
            Ok((socket, address)) => match socket.send_to_addr(message.as_bytes(), address) {
                Ok(_) => return true,
                Err(error) => error.to_string(),
            },
            Err(error) => error.clone(),
        };
        if !self.warned.swap(true, Relaxed) {
            eprintln!(
                "ferrited: cannot notify the service manager at {NOTIFY_SOCKET}={shown}: {sent}; \
                 carrying on without notifications"
            );
        }
        false
    }
}

/// The notifier for this process, from `NOTIFY_SOCKET` as it was at the first call.
pub fn global() -> &'static Notifier {
    static NOTIFIER: OnceLock<Notifier> = OnceLock::new();
    NOTIFIER.get_or_init(Notifier::from_env)
}

fn connect(address: &OsStr) -> io::Result<(UnixDatagram, SocketAddr)> {
    let target = socket_address(address)?;
    let socket = UnixDatagram::unbound()?;
    socket.set_write_timeout(Some(SEND_TIMEOUT))?;
    Ok((socket, target))
}

fn socket_address(address: &OsStr) -> io::Result<SocketAddr> {
    use std::os::unix::ffi::OsStrExt;

    let bytes = address.as_bytes();
    match bytes.first() {
        Some(b'/') => SocketAddr::from_pathname(address),
        Some(b'@') => abstract_address(&bytes[1..]),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not an absolute path or an abstract socket name",
        )),
    }
}

#[cfg(target_os = "linux")]
fn abstract_address(name: &[u8]) -> io::Result<SocketAddr> {
    use std::os::linux::net::SocketAddrExt;

    SocketAddr::from_abstract_name(name)
}

#[cfg(not(target_os = "linux"))]
fn abstract_address(_name: &[u8]) -> io::Result<SocketAddr> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "abstract socket names exist only on Linux",
    ))
}

/// Replaces line breaks with spaces, since each line of a notification is one setting.
fn one_line(text: &str) -> String {
    text.replace(['\r', '\n'], " ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The directory must outlive the socket.
    fn receiver() -> (UnixDatagram, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let socket = UnixDatagram::bind(dir.path().join("notify")).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        (socket, dir)
    }

    fn received(socket: &UnixDatagram) -> String {
        let mut buffer = [0u8; 4096];
        let length = socket.recv(&mut buffer).unwrap();
        String::from_utf8(buffer[..length].to_vec()).unwrap()
    }

    fn notifier_for(dir: &tempfile::TempDir) -> Notifier {
        Notifier::new(Some(dir.path().join("notify").as_os_str()))
    }

    #[test]
    fn without_an_address_nothing_is_sent() {
        for notifier in [Notifier::new(None), Notifier::new(Some(OsStr::new("")))] {
            assert!(!notifier.enabled());
            assert!(!notifier.ready("serving"));
            assert!(!notifier.warned.load(Relaxed));
        }
    }

    #[test]
    fn each_notification_is_one_datagram_of_settings() {
        let (socket, dir) = receiver();
        let notifier = notifier_for(&dir);
        assert!(notifier.enabled());
        assert!(notifier.status("not serving clients yet: a peer is still loading its data"));
        assert!(notifier.ready("serving clients on 127.0.0.1:11211"));
        assert!(notifier.stopping("stopping"));
        assert_eq!(
            received(&socket),
            "STATUS=not serving clients yet: a peer is still loading its data"
        );
        assert_eq!(
            received(&socket),
            "READY=1\nSTATUS=serving clients on 127.0.0.1:11211"
        );
        assert_eq!(received(&socket), "STOPPING=1\nSTATUS=stopping");
    }

    #[test]
    fn a_status_with_line_breaks_stays_one_setting() {
        let (socket, dir) = receiver();
        assert!(notifier_for(&dir).status("two\nlines\r\n"));
        assert_eq!(received(&socket), "STATUS=two lines  ");
    }

    #[test]
    fn a_socket_nobody_listens_on_fails_without_stopping_anything_and_is_logged_once() {
        let dir = tempfile::tempdir().unwrap();
        let notifier = notifier_for(&dir);
        assert!(notifier.enabled());
        assert!(!notifier.ready("serving"));
        assert!(notifier.warned.load(Relaxed));
        // Later failures are not logged again, and a socket that appears is used.
        assert!(!notifier.stopping("stopping"));
        let socket = UnixDatagram::bind(dir.path().join("notify")).unwrap();
        assert!(notifier.status("back"));
        let mut buffer = [0u8; 64];
        let length = socket.recv(&mut buffer).unwrap();
        assert_eq!(&buffer[..length], b"STATUS=back");
    }

    #[test]
    fn an_address_that_is_neither_a_path_nor_abstract_is_refused() {
        for address in ["relative/path", "vsock:2:1234"] {
            let notifier = Notifier::new(Some(OsStr::new(address)));
            assert!(notifier.enabled());
            assert!(!notifier.ready("serving"));
            assert!(notifier.warned.load(Relaxed));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn an_abstract_socket_name_is_used_on_linux() {
        use std::os::linux::net::SocketAddrExt;

        let name = format!("ferrited-notify-test-{}", std::process::id());
        let address = SocketAddr::from_abstract_name(name.as_bytes()).unwrap();
        let socket = UnixDatagram::bind_addr(&address).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let notifier = Notifier::new(Some(OsStr::new(&format!("@{name}"))));
        assert!(notifier.ready("serving"));
        assert_eq!(received(&socket), "READY=1\nSTATUS=serving");
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn an_abstract_socket_name_is_refused_off_linux() {
        let notifier = Notifier::new(Some(OsStr::new("@ferrited-notify-test")));
        assert!(!notifier.ready("serving"));
        assert!(notifier.warned.load(Relaxed));
    }
}
