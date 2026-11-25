mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::process::{free_port, spawn};

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn connect(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(stream) => return stream,
            Err(error) if Instant::now() > deadline => panic!("server did not start: {error}"),
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

fn read_exact_text(stream: &mut TcpStream, len: usize) -> String {
    let mut reply = vec![0u8; len];
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.read_exact(&mut reply).unwrap();
    String::from_utf8(reply).unwrap()
}

#[test]
fn binary_serves_expires_in_background_and_shuts_down() {
    let port = free_port();
    let child =
        spawn(Command::new(env!("CARGO_BIN_EXE_ferrited")).args(["-p", &port.to_string(), "-A"]))
            .unwrap();
    let mut server = Server(child);
    let mut stream = connect(port);

    stream.write_all(b"set k 0 2 1\r\na\r\nget k\r\n").unwrap();
    let expected = "STORED\r\nVALUE k 0 1\r\na\r\nEND\r\n";
    assert_eq!(read_exact_text(&mut stream, expected.len()), expected);

    // Swept without being read. TTL 2 s, sweep every 1 s, so allow 6 s.
    let deadline = Instant::now() + Duration::from_secs(6);
    while stat(&mut stream, "curr_items") != "0" {
        assert!(Instant::now() < deadline, "the expired item was not swept");
        std::thread::sleep(Duration::from_millis(100));
    }

    // An unknown mode is rejected and the server keeps running.
    stream.write_all(b"shutdown bogus\r\n").unwrap();
    let expected = "CLIENT_ERROR invalid shutdown mode\r\n";
    assert_eq!(read_exact_text(&mut stream, expected.len()), expected);

    stream.write_all(b"shutdown\r\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = server.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "server did not exit after shutdown"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Starts the server from a shell after running `setup`, with stderr captured.
#[cfg(unix)]
fn spawn_after(setup: &str, args: &str) -> Server {
    let child = spawn(
        Command::new("sh")
            .arg("-c")
            .arg(format!("{setup} && exec \"$0\" {args}"))
            .arg(env!("CARGO_BIN_EXE_ferrited"))
            .stderr(Stdio::piped()),
    )
    .unwrap();
    Server(child)
}

fn stat(stream: &mut TcpStream, name: &str) -> String {
    stream.write_all(b"stats\r\n").unwrap();
    let mut stats = String::new();
    while !stats.ends_with("END\r\n") {
        stats.push_str(&read_exact_text(stream, 1));
    }
    let prefix = format!("STAT {name} ");
    let line = stats.lines().find(|line| line.starts_with(&prefix));
    line.unwrap_or_else(|| panic!("no {name} in {stats}"))[prefix.len()..].to_string()
}

#[cfg(unix)]
fn stop_and_read_stderr(mut server: Server) -> String {
    let _ = server.0.kill();
    let mut text = String::new();
    server
        .0
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut text)
        .unwrap();
    text
}

#[cfg(unix)]
#[test]
fn a_low_soft_descriptor_limit_is_raised_to_cover_the_connection_limit() {
    let port = free_port();
    let server = spawn_after("ulimit -S -n 64", &format!("-p {port} -c 200"));
    // More connections than 64 descriptors allow.
    let mut streams: Vec<TcpStream> = (0..150).map(|_| connect(port)).collect();
    for stream in &mut streams {
        stream.write_all(b"mn\r\n").unwrap();
        assert_eq!(read_exact_text(stream, 4), "MN\r\n");
    }
    assert_eq!(stat(&mut streams[0], "max_connections"), "200");
    let stderr = stop_and_read_stderr(server);
    assert!(!stderr.contains("warning"), "{stderr}");
    assert!(!stderr.contains("accept failed"), "{stderr}");
}

#[cfg(unix)]
#[test]
fn a_low_hard_descriptor_limit_lowers_the_connection_limit_with_a_warning() {
    let port = free_port();
    // 64 descriptors, less 32 plus 2 for each of 2 event loops and the acceptor.
    let server = spawn_after("ulimit -n 64", &format!("-p {port} -c 200 -t 2"));
    let mut stream = connect(port);
    assert_eq!(stat(&mut stream, "max_connections"), "26");
    let stderr = stop_and_read_stderr(server);
    let warnings: Vec<&str> = stderr
        .lines()
        .filter(|line| line.contains("warning"))
        .collect();
    assert_eq!(warnings.len(), 1, "{stderr}");
    assert!(
        warnings[0].contains("26") && warnings[0].contains("200"),
        "{stderr}"
    );
}

#[cfg(unix)]
#[test]
fn the_unix_socket_file_is_removed_on_clean_exit() {
    use std::os::unix::net::UnixStream;

    let dir = std::env::temp_dir().join(format!("ferrited-{}-exit", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("s");
    let child = spawn(
        Command::new(env!("CARGO_BIN_EXE_ferrited"))
            .arg("-s")
            .arg(&path)
            .arg("-A"),
    )
    .unwrap();
    let mut server = Server(child);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut stream = loop {
        match UnixStream::connect(&path) {
            Ok(stream) => break stream,
            Err(error) if Instant::now() > deadline => panic!("server did not start: {error}"),
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(b"set k 0 0 1\r\nv\r\nget k\r\n").unwrap();
    let expected = b"STORED\r\nVALUE k 0 1\r\nv\r\nEND\r\n";
    let mut reply = vec![0u8; expected.len()];
    stream.read_exact(&mut reply).unwrap();
    assert_eq!(reply, expected);
    stream.write_all(b"shutdown graceful\r\n").unwrap();
    let status = loop {
        if let Some(status) = server.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "server did not exit after shutdown"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success());
    let left_behind = path.exists();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(!left_behind, "socket file was left behind");
}
