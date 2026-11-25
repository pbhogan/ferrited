use clap::Parser;

use super::reply::Reply;
use super::*;
use crate::protocol;
use crate::stats::LocalStats;

fn context() -> Arc<Context> {
    Context::new(&Config::parse_from(["ferrited"]))
}

fn run(ctx: &Context, request: &[u8], out: &mut Reply) {
    let parsed = protocol::parse(request, ctx.item_max).unwrap().unwrap();
    assert_eq!(parsed.consumed, request.len());
    let mut stats = LocalStats::default();
    exec::execute(ctx, parsed.cmd, (ctx.wall_clock)(), out, &mut stats);
}

#[test]
fn a_multi_key_get_of_a_large_value_does_not_copy_it() {
    const MIB: usize = 1024 * 1024;
    let ctx = context();
    let mut set = format!("set big 0 0 {MIB}\r\n").into_bytes();
    set.resize(set.len() + MIB, b'v');
    set.extend_from_slice(b"\r\n");
    let mut out = Reply::new();
    run(&ctx, &set, &mut out);

    let mut out = Reply::new();
    run(
        &ctx,
        format!("get{}\r\n", " big".repeat(300)).as_bytes(),
        &mut out,
    );
    assert!(out.len() > 300 * MIB && out.len() < 301 * MIB);
    assert!(
        out.inline_len() < 16 * 1024,
        "{} bytes copied",
        out.inline_len()
    );

    let mut out = Reply::new();
    run(&ctx, b"mg big v\r\n", &mut out);
    assert!(out.len() > MIB);
    assert!(out.inline_len() < 64, "{} bytes copied", out.inline_len());
}

#[test]
fn checking_the_client_address_accepts_no_connections() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let socket = bind_without_listening("127.0.0.1", port).unwrap();
    // Linux refuses the connection; macOS drops the SYN.
    let address = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let error = std::net::TcpStream::connect_timeout(&address, Duration::from_millis(500))
        .expect_err("a socket that only binds accepted a connection");
    assert!(
        matches!(
            error.kind(),
            io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut
        ),
        "{error}"
    );
    drop(socket);
}

#[test]
fn checking_the_client_address_fails_while_another_server_listens_there() {
    let listener = bind_tcp("127.0.0.1", 0).unwrap();
    let port = listener.local_addr().unwrap().port();
    let error = bind_without_listening("127.0.0.1", port).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    drop(listener);
    drop(bind_without_listening("127.0.0.1", port).unwrap());
}

#[test]
fn a_burst_of_connections_waits_for_the_acceptor() {
    let listener = bind_tcp("127.0.0.1", 0).unwrap();
    let address = listener.local_addr().unwrap();
    // More than a backlog of 128 holds, except on macOS, which caps every backlog at 128.
    let count = if cfg!(target_os = "macos") { 100 } else { 300 };
    let clients: Vec<_> = (0..count)
        .map(|_| std::net::TcpStream::connect(address).unwrap())
        .collect();
    for _ in 0..count {
        listener.accept().unwrap();
    }
    drop(clients);
}

#[cfg(unix)]
mod unix_socket {
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::path::PathBuf;

    use super::super::bind_unix;

    /// A socket path in its own directory, removed on drop.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("ferrited-{}-{name}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn path(&self) -> PathBuf {
            self.0.join("s")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn a_path_that_is_not_a_socket_is_left_alone() {
        let scratch = Scratch::new("file");
        std::fs::write(scratch.path(), "precious").unwrap();
        let error = bind_unix(&scratch.path()).unwrap_err();
        assert!(error.to_string().contains("not a socket"), "{error}");
        assert_eq!(std::fs::read_to_string(scratch.path()).unwrap(), "precious");

        let dir = scratch.0.join("d");
        std::fs::create_dir(&dir).unwrap();
        assert!(bind_unix(&dir).is_err());
        assert!(dir.is_dir());
    }

    #[test]
    fn a_socket_someone_is_listening_on_is_reported_as_in_use() {
        let scratch = Scratch::new("live");
        let owner = UnixListener::bind(scratch.path()).unwrap();
        let error = bind_unix(&scratch.path()).unwrap_err();
        assert!(error.to_string().contains("already in use"), "{error}");
        let _client = UnixStream::connect(scratch.path()).unwrap();
        owner.set_nonblocking(true).unwrap();
        owner.accept().unwrap();
        owner.accept().unwrap();
    }

    #[test]
    fn a_stale_socket_is_replaced() {
        let scratch = Scratch::new("stale");
        drop(UnixListener::bind(scratch.path()).unwrap());
        assert!(scratch.path().exists());
        let listener = bind_unix(&scratch.path()).unwrap();
        let _client = UnixStream::connect(scratch.path()).unwrap();
        listener.accept().unwrap();
    }

    #[test]
    fn a_free_path_is_bound() {
        let scratch = Scratch::new("free");
        let listener = bind_unix(&scratch.path()).unwrap();
        let _client = UnixStream::connect(scratch.path()).unwrap();
        listener.accept().unwrap();
    }
}
