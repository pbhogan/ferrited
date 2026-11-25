//! Tests of the event loops, the acceptor and shutdown, over loopback sockets.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

use clap::Parser;

use super::eventloop::{EventLoop, Handle, REQUESTS_PER_TURN};
use super::stream::Stream;
use super::*;
use crate::store::GetArgs;

fn context() -> Arc<Context> {
    Context::new(&Config::parse_from(["ferrited"]))
}

/// A connected loopback pair: (client, server).
fn socket_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    (client, server)
}

/// Hands a new connection to a loop, counted as the acceptor would, and returns the client.
fn connect(ctx: &Context, handle: &Handle) -> TcpStream {
    let (client, server) = socket_pair();
    ctx.stats.curr_connections.fetch_add(1, Relaxed);
    ctx.stats.total_connections.fetch_add(1, Relaxed);
    handle.hand(Stream::from(server));
    client
}

/// Turns the loop until `client` has received `len` bytes.
fn turn_until_reply(event_loop: &mut EventLoop, client: &mut TcpStream, len: usize) -> String {
    client.set_nonblocking(true).unwrap();
    let mut reply = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(5);
    while reply.len() < len {
        assert!(
            Instant::now() < deadline,
            "reply so far: {:?}",
            String::from_utf8_lossy(&reply)
        );
        event_loop.turn(Some(Duration::from_millis(10)));
        let mut chunk = [0u8; 64 * 1024];
        match client.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => reply.extend_from_slice(&chunk[..read]),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("{error}"),
        }
    }
    client.set_nonblocking(false).unwrap();
    String::from_utf8(reply).unwrap()
}

fn read_to_close(client: &mut TcpStream) -> Vec<u8> {
    let mut rest = Vec::new();
    let _ = client.read_to_end(&mut rest);
    rest
}

fn total(ctx: &Context, name: &str) -> u64 {
    let snapshot = ctx.stats.snapshot();
    let (_, value) = snapshot.iter().find(|(stat, _)| *stat == name).unwrap();
    *value
}

fn stored(ctx: &Context, key: &[u8]) -> bool {
    ctx.store.get(key, 0, &GetArgs::default()).is_some()
}

#[test]
fn every_request_in_a_turn_sees_the_same_second() {
    use std::sync::atomic::AtomicU64;

    // Advances a second on every read.
    static SECONDS: AtomicU64 = AtomicU64::new(2_000_000_000);
    let mut ctx = context();
    Arc::get_mut(&mut ctx).unwrap().wall_clock = || SECONDS.fetch_add(1, Relaxed);
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut client = connect(&ctx, &handle);
    client
        .write_all(b"ms k 1 T100\r\na\r\nmg k t l\r\nmg k t l\r\n")
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let expected = "HD\r\nHD t100 l0\r\nHD t100 l0\r\n";
    assert_eq!(
        turn_until_reply(&mut event_loop, &mut client, expected.len()),
        expected
    );
}

#[test]
fn bytes_read_and_written_count_what_crossed_the_socket() {
    let ctx = context();
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut client = connect(&ctx, &handle);
    client.write_all(b"version\r\nversion\r\n").unwrap();
    let reply = format!("VERSION {VERSION}\r\n").repeat(2);
    assert_eq!(
        turn_until_reply(&mut event_loop, &mut client, reply.len()),
        reply
    );
    assert_eq!(total(&ctx, "bytes_read"), 18);
    assert_eq!(total(&ctx, "bytes_written"), reply.len() as u64);
}

#[test]
fn a_panicking_connection_is_closed_and_the_loop_goes_on() {
    let ctx = context();
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut doomed = connect(&ctx, &handle);
    event_loop.panic_next = true;
    event_loop.turn(Some(Duration::from_millis(10)));
    assert_eq!(ctx.stats.curr_connections.load(Relaxed), 0);
    assert!(read_to_close(&mut doomed).is_empty());

    let mut client = connect(&ctx, &handle);
    client.write_all(b"mn\r\n").unwrap();
    assert_eq!(turn_until_reply(&mut event_loop, &mut client, 4), "MN\r\n");
}

#[test]
fn a_connection_with_requests_waiting_stops_once_shutdown_begins() {
    let ctx = context();
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut client = connect(&ctx, &handle);
    client.write_all(b"set late 0 0 1\r\nx\r\n").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    ctx.begin_shutdown();
    let deadline = Instant::now() + Duration::from_secs(5);
    while event_loop.turn(Some(Duration::from_millis(10))) {
        assert!(Instant::now() < deadline, "the loop did not finish");
    }
    assert!(!stored(&ctx, b"late"));
    assert_eq!(ctx.stats.curr_connections.load(Relaxed), 0);
    assert!(read_to_close(&mut client).is_empty());
}

#[test]
fn requests_read_before_shutdown_are_answered_before_the_connection_closes() {
    let ctx = context();
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut client = connect(&ctx, &handle);
    // More than one turn runs, so some are still buffered when shutdown begins.
    let count = REQUESTS_PER_TURN * 3;
    let batch: String = (0..count)
        .map(|index| format!("ms k{index} 1 q\r\nv\r\n"))
        .collect();
    client.write_all(batch.as_bytes()).unwrap();
    client.write_all(b"mn\r\n").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    event_loop.turn(Some(Duration::from_millis(10)));
    assert!(!stored(&ctx, format!("k{}", count - 1).as_bytes()));
    ctx.begin_shutdown();
    while event_loop.turn(Some(Duration::from_millis(10))) {}
    assert!(stored(&ctx, format!("k{}", count - 1).as_bytes()));
    assert_eq!(read_to_close(&mut client), b"MN\r\n");
}

#[test]
fn one_pipelining_client_does_not_hold_up_another() {
    let ctx = context();
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut heavy = connect(&ctx, &handle);
    let mut light = connect(&ctx, &handle);
    event_loop.turn(Some(Duration::from_millis(10)));
    let count = 2000;
    let heavy_done = || {
        (0..count)
            .filter(|index| stored(&ctx, format!("h{index}").as_bytes()))
            .count()
    };
    let batch: String = (0..count)
        .map(|index| format!("ms h{index} 1 q\r\nv\r\n"))
        .collect();
    heavy.write_all(batch.as_bytes()).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    event_loop.turn(Some(Duration::from_millis(10)));
    let done = heavy_done();
    assert!(
        done > 0 && done <= REQUESTS_PER_TURN,
        "{done} ran in the first turn"
    );
    light.write_all(b"ms light 1 q\r\nv\r\nmn\r\n").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    event_loop.turn(Some(Duration::from_millis(10)));
    assert!(stored(&ctx, b"light"), "the light client was not served");
    let done = heavy_done();
    assert!(
        done <= 2 * REQUESTS_PER_TURN,
        "{done} of the heavy client's requests ran first"
    );
    assert_eq!(turn_until_reply(&mut event_loop, &mut light, 4), "MN\r\n");
    heavy.write_all(b"mn\r\n").unwrap();
    assert_eq!(turn_until_reply(&mut event_loop, &mut heavy, 4), "MN\r\n");
    assert!(stored(&ctx, format!("h{}", count - 1).as_bytes()));
}

#[test]
fn a_client_that_closes_right_after_its_last_request_is_closed() {
    let ctx = context();
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut client = connect(&ctx, &handle);
    event_loop.turn(Some(Duration::from_millis(10)));
    // The request and the end of input arrive together.
    client.write_all(b"ms k 1 q\r\nv\r\n").unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let deadline = Instant::now() + Duration::from_secs(5);
    while ctx.stats.curr_connections.load(Relaxed) > 0 {
        assert!(Instant::now() < deadline, "the connection was left open");
        event_loop.turn(Some(Duration::from_millis(10)));
    }
    assert!(stored(&ctx, b"k"));
    assert!(read_to_close(&mut client).is_empty());
}

/// Turns the loop, collecting what `client` receives, until the server closes it.
fn turn_until_closed(event_loop: &mut EventLoop, client: &mut TcpStream) -> Vec<u8> {
    client.set_nonblocking(true).unwrap();
    let mut reply = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < deadline, "the connection was left open");
        event_loop.turn(Some(Duration::from_millis(1)));
        let mut chunk = [0u8; 64 * 1024];
        loop {
            match client.read(&mut chunk) {
                Ok(0) => {
                    client.set_nonblocking(false).unwrap();
                    return reply;
                }
                Ok(read) => reply.extend_from_slice(&chunk[..read]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("{error}"),
            }
        }
    }
}

#[test]
fn a_client_that_closes_right_after_its_last_request_still_gets_the_reply() {
    let ctx = context();
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut client = connect(&ctx, &handle);
    event_loop.turn(Some(Duration::from_millis(10)));
    client.write_all(b"mn\r\n").unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(turn_until_closed(&mut event_loop, &mut client), b"MN\r\n");
    assert_eq!(ctx.stats.curr_connections.load(Relaxed), 0);
}

#[test]
fn a_half_closed_client_gets_replies_larger_than_the_socket_takes_at_once() {
    let ctx = context();
    let value = vec![b'v'; 512 * 1024];
    ctx.store.set(
        b"big",
        bytes::Bytes::from(value.clone()),
        0,
        &Default::default(),
    );
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut client = connect(&ctx, &handle);
    event_loop.turn(Some(Duration::from_millis(10)));
    // Far more than the socket buffers, so the connection blocks after the end of input.
    client.write_all(&b"get big\r\n".repeat(8)).unwrap();
    client.write_all(b"mn\r\n").unwrap();
    client.shutdown(std::net::Shutdown::Write).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let mut one = format!("VALUE big 0 {}\r\n", value.len()).into_bytes();
    one.extend_from_slice(&value);
    one.extend_from_slice(b"\r\nEND\r\n");
    let mut expected = one.repeat(8);
    expected.extend_from_slice(b"MN\r\n");
    let reply = turn_until_closed(&mut event_loop, &mut client);
    assert!(
        reply == expected,
        "{} of {} bytes",
        reply.len(),
        expected.len()
    );
    assert_eq!(ctx.stats.curr_connections.load(Relaxed), 0);
}

#[test]
fn a_write_that_fails_counts_only_the_bytes_written() {
    let ctx = context();
    let value = vec![b'v'; 512 * 1024];
    ctx.store.set(
        b"big",
        bytes::Bytes::from(value.clone()),
        0,
        &Default::default(),
    );
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut client = connect(&ctx, &handle);
    event_loop.turn(Some(Duration::from_millis(10)));
    let requests = b"get big\r\n".repeat(20);
    client.write_all(&requests).unwrap();
    // Closing with replies unread resets the connection and fails the server's next write.
    let mut first = vec![0u8; 100 * 1024];
    client.set_nonblocking(true).unwrap();
    let mut got = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while got < first.len() {
        assert!(Instant::now() < deadline, "only {got} bytes arrived");
        event_loop.turn(Some(Duration::from_millis(1)));
        match client.read(&mut first[got..]) {
            Ok(read) => got += read,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("{error}"),
        }
    }
    drop(client);
    while ctx.stats.curr_connections.load(Relaxed) > 0 {
        assert!(Instant::now() < deadline, "the connection was left open");
        event_loop.turn(Some(Duration::from_millis(10)));
    }
    let all = 20 * (value.len() + format!("VALUE big 0 {}\r\n\r\nEND\r\n", value.len()).len());
    let written = total(&ctx, "bytes_written") as usize;
    assert!(
        written >= got && written < all,
        "{written} bytes counted, {got} read, {all} in all"
    );
    assert_eq!(total(&ctx, "bytes_read"), requests.len() as u64);
}

#[test]
fn a_connection_served_after_shutdown_begins_in_the_same_turn_reads_nothing_more() {
    let ctx = context_with(&["-A"]);
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut stopper = connect(&ctx, &handle);
    let mut other = connect(&ctx, &handle);
    stopper.write_all(b"shutdown\r\n").unwrap();
    other.write_all(b"set late 0 0 1\r\nx\r\n").unwrap();
    std::thread::sleep(Duration::from_millis(50));
    event_loop.turn(Some(Duration::from_millis(10)));
    assert!(ctx.stopping.load(Relaxed));
    assert!(!stored(&ctx, b"late"));
    while event_loop.turn(Some(Duration::from_millis(10))) {}
    assert!(!stored(&ctx, b"late"));
    assert!(read_to_close(&mut other).is_empty());
    read_to_close(&mut stopper);
}

#[test]
fn input_split_across_reads_is_framed_the_same() {
    let ctx = context();
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut client = connect(&ctx, &handle);
    let input = b"set a 0 0 3\r\nabc\r\nget a\r\nmg a v\r\nmn\r\n";
    let expected = "STORED\r\nVALUE a 0 3\r\nabc\r\nEND\r\nVA 3\r\nabc\r\nMN\r\n";
    for byte in input {
        client.write_all(std::slice::from_ref(byte)).unwrap();
        event_loop.turn(Some(Duration::from_millis(1)));
    }
    assert_eq!(
        turn_until_reply(&mut event_loop, &mut client, expected.len()),
        expected
    );
}

#[test]
fn a_value_larger_than_a_turns_reads_is_stored() {
    let ctx = context();
    let (mut event_loop, handle) = EventLoop::new(ctx.clone()).unwrap();
    let mut client = connect(&ctx, &handle);
    // Far more than `READS_PER_TURN` reads of the buffer.
    let value = vec![b'v'; 1000 * 1000];
    let writer = {
        let mut client = client.try_clone().unwrap();
        std::thread::spawn(move || {
            write!(client, "set big 0 0 {}\r\n", value.len()).unwrap();
            client.write_all(&value).unwrap();
            client.write_all(b"\r\n").unwrap();
        })
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while !stored(&ctx, b"big") {
        assert!(Instant::now() < deadline, "the value was not stored");
        event_loop.turn(Some(Duration::from_millis(10)));
    }
    writer.join().unwrap();
    assert_eq!(
        turn_until_reply(&mut event_loop, &mut client, 8),
        "STORED\r\n"
    );
}

// With the client threads running.

use super::clients::Clients;
use super::stream::Listener;

fn context_with(args: &[&str]) -> Arc<Context> {
    let mut all = vec!["ferrited"];
    all.extend_from_slice(args);
    Context::new(&Config::parse_from(all))
}

fn serve(ctx: &Arc<Context>) -> (Clients, std::net::SocketAddr) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    (
        Clients::start(ctx, Listener::from(listener), None).unwrap(),
        address,
    )
}

#[test]
fn a_client_that_does_not_read_stalls_only_its_own_pipeline() {
    let ctx = context_with(&["-t", "1"]);
    let (_clients, address) = serve(&ctx);
    let mut client = TcpStream::connect(address).unwrap();
    let value = "v".repeat(100 * 1024);
    write!(client, "set big 0 0 {}\r\n{value}\r\n", value.len()).unwrap();
    let mut stored_reply = [0u8; 8];
    client.read_exact(&mut stored_reply).unwrap();
    assert_eq!(&stored_reply, b"STORED\r\n");

    // Then a command whose effect is visible in the store.
    let mut pipeline = "get big\r\n".repeat(200);
    pipeline.push_str("set marker 0 0 1\r\nx\r\n");
    client.write_all(pipeline.as_bytes()).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !stored(&ctx, b"marker"),
        "the server ran the whole pipeline although nothing was read"
    );

    let mut other = TcpStream::connect(address).unwrap();
    other.write_all(b"mn\r\n").unwrap();
    let mut mn = [0u8; 4];
    other.read_exact(&mut mn).unwrap();
    assert_eq!(&mn, b"MN\r\n");

    let one = format!("VALUE big 0 {}\r\n{value}\r\nEND\r\n", value.len());
    let expected = format!("{}STORED\r\n", one.repeat(200));
    let mut replies = vec![0u8; expected.len()];
    client.read_exact(&mut replies).unwrap();
    assert!(replies == expected.as_bytes());
    assert!(stored(&ctx, b"marker"));
}

#[test]
fn a_client_over_the_connection_limit_is_told_and_closed() {
    let ctx = context_with(&["-c", "1", "-t", "1"]);
    let (_clients, address) = serve(&ctx);
    let mut first = TcpStream::connect(address).unwrap();
    first.write_all(b"mn\r\n").unwrap();
    let mut mn = [0u8; 4];
    first.read_exact(&mut mn).unwrap();

    let mut second = TcpStream::connect(address).unwrap();
    second
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    assert_eq!(
        read_to_close(&mut second),
        b"ERROR Too many open connections\r\n"
    );
    assert_eq!(ctx.stats.rejected_connections.load(Relaxed), 1);

    drop(first);
    let deadline = Instant::now() + Duration::from_secs(5);
    while ctx.stats.curr_connections.load(Relaxed) > 0 {
        assert!(
            Instant::now() < deadline,
            "the first connection was not closed"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut third = TcpStream::connect(address).unwrap();
    third.write_all(b"mn\r\n").unwrap();
    third.read_exact(&mut mn).unwrap();
    assert_eq!(&mn, b"MN\r\n");
}

#[tokio::test]
async fn drain_closes_a_connection_whose_client_does_not_read() {
    let ctx = context_with(&["-t", "1"]);
    let (clients, address) = serve(&ctx);
    let mut client = TcpStream::connect(address).unwrap();
    let value = "v".repeat(64 * 1024);
    let mut pipeline = format!("set big 0 0 {}\r\n{value}\r\n", value.len());
    pipeline.push_str(&"get big\r\n".repeat(100));
    // Blocks on unread replies, so this request stays buffered.
    pipeline.push_str("set after 0 0 1\r\nx\r\n");
    client.write_all(pipeline.as_bytes()).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(stored(&ctx, b"big"));
    assert_eq!(ctx.stats.curr_connections.load(Relaxed), 1);
    clients.drain(&ctx, Duration::from_millis(50)).await;
    assert_eq!(ctx.stats.curr_connections.load(Relaxed), 0);
    // Reading now would unblock a connection that was still open.
    read_to_close(&mut client);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!stored(&ctx, b"after"));
}

#[tokio::test]
async fn drain_stops_accepting_and_closes_idle_connections_at_once() {
    let ctx = context_with(&["-t", "2"]);
    let (clients, address) = serve(&ctx);
    let mut idle: Vec<TcpStream> = (0..4)
        .map(|_| TcpStream::connect(address).unwrap())
        .collect();
    for stream in &mut idle {
        stream.write_all(b"mn\r\n").unwrap();
        let mut mn = [0u8; 4];
        stream.read_exact(&mut mn).unwrap();
    }
    let started = Instant::now();
    clients.drain(&ctx, Duration::from_secs(5)).await;
    assert!(started.elapsed() < Duration::from_secs(1));
    for stream in &mut idle {
        assert!(read_to_close(stream).is_empty());
    }
    // The port may already serve another test, so judge by this server's counters.
    let accepted = ctx.stats.total_connections.load(Relaxed);
    let late = TcpStream::connect(address);
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(ctx.stats.total_connections.load(Relaxed), accepted);
    assert_eq!(ctx.stats.curr_connections.load(Relaxed), 0);
    drop(late);
}

#[tokio::test]
async fn drain_finishes_promptly_while_clients_keep_connecting() {
    let ctx = context_with(&["-t", "4"]);
    let (clients, address) = serve(&ctx);
    let stop = Arc::new(AtomicBool::new(false));
    let connecting = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            // Paced so as not to crowd out other tests' loopback traffic.
            let mut open = Vec::new();
            while !stop.load(Relaxed) && open.len() < 1000 {
                match TcpStream::connect(address) {
                    Ok(stream) => open.push(stream),
                    Err(_) => break,
                }
                std::thread::sleep(Duration::from_micros(200));
            }
            open
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let started = Instant::now();
    clients.drain(&ctx, Duration::from_secs(5)).await;
    let elapsed = started.elapsed();
    stop.store(true, Relaxed);
    let open = connecting.join().unwrap();
    assert!(!open.is_empty());
    assert!(elapsed < Duration::from_secs(2), "drain took {elapsed:?}");
    assert_eq!(ctx.stats.curr_connections.load(Relaxed), 0);
}
