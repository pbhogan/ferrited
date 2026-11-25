#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use ferrited::config::Config;
use ferrited::server::{self, Context};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub mod process;

/// Starts a server on an ephemeral port with default settings.
pub async fn start() -> SocketAddr {
    start_with(|_| {}).await
}

/// Starts a server after letting the caller adjust the default config.
pub async fn start_with(adjust: impl FnOnce(&mut Config)) -> SocketAddr {
    let mut config = Config::parse_from(["ferrited"]);
    adjust(&mut config);
    let ctx = Context::new(&config);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    // The client threads serve until the test process exits.
    std::mem::forget(server::run(listener, &ctx).unwrap());
    addr
}

/// Sends `input` and an `mn` sentinel on a new connection; returns the reply before `MN`.
pub async fn exchange(addr: SocketAddr, input: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    send(&mut stream, input).await
}

/// Like `exchange`, on an existing connection.
pub async fn send(stream: &mut TcpStream, input: &str) -> String {
    try_send(stream, input)
        .await
        .unwrap_or_else(|problem| panic!("{problem}"))
}

/// Like `exchange`, but returns an error if the reply times out or ends early.
pub async fn try_exchange(addr: SocketAddr, input: &str) -> Result<String, String> {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    try_send(&mut stream, input).await
}

/// Like `send`, but returns an error if the reply times out or ends early.
pub async fn try_send(stream: &mut TcpStream, input: &str) -> Result<String, String> {
    stream.write_all(input.as_bytes()).await.unwrap();
    stream.write_all(b"mn\r\n").await.unwrap();
    let mut received = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
    while !received.ends_with(b"MN\r\n") {
        let so_far = || String::from_utf8_lossy(&received).into_owned();
        let read = match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut chunk)).await
        {
            Ok(read) => read.unwrap(),
            Err(_) => return Err(format!("timed out; received so far: {:?}", so_far())),
        };
        if read == 0 {
            return Err(format!(
                "connection closed; received so far: {:?}",
                so_far()
            ));
        }
        received.extend_from_slice(&chunk[..read]);
    }
    received.truncate(received.len() - 4);
    Ok(String::from_utf8(received).unwrap())
}
