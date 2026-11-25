//! The Prometheus metrics endpoint (`--metrics-addr`). Serves one HTTP/1.1 request per
//! connection. Bound before local recovery and stopped last, so a node can be scraped while it
//! loads, waits for peers or stops.

pub(crate) mod http;
pub(crate) mod render;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};

use crate::server::Context;
use http::Route;

/// How long a client may take to send its request's head.
const READ_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a client may take to read the response.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// Pause after a failed accept (e.g. out of file descriptors).
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// Renders the metrics. Runs on the blocking pool, since it takes each shard lock in turn and a
/// full sync can hold one for a while.
pub(crate) type Source = Arc<dyn Fn() -> String + Send + Sync>;

#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    /// Most connections served at once; past it a new one gets `503` and is closed.
    pub connections: usize,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}

impl Limits {
    pub(crate) const DEFAULT: Limits = Limits {
        connections: crate::server::METRICS_CONNECTIONS,
        read_timeout: READ_TIMEOUT,
        write_timeout: WRITE_TIMEOUT,
    };
}

pub struct Metrics {
    task: JoinHandle<()>,
    pub local_addr: SocketAddr,
}

impl Metrics {
    pub async fn bind(address: &str, ctx: Arc<Context>) -> io::Result<Self> {
        let listener = TcpListener::bind(address).await.map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("cannot listen for metrics on {address}: {error}"),
            )
        })?;
        let local_addr = listener.local_addr()?;
        eprintln!("ferrited metrics listening on {local_addr}");
        let source: Source = Arc::new(move || render::render(&ctx));
        let task = tokio::spawn(serve(listener, source, Limits::DEFAULT));
        Ok(Self { task, local_addr })
    }

    /// Closes the listener and every connection still open.
    pub async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

/// Serves until the task is aborted, which aborts the connections too.
pub(crate) async fn serve(listener: TcpListener, source: Source, limits: Limits) {
    let permits = Arc::new(Semaphore::new(limits.connections));
    let mut connections = JoinSet::new();
    // Log accept errors once per streak.
    let mut warned = false;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let stream = match accepted {
                    Ok((stream, _)) => {
                        warned = false;
                        stream
                    }
                    Err(error) => {
                        if !warned {
                            eprintln!("ferrited: metrics: cannot accept a connection: {error}");
                            warned = true;
                        }
                        tokio::time::sleep(ACCEPT_RETRY).await;
                        continue;
                    }
                };
                match permits.clone().try_acquire_owned() {
                    Ok(permit) => {
                        let source = source.clone();
                        connections.spawn(async move {
                            answer(stream, &source, limits).await;
                            drop(permit);
                        });
                    }
                    // The response fits the new socket's empty send buffer.
                    Err(_) => {
                        let _ = stream.try_write(&http::busy());
                    }
                }
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

/// How reading a request's head ended.
enum Head {
    Complete(Route),
    /// The client closed its side before the head ended.
    Closed {
        partial: bool,
    },
}

/// Reads one request, answers it and closes. A head not sent within the read timeout gets
/// `408`; a partial head followed by a close gets `400`.
async fn answer(mut stream: TcpStream, source: &Source, limits: Limits) {
    let response = match tokio::time::timeout(limits.read_timeout, read_head(&mut stream)).await {
        Ok(Ok(Head::Complete(route @ Route::Metrics { .. }))) => {
            let source = source.clone();
            let rendered =
                tokio::task::spawn_blocking(move || http::respond(&route, || source())).await;
            match rendered {
                Ok(response) => response,
                Err(_) => http::internal_error(),
            }
        }
        Ok(Ok(Head::Complete(route))) => http::respond(&route, String::new),
        Ok(Ok(Head::Closed { partial: true })) => http::respond(&Route::BadRequest, String::new),
        Ok(Ok(Head::Closed { partial: false })) | Ok(Err(_)) => return,
        Err(_) => http::timed_out(),
    };
    let _ = tokio::time::timeout(limits.write_timeout, async {
        stream.write_all(&response).await?;
        stream.shutdown().await
    })
    .await;
}

/// Reads until the request's head is complete, too large or malformed, or the client closes.
async fn read_head(stream: &mut TcpStream) -> io::Result<Head> {
    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(Head::Closed {
                partial: !buffer.is_empty(),
            });
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(route) = http::route(&http::parse(&buffer)) {
            return Ok(Head::Complete(route));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    const QUICK: Limits = Limits {
        connections: 2,
        read_timeout: Duration::from_millis(300),
        write_timeout: Duration::from_millis(300),
    };

    async fn endpoint(limits: Limits) -> (SocketAddr, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let source: Source = Arc::new(|| "# HELP x X.\n# TYPE x gauge\nx 1\n".to_string());
        (address, tokio::spawn(serve(listener, source, limits)))
    }

    /// Sends `request` and reads until the server closes.
    async fn exchange(address: SocketAddr, request: &[u8]) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request).await.unwrap();
        read_all(&mut stream).await
    }

    async fn read_all(stream: &mut TcpStream) -> String {
        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut reply))
            .await
            .expect("the server did not close the connection")
            .unwrap();
        String::from_utf8(reply).unwrap()
    }

    #[tokio::test]
    async fn a_get_of_metrics_is_answered_and_the_connection_closed() {
        let (address, server) = endpoint(QUICK).await;
        let reply = exchange(address, b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(reply.starts_with("HTTP/1.1 200 OK\r\n"), "{reply}");
        assert!(
            reply.ends_with("\r\n\r\n# HELP x X.\n# TYPE x gauge\nx 1\n"),
            "{reply}"
        );
        let reply = exchange(address, b"HEAD /metrics HTTP/1.1\r\n\r\n").await;
        assert!(reply.starts_with("HTTP/1.1 200 OK\r\n") && reply.ends_with("\r\n\r\n"));
        let reply = exchange(address, b"GET /nope HTTP/1.1\r\n\r\n").await;
        assert!(reply.starts_with("HTTP/1.1 404 "), "{reply}");
        // A client that closes its side before the head ends.
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\n\n")
            .await
            .unwrap();
        stream.shutdown().await.unwrap();
        let reply = read_all(&mut stream).await;
        assert!(reply.starts_with("HTTP/1.1 400 "), "{reply}");
        server.abort();
    }

    #[tokio::test]
    async fn a_slow_client_gets_408_at_the_read_timeout() {
        let (address, server) = endpoint(QUICK).await;
        let started = Instant::now();
        let reply = exchange(address, b"GET /metrics HTTP/1.1\r\nHost:").await;
        let waited = started.elapsed();
        assert!(reply.starts_with("HTTP/1.1 408 "), "{reply}");
        assert!(waited >= QUICK.read_timeout, "{waited:?}");
        assert!(waited < Duration::from_secs(3), "{waited:?}");
        server.abort();
    }

    #[tokio::test]
    async fn an_oversized_head_gets_431() {
        let (address, server) = endpoint(QUICK).await;
        let mut request = b"GET /metrics HTTP/1.1\r\nX: ".to_vec();
        // Exactly the limit, so the server has read all of it when it answers: closing with
        // unread bytes would reset the connection, and the reply could be lost.
        request.resize(http::MAX_HEAD, b'a');
        let reply = exchange(address, &request).await;
        assert!(reply.starts_with("HTTP/1.1 431 "), "{reply}");
        server.abort();
    }

    #[tokio::test]
    async fn past_the_connection_cap_a_client_gets_503_until_one_finishes() {
        let (address, server) = endpoint(QUICK).await;
        // Two idle clients hold both places.
        let mut idle = Vec::new();
        for _ in 0..QUICK.connections {
            idle.push(TcpStream::connect(address).await.unwrap());
        }
        // Let the server accept them before the next one.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut turned_away = TcpStream::connect(address).await.unwrap();
        let reply = read_all(&mut turned_away).await;
        assert!(reply.starts_with("HTTP/1.1 503 "), "{reply}");
        // The idle ones time out, which frees their places.
        for mut stream in idle {
            let reply = read_all(&mut stream).await;
            assert!(reply.starts_with("HTTP/1.1 408 "), "{reply}");
        }
        let reply = exchange(address, b"GET /metrics HTTP/1.1\r\n\r\n").await;
        assert!(reply.starts_with("HTTP/1.1 200 "), "{reply}");
        server.abort();
    }
}
