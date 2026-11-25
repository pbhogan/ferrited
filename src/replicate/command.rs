//! The status command on the replication port: `status` and a line break gets readiness as
//! `STAT <name> <value>` lines and `END`, then the connection closes. Read as a frame length,
//! `stat` is far past the handshake's frame limit, so the first four bytes tell the two apart.

use std::io;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::handshake::HANDSHAKE_TIMEOUT;
use super::ready::Readiness;

const PREFIX: &[u8; 4] = b"stat";
const MAX_LINE: usize = 64;
/// Pause before peeking again when only part of the prefix has arrived.
const PEEK_WAIT: Duration = Duration::from_millis(5);

/// What a new connection begins with.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Sniffed {
    Status,
    /// Anything else; the handshake reads it as frames.
    Frames,
    /// The connection closed or failed, or nothing arrived within the handshake timeout.
    Nothing,
}

/// Looks at a connection's first bytes without taking them.
pub(super) async fn sniff(stream: &TcpStream) -> Sniffed {
    let looking = async {
        let mut first = [0u8; 4];
        loop {
            let peeked = stream.peek(&mut first).await?;
            if peeked == 0 {
                return Ok::<_, io::Error>(Sniffed::Nothing);
            }
            if first[..peeked] != PREFIX[..peeked] {
                return Ok(Sniffed::Frames);
            }
            if peeked == PREFIX.len() {
                return Ok(Sniffed::Status);
            }
            tokio::time::sleep(PEEK_WAIT).await;
        }
    };
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, looking).await {
        Ok(Ok(sniffed)) => sniffed,
        _ => Sniffed::Nothing,
    }
}

/// Answers one command line and closes. With a secret, only loopback gets the status, so an
/// unauthenticated stranger learns nothing of the peers and logs; without one, anyone can link.
pub(super) async fn answer(
    mut stream: TcpStream,
    address: SocketAddr,
    readiness: &Readiness,
    secret: bool,
) {
    let line = match tokio::time::timeout(HANDSHAKE_TIMEOUT, read_line(&mut stream)).await {
        Ok(Ok(line)) => line,
        _ => return,
    };
    let reply = if line != "status" {
        "ERROR\r\n".to_string()
    } else if secret && !address.ip().to_canonical().is_loopback() {
        "SERVER_ERROR status is answered only on loopback when a replication secret is configured\r\n"
            .to_string()
    } else {
        render(&readiness.lines(Instant::now()))
    };
    let _ = tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.write_all(reply.as_bytes())).await;
    let _ = stream.shutdown().await;
}

/// Reads one line, without its line break or a carriage return before it.
async fn read_line(stream: &mut TcpStream) -> io::Result<String> {
    let mut line = Vec::new();
    let mut chunk = [0u8; MAX_LINE];
    loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        line.extend_from_slice(&chunk[..read]);
        if let Some(end) = line.iter().position(|&byte| byte == b'\n') {
            line.truncate(end);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(String::from_utf8_lossy(&line).into_owned());
        }
        if line.len() > MAX_LINE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the command line is too long",
            ));
        }
    }
}

/// `STAT` lines and `END`, as memcached's `stats` replies.
fn render(lines: &[(String, String)]) -> String {
    let mut out = String::new();
    for (name, value) in lines {
        out.push_str(&format!("STAT {name} {value}\r\n"));
    }
    out.push_str("END\r\n");
    out
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;

    /// Two ends of a loopback connection: (accepted, connecting, the connecting end's address).
    async fn pair() -> (TcpStream, TcpStream, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, address) = listener.accept().await.unwrap();
        (server, client, address)
    }

    async fn reply(mut client: TcpStream) -> String {
        let mut reply = String::new();
        client.read_to_string(&mut reply).await.unwrap();
        reply
    }

    #[tokio::test]
    async fn a_status_command_is_told_apart_from_a_frame() {
        let (server, mut client, _) = pair().await;
        client.write_all(b"status\r\n").await.unwrap();
        assert_eq!(sniff(&server).await, Sniffed::Status);
        // A hello's length field: 56 bytes of payload.
        let (server, mut client, _) = pair().await;
        client.write_all(&[56, 0, 0, 0, 1]).await.unwrap();
        assert_eq!(sniff(&server).await, Sniffed::Frames);
        let (server, client, _) = pair().await;
        drop(client);
        assert_eq!(sniff(&server).await, Sniffed::Nothing);
    }

    #[tokio::test]
    async fn a_command_split_across_writes_is_still_a_command() {
        let (server, mut client, _) = pair().await;
        client.write_all(b"st").await.unwrap();
        let writing = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            client.write_all(b"atus\n").await.unwrap();
            client
        });
        assert_eq!(sniff(&server).await, Sniffed::Status);
        drop(writing.await.unwrap());
    }

    #[tokio::test]
    async fn the_status_command_answers_with_stat_lines() {
        let readiness = Readiness::new(Some(Duration::from_secs(60)), false, 0);
        let (server, mut client, address) = pair().await;
        client.write_all(b"status\n").await.unwrap();
        answer(server, address, &readiness, true).await;
        let reply = reply(client).await;
        assert!(reply.starts_with("STAT state recovering\r\n"), "{reply}");
        assert!(reply.contains("STAT sync_timeout 60\r\n"), "{reply}");
        assert!(reply.ends_with("END\r\n"), "{reply}");
    }

    #[tokio::test]
    async fn with_a_secret_only_loopback_gets_the_status_and_other_lines_get_error() {
        let readiness = Readiness::new(None, false, 0);
        let remote: SocketAddr = "10.0.0.9:5000".parse().unwrap();
        let (server, mut client, _) = pair().await;
        client.write_all(b"status\r\n").await.unwrap();
        answer(server, remote, &readiness, true).await;
        assert!(reply(client).await.starts_with("SERVER_ERROR"));
        let (server, mut client, _) = pair().await;
        client.write_all(b"status\r\n").await.unwrap();
        answer(server, remote, &readiness, false).await;
        assert!(reply(client).await.starts_with("STAT state"));
        let (server, mut client, address) = pair().await;
        client.write_all(b"stats\r\n").await.unwrap();
        answer(server, address, &readiness, false).await;
        assert_eq!(reply(client).await, "ERROR\r\n");
    }
}
