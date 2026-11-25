//! What both ends of a link share: timing, the frame reader task, and timed writes.

use std::io;
use std::time::Duration;

use tokio::io::AsyncWrite;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::mpsc::{self, error::TryRecvError};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::frame::{self, Frame, Signing};

/// Silence after which a link is dropped.
#[cfg(not(test))]
pub const SILENCE: Duration = Duration::from_secs(5);
/// Time without sending anything after which a heartbeat is sent.
#[cfg(not(test))]
pub const HEARTBEAT: Duration = Duration::from_secs(1);
/// Shorter in unit tests, so stall and silence tests run quickly.
#[cfg(test)]
pub const SILENCE: Duration = Duration::from_secs(1);
#[cfg(test)]
pub const HEARTBEAT: Duration = Duration::from_millis(200);

/// Frames read by a task of their own, so a link's loop can select on them without losing a
/// partly read frame. The one-slot channel caps a link at about three frames in memory.
pub struct Frames {
    receive: mpsc::Receiver<io::Result<Frame>>,
    task: JoinHandle<()>,
}

impl Frames {
    /// Reads frames of at most `max` bytes, checking tags with `signing` if given. A frame
    /// with a wrong tag fails the link and is never decoded.
    pub fn spawn(mut read: OwnedReadHalf, max: u32, mut signing: Option<Signing>) -> Self {
        let (send, receive) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            let mut buf = Vec::new();
            loop {
                let frame =
                    frame::read_frame_with(&mut read, max, &mut buf, signing.as_mut()).await;
                let failed = frame.is_err();
                if send.send(frame).await.is_err() || failed {
                    return;
                }
            }
        });
        Self { receive, task }
    }

    /// The next frame, or an error once the link has failed.
    pub async fn recv(&mut self) -> io::Result<Frame> {
        self.receive.recv().await.unwrap_or_else(|| Err(closed()))
    }

    /// Whether a frame (or the link's failure) is waiting to be taken.
    pub fn is_waiting(&self) -> bool {
        !self.receive.is_empty()
    }

    /// The next frame, if one has arrived.
    pub fn try_recv(&mut self) -> Option<io::Result<Frame>> {
        match self.receive.try_recv() {
            Ok(frame) => Some(frame),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => Some(Err(closed())),
        }
    }
}

impl Drop for Frames {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Writes a frame, failing if the peer has not taken it within `SILENCE`. A failed or
/// canceled write has already counted its frame, so the link must end with it.
pub async fn send<W: AsyncWrite + Unpin>(
    write: &mut W,
    frame: &Frame,
    buf: &mut Vec<u8>,
    signing: Option<&mut Signing>,
) -> io::Result<()> {
    tokio::time::timeout(SILENCE, frame::write_frame_with(write, frame, buf, signing))
        .await
        .map_err(|_| timed_out("the peer stopped reading"))?
}

/// Resolves once replication is stopping, holding no borrow of the flag afterward.
pub async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|stopping| *stopping).await;
}

/// The error a link ends with when replication stops while it waits.
pub fn stopping() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "replication is stopping")
}

pub fn is_stopping(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::Interrupted
}

pub fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, "the peer closed the link")
}

pub fn timed_out(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, what.to_string())
}

pub fn invalid(what: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what)
}
