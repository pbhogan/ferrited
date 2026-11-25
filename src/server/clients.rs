//! The client threads, started once the node is ready and stopped by shutdown.

use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering::{Relaxed, Release};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::Context;
use super::accept::Acceptor;
use super::eventloop::EventLoop;
use super::stream::Listener;

/// The acceptor and event loop threads. Dropping this leaves them running; `drain` stops them.
pub struct Clients {
    acceptor: Option<JoinHandle<()>>,
    loops: Vec<JoinHandle<()>>,
    /// Removed once the acceptor has closed the listener.
    socket_file: Option<SocketFile>,
}

impl Clients {
    /// Starts `ctx.threads` event loops and an acceptor serving `listener`. `socket_file` is
    /// removed when the server stops accepting, or at once if the threads cannot start.
    pub(crate) fn start(
        ctx: &Arc<Context>,
        listener: Listener,
        socket_file: Option<SocketFile>,
    ) -> io::Result<Self> {
        // Before any loop starts, so none finishes while the acceptor may still hand it one.
        ctx.accepting.store(true, Relaxed);
        let mut handles = Vec::with_capacity(ctx.threads);
        let mut loops = Vec::with_capacity(ctx.threads);
        for index in 0..ctx.threads {
            let (event_loop, handle) = EventLoop::new(ctx.clone())?;
            ctx.add_waker(handle.waker());
            loops.push(
                std::thread::Builder::new()
                    .name(format!("ferrited-loop-{index}"))
                    .spawn(move || event_loop.run())?,
            );
            handles.push(handle);
        }
        let (acceptor, waker) = Acceptor::new(ctx.clone(), listener, handles)?;
        ctx.add_waker(waker);
        let acceptor = std::thread::Builder::new()
            .name("ferrited-accept".into())
            .spawn(move || acceptor.run())?;
        Ok(Self {
            acceptor: Some(acceptor),
            loops,
            socket_file,
        })
    }

    /// Stops accepting, waits up to `timeout` for connections to finish the requests they have
    /// read, then closes the rest. No connection changes the store once this returns.
    pub async fn drain(mut self, ctx: &Context, timeout: Duration) {
        ctx.begin_shutdown();
        let deadline = Instant::now() + timeout;
        if let Some(acceptor) = self.acceptor.take() {
            let _ = tokio::task::spawn_blocking(move || acceptor.join()).await;
        }
        drop(self.socket_file.take());
        // The acceptor has stopped. Release, so a loop that sees this also sees every
        // connection the acceptor counted in its `open`.
        ctx.accepting.store(false, Release);
        ctx.wake_all();
        while ctx.stats.curr_connections.load(Relaxed) > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let stragglers = ctx.stats.curr_connections.load(Relaxed);
        if stragglers > 0 {
            eprintln!("ferrited: closing {stragglers} connection(s) still open after {timeout:?}");
        }
        ctx.close_now.store(true, Relaxed);
        ctx.wake_all();
        let loops = std::mem::take(&mut self.loops);
        let _ = tokio::task::spawn_blocking(move || {
            for event_loop in loops {
                let _ = event_loop.join();
            }
        })
        .await;
    }
}

/// Removes the server's Unix socket file when dropped.
pub(crate) struct SocketFile(pub(crate) std::path::PathBuf);

impl Drop for SocketFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
