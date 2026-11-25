//! Client sockets, TCP or Unix, as the event loops see them.

use std::io::{self, IoSlice, Write};
use std::os::fd::{AsRawFd, RawFd};

use mio::event::Source;
use mio::{Interest, Registry, Token};

pub enum Listener {
    Tcp(mio::net::TcpListener),
    Unix(mio::net::UnixListener),
}

impl Listener {
    /// Accepts one connection, set non-blocking, with Nagle's algorithm off for TCP.
    pub fn accept(&self) -> io::Result<Stream> {
        match self {
            Self::Tcp(listener) => {
                let (stream, _) = listener.accept()?;
                let _ = stream.set_nodelay(true);
                Ok(Stream::Tcp(stream))
            }
            Self::Unix(listener) => Ok(Stream::Unix(listener.accept()?.0)),
        }
    }
}

impl From<std::net::TcpListener> for Listener {
    fn from(listener: std::net::TcpListener) -> Self {
        // mio needs the socket non-blocking; a failure here shows up as an accept error later.
        let _ = listener.set_nonblocking(true);
        Self::Tcp(mio::net::TcpListener::from_std(listener))
    }
}

impl From<std::os::unix::net::UnixListener> for Listener {
    fn from(listener: std::os::unix::net::UnixListener) -> Self {
        let _ = listener.set_nonblocking(true);
        Self::Unix(mio::net::UnixListener::from_std(listener))
    }
}

impl Source for Listener {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interest: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Tcp(listener) => listener.register(registry, token, interest),
            Self::Unix(listener) => listener.register(registry, token, interest),
        }
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interest: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Tcp(listener) => listener.reregister(registry, token, interest),
            Self::Unix(listener) => listener.reregister(registry, token, interest),
        }
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        match self {
            Self::Tcp(listener) => listener.deregister(registry),
            Self::Unix(listener) => listener.deregister(registry),
        }
    }
}

pub enum Stream {
    Tcp(mio::net::TcpStream),
    Unix(mio::net::UnixStream),
}

impl Stream {
    /// Writes as much of `slices` as the socket takes now.
    pub fn write_vectored(&mut self, slices: &[IoSlice<'_>]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write_vectored(slices),
            Self::Unix(stream) => stream.write_vectored(slices),
        }
    }
}

impl From<std::net::TcpStream> for Stream {
    fn from(stream: std::net::TcpStream) -> Self {
        let _ = stream.set_nonblocking(true);
        let _ = stream.set_nodelay(true);
        Self::Tcp(mio::net::TcpStream::from_std(stream))
    }
}

impl From<std::os::unix::net::UnixStream> for Stream {
    fn from(stream: std::os::unix::net::UnixStream) -> Self {
        let _ = stream.set_nonblocking(true);
        Self::Unix(mio::net::UnixStream::from_std(stream))
    }
}

impl AsRawFd for Stream {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            Self::Tcp(stream) => stream.as_raw_fd(),
            Self::Unix(stream) => stream.as_raw_fd(),
        }
    }
}

impl Source for Stream {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interest: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.register(registry, token, interest),
            Self::Unix(stream) => stream.register(registry, token, interest),
        }
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interest: Interest,
    ) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.reregister(registry, token, interest),
            Self::Unix(stream) => stream.reregister(registry, token, interest),
        }
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.deregister(registry),
            Self::Unix(stream) => stream.deregister(registry),
        }
    }
}
