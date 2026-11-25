//! Request parsing for the classic text and meta protocols. No I/O.

mod classic;
mod meta;
mod tokens;

#[cfg(test)]
mod tests;

use std::borrow::Cow;

pub use meta::{MetaOpts, ReturnFlags};
use tokens::Tokens;

pub const MAX_KEY: usize = 250;
/// Longest request line accepted, not counting its newline. A longer line closes the connection.
pub const MAX_LINE: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseError {
    UnknownCommand,
    BadFormat,
    BadDataChunk,
    TooLarge,
    LineTooLong,
    InvalidFlag,
    DuplicateFlag,
    TooManyFlags,
    /// Any flag error on `ma`, which memcached reports with one catch-all message.
    InvalidArithFlag,
    BadToken,
    BadKeyEncoding,
    OpaqueTooLong,
    InvalidSetMode,
    InvalidArithMode,
    InvalidDelta,
    InvalidDeltaValue,
    InvalidInitialValue,
    InvalidExptime,
}

impl ParseError {
    /// The reply line, without the trailing CRLF.
    pub fn message(self) -> &'static str {
        match self {
            Self::UnknownCommand => "ERROR",
            Self::BadFormat => "CLIENT_ERROR bad command line format",
            Self::BadDataChunk => "CLIENT_ERROR bad data chunk",
            Self::TooLarge => "SERVER_ERROR object too large for cache",
            Self::LineTooLong => "CLIENT_ERROR line too long",
            Self::InvalidFlag => "CLIENT_ERROR invalid flag",
            Self::DuplicateFlag => "CLIENT_ERROR duplicate flag",
            Self::TooManyFlags => "CLIENT_ERROR options flags are too long",
            Self::InvalidArithFlag => "CLIENT_ERROR invalid or duplicate flag",
            Self::BadToken => "CLIENT_ERROR bad token in command line format",
            Self::BadKeyEncoding => "CLIENT_ERROR error decoding key",
            Self::OpaqueTooLong => "CLIENT_ERROR opaque token too long",
            Self::InvalidSetMode => "CLIENT_ERROR invalid mode for ms M token",
            Self::InvalidArithMode => "CLIENT_ERROR invalid mode for ma M token",
            Self::InvalidDelta => "CLIENT_ERROR invalid numeric delta argument",
            Self::InvalidDeltaValue => "CLIENT_ERROR invalid numeric delta value",
            Self::InvalidInitialValue => "CLIENT_ERROR invalid numeric initial value",
            Self::InvalidExptime => "CLIENT_ERROR invalid exptime argument",
        }
    }

    /// Whether the connection must be closed after replying.
    pub fn is_fatal(self) -> bool {
        matches!(self, Self::LineTooLong)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreMode {
    Set,
    Add,
    Replace,
    Append,
    Prepend,
}

#[derive(Debug, PartialEq)]
pub enum Command<'a> {
    /// `get`/`gets`. `keys` is the rest of the line; split it on spaces.
    Get {
        keys: &'a [u8],
        cas: bool,
    },
    /// `gat`/`gats`.
    Gat {
        exptime: i64,
        keys: &'a [u8],
        cas: bool,
    },
    /// `set`/`add`/`replace`/`append`/`prepend`, and `cas` (mode `Set` with `cas` present).
    Store {
        mode: StoreMode,
        key: &'a [u8],
        flags: u32,
        exptime: i64,
        cas: Option<u64>,
        noreply: bool,
        data: &'a [u8],
    },
    Delete {
        key: &'a [u8],
        noreply: bool,
    },
    Arith {
        key: &'a [u8],
        delta: u64,
        decr: bool,
        noreply: bool,
    },
    Touch {
        key: &'a [u8],
        exptime: i64,
        noreply: bool,
    },
    FlushAll {
        delay: u64,
        noreply: bool,
    },
    Stats {
        arg: &'a [u8],
    },
    Version,
    Quit,
    /// `shutdown`. `valid` is false when a mode other than `graceful` was given.
    Shutdown {
        valid: bool,
        noreply: bool,
    },
    /// Tuning commands that are accepted and do nothing.
    Accepted {
        noreply: bool,
    },
    MetaGet(MetaOpts<'a>),
    MetaSet {
        opts: MetaOpts<'a>,
        data: &'a [u8],
    },
    MetaDelete(MetaOpts<'a>),
    MetaArith(MetaOpts<'a>),
    MetaDebug(MetaOpts<'a>),
    MetaNoop,
}

impl<'a> Command<'a> {
    fn set_data(&mut self, body: &'a [u8]) {
        if let Command::Store { data, .. } | Command::MetaSet { data, .. } = self {
            *data = body;
        }
    }
}

/// A complete request and the number of bytes it occupied.
#[derive(Debug, PartialEq)]
pub struct Request<'a> {
    pub cmd: Command<'a>,
    pub consumed: usize,
}

/// A request that cannot be executed.
#[derive(Debug, PartialEq)]
pub struct Failure<'a> {
    pub error: ParseError,
    /// Bytes of the request line (and any data block already accounted for) to discard.
    pub consumed: usize,
    /// Further bytes to discard after `consumed`; they may not have arrived yet.
    pub swallow: usize,
    /// The client asked for no reply.
    pub silent: bool,
    /// A key whose item is removed, so a failed overwrite leaves no old data behind.
    pub unlink: Option<Cow<'a, [u8]>>,
}

/// An error found while parsing a request line.
struct LineError<'a> {
    error: ParseError,
    swallow: usize,
    silent: bool,
    unlink: Option<Cow<'a, [u8]>>,
}

impl From<ParseError> for LineError<'_> {
    fn from(error: ParseError) -> Self {
        Self {
            error,
            swallow: 0,
            silent: false,
            unlink: None,
        }
    }
}

/// A parsed line: the command, and the length of the data block that must follow it, if any.
type Line<'a> = Result<(Command<'a>, Option<usize>), LineError<'a>>;

/// The full length of the request at the front of `buf` if its line is valid but its data
/// block is incomplete. At most the line plus `item_max` plus 2, so safe to reserve.
pub fn pending_len(buf: &[u8], item_max: usize) -> Option<usize> {
    let newline = memchr::memchr(b'\n', buf)?;
    let consumed = newline + 1;
    if newline > MAX_LINE || consumed < 4 {
        return None;
    }
    let line = &buf[..newline];
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let leading_space = line.first() == Some(&b' ');
    let mut tokens = Tokens::new(line);
    let name = tokens.next().unwrap_or(b"");
    let parsed = match name {
        b"mg" | b"ms" | b"md" | b"ma" | b"mn" | b"me" if !leading_space => {
            meta::parse(name, tokens, item_max)
        }
        _ => classic::parse(name, tokens, item_max, leading_space),
    };
    match parsed {
        Ok((_, Some(len))) => Some(consumed + len + 2),
        _ => None,
    }
}

/// Parses one request from the front of `buf`. `Ok(None)` means more bytes are needed.
pub fn parse(buf: &[u8], item_max: usize) -> Result<Option<Request<'_>>, Failure<'_>> {
    // Counts every byte before the newline, so a line is judged the same whether or not its
    // newline has arrived.
    let too_long = |consumed| Failure {
        error: ParseError::LineTooLong,
        consumed,
        swallow: 0,
        silent: false,
        unlink: None,
    };
    let Some(newline) = memchr::memchr(b'\n', buf) else {
        if buf.len() > MAX_LINE {
            return Err(too_long(buf.len()));
        }
        return Ok(None);
    };
    if newline > MAX_LINE {
        return Err(too_long(newline + 1));
    }
    let consumed = newline + 1;
    let fail = |error, silent| Failure {
        error,
        consumed,
        swallow: 0,
        silent,
        unlink: None,
    };
    // memcached rejects a line too short to hold a two-letter command and CRLF before reading it.
    if consumed < 4 {
        return Err(fail(ParseError::BadFormat, false));
    }
    let line = &buf[..newline];
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    // memcached takes the command name from the start of the line, so a leading space hides it.
    let leading_space = line.first() == Some(&b' ');
    let mut tokens = Tokens::new(line);
    let name = tokens.next().unwrap_or(b"");
    let parsed = match name {
        b"mg" | b"ms" | b"md" | b"ma" | b"mn" | b"me" if !leading_space => {
            meta::parse(name, tokens, item_max)
        }
        _ => classic::parse(name, tokens, item_max, leading_space),
    };
    match parsed {
        Err(line_error) => Err(Failure {
            error: line_error.error,
            consumed,
            swallow: line_error.swallow,
            silent: line_error.silent,
            unlink: line_error.unlink,
        }),
        Ok((cmd, None)) => Ok(Some(Request { cmd, consumed })),
        Ok((mut cmd, Some(len))) => {
            let body = &buf[consumed..];
            if body.len() < len + 2 {
                return Ok(None);
            }
            if &body[len..len + 2] != b"\r\n" {
                // `noreply` silences this error; the meta `q` flag does not.
                let silent = matches!(cmd, Command::Store { noreply: true, .. });
                return Err(Failure {
                    consumed: consumed + len + 2,
                    ..fail(ParseError::BadDataChunk, silent)
                });
            }
            cmd.set_data(&body[..len]);
            Ok(Some(Request {
                cmd,
                consumed: consumed + len + 2,
            }))
        }
    }
}
