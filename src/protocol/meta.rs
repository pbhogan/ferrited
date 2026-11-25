use std::borrow::Cow;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use super::tokens::{Tokens, number, valid_key};
use super::{Command, Line, LineError, MAX_KEY, ParseError};

/// Parsed flags of a meta command. One type serves all meta commands; each uses a subset.
#[derive(Debug, Default, PartialEq)]
pub struct MetaOpts<'a> {
    /// The key to use in the store (decoded if it was sent as base64).
    pub key: Cow<'a, [u8]>,
    /// The key exactly as sent, for echoing with the `k` flag.
    pub raw_key: &'a [u8],
    /// The raw flag text.
    pub flags: &'a [u8],
    /// Return flags to echo, in request order.
    pub returns: ReturnFlags<'a>,
    /// `b`: the key was sent base64-encoded.
    pub base64: bool,
    /// `q`: suppress the common-case reply.
    pub quiet: bool,
    /// `v`: return the value.
    pub value: bool,
    /// `u`: do not update access state.
    pub no_bump: bool,
    /// `I`: invalidate (mark stale) instead of failing or deleting.
    pub invalidate: bool,
    /// `x`: empty the value but keep the item.
    pub remove_value: bool,
    /// `C`: compare-and-swap token to check.
    pub cas: Option<u64>,
    /// `E`: CAS token to assign.
    pub new_cas: Option<u64>,
    /// `T`: TTL in seconds.
    pub ttl: Option<i64>,
    /// `N`: create on miss with this TTL.
    pub vivify: Option<i64>,
    /// `R`: win the recache if the remaining TTL is below this.
    pub recache: Option<i64>,
    /// `F`: client flags.
    pub client_flags: Option<u32>,
    /// `M`: mode character.
    pub mode: Option<u8>,
    /// `D`: arithmetic delta.
    pub delta: Option<u64>,
    /// `J`: initial value when arithmetic creates the item.
    pub initial: Option<u64>,
}

/// The return flags of a meta command, in request order. Each letter appears at most once,
/// which bounds `letters`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct ReturnFlags<'a> {
    letters: [u8; 8],
    len: u8,
    /// The whole `O` token, which is echoed as sent.
    pub opaque: &'a [u8],
}

impl ReturnFlags<'_> {
    fn add(&mut self, letter: u8) {
        self.letters[usize::from(self.len)] = letter;
        self.len += 1;
    }

    /// The flag letters in request order.
    pub fn letters(&self) -> &[u8] {
        &self.letters[..usize::from(self.len)]
    }
}

/// What one pass over a meta command's flag tokens found.
struct FlagScan {
    count: usize,
    /// A token starts with something other than a digit or a character from `A` to `z`,
    /// which makes memcached's tokenizer reject the whole line.
    bad_start: bool,
    opaque_too_long: bool,
}

fn scan_flags(flags: &[u8]) -> FlagScan {
    let mut scan = FlagScan {
        count: 0,
        bad_start: false,
        opaque_too_long: false,
    };
    for token in Tokens::new(flags) {
        scan.count += 1;
        scan.bad_start |= !matches!(token[0], b'A'..=b'z' | b'0'..=b'9');
        scan.opaque_too_long |= token[0] == b'O' && token.len() > MAX_OPAQUE;
    }
    scan
}

/// Most tokens memcached accepts on a meta command line, counting the command and key.
const MAX_TOKENS: usize = 20;
/// Longest `O` token memcached echoes, counting the `O` itself.
const MAX_OPAQUE: usize = 32;

const MS_MODES: &[u8] = b"EAPRS";
const MA_MODES: &[u8] = b"I+D-";

pub(super) fn parse<'a>(name: &'a [u8], mut tokens: Tokens<'a>, item_max: usize) -> Line<'a> {
    if name == b"mn" {
        return Ok((Command::MetaNoop, None));
    }
    let raw_key = tokens.next().ok_or(ParseError::BadFormat)?;
    if name == b"ms" {
        return set(raw_key, tokens, item_max);
    }
    let flags = tokens.rest();
    let scan = scan_flags(flags);
    if scan.bad_start {
        return Err(ParseError::BadFormat.into());
    }
    if name == b"me" {
        return Ok((Command::MetaDebug(debug_options(raw_key, flags)?), None));
    }
    let opts = options(raw_key, flags, 2 + scan.count).map_err(|error| match name {
        // memcached reports every flag problem on `ma` with one message.
        b"ma" => ParseError::InvalidArithFlag,
        _ => error,
    })?;
    if name == b"ma" && opts.mode.is_some_and(|mode| !MA_MODES.contains(&mode)) {
        return Err(ParseError::InvalidArithMode.into());
    }
    if scan.opaque_too_long {
        return Err(ParseError::OpaqueTooLong.into());
    }
    let cmd = match name {
        b"mg" => Command::MetaGet(opts),
        b"md" => Command::MetaDelete(opts),
        _ => Command::MetaArith(opts),
    };
    Ok((cmd, None))
}

fn set<'a>(raw_key: &'a [u8], mut tokens: Tokens<'a>, item_max: usize) -> Line<'a> {
    // Without a usable length there is no telling how much data follows, so nothing is swallowed.
    let len = tokens
        .next()
        .and_then(number::<i32>)
        .filter(|len| (0..=i32::MAX - 2).contains(len))
        .ok_or(ParseError::BadFormat)? as usize;
    let flags = tokens.rest();
    let scan = scan_flags(flags);
    if scan.bad_start {
        return Err(ParseError::BadFormat.into());
    }
    let fail = |error, unlink| LineError {
        error,
        swallow: len + 2,
        silent: false,
        unlink,
    };
    let opts = options(raw_key, flags, 3 + scan.count).map_err(|error| fail(error, None))?;
    if opts.mode.is_some_and(|mode| !MS_MODES.contains(&mode)) {
        return Err(fail(ParseError::InvalidSetMode, None));
    }
    if len > item_max {
        let plain_set = opts.mode.is_none_or(|mode| mode == b'S');
        return Err(fail(ParseError::TooLarge, plain_set.then_some(opts.key)));
    }
    if scan.opaque_too_long {
        return Err(fail(ParseError::OpaqueTooLong, None));
    }
    Ok((Command::MetaSet { opts, data: &[] }, Some(len)))
}

fn decode_key(raw_key: &[u8]) -> Option<Vec<u8>> {
    STANDARD
        .decode(raw_key)
        .ok()
        .filter(|key| !key.is_empty() && key.len() <= MAX_KEY)
}

/// `me` looks only at the `b` flag and ignores every other one.
fn debug_options<'a>(raw_key: &'a [u8], flags: &'a [u8]) -> Result<MetaOpts<'a>, ParseError> {
    let base64 = Tokens::new(flags).any(|token| token == b"b");
    let key = match base64 {
        true => Cow::Owned(decode_key(raw_key).ok_or(ParseError::BadFormat)?),
        false if valid_key(raw_key) => Cow::Borrowed(raw_key),
        false => return Err(ParseError::BadFormat),
    };
    Ok(MetaOpts {
        key,
        raw_key,
        flags,
        base64,
        ..Default::default()
    })
}

/// Parses the flags of `mg`, `ms`, `md` and `ma`, which all accept the same flags and ignore
/// those irrelevant to them. `tokens` counts every token on the line.
fn options<'a>(
    raw_key: &'a [u8],
    flags: &'a [u8],
    tokens: usize,
) -> Result<MetaOpts<'a>, ParseError> {
    if raw_key.len() > MAX_KEY {
        return Err(ParseError::BadFormat);
    }
    if tokens > MAX_TOKENS {
        return Err(ParseError::TooManyFlags);
    }
    let mut opts = MetaOpts {
        key: Cow::Borrowed(raw_key),
        raw_key,
        flags,
        ..Default::default()
    };
    // A bad argument does not stop memcached's scan: a later bad argument replaces the
    // message, and a duplicate or unknown flag found later takes over entirely.
    let mut error = None;
    let mut seen = 0u128;
    for token in Tokens::new(flags) {
        let flag = token[0];
        let arg = &token[1..];
        if flag >= 127 || seen & (1 << flag) != 0 {
            return Err(ParseError::DuplicateFlag);
        }
        seen |= 1 << flag;
        match flag {
            b'b' => {
                opts.base64 = true;
                match decode_key(raw_key) {
                    Some(key) => opts.key = Cow::Owned(key),
                    None => error = Some(ParseError::BadKeyEncoding),
                }
            }
            b'q' => opts.quiet = true,
            b'v' => opts.value = true,
            b'u' => opts.no_bump = true,
            b'I' => opts.invalidate = true,
            b'x' => opts.remove_value = true,
            b'N' => opts.vivify = seconds(arg, &mut error),
            b'T' => opts.ttl = seconds(arg, &mut error),
            b'R' => opts.recache = seconds(arg, &mut error),
            b'C' => opts.cas = argument(arg, &mut error, ParseError::BadToken),
            b'E' => opts.new_cas = argument(arg, &mut error, ParseError::BadToken),
            b'J' => opts.initial = argument(arg, &mut error, ParseError::InvalidInitialValue),
            b'D' => opts.delta = argument(arg, &mut error, ParseError::InvalidDeltaValue),
            b'F' => {
                // A bad `F` fails the command without choosing a message of its own.
                opts.client_flags = number(arg);
                if opts.client_flags.is_none() {
                    error.get_or_insert(ParseError::BadFormat);
                }
            }
            // A bare `M` reads as an unknown mode.
            b'M' => opts.mode = Some(arg.first().copied().unwrap_or(b' ')),
            b'O' => {
                opts.returns.add(flag);
                opts.returns.opaque = token;
            }
            b'c' | b'f' | b'h' | b'k' | b'l' | b's' | b't' => opts.returns.add(flag),
            b'P' | b'L' => {}
            _ => return Err(ParseError::InvalidFlag),
        }
    }
    if let Some(error) = error {
        return Err(error);
    }
    if !opts.base64 && !valid_key(raw_key) {
        return Err(ParseError::BadFormat);
    }
    Ok(opts)
}

/// Parses a flag's numeric argument, recording `kind` as the error if it is missing or malformed.
fn argument<T: std::str::FromStr>(
    arg: &[u8],
    error: &mut Option<ParseError>,
    kind: ParseError,
) -> Option<T> {
    let value = number(arg);
    if value.is_none() {
        *error = Some(kind);
    }
    value
}

/// Parses a TTL argument, which memcached reads as a 32-bit signed number.
fn seconds(arg: &[u8], error: &mut Option<ParseError>) -> Option<i64> {
    argument::<i32>(arg, error, ParseError::BadToken).map(i64::from)
}
