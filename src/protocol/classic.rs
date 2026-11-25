use std::borrow::Cow;

use super::tokens::{Tokens, lenient_number, number, valid_key, valid_key_list};
use super::{Command, Line, LineError, ParseError, StoreMode};

/// Commands memcached recognizes even after a leading space.
const SPACE_TOLERANT: &[&[u8]] = &[
    b"flush_all",
    b"verbosity",
    b"shutdown",
    b"cache_memlimit",
    b"slabs",
    b"lru",
    b"lru_crawler",
];

pub(super) fn parse<'a>(
    name: &'a [u8],
    mut tokens: Tokens<'a>,
    item_max: usize,
    leading_space: bool,
) -> Line<'a> {
    // memcached reads `noreply` from the last token of the line, whatever its position,
    // and it silences error replies as well as normal ones.
    let noreply = tokens.last_token().unwrap_or(name) == b"noreply";
    let fail = |error| LineError {
        error,
        swallow: 0,
        silent: noreply,
        unlink: None,
    };
    if leading_space && !SPACE_TOLERANT.contains(&name) {
        return Err(fail(ParseError::UnknownCommand));
    }
    let cmd = match name {
        b"get" | b"gets" => Command::Get {
            keys: keys(&tokens)?,
            cas: name == b"gets",
        },
        b"gat" | b"gats" => {
            let exptime = tokens.next().ok_or(ParseError::BadFormat)?;
            if tokens.clone().next().is_none() {
                return Err(ParseError::BadFormat.into());
            }
            let exptime: i32 = number(exptime).ok_or(ParseError::InvalidExptime)?;
            Command::Gat {
                exptime: exptime.into(),
                keys: keys(&tokens)?,
                cas: name == b"gats",
            }
        }
        b"set" => return storage(StoreMode::Set, false, tokens, item_max, noreply),
        b"add" => return storage(StoreMode::Add, false, tokens, item_max, noreply),
        b"replace" => return storage(StoreMode::Replace, false, tokens, item_max, noreply),
        b"append" => return storage(StoreMode::Append, false, tokens, item_max, noreply),
        b"prepend" => return storage(StoreMode::Prepend, false, tokens, item_max, noreply),
        b"cas" => return storage(StoreMode::Set, true, tokens, item_max, noreply),
        b"delete" => {
            // Anything after the key, such as a hold time, is ignored.
            let key = tokens.next().ok_or(ParseError::BadFormat)?;
            if !valid_key(key) {
                return Err(fail(ParseError::BadFormat));
            }
            Command::Delete { key, noreply }
        }
        b"incr" | b"decr" => {
            let (Some(key), Some(delta)) = (tokens.next(), tokens.next()) else {
                return Err(ParseError::BadFormat.into());
            };
            if !valid_key(key) {
                return Err(fail(ParseError::BadFormat));
            }
            let delta: u64 = number(delta).ok_or_else(|| fail(ParseError::InvalidDelta))?;
            Command::Arith {
                key,
                delta,
                decr: name == b"decr",
                noreply,
            }
        }
        b"touch" => {
            let (Some(key), Some(exptime)) = (tokens.next(), tokens.next()) else {
                return Err(ParseError::BadFormat.into());
            };
            if !valid_key(key) {
                return Err(fail(ParseError::BadFormat));
            }
            let exptime: i32 = number(exptime).ok_or_else(|| fail(ParseError::InvalidExptime))?;
            Command::Touch {
                key,
                exptime: exptime.into(),
                noreply,
            }
        }
        b"flush_all" => {
            // A delay is expected unless the line is exactly `flush_all` or `flush_all noreply`.
            let delay: i32 = match tokens.next() {
                None => 0,
                Some(_) if noreply && tokens.clone().next().is_none() => 0,
                Some(token) => number(token).ok_or_else(|| fail(ParseError::InvalidExptime))?,
            };
            Command::FlushAll {
                delay: delay.max(0) as u64,
                noreply,
            }
        }
        b"stats" => Command::Stats {
            arg: tokens.next().unwrap_or(b""),
        },
        b"version" => Command::Version,
        b"quit" => Command::Quit,
        b"shutdown" => {
            // No mode or `graceful` alone. Whether the mode matters is decided at execution,
            // because a server with shutdown disabled reports that first.
            let valid = match (tokens.next(), tokens.next()) {
                (None, _) => true,
                (Some(mode), None) => mode == b"graceful",
                _ => false,
            };
            Command::Shutdown { valid, noreply }
        }
        b"verbosity" => {
            tokens
                .next()
                .and_then(number::<u32>)
                .ok_or_else(|| fail(ParseError::BadFormat))?;
            Command::Accepted { noreply }
        }
        b"cache_memlimit" => {
            tokens
                .next()
                .ok_or_else(|| fail(ParseError::UnknownCommand))?;
            Command::Accepted { noreply }
        }
        b"slabs" => match tokens.next() {
            Some(b"automove") => Command::Accepted { noreply: false },
            _ => return Err(fail(ParseError::UnknownCommand)),
        },
        b"lru" => Command::Accepted { noreply: false },
        _ => return Err(fail(ParseError::UnknownCommand)),
    };
    Ok((cmd, None))
}

fn storage<'a>(
    mode: StoreMode,
    with_cas: bool,
    mut tokens: Tokens<'a>,
    item_max: usize,
    noreply: bool,
) -> Line<'a> {
    let (Some(key), Some(flags), Some(exptime), Some(len)) =
        (tokens.next(), tokens.next(), tokens.next(), tokens.next())
    else {
        return Err(ParseError::BadFormat.into());
    };
    // Without a usable length there is no telling how much data follows, so nothing is
    // swallowed and `noreply` is not honored.
    let len = number::<i32>(len)
        .filter(|len| (0..=i32::MAX - 2).contains(len))
        .ok_or(ParseError::BadFormat)? as usize;
    // Every later error discards the data block.
    let fail = |error, unlink| LineError {
        error,
        swallow: len + 2,
        silent: noreply,
        unlink,
    };
    if !valid_key(key) {
        return Err(fail(ParseError::BadFormat, None));
    }
    let (Some(flags), Some(exptime)) =
        (lenient_number::<u32>(flags), lenient_number::<i64>(exptime))
    else {
        return Err(fail(ParseError::BadFormat, None));
    };
    let cas = match with_cas {
        true => Some(
            tokens
                .next()
                .and_then(number::<u64>)
                .ok_or_else(|| fail(ParseError::BadFormat, None))?,
        ),
        false => None,
    };
    if len > item_max {
        let unlink = (mode == StoreMode::Set && !with_cas).then_some(Cow::Borrowed(key));
        return Err(fail(ParseError::TooLarge, unlink));
    }
    Ok((
        Command::Store {
            mode,
            key,
            flags,
            exptime,
            cas,
            noreply,
            data: &[],
        },
        Some(len),
    ))
}

/// The rest of the line as a key list, checked to hold at least one key, all valid.
fn keys<'a>(tokens: &Tokens<'a>) -> Result<&'a [u8], ParseError> {
    let rest = tokens.rest();
    match valid_key_list(rest) {
        true => Ok(rest),
        false => Err(ParseError::BadFormat),
    }
}
