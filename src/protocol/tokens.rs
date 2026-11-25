use super::MAX_KEY;

/// Iterates over the space-separated tokens of a request line.
#[derive(Clone)]
pub(super) struct Tokens<'a> {
    rest: &'a [u8],
}

impl<'a> Tokens<'a> {
    pub fn new(line: &'a [u8]) -> Self {
        Self { rest: line }
    }

    /// Everything not yet consumed, without leading spaces.
    pub fn rest(&self) -> &'a [u8] {
        let start = self
            .rest
            .iter()
            .position(|byte| *byte != b' ')
            .unwrap_or(self.rest.len());
        &self.rest[start..]
    }

    /// The last token not yet consumed, found without scanning the earlier ones.
    pub fn last_token(&self) -> Option<&'a [u8]> {
        let end = self.rest.iter().rposition(|byte| *byte != b' ')? + 1;
        let start = memchr::memrchr(b' ', &self.rest[..end]).map_or(0, |space| space + 1);
        Some(&self.rest[start..end])
    }
}

impl<'a> Iterator for Tokens<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        let start = self.rest.iter().position(|byte| *byte != b' ')?;
        let rest = &self.rest[start..];
        let end = rest
            .iter()
            .position(|byte| *byte == b' ')
            .unwrap_or(rest.len());
        self.rest = &rest[end..];
        Some(&rest[..end])
    }
}

/// Parses a decimal token the way memcached's tokenizer does: digits only, with a
/// leading `-` for signed types. A leading `+` is rejected.
pub(super) fn number<T: std::str::FromStr>(token: &[u8]) -> Option<T> {
    if token.first() == Some(&b'+') {
        return None;
    }
    lenient_number(token)
}

/// Parses a decimal token like C's `strtol`, accepting a leading `+`. memcached reads the flags
/// and exptime of classic storage commands this way.
pub(super) fn lenient_number<T: std::str::FromStr>(token: &[u8]) -> Option<T> {
    std::str::from_utf8(token).ok()?.parse().ok()
}

/// Whether `list` holds at least one key and every space-separated key in it is valid.
pub(super) fn valid_key_list(list: &[u8]) -> bool {
    // A branch-free pass vectorizes; most lists hold one key, so spaces are searched for only
    // when the pass saw one.
    let (bad, spaces) = list.iter().fold((0u8, 0u8), |(bad, spaces), &byte| {
        (
            bad | u8::from(byte < 32 || byte == 127),
            spaces | u8::from(byte == b' '),
        )
    });
    if bad != 0 {
        return false;
    }
    if spaces == 0 {
        return !list.is_empty() && list.len() <= MAX_KEY;
    }
    let mut start = 0;
    let mut any = false;
    for space in memchr::memchr_iter(b' ', list).chain([list.len()]) {
        let len = space - start;
        if len > MAX_KEY {
            return false;
        }
        any |= len > 0;
        start = space + 1;
    }
    any
}

/// A key is 1 to 250 bytes with no spaces or control characters.
pub(super) fn valid_key(key: &[u8]) -> bool {
    if key.is_empty() || key.len() > MAX_KEY {
        return false;
    }
    // Checking every byte without stopping early lets the compiler vectorize the loop.
    let bad = key
        .iter()
        .fold(0u8, |bad, &byte| bad | u8::from(byte <= 32 || byte == 127));
    bad == 0
}
