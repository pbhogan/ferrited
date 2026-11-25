use bytes::Bytes;

/// Fixed per-item bookkeeping charge in bytes.
pub const ITEM_OVERHEAD: usize = 96;

/// Memory charged for one item.
pub fn cost(key_len: usize, value_len: usize) -> usize {
    key_len + value_len + ITEM_OVERHEAD
}

#[derive(Clone, Debug)]
pub struct Item {
    pub value: Bytes,
    /// Opaque 32-bit client flags.
    pub flags: u32,
    /// Absolute Unix seconds; 0 means never.
    pub expiry: u64,
    /// Ordering version from the clock.
    pub version: u64,
    /// CAS token shown to clients. Equals `version` unless a client supplied its own.
    pub cas: u64,
    pub last_access: u64,
    /// Whether the item has been read since it was stored.
    pub fetched: bool,
    /// Marked stale by an invalidating delete or set.
    pub stale: bool,
    /// Whether a recache token has already been handed to a client.
    pub token_sent: bool,
}

pub struct StoreConfig {
    /// Total memory budget in bytes.
    pub memory_limit: usize,
    /// Largest allowed value in bytes.
    pub item_max: usize,
    /// Number of shards; rounded up to a power of two.
    pub shards: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SetMode {
    #[default]
    Set,
    Add,
    Replace,
    Append,
    Prepend,
}

#[derive(Debug, Default)]
pub struct SetArgs {
    pub mode: SetMode,
    pub flags: u32,
    pub expiry: u64,
    /// Store only if the item's CAS token equals this. Ignored by `Add`; `Append` and
    /// `Prepend` check it only when the item exists.
    pub cas: Option<u64>,
    /// Use this as the new item's CAS token.
    pub cas_override: Option<u64>,
    /// With `cas` on `Set` or `Replace`: accept an older token but mark the item stale.
    pub invalidate: bool,
    /// For append/prepend on a miss: create the item with this expiry.
    pub vivify: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum SetOutcome {
    /// `size` is the length of the stored value, which for append and prepend is the joined value.
    Stored {
        cas: u64,
        size: usize,
    },
    NotStored,
    Exists,
    NotFound,
    TooLarge,
}

#[derive(Debug, Default)]
pub struct GetArgs {
    /// Set a new absolute expiry on a hit.
    pub touch: Option<u64>,
    /// Do not update last-access time or the fetched bit.
    pub no_bump: bool,
    /// On a miss, create an empty item with this expiry and report a win.
    pub vivify: Option<u64>,
    /// Report a win if the remaining TTL in seconds is below this.
    pub recache: Option<u64>,
    /// CAS token for an item created by `vivify`.
    pub cas_override: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct GetHit {
    pub value: Bytes,
    pub flags: u32,
    pub cas: u64,
    pub expiry: u64,
    /// Last-access time before this read.
    pub last_access: u64,
    /// Whether the item had been read before this read.
    pub fetched: bool,
    pub stale: bool,
    /// This client won the right to recache.
    pub win: bool,
    /// Another client already holds the recache token.
    pub lost: bool,
}

#[derive(Debug, Default)]
pub struct DeleteArgs {
    pub cas: Option<u64>,
    /// Mark stale instead of removing.
    pub invalidate: bool,
    /// With `invalidate` or `keep_empty`: new absolute expiry.
    pub expiry: Option<u64>,
    /// Replace the item with an empty one that has `flags` and `expiry` (or no expiry).
    pub keep_empty: bool,
    /// With `keep_empty`: client flags of the empty item.
    pub flags: u32,
    pub cas_override: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    NotFound,
    Exists,
}

#[derive(Debug, Default)]
pub struct ArithArgs {
    pub decr: bool,
    pub delta: u64,
    pub cas: Option<u64>,
    /// On a miss, create the item with (expiry, initial value).
    pub vivify: Option<(u64, u64)>,
    /// New absolute expiry on success.
    pub touch: Option<u64>,
    pub cas_override: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ArithOutcome {
    Value { value: u64, cas: u64, expiry: u64 },
    NotFound,
    Exists,
    NonNumeric,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ItemMeta {
    pub expiry: u64,
    pub last_access: u64,
    pub cas: u64,
    pub fetched: bool,
    pub size: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct StoreStats {
    pub bytes: u64,
    pub curr_items: u64,
    pub total_items: u64,
    pub evictions: u64,
    pub reclaimed: u64,
    pub limit_maxbytes: u64,
}

/// What applying a peer's record did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    /// The record was newer and took effect.
    Applied,
    /// The record was not newer; nothing changed.
    Stale,
    /// A newer put too large for the memory limit; applied and logged as a delete at its version.
    Oversized,
}
