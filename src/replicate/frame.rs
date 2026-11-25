//! Frames of the replication link: `[payload length: u32 LE][type: u8][payload]`, with records
//! in the persistence codec's encoding and no checksum, since TCP checks the bytes. With a
//! secret, frames after the handshake are `[header][header tag][payload][payload tag]`, so the
//! header is authenticated before the payload is read (see `Signing`).

use std::fmt;
use std::io;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::oplog::Record;
use crate::persist::codec::{self, Entry, ReadOutcome};

/// Opens every hello, so a stray connection is told apart from a peer.
pub const MAGIC: u32 = u32::from_le_bytes(*b"FRTD");
/// Covers the handshake's proofs and the signed frame layout. Nodes of different versions
/// refuse each other for the version, not for a wrong secret or a bad frame.
pub const PROTOCOL_VERSION: u16 = 1;
/// Bytes in a nonce, and in a proof of the secret.
pub const NONCE_LEN: usize = 32;
/// Largest frame accepted before the handshake has finished.
pub const HANDSHAKE_FRAME_MAX: u32 = 4096;
/// Largest frame accepted after it: a record of the largest value the codec holds, plus room.
pub const FRAME_MAX: u32 = (1 << 30) + (1 << 20);

/// A frame's payload length and type.
pub const HEADER_LEN: usize = 5;
/// Bytes in each of a signed frame's two tags.
pub const TAG_LEN: usize = 16;

/// Tag domains, so a header tag never passes as a payload tag, even with an empty payload.
const HEADER_TAG: u8 = 1;
const PAYLOAD_TAG: u8 = 2;

type HmacSha256 = Hmac<Sha256>;

const HELLO: u8 = 1;
const PROOF: u8 = 2;
const ACCEPT: u8 = 3;
const REFUSE: u8 = 4;
const BATCH: u8 = 5;
const ACK: u8 = 6;
const HEARTBEAT: u8 = 7;
const SYNC_START: u8 = 8;
const SYNC_ITEMS: u8 = 9;
const SYNC_END: u8 = 10;

/// How a node introduces itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hello {
    pub version: u16,
    pub node_id: u8,
    pub run: u64,
    /// The sequence number the node's next log entry will have.
    pub head: u64,
    /// Still loading its data. A recovering listener completes the handshake, then closes.
    pub recovering: bool,
    /// Whether the node has a secret configured.
    pub auth: bool,
    pub nonce: [u8; NONCE_LEN],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Hello(Hello),
    /// Proof that the sender knows the secret.
    Proof([u8; NONCE_LEN]),
    /// The listener's proof, with a secret, and its position on the sender's log as
    /// `(run, next)`, if it has one.
    Accept {
        proof: Option<[u8; NONCE_LEN]>,
        position: Option<(u64, u64)>,
    },
    Refuse(String),
    /// This node's own records among the log entries below `next` not yet sent.
    Batch {
        next: u64,
        records: Vec<Record>,
    },
    /// The receiver has applied every entry below `next`.
    Ack {
        next: u64,
    },
    Heartbeat,
    SyncStart,
    SyncItems(Vec<Record>),
    /// The full sync is complete; the stream continues from `next`.
    SyncEnd {
        next: u64,
    },
}

impl Frame {
    /// A short name for messages.
    pub fn name(&self) -> &'static str {
        match self {
            Frame::Hello(_) => "hello",
            Frame::Proof(_) => "proof",
            Frame::Accept { .. } => "accept",
            Frame::Refuse(_) => "refusal",
            Frame::Batch { .. } => "batch",
            Frame::Ack { .. } => "ack",
            Frame::Heartbeat => "heartbeat",
            Frame::SyncStart => "sync start",
            Frame::SyncItems(_) => "sync items",
            Frame::SyncEnd { .. } => "sync end",
        }
    }

    /// Appends the framed encoding to `out`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        self.encode_with_gap(out, 0);
    }

    /// Leaves `gap` zero bytes (at most `TAG_LEN`) after the header, for a header tag.
    fn encode_with_gap(&self, out: &mut Vec<u8>, gap: usize) {
        let start = out.len();
        out.extend_from_slice(&[0; HEADER_LEN + TAG_LEN][..HEADER_LEN + gap]);
        let kind = match self {
            Frame::Hello(hello) => {
                out.extend_from_slice(&MAGIC.to_le_bytes());
                out.extend_from_slice(&hello.version.to_le_bytes());
                out.push(hello.node_id);
                out.extend_from_slice(&hello.run.to_le_bytes());
                out.extend_from_slice(&hello.head.to_le_bytes());
                out.push(u8::from(hello.recovering) | (u8::from(hello.auth) << 1));
                out.extend_from_slice(&hello.nonce);
                HELLO
            }
            Frame::Proof(proof) => {
                out.extend_from_slice(proof);
                PROOF
            }
            Frame::Accept { proof, position } => {
                out.push(u8::from(proof.is_some()) | (u8::from(position.is_some()) << 1));
                if let Some(proof) = proof {
                    out.extend_from_slice(proof);
                }
                if let Some((run, next)) = position {
                    out.extend_from_slice(&run.to_le_bytes());
                    out.extend_from_slice(&next.to_le_bytes());
                }
                ACCEPT
            }
            Frame::Refuse(reason) => {
                out.extend_from_slice(reason.as_bytes());
                REFUSE
            }
            Frame::Batch { next, records } => {
                out.extend_from_slice(&next.to_le_bytes());
                for record in records {
                    codec::encode_record(record, out);
                }
                BATCH
            }
            Frame::Ack { next } => {
                out.extend_from_slice(&next.to_le_bytes());
                ACK
            }
            Frame::Heartbeat => HEARTBEAT,
            Frame::SyncStart => SYNC_START,
            Frame::SyncItems(records) => {
                for record in records {
                    codec::encode_record(record, out);
                }
                SYNC_ITEMS
            }
            Frame::SyncEnd { next } => {
                out.extend_from_slice(&next.to_le_bytes());
                SYNC_END
            }
        };
        let len = out.len() - start - HEADER_LEN - gap;
        assert!(
            len <= FRAME_MAX as usize,
            "a frame of {len} bytes is over the {FRAME_MAX}-byte limit; the sender must split it"
        );
        let len = len as u32;
        out[start..start + 4].copy_from_slice(&len.to_le_bytes());
        out[start + 4] = kind;
    }

    /// Decodes a frame's payload, or `None` if it is malformed.
    fn decode(kind: u8, payload: &[u8]) -> Option<Frame> {
        let mut input = Input(payload);
        let frame = match kind {
            HELLO => {
                if input.u32()? != MAGIC {
                    return None;
                }
                let version = input.u16()?;
                if version != PROTOCOL_VERSION {
                    // Another version may lay out the rest differently.
                    return Some(Frame::Hello(Hello {
                        version,
                        node_id: 0,
                        run: 0,
                        head: 0,
                        recovering: false,
                        auth: false,
                        nonce: [0; NONCE_LEN],
                    }));
                }
                let node_id = input.u8()?;
                let run = input.u64()?;
                let head = input.u64()?;
                let flags = input.u8()?;
                Frame::Hello(Hello {
                    version,
                    node_id,
                    run,
                    head,
                    recovering: flags & 1 != 0,
                    auth: flags & 2 != 0,
                    nonce: input.array()?,
                })
            }
            PROOF => Frame::Proof(input.array()?),
            ACCEPT => {
                let flags = input.u8()?;
                let proof = if flags & 1 != 0 {
                    Some(input.array()?)
                } else {
                    None
                };
                let position = if flags & 2 != 0 {
                    Some((input.u64()?, input.u64()?))
                } else {
                    None
                };
                Frame::Accept { proof, position }
            }
            REFUSE => Frame::Refuse(sanitize(input.rest())),
            BATCH => {
                let next = input.u64()?;
                Frame::Batch {
                    next,
                    records: records(input.rest())?,
                }
            }
            ACK => Frame::Ack { next: input.u64()? },
            HEARTBEAT => Frame::Heartbeat,
            SYNC_START => Frame::SyncStart,
            SYNC_ITEMS => Frame::SyncItems(records(input.rest())?),
            SYNC_END => Frame::SyncEnd { next: input.u64()? },
            _ => return None,
        };
        input.0.is_empty().then_some(frame)
    }
}

/// The longest refusal text kept, in characters.
const REASON_MAX: usize = 200;

/// A peer's refusal text made safe to log: truncated, control characters replaced with `?`.
fn sanitize(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .chars()
        .take(REASON_MAX)
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

/// Decodes back-to-back codec records.
fn records(mut bytes: &[u8]) -> Option<Vec<Record>> {
    let mut records = Vec::new();
    let mut scratch = Vec::new();
    loop {
        match codec::read_entry(&mut bytes, &mut scratch).ok()? {
            ReadOutcome::Entry(Entry::Record(record)) => records.push(record),
            ReadOutcome::Eof => return Some(records),
            _ => return None,
        }
    }
}

struct Input<'a>(&'a [u8]);

impl<'a> Input<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.0.len() < len {
            return None;
        }
        let (taken, rest) = self.0.split_at(len);
        self.0 = rest;
        Some(taken)
    }

    fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.0)
    }

    fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        self.take(N)?.try_into().ok()
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.array::<1>()?[0])
    }

    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.array()?))
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.array()?))
    }
}

/// One direction of a signed link. Each tag is a truncated HMAC-SHA256 of its domain, the
/// frame's number in this direction and the header; the payload tag also covers the payload.
/// The header tag lets a reader trust the length before reading the payload. Numbers are
/// counted, not sent, so a dropped, inserted, reordered or replayed frame fails. Each link and
/// direction has its own key (see `handshake`).
///
/// Not `Clone`: a copy would reuse the key and count.
pub struct Signing {
    /// Keyed once and cloned per tag, without allocating.
    mac: HmacSha256,
    count: u64,
}

impl Signing {
    pub fn new(key: &[u8]) -> Self {
        Self {
            mac: HmacSha256::new_from_slice(key).expect("HMAC takes a key of any length"),
            count: 0,
        }
    }

    /// The next frame's `domain` tag, so far over its number and `header`.
    fn start(&self, domain: u8, header: &[u8]) -> HmacSha256 {
        let mut mac = self.mac.clone();
        mac.update(&[domain]);
        mac.update(&self.count.to_le_bytes());
        mac.update(header);
        mac
    }

    /// The next frame's header tag. The frame is counted only with its payload tag.
    pub(crate) fn header_tag(&self, header: &[u8]) -> [u8; TAG_LEN] {
        truncated(self.start(HEADER_TAG, header))
    }

    /// Checks the next frame's header tag in constant time.
    pub(crate) fn check_header(&self, header: &[u8], tag: &[u8]) -> bool {
        self.start(HEADER_TAG, header)
            .verify_truncated_left(tag)
            .is_ok()
    }

    /// The next frame's payload tag; counts the frame.
    fn next(&mut self, header: &[u8], payload: &[u8]) -> HmacSha256 {
        let mut mac = self.start(PAYLOAD_TAG, header);
        mac.update(payload);
        self.count += 1;
        mac
    }

    pub(crate) fn tag(&mut self, header: &[u8], payload: &[u8]) -> [u8; TAG_LEN] {
        truncated(self.next(header, payload))
    }

    /// Checks the next frame's payload tag in constant time.
    pub(crate) fn check(&mut self, header: &[u8], payload: &[u8], tag: &[u8]) -> bool {
        self.next(header, payload)
            .verify_truncated_left(tag)
            .is_ok()
    }
}

/// The first `TAG_LEN` bytes of `mac`.
fn truncated(mac: HmacSha256) -> [u8; TAG_LEN] {
    let mut tag = [0; TAG_LEN];
    tag.copy_from_slice(&mac.finalize().into_bytes()[..TAG_LEN]);
    tag
}

impl fmt::Debug for Signing {
    /// Leaves the key out, so it never reaches a log.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signing")
            .field("count", &self.count)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct BadTag;

impl fmt::Display for BadTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "a frame failed authentication: it was changed, inserted, dropped, reordered or replayed on the way",
        )
    }
}

impl std::error::Error for BadTag {}

/// Whether `error` is a frame that failed authentication.
pub fn is_bad_tag(error: &io::Error) -> bool {
    error.get_ref().is_some_and(|inner| inner.is::<BadTag>())
}

/// A read buffer that grew past this many bytes is shrunk back after the frame.
const SHRINK_ABOVE: usize = 4 << 20;

/// Reads one unsigned frame, refusing one longer than `max` before reading its payload.
pub async fn read_frame<R: AsyncRead + Unpin>(
    input: &mut R,
    max: u32,
    buf: &mut Vec<u8>,
) -> io::Result<Frame> {
    read_frame_with(input, max, buf, None).await
}

/// Like `read_frame`, checking both tags when `signing` is given; a wrong tag fails as
/// `is_bad_tag` and is never decoded. The header tag is checked before the length is trusted,
/// so a forged header allocates nothing for its payload.
pub async fn read_frame_with<R: AsyncRead + Unpin>(
    input: &mut R,
    max: u32,
    buf: &mut Vec<u8>,
    signing: Option<&mut Signing>,
) -> io::Result<Frame> {
    let mut head = [0u8; HEADER_LEN + TAG_LEN];
    let head_len = if signing.is_some() {
        head.len()
    } else {
        HEADER_LEN
    };
    input.read_exact(&mut head[..head_len]).await?;
    let (header, header_tag) = head.split_at(HEADER_LEN);
    if let Some(signing) = signing.as_deref()
        && !signing.check_header(header, header_tag)
    {
        return Err(bad_tag());
    }
    let len = u32::from_le_bytes(header[..4].try_into().unwrap());
    if len > max {
        return Err(invalid(format!(
            "a frame of {len} bytes is over the {max}-byte limit"
        )));
    }
    // Read incrementally, so a claimed length commits no memory until the bytes arrive.
    buf.clear();
    let read = (&mut *input).take(u64::from(len)).read_to_end(buf).await?;
    if read != len as usize {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    if let Some(signing) = signing {
        let mut tag = [0; TAG_LEN];
        input.read_exact(&mut tag).await?;
        if !signing.check(header, buf, &tag) {
            return Err(bad_tag());
        }
    }
    let frame = Frame::decode(header[4], buf)
        .ok_or_else(|| invalid(format!("a malformed frame of type {}", header[4])));
    if buf.capacity() > SHRINK_ABOVE {
        buf.clear();
        buf.shrink_to(SHRINK_ABOVE);
    }
    frame
}

/// Writes one unsigned frame.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    output: &mut W,
    frame: &Frame,
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    write_frame_with(output, frame, buf, None).await
}

/// Like `write_frame`, adding both tags when `signing` is given.
pub async fn write_frame_with<W: AsyncWrite + Unpin>(
    output: &mut W,
    frame: &Frame,
    buf: &mut Vec<u8>,
    signing: Option<&mut Signing>,
) -> io::Result<()> {
    buf.clear();
    match signing {
        None => frame.encode(buf),
        Some(signing) => {
            frame.encode_with_gap(buf, TAG_LEN);
            let (head, payload) = buf.split_at_mut(HEADER_LEN + TAG_LEN);
            let (header, header_tag) = head.split_at_mut(HEADER_LEN);
            header_tag.copy_from_slice(&signing.header_tag(header));
            let tag = signing.tag(header, payload);
            buf.extend_from_slice(&tag);
        }
    }
    output.write_all(buf).await
}

fn bad_tag() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, BadTag)
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn put(key: &str) -> Record {
        Record::Put {
            key: Bytes::from(key.to_string()),
            version: 5,
            cas: 6,
            flags: 7,
            expiry: 8,
            stale: true,
            value: Bytes::from_static(b"value"),
        }
    }

    fn every_frame() -> Vec<Frame> {
        let hello = Hello {
            version: PROTOCOL_VERSION,
            node_id: 3,
            run: 99,
            head: 1234,
            recovering: false,
            auth: true,
            nonce: [7; NONCE_LEN],
        };
        vec![
            Frame::Hello(hello),
            Frame::Proof([9; NONCE_LEN]),
            Frame::Accept {
                proof: Some([1; NONCE_LEN]),
                position: Some((99, 50)),
            },
            Frame::Accept {
                proof: None,
                position: None,
            },
            Frame::Refuse("wrong secret".into()),
            Frame::Batch {
                next: 77,
                records: vec![
                    put("a"),
                    Record::Delete {
                        key: Bytes::from_static(b"b"),
                        version: 9,
                    },
                ],
            },
            Frame::Batch {
                next: 78,
                records: Vec::new(),
            },
            Frame::Ack { next: 78 },
            Frame::Heartbeat,
            Frame::SyncStart,
            Frame::SyncItems(vec![Record::Flush { version: 4 }, put("c")]),
            Frame::SyncEnd { next: 80 },
        ]
    }

    #[tokio::test]
    async fn every_frame_reads_back_in_order() {
        let mut bytes = Vec::new();
        for frame in every_frame() {
            frame.encode(&mut bytes);
        }
        let mut input = bytes.as_slice();
        let mut buf = Vec::new();
        for frame in every_frame() {
            assert_eq!(
                read_frame(&mut input, FRAME_MAX, &mut buf).await.unwrap(),
                frame
            );
        }
        assert!(input.is_empty());
    }

    #[tokio::test]
    async fn a_frame_over_the_limit_is_refused_before_its_payload_is_read() {
        let mut bytes = 10_000u32.to_le_bytes().to_vec();
        bytes.push(PROOF);
        let error = read_frame(&mut bytes.as_slice(), HANDSHAKE_FRAME_MAX, &mut Vec::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn a_claimed_length_commits_no_memory_before_the_payload_arrives() {
        let mut bytes = (FRAME_MAX - 1).to_le_bytes().to_vec();
        bytes.push(PROOF);
        bytes.extend_from_slice(&[0; 100]);
        let mut buf = Vec::new();
        let error = read_frame(&mut bytes.as_slice(), FRAME_MAX, &mut buf)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(buf.capacity() < 1 << 20, "capacity {}", buf.capacity());
    }

    #[tokio::test]
    async fn a_big_frame_does_not_leave_a_big_buffer() {
        let frame = Frame::Refuse("x".into());
        let mut bytes = Vec::new();
        frame.encode(&mut bytes);
        let mut buf = Vec::with_capacity(16 << 20);
        read_frame(&mut bytes.as_slice(), FRAME_MAX, &mut buf)
            .await
            .unwrap();
        assert!(
            buf.capacity() <= SHRINK_ABOVE,
            "capacity {}",
            buf.capacity()
        );
    }

    #[test]
    fn a_refusal_is_cut_and_stripped_of_control_characters() {
        let mut bytes = Vec::new();
        Frame::Refuse(format!("a\nb\x1b[31m{}", "z".repeat(1000))).encode(&mut bytes);
        let Some(Frame::Refuse(text)) = Frame::decode(bytes[4], &bytes[5..]) else {
            panic!("not a refusal");
        };
        assert_eq!(text.chars().count(), 200);
        assert!(text.starts_with("a?b?[31m"), "{text}");
    }

    #[tokio::test]
    async fn a_malformed_frame_is_an_error() {
        // An ack with a byte too many, and an unknown type.
        for (kind, payload) in [(ACK, vec![0u8; 9]), (99, Vec::new())] {
            let mut bytes = (payload.len() as u32).to_le_bytes().to_vec();
            bytes.push(kind);
            bytes.extend_from_slice(&payload);
            let error = read_frame(&mut bytes.as_slice(), FRAME_MAX, &mut Vec::new())
                .await
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "type {kind}");
        }
    }

    #[test]
    fn a_hello_of_another_version_reports_only_its_version() {
        let mut payload = MAGIC.to_le_bytes().to_vec();
        payload.extend_from_slice(&2u16.to_le_bytes());
        payload.extend_from_slice(b"laid out differently");
        let Some(Frame::Hello(hello)) = Frame::decode(HELLO, &payload) else {
            panic!("not a hello");
        };
        assert_eq!((hello.version, hello.node_id), (2, 0));
    }

    /// The writer's and reader's signing for one direction.
    fn signed_pair() -> (Signing, Signing) {
        (Signing::new(&[5; 32]), Signing::new(&[5; 32]))
    }

    /// `frames` signed in order, each as its own bytes.
    async fn signed(frames: &[Frame], signing: &mut Signing) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for frame in frames {
            let mut bytes = Vec::new();
            write_frame_with(&mut bytes, frame, &mut Vec::new(), Some(&mut *signing))
                .await
                .unwrap();
            out.push(bytes);
        }
        out
    }

    #[tokio::test]
    async fn signed_frames_read_back_in_order_and_carry_a_tag() {
        let (mut writer, mut reader) = signed_pair();
        let frames = every_frame();
        let written = signed(&frames, &mut writer).await;
        for (frame, bytes) in frames.iter().zip(&written) {
            let mut plain = Vec::new();
            frame.encode(&mut plain);
            assert_eq!(bytes.len(), plain.len() + 2 * TAG_LEN);
            assert_eq!(
                bytes[..HEADER_LEN],
                plain[..HEADER_LEN],
                "the header is unchanged"
            );
            assert_eq!(
                bytes[HEADER_LEN + TAG_LEN..bytes.len() - TAG_LEN],
                plain[HEADER_LEN..],
                "the payload is unchanged"
            );
        }
        let all = written.concat();
        let mut input = all.as_slice();
        let mut buf = Vec::new();
        for frame in frames {
            let read = read_frame_with(&mut input, FRAME_MAX, &mut buf, Some(&mut reader));
            assert_eq!(read.await.unwrap(), frame);
        }
        assert!(input.is_empty());
    }

    #[tokio::test]
    async fn a_signed_frame_with_any_byte_changed_is_refused() {
        let frame = Frame::Batch {
            next: 77,
            records: vec![put("a")],
        };
        let (mut writer, _) = signed_pair();
        let bytes = signed(&[frame], &mut writer).await.concat();
        for index in 0..bytes.len() {
            let mut changed = bytes.clone();
            changed[index] ^= 1;
            let (_, mut reader) = signed_pair();
            let read = read_frame_with(
                &mut changed.as_slice(),
                FRAME_MAX,
                &mut Vec::new(),
                Some(&mut reader),
            )
            .await;
            let error = read.expect_err(&format!("byte {index} changed"));
            assert!(is_bad_tag(&error), "byte {index}: {error}");
        }
    }

    /// Bytes before a signed frame's payload.
    const HEAD: usize = HEADER_LEN + TAG_LEN;

    /// A signed batch, as the first frame of a fresh writer.
    async fn signed_batch() -> Vec<u8> {
        let frame = Frame::Batch {
            next: 77,
            records: vec![put("a"), put("b")],
        };
        signed(&[frame], &mut signed_pair().0).await.concat()
    }

    /// Reads `bytes`, then padding, as a fresh reader's first frame; returns the error and the
    /// bytes consumed. A read stopped after the header tag must not allocate for the payload.
    async fn refused(bytes: &[u8]) -> (io::Error, usize) {
        let mut all = bytes.to_vec();
        all.extend_from_slice(&[0; 4096]);
        let mut input = all.as_slice();
        let mut buf = Vec::new();
        let (_, mut reader) = signed_pair();
        let read = read_frame_with(&mut input, FRAME_MAX, &mut buf, Some(&mut reader)).await;
        let error = read.expect_err("a refused frame");
        let consumed = all.len() - input.len();
        if consumed == HEAD {
            assert_eq!(buf.capacity(), 0, "memory committed for the payload");
        }
        (error, consumed)
    }

    #[tokio::test]
    async fn a_raised_length_is_refused_after_the_header_tag_without_reading_the_payload() {
        let bytes = signed_batch().await;
        let len = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        for raised in [
            len + 1,
            len + 100,
            1 << 20,
            FRAME_MAX - 1,
            FRAME_MAX,
            u32::MAX,
        ] {
            let mut changed = bytes.clone();
            changed[..4].copy_from_slice(&raised.to_le_bytes());
            let (error, consumed) = refused(&changed).await;
            assert!(is_bad_tag(&error), "length {raised}: {error}");
            assert_eq!(consumed, HEAD, "length {raised}");
        }
    }

    #[tokio::test]
    async fn a_forged_header_is_refused_without_waiting_for_its_payload() {
        let (mut write, mut read) = tokio::io::duplex(1024);
        let mut forged = (1u32 << 30).to_le_bytes().to_vec();
        forged.push(BATCH);
        forged.extend_from_slice(&[0; TAG_LEN]);
        write.write_all(&forged).await.unwrap();
        // `write` stays open: a reader that waited for the payload would wait forever.
        let (_, mut reader) = signed_pair();
        let mut buf = Vec::new();
        let read = read_frame_with(&mut read, FRAME_MAX, &mut buf, Some(&mut reader));
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), read)
            .await
            .expect("refused without waiting for the payload")
            .unwrap_err();
        assert!(is_bad_tag(&error), "{error}");
        assert_eq!(buf.capacity(), 0);
        drop(write);
    }

    #[tokio::test]
    async fn a_changed_type_is_refused_after_the_header_tag() {
        let bytes = signed_batch().await;
        for kind in [SYNC_ITEMS, ACK, HEARTBEAT, 99] {
            let mut changed = bytes.clone();
            changed[4] = kind;
            let (error, consumed) = refused(&changed).await;
            assert!(is_bad_tag(&error), "type {kind}: {error}");
            assert_eq!(consumed, HEAD, "type {kind}");
        }
    }

    #[tokio::test]
    async fn a_header_tag_from_another_frame_is_refused() {
        // Two heartbeats: the same header under the next number.
        let (mut writer, _) = signed_pair();
        let written = signed(&[Frame::Heartbeat, Frame::Heartbeat], &mut writer).await;
        let mut second = written[1].clone();
        second[HEADER_LEN..HEAD].copy_from_slice(&written[0][HEADER_LEN..HEAD]);
        let (_, mut reader) = signed_pair();
        let mut buf = Vec::new();
        let first = read_frame_with(
            &mut written[0].as_slice(),
            FRAME_MAX,
            &mut buf,
            Some(&mut reader),
        )
        .await
        .unwrap();
        assert_eq!(first, Frame::Heartbeat);
        let mut input = second.as_slice();
        let error = read_frame_with(&mut input, FRAME_MAX, &mut buf, Some(&mut reader))
            .await
            .unwrap_err();
        assert!(is_bad_tag(&error), "{error}");
        assert_eq!(second.len() - input.len(), HEAD);
        // Another header under the same number.
        let heartbeat = signed(&[Frame::Heartbeat], &mut signed_pair().0)
            .await
            .concat();
        let mut ack = signed(&[Frame::Ack { next: 1 }], &mut signed_pair().0)
            .await
            .concat();
        ack[HEADER_LEN..HEAD].copy_from_slice(&heartbeat[HEADER_LEN..HEAD]);
        let (error, consumed) = refused(&ack).await;
        assert!(is_bad_tag(&error), "{error}");
        assert_eq!(consumed, HEAD);
    }

    #[tokio::test]
    async fn a_payload_tag_is_never_a_header_tag() {
        // With no payload, both tags cover the same number and header; only domains differ.
        let bytes = signed(&[Frame::Heartbeat], &mut signed_pair().0)
            .await
            .concat();
        assert_eq!(bytes.len(), HEAD + TAG_LEN);
        let (header_tag, payload_tag) = (&bytes[HEADER_LEN..HEAD], &bytes[HEAD..]);
        assert_ne!(header_tag, payload_tag);
        let swapped = [&bytes[..HEADER_LEN], payload_tag, header_tag].concat();
        let (error, consumed) = refused(&swapped).await;
        assert!(is_bad_tag(&error), "{error}");
        assert_eq!(consumed, HEAD);
        // The header tag in both places.
        let doubled = [&bytes[..HEADER_LEN], header_tag, header_tag].concat();
        let (error, _) = refused(&doubled).await;
        assert!(is_bad_tag(&error), "{error}");
    }

    /// Reads `written` in `order` with a fresh reader; returns the position of the first frame
    /// refused for its tag.
    async fn first_refused(written: &[Vec<u8>], order: &[usize]) -> Option<usize> {
        let (_, mut reader) = signed_pair();
        let mut buf = Vec::new();
        for (position, &index) in order.iter().enumerate() {
            let read = read_frame_with(
                &mut written[index].as_slice(),
                FRAME_MAX,
                &mut buf,
                Some(&mut reader),
            )
            .await;
            match read {
                Ok(_) => {}
                Err(error) if is_bad_tag(&error) => return Some(position),
                Err(error) => panic!("{error}"),
            }
        }
        None
    }

    #[tokio::test]
    async fn a_frame_dropped_inserted_reordered_or_replayed_is_refused() {
        let frames = [
            Frame::Heartbeat,
            Frame::Ack { next: 1 },
            Frame::Ack { next: 2 },
        ];
        let (mut writer, _) = signed_pair();
        let written = signed(&frames, &mut writer).await;
        assert_eq!(first_refused(&written, &[0, 1, 2]).await, None);
        // Dropped: the next frame is checked under the dropped one's number.
        assert_eq!(first_refused(&written, &[0, 2]).await, Some(1));
        // Reordered.
        assert_eq!(first_refused(&written, &[1, 0]).await, Some(0));
        // Replayed within the link.
        assert_eq!(first_refused(&written, &[0, 1, 1]).await, Some(2));
        assert_eq!(first_refused(&written, &[0, 0]).await, Some(1));
        // Inserted: a frame the writer never signed.
        let mut plain = Vec::new();
        Frame::Ack { next: 9 }.encode(&mut plain);
        let forged = [
            &plain[..HEADER_LEN],
            &[0; TAG_LEN],
            &plain[HEADER_LEN..],
            &[0; TAG_LEN],
        ]
        .concat();
        let with_forged = [written[0].clone(), forged];
        assert_eq!(first_refused(&with_forged, &[0, 1]).await, Some(1));
        // Signed under another key, as on another link.
        let mut other = Signing::new(&[6; 32]);
        let elsewhere = signed(&frames, &mut other).await;
        assert_eq!(first_refused(&elsewhere, &[0]).await, Some(0));
    }

    #[test]
    fn a_hello_without_the_magic_number_is_malformed() {
        let mut payload = 0u32.to_le_bytes().to_vec();
        payload.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
        assert_eq!(Frame::decode(HELLO, &payload), None);
    }
}
