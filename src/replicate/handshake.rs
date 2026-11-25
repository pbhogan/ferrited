//! The link handshake. The connector (the sender) sends a hello and the listener replies with
//! its own; with a secret, the connector sends a proof; the listener registers the link and
//! accepts it with its own proof and its position on the sender's log. Either side refuses a
//! different protocol version, its own node ID, a secret on one side only, or a wrong proof;
//! the listener also refuses a node ID that already has a live link.
//!
//! Each proof is an HMAC-SHA256 over a domain string, the prover's role and every field of both
//! hellos (the listener's also covers the position), so it fits only its own handshake. Each
//! direction's frame key comes from the same transcript under another domain, so an attacker
//! on the path can stop a link but not change, add, drop, reorder or replay frames. Records are
//! not encrypted.

use std::fmt;
use std::io;
use std::time::Duration;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncWrite};

use super::frame::{self, Frame, HANDSHAKE_FRAME_MAX, Hello, NONCE_LEN, PROTOCOL_VERSION, Signing};

/// How long either side waits for each of the other's handshake frames.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The role byte keeps a proof, or a frame (via its key), from being reflected back to the side
/// that made it.
const CONNECTOR: u8 = 1;
const LISTENER: u8 = 2;

/// Names the protocol version, so a proof from another version fits nothing here.
const PROOF_DOMAIN: &[u8] = b"ferrited replication v1";
/// Distinct from `PROOF_DOMAIN`, so no frame key equals a proof, which crosses the network.
const FRAME_KEY_DOMAIN: &[u8] = b"ferrited replication v1 frame key";

type HmacSha256 = Hmac<Sha256>;

/// This node, as it introduces itself.
#[derive(Clone, Debug)]
pub struct Me {
    pub node_id: u8,
    pub run: u64,
    /// The sequence number this node's next log entry will have.
    pub head: u64,
    /// Still loading its data. A recovering listener completes the handshake and closes, so
    /// the peer knows it is alive.
    pub recovering: bool,
    pub secret: Option<Vec<u8>>,
}

impl Me {
    fn hello(&self, nonce: [u8; NONCE_LEN]) -> Hello {
        Hello {
            version: PROTOCOL_VERSION,
            node_id: self.node_id,
            run: self.run,
            head: self.head,
            recovering: self.recovering,
            auth: self.secret.is_some(),
            nonce,
        }
    }
}

/// Why a handshake failed.
#[derive(Debug)]
pub enum Failure {
    /// The connection failed or timed out, or the other side sent something unexpected.
    Io(io::Error),
    /// This side refused the link.
    Refused(String),
    /// The other side refused the link.
    RefusedBy(String),
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Io(error) => write!(f, "{error}"),
            Failure::Refused(reason) => write!(f, "refused the link: {reason}"),
            Failure::RefusedBy(reason) => write!(f, "the peer refused the link: {reason}"),
        }
    }
}

/// What the connecting node learns from a successful handshake.
#[derive(Debug)]
pub struct Accepted {
    pub peer: Hello,
    /// The listener's position on this node's log, as `(run, next)`.
    pub position: Option<(u64, u64)>,
    /// Present when a secret is configured.
    pub keys: Option<Keys>,
}

/// One side's frame keys, one per direction.
#[derive(Debug)]
pub struct Keys {
    pub send: Signing,
    pub receive: Signing,
}

impl Keys {
    fn new(secret: &[u8], role: u8, transcript: &Transcript) -> Self {
        let other = if role == CONNECTOR {
            LISTENER
        } else {
            CONNECTOR
        };
        Self {
            send: Signing::new(&frame_key(secret, role, transcript)),
            receive: Signing::new(&frame_key(secret, other, transcript)),
        }
    }
}

/// Shakes hands as the connecting node.
pub async fn connect<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    me: &Me,
) -> Result<Accepted, Failure> {
    let mut buf = Vec::new();
    let nonce = nonce();
    send(stream, &Frame::Hello(me.hello(nonce)), &mut buf).await?;
    let peer = match receive(stream, &mut buf).await? {
        Frame::Hello(peer) => peer,
        Frame::Refuse(reason) => return Err(Failure::RefusedBy(reason)),
        other => return Err(unexpected(&other)),
    };
    // The listener checked this side's hello first, so closing tells it enough.
    if let Some(reason) = check(&peer, me) {
        return Err(Failure::Refused(reason));
    }
    let transcript = Transcript::new(me, &peer, &nonce, &peer.nonce, true);
    if let Some(secret) = &me.secret {
        let proof = proof(secret, CONNECTOR, &transcript, &[]);
        send(stream, &Frame::Proof(proof), &mut buf).await?;
    }
    match receive(stream, &mut buf).await? {
        Frame::Accept { proof, position } => {
            let keys = match &me.secret {
                Some(secret) => {
                    let covered = position_bytes(position);
                    if !verify(secret, LISTENER, &transcript, &covered, proof.as_ref()) {
                        return Err(Failure::Refused(
                            "the peer did not prove it knows the secret".into(),
                        ));
                    }
                    Some(Keys::new(secret, CONNECTOR, &transcript))
                }
                None => None,
            };
            Ok(Accepted {
                peer,
                position,
                keys,
            })
        }
        Frame::Refuse(reason) => Err(Failure::RefusedBy(reason)),
        other => Err(unexpected(&other)),
    }
}

/// Shakes hands as the listener. `register` runs only once the sender has proved the secret
/// and may refuse with a reason; its result (such as a guard on the sender's slot) is returned.
/// `position` gives this node's position on the sender's log.
pub async fn accept<S, R>(
    stream: &mut S,
    me: &Me,
    register: impl FnOnce(&Hello) -> Result<R, String>,
    position: impl FnOnce(u8) -> Option<(u64, u64)>,
) -> Result<(Hello, R, Option<Keys>), Failure>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = Vec::new();
    let peer = match receive(stream, &mut buf).await? {
        Frame::Hello(peer) => peer,
        other => return Err(unexpected(&other)),
    };
    if let Some(reason) = check(&peer, me) {
        return refuse(stream, reason, &mut buf).await;
    }
    let nonce = nonce();
    send(stream, &Frame::Hello(me.hello(nonce)), &mut buf).await?;
    let transcript = Transcript::new(me, &peer, &peer.nonce, &nonce, false);
    if let Some(secret) = &me.secret {
        let proven = match receive(stream, &mut buf).await? {
            Frame::Proof(proof) => verify(secret, CONNECTOR, &transcript, &[], Some(&proof)),
            other => return Err(unexpected(&other)),
        };
        if !proven {
            return refuse(stream, "wrong secret".into(), &mut buf).await;
        }
    }
    let registered = match register(&peer) {
        Ok(registered) => registered,
        Err(reason) => return refuse(stream, reason, &mut buf).await,
    };
    let position = position(peer.node_id);
    let proof = me
        .secret
        .as_ref()
        .map(|secret| proof(secret, LISTENER, &transcript, &position_bytes(position)));
    send(stream, &Frame::Accept { proof, position }, &mut buf).await?;
    let keys = me
        .secret
        .as_ref()
        .map(|secret| Keys::new(secret, LISTENER, &transcript));
    Ok((peer, registered, keys))
}

/// The reason to refuse the other side's hello, if there is one.
fn check(peer: &Hello, me: &Me) -> Option<String> {
    if peer.version != PROTOCOL_VERSION {
        return Some(format!(
            "protocol version {} is not this node's version {PROTOCOL_VERSION}",
            peer.version
        ));
    }
    if peer.node_id == me.node_id {
        return Some(format!("node ID {} is this node's own ID", peer.node_id));
    }
    match (me.secret.is_some(), peer.auth) {
        (true, false) => Some("this node requires a secret and the peer has none".into()),
        (false, true) => Some("the peer requires a secret and this node has none".into()),
        _ => None,
    }
}

async fn refuse<S, T>(stream: &mut S, reason: String, buf: &mut Vec<u8>) -> Result<T, Failure>
where
    S: AsyncWrite + Unpin,
{
    // Best effort: the link is refused whether or not the peer hears why.
    let _ = send(stream, &Frame::Refuse(reason.clone()), buf).await;
    Err(Failure::Refused(reason))
}

async fn send<S: AsyncWrite + Unpin>(
    stream: &mut S,
    frame: &Frame,
    buf: &mut Vec<u8>,
) -> Result<(), Failure> {
    tokio::time::timeout(HANDSHAKE_TIMEOUT, frame::write_frame(stream, frame, buf))
        .await
        .map_err(|_| timed_out())?
        .map_err(Failure::Io)
}

async fn receive<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut Vec<u8>,
) -> Result<Frame, Failure> {
    tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        frame::read_frame(stream, HANDSHAKE_FRAME_MAX, buf),
    )
    .await
    .map_err(|_| timed_out())?
    .map_err(Failure::Io)
}

fn timed_out() -> Failure {
    Failure::Io(io::Error::new(
        io::ErrorKind::TimedOut,
        "the handshake timed out",
    ))
}

fn unexpected(frame: &Frame) -> Failure {
    Failure::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unexpected {} during the handshake", frame.name()),
    ))
}

fn nonce() -> [u8; NONCE_LEN] {
    let mut nonce = [0; NONCE_LEN];
    getrandom::fill(&mut nonce).expect("the system's random source failed");
    nonce
}

/// Everything a proof covers, connector first so both sides compute it alike. Every hello field
/// is covered: a flipped recovering flag could freeze a node's sync timeout, and a forged head
/// could make it catch up too early or never.
struct Transcript {
    nonces: [[u8; NONCE_LEN]; 2],
    node_ids: [u8; 2],
    runs: [u64; 2],
    heads: [u64; 2],
    recovering: [bool; 2],
}

impl Transcript {
    /// `connecting` says whether `me` is the connector.
    fn new(
        me: &Me,
        peer: &Hello,
        connector_nonce: &[u8; NONCE_LEN],
        listener_nonce: &[u8; NONCE_LEN],
        connecting: bool,
    ) -> Transcript {
        let mine = (me.node_id, me.run, me.head, me.recovering);
        let theirs = (peer.node_id, peer.run, peer.head, peer.recovering);
        let (connector, listener) = if connecting {
            (mine, theirs)
        } else {
            (theirs, mine)
        };
        Transcript {
            nonces: [*connector_nonce, *listener_nonce],
            node_ids: [connector.0, listener.0],
            runs: [connector.1, listener.1],
            heads: [connector.2, listener.2],
            recovering: [connector.3, listener.3],
        }
    }
}

/// An HMAC under `secret` of `domain`, `role` and the whole transcript.
fn mac(secret: &[u8], domain: &[u8], role: u8, transcript: &Transcript) -> HmacSha256 {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC takes a key of any length");
    mac.update(domain);
    mac.update(&[role]);
    for nonce in &transcript.nonces {
        mac.update(nonce);
    }
    mac.update(&transcript.node_ids);
    for run in &transcript.runs {
        mac.update(&run.to_le_bytes());
    }
    for head in &transcript.heads {
        mac.update(&head.to_le_bytes());
    }
    mac.update(&transcript.recovering.map(u8::from));
    mac
}

/// Proof, by the side in `role`, that it knows `secret`, covering the transcript and `extra`.
fn proof(secret: &[u8], role: u8, transcript: &Transcript, extra: &[u8]) -> [u8; NONCE_LEN] {
    let mut mac = mac(secret, PROOF_DOMAIN, role, transcript);
    mac.update(extra);
    let mut proof = [0; NONCE_LEN];
    proof.copy_from_slice(&mac.finalize().into_bytes());
    proof
}

/// Whether `proof` is right, compared in constant time.
fn verify(
    secret: &[u8],
    role: u8,
    transcript: &Transcript,
    extra: &[u8],
    proof: Option<&[u8; NONCE_LEN]>,
) -> bool {
    proof.is_some_and(|proof| {
        let mut mac = mac(secret, PROOF_DOMAIN, role, transcript);
        mac.update(extra);
        mac.verify_slice(proof).is_ok()
    })
}

/// The key for frames the side in `role` sends. Fresh nonces give each link and direction its
/// own key.
fn frame_key(secret: &[u8], role: u8, transcript: &Transcript) -> [u8; NONCE_LEN] {
    let mut key = [0; NONCE_LEN];
    key.copy_from_slice(
        &mac(secret, FRAME_KEY_DOMAIN, role, transcript)
            .finalize()
            .into_bytes(),
    );
    key
}

/// A position as the listener's proof covers it: a presence byte, then run and next (zeros
/// without one).
fn position_bytes(position: Option<(u64, u64)>) -> [u8; 17] {
    let mut bytes = [0; 17];
    if let Some((run, next)) = position {
        bytes[0] = 1;
        bytes[1..9].copy_from_slice(&run.to_le_bytes());
        bytes[9..].copy_from_slice(&next.to_le_bytes());
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn me(node_id: u8, secret: Option<&str>) -> Me {
        Me {
            node_id,
            run: u64::from(node_id) * 100,
            head: 7,
            recovering: false,
            secret: secret.map(|secret| secret.as_bytes().to_vec()),
        }
    }

    /// Runs both sides over an in-memory stream; registration succeeds when `register` is true.
    async fn shake(
        connector: Me,
        listener: Me,
        position: Option<(u64, u64)>,
        register: bool,
    ) -> (
        Result<Accepted, Failure>,
        Result<(Hello, (), Option<Keys>), Failure>,
    ) {
        let (mut a, mut b) = tokio::io::duplex(64 * 1024);
        let connecting = async move { connect(&mut a, &connector).await };
        let accepting = async move {
            let registering = |hello: &Hello| {
                if register {
                    Ok(())
                } else {
                    Err(format!("node {} already has a live link", hello.node_id))
                }
            };
            accept(&mut b, &listener, registering, |_| position).await
        };
        tokio::join!(connecting, accepting)
    }

    #[tokio::test]
    async fn nodes_with_the_same_secret_shake_hands() {
        let (connected, accepted) =
            shake(me(1, Some("s")), me(2, Some("s")), Some((5, 9)), true).await;
        let connected = connected.unwrap();
        assert_eq!(connected.peer.node_id, 2);
        assert_eq!(connected.position, Some((5, 9)));
        let (hello, (), _) = accepted.unwrap();
        assert_eq!((hello.node_id, hello.run, hello.head), (1, 100, 7));
    }

    #[tokio::test]
    async fn a_recovering_listener_completes_the_handshake_and_says_so() {
        let recovering = Me {
            recovering: true,
            ..me(2, Some("s"))
        };
        let (connected, accepted) = shake(me(1, Some("s")), recovering, None, true).await;
        let connected = connected.unwrap();
        assert!(connected.peer.recovering);
        assert_eq!(connected.position, None);
        assert!(!accepted.unwrap().0.recovering);
    }

    #[tokio::test]
    async fn nodes_without_a_secret_shake_hands() {
        let (connected, accepted) = shake(me(1, None), me(2, None), None, true).await;
        assert_eq!(connected.unwrap().position, None);
        assert!(accepted.is_ok());
    }

    #[tokio::test]
    async fn a_wrong_secret_is_refused() {
        let (connected, accepted) = shake(me(1, Some("a")), me(2, Some("b")), None, true).await;
        assert!(matches!(accepted, Err(Failure::Refused(reason)) if reason == "wrong secret"));
        assert!(matches!(connected, Err(Failure::RefusedBy(reason)) if reason == "wrong secret"));
    }

    #[tokio::test]
    async fn the_same_node_id_is_refused() {
        let (connected, accepted) = shake(me(1, None), me(1, None), None, true).await;
        assert!(matches!(accepted, Err(Failure::Refused(reason)) if reason.contains("own ID")));
        assert!(matches!(connected, Err(Failure::RefusedBy(reason)) if reason.contains("own ID")));
    }

    #[tokio::test]
    async fn a_secret_on_one_side_only_is_refused() {
        let (connected, accepted) = shake(me(1, Some("s")), me(2, None), None, true).await;
        assert!(matches!(accepted, Err(Failure::Refused(_))));
        assert!(matches!(connected, Err(Failure::RefusedBy(_))));
    }

    #[tokio::test]
    async fn a_refused_registration_comes_after_the_secret() {
        let (connected, accepted) = shake(me(1, Some("s")), me(2, Some("s")), None, false).await;
        assert!(matches!(accepted, Err(Failure::Refused(reason)) if reason.contains("live link")));
        assert!(matches!(connected, Err(Failure::RefusedBy(_))));
    }

    #[tokio::test]
    async fn another_protocol_version_is_refused() {
        let (mut a, mut b) = tokio::io::duplex(64 * 1024);
        let mut buf = Vec::new();
        let mut hello = me(1, None).hello([0; NONCE_LEN]);
        hello.version = 2;
        frame::write_frame(&mut a, &Frame::Hello(hello), &mut buf)
            .await
            .unwrap();
        let accepted = accept(&mut b, &me(2, None), |_| Ok(()), |_| None).await;
        assert!(
            matches!(accepted, Err(Failure::Refused(reason)) if reason.contains("protocol version 2 is not this node's version 1"))
        );
        let reply = frame::read_frame(&mut a, HANDSHAKE_FRAME_MAX, &mut buf)
            .await
            .unwrap();
        assert!(matches!(reply, Frame::Refuse(_)));
    }

    #[tokio::test]
    async fn a_listener_that_cannot_prove_the_secret_is_refused() {
        let (mut a, mut b) = tokio::io::duplex(64 * 1024);
        let impostor = async move {
            let mut buf = Vec::new();
            frame::read_frame(&mut b, HANDSHAKE_FRAME_MAX, &mut buf)
                .await
                .unwrap();
            let hello = me(2, Some("guess")).hello([3; NONCE_LEN]);
            frame::write_frame(&mut b, &Frame::Hello(hello), &mut buf)
                .await
                .unwrap();
            frame::read_frame(&mut b, HANDSHAKE_FRAME_MAX, &mut buf)
                .await
                .unwrap();
            let accept = Frame::Accept {
                proof: Some([0; NONCE_LEN]),
                position: None,
            };
            frame::write_frame(&mut b, &accept, &mut buf).await.unwrap();
        };
        let connector = me(1, Some("s"));
        let (connected, ()) = tokio::join!(connect(&mut a, &connector), impostor);
        assert!(
            matches!(connected, Err(Failure::Refused(reason)) if reason.contains("did not prove"))
        );
    }

    #[test]
    fn a_proof_fits_only_its_own_handshake() {
        let a = me(1, Some("s"));
        let b = me(2, Some("s"));
        let hello = |me: &Me, nonce| me.hello(nonce);
        let (n1, n2) = ([1; NONCE_LEN], [2; NONCE_LEN]);
        let real = Transcript::new(&a, &hello(&b, n2), &n1, &n2, true);
        let made = proof(b"s", CONNECTOR, &real, &[]);
        assert!(verify(b"s", CONNECTOR, &real, &[], Some(&made)));
        // The listener computes the same transcript from its side.
        let theirs = Transcript::new(&b, &hello(&a, n1), &n1, &n2, false);
        assert!(verify(b"s", CONNECTOR, &theirs, &[], Some(&made)));
        // A different connector ID, nonce pair or run fails.
        let other = me(3, Some("s"));
        let wrong_id = Transcript::new(&other, &hello(&b, n2), &n1, &n2, true);
        assert!(!verify(b"s", CONNECTOR, &wrong_id, &[], Some(&made)));
        let wrong_nonce = Transcript::new(&a, &hello(&b, n2), &n2, &n1, true);
        assert!(!verify(b"s", CONNECTOR, &wrong_nonce, &[], Some(&made)));
        let mut rerun = a.clone();
        rerun.run += 1;
        let wrong_run = Transcript::new(&rerun, &hello(&b, n2), &n1, &n2, true);
        assert!(!verify(b"s", CONNECTOR, &wrong_run, &[], Some(&made)));
        // So does a changed head or recovering flag on either side.
        let forged_head = Me {
            head: 8,
            ..b.clone()
        };
        let wrong_head = Transcript::new(&a, &hello(&forged_head, n2), &n1, &n2, true);
        assert!(!verify(b"s", CONNECTOR, &wrong_head, &[], Some(&made)));
        let flipped = Me {
            recovering: true,
            ..b.clone()
        };
        let wrong_flag = Transcript::new(&a, &hello(&flipped, n2), &n1, &n2, true);
        assert!(!verify(b"s", CONNECTOR, &wrong_flag, &[], Some(&made)));
        let own_flag = Me {
            recovering: true,
            ..a.clone()
        };
        let wrong_own = Transcript::new(&own_flag, &hello(&b, n2), &n1, &n2, true);
        assert!(!verify(b"s", CONNECTOR, &wrong_own, &[], Some(&made)));
    }

    /// Whether a frame signed by `writer` passes `reader`, both counting it.
    fn fits(writer: &mut Signing, reader: &mut Signing) -> bool {
        let header = [0, 0, 0, 0, 7];
        let header_tag = writer.header_tag(&header);
        let tag = writer.tag(&header, &[]);
        let header_fits = reader.check_header(&header, &header_tag);
        // Checked either way, so both sides count the frame.
        let fits = reader.check(&header, &[], &tag);
        assert_eq!(header_fits, fits, "the two tags of one frame disagree");
        fits
    }

    #[tokio::test]
    async fn both_sides_derive_the_same_key_for_each_direction_and_a_different_one_for_each() {
        let (connected, accepted) =
            shake(me(1, Some("s")), me(2, Some("s")), Some((5, 9)), true).await;
        let mut connector = connected.unwrap().keys.expect("keys with a secret");
        let (_, (), listener) = accepted.unwrap();
        let mut listener = listener.expect("keys with a secret");
        assert!(fits(&mut connector.send, &mut listener.receive));
        assert!(fits(&mut listener.send, &mut connector.receive));
        // Reflected back to its sender. Both have counted one frame, so only the key fails it.
        assert!(!fits(&mut connector.send, &mut connector.receive));
        assert!(!fits(&mut listener.send, &mut listener.receive));
    }

    #[tokio::test]
    async fn each_link_between_the_same_nodes_has_keys_of_its_own() {
        let (first, _) = shake(me(1, Some("s")), me(2, Some("s")), None, true).await;
        let (_, second) = shake(me(1, Some("s")), me(2, Some("s")), None, true).await;
        let mut first = first.unwrap().keys.unwrap();
        let mut second = second.unwrap().2.unwrap();
        // The earlier link's first frame, replayed on the later one.
        assert!(!fits(&mut first.send, &mut second.receive));
    }

    #[test]
    fn a_frame_key_depends_on_every_field_of_the_transcript_and_on_the_role() {
        let base = || Transcript {
            nonces: [[1; NONCE_LEN], [2; NONCE_LEN]],
            node_ids: [1, 2],
            runs: [10, 20],
            heads: [100, 200],
            recovering: [false, false],
        };
        let key = |transcript: &Transcript| frame_key(b"s", CONNECTOR, transcript);
        let original = key(&base());
        type Change = fn(&mut Transcript);
        let changes: [(&str, Change); 10] = [
            ("the connector's nonce", |t| t.nonces[0][0] ^= 1),
            ("the listener's nonce", |t| t.nonces[1][0] ^= 1),
            ("the connector's node ID", |t| t.node_ids[0] = 3),
            ("the listener's node ID", |t| t.node_ids[1] = 3),
            ("the connector's run", |t| t.runs[0] += 1),
            ("the listener's run", |t| t.runs[1] += 1),
            ("the connector's head", |t| t.heads[0] += 1),
            ("the listener's head", |t| t.heads[1] += 1),
            ("the connector's recovering flag", |t| {
                t.recovering[0] = true
            }),
            ("the listener's recovering flag", |t| t.recovering[1] = true),
        ];
        for (field, change) in changes {
            let mut changed = base();
            change(&mut changed);
            assert_ne!(key(&changed), original, "the key ignores {field}");
        }
        assert_ne!(
            frame_key(b"s", LISTENER, &base()),
            original,
            "the key ignores the role (the direction)"
        );
    }

    #[tokio::test]
    async fn without_a_secret_frames_are_not_signed() {
        let (connected, accepted) = shake(me(1, None), me(2, None), None, true).await;
        assert!(connected.unwrap().keys.is_none());
        assert!(accepted.unwrap().2.is_none());
    }

    #[test]
    fn a_frame_key_is_not_a_proof() {
        // Proofs cross the network in the clear.
        let a = me(1, Some("s"));
        let b = me(2, Some("s"));
        let (n1, n2) = ([1; NONCE_LEN], [2; NONCE_LEN]);
        let transcript = Transcript::new(&a, &b.hello(n2), &n1, &n2, true);
        for role in [CONNECTOR, LISTENER] {
            let proof = proof(b"s", role, &transcript, &[]);
            assert_ne!(frame_key(b"s", CONNECTOR, &transcript), proof);
            assert_ne!(frame_key(b"s", LISTENER, &transcript), proof);
        }
        assert_ne!(
            frame_key(b"s", CONNECTOR, &transcript),
            frame_key(b"s", LISTENER, &transcript)
        );
    }

    #[tokio::test]
    async fn an_accept_whose_position_was_changed_on_the_way_is_refused() {
        // A forged position would make the sender resume past entries the receiver lacks.
        let (mut a, mut b) = tokio::io::duplex(64 * 1024);
        let (mut c, mut d) = tokio::io::duplex(64 * 1024);
        let tamper = async move {
            let mut buf = Vec::new();
            for _ in 0..2 {
                // Hello then proof from the connector; hello then accept from the listener.
                let frame = frame::read_frame(&mut b, HANDSHAKE_FRAME_MAX, &mut buf)
                    .await
                    .unwrap();
                frame::write_frame(&mut c, &frame, &mut buf).await.unwrap();
                let frame = frame::read_frame(&mut c, HANDSHAKE_FRAME_MAX, &mut buf)
                    .await
                    .unwrap();
                let frame = match frame {
                    Frame::Accept {
                        proof,
                        position: Some((run, next)),
                    } => Frame::Accept {
                        proof,
                        position: Some((run, next + 100)),
                    },
                    other => other,
                };
                frame::write_frame(&mut b, &frame, &mut buf).await.unwrap();
            }
        };
        let connector = me(1, Some("s"));
        let listener = me(2, Some("s"));
        let (connected, accepted, ()) = tokio::join!(
            connect(&mut a, &connector),
            accept(&mut d, &listener, |_| Ok(()), |_| Some((5, 9))),
            tamper
        );
        assert!(accepted.is_ok());
        assert!(
            matches!(connected, Err(Failure::Refused(reason)) if reason.contains("did not prove"))
        );
    }

    #[tokio::test]
    async fn a_recovering_flag_flipped_on_the_way_is_refused() {
        // An attacker clears the recovering listener's flag on the way to the connector.
        let (mut a, mut b) = tokio::io::duplex(64 * 1024);
        let (mut c, mut d) = tokio::io::duplex(64 * 1024);
        let tamper = async move {
            let mut buf = Vec::new();
            let hello = frame::read_frame(&mut b, HANDSHAKE_FRAME_MAX, &mut buf)
                .await
                .unwrap();
            frame::write_frame(&mut c, &hello, &mut buf).await.unwrap();
            let Frame::Hello(mut reply) = frame::read_frame(&mut c, HANDSHAKE_FRAME_MAX, &mut buf)
                .await
                .unwrap()
            else {
                panic!("expected a hello");
            };
            reply.recovering = false;
            frame::write_frame(&mut b, &Frame::Hello(reply), &mut buf)
                .await
                .unwrap();
            let proof = frame::read_frame(&mut b, HANDSHAKE_FRAME_MAX, &mut buf)
                .await
                .unwrap();
            frame::write_frame(&mut c, &proof, &mut buf).await.unwrap();
            let accept = frame::read_frame(&mut c, HANDSHAKE_FRAME_MAX, &mut buf).await;
            if let Ok(accept) = accept {
                let _ = frame::write_frame(&mut b, &accept, &mut buf).await;
            }
        };
        let listener = Me {
            recovering: true,
            ..me(2, Some("s"))
        };
        let connector = me(1, Some("s"));
        let (connected, accepted, ()) = tokio::join!(
            connect(&mut a, &connector),
            accept(&mut d, &listener, |_| Ok(()), |_| None),
            tamper
        );
        // The connector's proof does not fit the hello the listener sent.
        assert!(matches!(accepted, Err(Failure::Refused(reason)) if reason == "wrong secret"));
        assert!(connected.is_err());
    }
}
