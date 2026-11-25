# ferrited

A memcached-compatible cache server written in Rust. It speaks memcached's meta and classic text protocols, so existing clients such as Dalli work unchanged.

Status: not yet released. Nodes persist their data to disk and replicate every change to each other, and a node that restarts waits until it has caught up with its peers before it serves clients.

## Running

```bash
cargo run --release -- -p 11211 -m 1024
```

| Flag | Meaning | Default |
|---|---|---|
| `-p`, `--port` | TCP port | 11211 |
| `-l`, `--listen` | Listen address | 127.0.0.1 |
| `-s`, `--socket` | Unix socket path (instead of TCP) | none |
| `-m`, `--memory-limit` | Memory limit in megabytes | 64 |
| `-c`, `--conn-limit` | Maximum simultaneous connections; past it a new connection gets `ERROR Too many open connections` and is closed, as in memcached | 1024 |
| `-I`, `--max-item-size` | Largest value, e.g. `1m`, `512k` | 1m |
| `-t`, `--threads` | Event loop threads serving clients (replication and timers run on at most 4 more Tokio workers, beside a log thread and a blocking pool) | one per core |
| `-A`, `--enable-shutdown` | Allow the `shutdown` command | off |
| `--node-id` | Node ID, 0 to 255; with `--peer` it is required and must be 1 to 255 | 1 |
| `--metrics-addr` | Address of the Prometheus metrics endpoint, `HOST:PORT`, on a port other than `-p` and `--repl-port` (see "Metrics") | off |

ferrited has no authentication. It listens on `127.0.0.1` unless told otherwise; only expose it on a trusted network.

## Persistence

Persistence is off unless you give a data directory. Without one, ferrited keeps everything in memory, like memcached, and writes no files.

```bash
cargo run --release -- -m 1024 --data-dir /var/lib/ferrited
```

With `--data-dir`, every change is queued in memory (the oplog) and a background thread appends it to a log on disk and fsyncs the log on a timer, so requests never wait for the disk. After a restart or a crash, ferrited reloads its data before it opens the client port.

What a crash can lose:

- When the writer keeps up and the disk is healthy, a crash loses at most about one fsync interval of writes: those still queued, plus those written since the last fsync.
- If changes arrive faster than the writer saves them, or the disk fails, some changes never reach the log (see below). Until the snapshot that covers them is complete, a crash loses them.

A clean stop (SIGTERM, Ctrl-C or the `shutdown` command) stops accepting, lets each connection finish the requests it has read, closes connections still open after 5 seconds, then writes and syncs everything queued, and takes a snapshot if changes were lost from the log. If that final sync or snapshot fails, ferrited says so and exits with a non-zero status, and changes may be lost. A second signal during the stop exits at once, which is as safe as a crash.

| Flag | Meaning | Default |
|---|---|---|
| `--data-dir` | Directory for the log and snapshots; turns persistence on | off |
| `--fsync-interval-ms` | Milliseconds between fsyncs, about the most a crash loses while the writer keeps up | 1000 |
| `--segment-size` | Size at which a log file is closed and a new one begun | 64m |
| `--oplog-capacity` | Changes held in memory while the writer catches up, at most 16777216 | 65536 |
| `--oplog-bytes` | Bytes of values held in memory while the writer catches up, at least `--max-item-size` | 64m, or 4 × `--max-item-size` if larger |

`--fsync-interval-ms` and `--segment-size` require `--data-dir`; `--oplog-capacity` and `--oplog-bytes` require `--data-dir` or `--peer`. The oplog's memory is on top of `-m`. With persistence or peers, `--max-item-size` can be at most about 1 GiB.

The log is compacted into snapshots in the background. Normally the directory keeps the two newest snapshots and the log after the older one, so a damaged newest snapshot falls back to the previous one. After a snapshot that covers lost changes, only that snapshot and the log after it are kept, since replaying older log across the loss could undo changes; if that snapshot is then damaged, recovery uses the log alone and warns that changes before it are lost. A damaged snapshot is renamed to `*.damaged` and left for inspection.

Each file in the directory records its format version, and ferrited reads only its own. At each clean stop it also writes a `CLEAN` file, which marks a stop that saved everything: ferrited removes it at the next start, and a directory holding data without it is treated as having crashed, so with peers configured each peer sends the node a full sync to restore its own last writes.

Only one ferrited can use a directory at a time. ferrited refuses to start, with an error naming the problem, if it cannot open, lock or write to the directory (a full or read-only disk, for example), or if the directory holds a file of a format this version cannot read, such as one written by a later version; it never discards such a file.

Not kept across a restart: last-access times and "fetched" flags (so recovered items start equal for eviction), a delayed `flush_all` that had not yet taken effect, and evictions (an evicted item can reappear until memory pressure evicts it again).

If changes arrive faster than the writer can save them, so that the oplog is full (`--oplog-capacity` changes or `--oplog-bytes` of values), the excess is dropped from the log, and a snapshot is taken to cover it. The log records where changes were dropped, so if a crash comes before that snapshot is complete, the next start reports the loss.

If the disk fails, ferrited logs the error, keeps serving from memory and discards changes rather than queue them, retries every second, and takes a snapshot once it can write again; `persist_degraded` stays 1 until that snapshot is complete. When the disk first fails, ferrited tries to write a `DEGRADED` file in the data directory, which the next start reports and which is removed once the snapshot is complete. If the disk is so broken that even that file cannot be written, a crash before the disk recovers can lose changes with no report at the next start.

Both cases are logged and counted in `stats`.

`stats` reports `persist_enabled` and, when it is 1, `persist_degraded`, `persist_records`, `persist_bytes`, `persist_fsyncs`, `persist_errors`, `persist_dropped` (changes the oplog had no room for), `persist_lost` (changes discarded while degraded), `persist_snapshots`, `persist_snapshot_items` and `persist_log_bytes`.

## Replication

Nodes can replicate every change to each other. Each node lists the others' replication addresses, and every pair of nodes needs a direct link: a node sends only the changes made on it, never ones it received from another node, so a node that lists no peer for some other node will never exchange writes with it.

```bash
ferrited -l 10.0.0.1 --node-id 1 --repl-port 7000 --peer 10.0.0.2:7000 --repl-secret-file /etc/ferrited/secret
ferrited -l 10.0.0.2 --node-id 2 --repl-port 7000 --peer 10.0.0.1:7000 --repl-secret-file /etc/ferrited/secret
```

For three nodes, each lists the other two with a `--peer` each (A lists B and C, B lists A and C, C lists A and B). Node IDs must differ; a node refuses a link from an ID equal to its own or from an ID that already has a link to it. The secret file holds the same secret on every node; its trailing line breaks are ignored and it must not be empty. The replication listener binds to the `-l` address on `--repl-port`. Without a secret file, ferrited warns at startup, and any host that can reach the replication port can link to the node and send it changes.

| Flag | Meaning | Default |
|---|---|---|
| `--peer` | Another node's replication address, `HOST:PORT`; repeat for each, without giving one twice | none |
| `--repl-port` | Port of this node's replication listener; required with `--peer` | none |
| `--repl-secret-file` | File holding the shared secret | none |
| `--repl-backlog` | Bytes of recent changes kept in memory for peers that fall behind, at least `--max-item-size` | 64m |
| `--tombstone-ttl` | Seconds a deleted key's tombstone is kept | 3600 |
| `--tombstone-memory` | Memory tombstones may use, at most half of `-m`; past it the oldest are evicted | a tenth of `-m` |

`--peer` requires `--node-id`, and the other four flags require `--peer`. `--repl-port` must differ from `-p` unless clients use a Unix socket (`-s`).

Nodes link only when they speak the same link protocol version; a node whose peer speaks another refuses the link and logs the mismatch, and until both match each serves alone, so upgrade every node of a cluster together. The backlog's memory is on top of `-m`; tombstones count toward `-m`, up to `--tombstone-memory`.

What replication guarantees, and what it does not:

- It is asynchronous. A node acknowledges a write once it has applied it; peers receive it shortly after. A write can be lost if its node fails before the change reaches a peer.
- Every change carries a version made of the time, a counter and the node ID, and every node keeps the change with the highest version, so nodes converge whatever order changes arrive in. This is last-writer-wins: two nodes that write the same key within the replication delay keep only the later write. `incr`, `decr`, `append`, `prepend` and `cas` are atomic on one node only, so concurrent updates to one key on two nodes lose one of them. Eviction is local, so nodes under memory pressure can hold different keys. See "Deliberate differences from memcached" under "Compatibility".
- A delete leaves a tombstone for `--tombstone-ttl` seconds, counted from the time in the delete's version, so a delayed older write cannot bring the key back. Tombstones use memory, at most `--tombstone-memory` (default a tenth of `-m`); past it the oldest are evicted, and evicting items never removes them. A plain delete leaves one even when the key is not stored (deletes with a CAS token, invalidating deletes and deletes that keep an empty item leave none when they miss), and so does every evicted or expired item, unless a `flush_all` covers it. A write older than an evicted tombstone that arrives afterward brings the key back, so under a heavy delete load a larger `--tombstone-memory` keeps deletes protected for longer (watch `repl_tombstones_evicted`). A peer that is down longer than the tombstone lifetime, or long enough to need a full sync after its deletes' tombstones were evicted, can bring back a key deleted in the meantime: a full sync carries deletes only as tombstones.
- `flush_all` reaches every node.
- A peer that is slow or down never slows a client. A peer that comes back resumes where it left off if this node still holds the changes it missed, in the backlog or, with persistence, on disk. Otherwise it receives a full sync: this node's whole data set, merged by version so that nothing newer on the peer is lost.
- A node without persistence that restarts has no positions and gets a full sync from each peer. A node with persistence that stops cleanly resumes. A node that crashes (or is killed) is treated as having lost data, so each peer does a full sync to it; this is how it gets back its own last writes that never reached its disk.
- With `--repl-secret-file`, each side proves the secret in the handshake, and every frame after it carries two tags made with a key of that link's own, one for its header and one for the whole frame, so a host on the network path cannot change, add, drop, reorder or replay what crosses a link: a node that receives such a frame drops the link without applying it, logs why and counts it in `repl_bad_frames`, and the link resumes once the sender reconnects. A changed frame length is caught by the header's tag before the payload is read, so it cannot make a node set aside memory for a frame that never arrives. Such a host can still stop links. The link is not encrypted, so it can read every record, and the client port has no authentication. Without a secret, frames are not authenticated at all, so run replication only on a trusted network. Signing costs CPU on both sides: on arm64 and x86_64 ferrited uses the CPU's SHA instructions when it has them, and signing then costs about 0.3 µs per replicated 512-byte set on Apple Silicon and on Linux arm64; in software it costs about 1.6 µs (see `docs/benchmarks.md`).
- A full sync that a node receives is logged like any applied change, so items in it carrying the node's own ID (its own earlier writes coming back, such as after a crash) are streamed again to its other peers, which already hold them and count them stale. After a large full sync this can push a slow sender behind its backlog and into a full sync of its own, visible as `repeat_syncs` in `stats repl`.
- A node with peers does not serve clients until it is ready; see "Startup readiness" below.

`stats` reports `repl_peers` (links up, counting each direction), `repl_records_sent`, `repl_records_applied`, `repl_records_stale` (received but not newer than what the node held), `repl_records_oversized` (received puts newer than what the node held but too large for its `-m`, which remove the older item and take effect as deletes), `repl_full_syncs_sent`, `repl_full_syncs_received`, `repl_backlog_bytes`, `repl_tombstones` (tombstones held, including expired ones not yet swept), `repl_tombstone_bytes` (the memory they are charged), `repl_tombstones_evicted` (tombstones evicted to stay within `--tombstone-memory` before they expired), `repl_positions_dropped` (positions on peers' logs that the full oplog had no room for; each is logged again later, so this is not a loss) `repl_sync_timeouts` (1 if the node began serving because no peer was reachable for `--sync-timeout`) and `repl_bad_frames` (links dropped because a frame failed authentication, either direction; always 0 without a secret); all are 0 without peers. On a node with peers, `bytes` includes the memory tombstones use. `stats repl` lists, for each configured peer, `peer:<address>:id`, `:out` and `:in` (the state of the link in each direction: `down`, `syncing` or `streaming`), `:acked` (the last position the peer acknowledged on this node's log), `:lag` (entries it is behind) and `:repeat_syncs` (full syncs to it that started within a minute of the last one finishing, a sign `--repl-backlog` is too small); an inbound link from a node not in the list appears as `peer:node-<id>:in`.

### Startup readiness

A node with peers does not open its client port until it is ready, so a client such as Dalli gets a refused connection, treats the node as down and uses another, as it did while the node was stopped. The replication port opens first, before the node loads its data, and tells peers that ask that the node is still loading.

A node is ready when:

- it has caught up with every peer it is linked with, and at least one has linked to it: it has applied everything each peer had when it linked. A peer this node reaches that has not linked back is waited for up to 10 seconds, then logged and left out. Every configured peer is tried once first, which is waited for up to 10 seconds; or
- it was the last node standing the last time it served: every peer was down then, so none can hold writes it lacks. It still tries its peers for 2 seconds, and waits to catch up with any that answer; or
- no peer has been reachable for `--sync-timeout` seconds. A peer counts as reachable while it is linked or answers that it is still loading its data, and for 10 seconds after; or
- it was started with `--force-standalone`.

A node that serves never closes its client port again, even if it loses every peer. If its client address is taken by the time it is ready, it says so, stops cleanly and exits with a non-zero status; an address already taken at startup is reported before the node loads its data. A stop signal while the node waits stops it at once; one while it loads its data (with or without peers) stops it cleanly as soon as loading ends, without serving, and a second one exits at once.

| Flag | Meaning | Default |
|---|---|---|
| `--sync-timeout` | Seconds with no peer reachable before serving with local data alone; `0` waits forever | 60 |
| `--force-standalone` | Serve as soon as the local data is loaded | off |

Both require `--peer`. Serving because of either logs a warning, and the timeout also sets `repl_sync_timeouts` to 1: writes peers accepted while this node was down are missing until those peers return, when they are merged by version as usual. A network partition makes both sides last standing; both keep serving, and their data is merged when the link returns.

A node knows it was last standing from link records in its log, which list the peers it is linked with each time that changes while it serves. A node without persistence and a new data directory have none, so such a node waits for a peer or for the sync timeout. A node that became ready by the sync timeout or `--force-standalone` logs no link record until it has caught up with a peer that linked to it, so its next start waits again. A node whose data directory has a hole in its history (a gap or an unreadable stretch in the log, a damaged snapshot, a torn entry other than a crash's last one, or changes dropped or lost to a failing disk that no snapshot covers yet) is not last standing either: recovery writes a `HISTORY_LOST` file in the directory, which keeps every start from counting the node last standing until a snapshot covering the hole is complete, and is then removed.

While it waits, a node logs why (`ferrited: not serving clients yet: ...`), when the reason changes and every 10 seconds, and one line when it becomes ready. Since `stats` is unreachable while the client port is closed, the replication port answers a status command:

```bash
$ printf 'status\r\n' | nc 127.0.0.1 7000
STAT state waiting
STAT reason catching_up
STAT last_standing 0
STAT sync_timeout 60
STAT unreachable_secs 0
STAT reachable 1
STAT linked_peers 1
STAT peer:node-2:target 5120
STAT peer:node-2:applied 1210
STAT peer:node-2:caught_up 0
END
```

`state` is `recovering`, `waiting` or `ready`; while waiting, `reason` is `grace_period`, `no_peer_reachable`, `peer_recovering`, `no_inbound_link` (a peer is reachable but has not linked to this node), `catching_up` or `contacting_peers` (caught up, but a configured peer has not been tried yet); once ready, `ready_by` is `caught_up`, `last_standing`, `sync_timeout` or `force_standalone`. `sync_timeout` is 0 when the node waits forever, and `unreachable_secs` is how long no peer has been reachable so far. The `peer:node-<id>` lines show, for each peer linked to this node, its log's head when it linked and how far this node has applied it (`none` until the link has started). With `--repl-secret-file`, the status command is answered only on loopback; other connections get `SERVER_ERROR`.

## Metrics

With `--metrics-addr HOST:PORT`, ferrited serves Prometheus metrics over HTTP at `/metrics` on that address:

```bash
ferrited -l 10.0.0.1 --metrics-addr 10.0.0.1:11213 ...
curl http://10.0.0.1:11213/metrics
```

It reports every value of `stats` (`get_hits` as `ferrited_get_hits_total`, `curr_items` as `ferrited_curr_items`, `bytes` as `ferrited_memory_used_bytes`, and so on; each metric's help text names its stat), each peer's links from `stats repl` (`ferrited_repl_link_state` with `peer`, `direction` (`out` or `in`) and `state` labels, one series per state with 1 for the current one, and `ferrited_repl_peer_lag_entries`, `_acked_position`, `_repeat_syncs_total` and `_node_id`), and the node's readiness: `ferrited_ready` (1 while it serves clients), `ferrited_state` (`recovering`, `waiting`, `ready` or `stopping`), and with peers the replication port's status command's values that are not per peer: `ferrited_waiting_reason`, `ferrited_ready_by`, `ferrited_last_standing`, `ferrited_sync_timeout_seconds`, `ferrited_unreachable_seconds`, `ferrited_peer_reachable` and `ferrited_linked_peers`. `ferrited_build_info` carries the version.

The endpoint opens before the node loads its data and closes last when it stops, so a node that is recovering, waiting for its peers or stopping can be scraped while its client port is closed. While the node loads its data, the store's values show what has been loaded so far, and persistence and replication values are left out until they start. A scrape reads the same counters as `stats`, which locks each shard of the store briefly in turn; that is negligible at normal scrape intervals. `stats reset` zeroes the same counters here, which Prometheus treats as a counter reset.

It answers `GET` and `HEAD` of `/metrics`, one request per connection, at most 8 connections at once (others get `503`), and closes a connection whose request has not arrived within 5 seconds or is over 8 KiB. Like the client port it has no authentication: put it on a private address and let the firewall admit only the monitoring host. `docs/operations.md` has a scrape configuration and alert rules.

## Running under systemd

`deploy/` has a systemd unit (`ferrited.service`), environment files for a two-node pair (`node1.env`, `node2.env`, and the commented `ferrited.env.example`) and nftables rules (`ferrited.nft`). [`docs/operations.md`](docs/operations.md) is the runbook: installing, sizing each setting, the firewall, the Rails cache store configuration, rolling restarts, upgrades, backups, monitoring, and what to do when a node or a disk fails.

The unit is `Type=notify`. When `NOTIFY_SOCKET` is set, ferrited sends the service manager `READY=1` once its client port accepts connections (with peers, only once it is ready as described above), `STATUS=` lines saying why it is not serving yet (the same text it logs), and `STOPPING=1` when a clean stop begins. So `systemctl start` and `systemctl restart` return only once the node serves, and `systemctl status` shows what it is waiting for. A notification that cannot be sent is logged once and otherwise ignored. ferrited reads `NOTIFY_SOCKET` but leaves it set, since it starts no other process.

## Compatibility

Supported: every meta command (`mg`, `ms`, `md`, `ma`, `mn`, `me`) and the classic text commands. Not supported: the binary protocol, SASL, TLS, and UDP.

Deliberate differences from memcached:

| Area | memcached | ferrited |
|---|---|---|
| CAS token | Per-node counter | Derived from the item version, identical on every node, so a token obtained from one node is valid on another after failover |
| `incr`/`decr`/`ma` | Atomic | Atomic on one node. The resulting value is replicated, so two nodes incrementing the same key concurrently converge on one of the two results and lose the other increment |
| `append`/`prepend` | Atomic | Atomic on one node. The resulting value is replicated; concurrent appends to one key on two nodes keep only one |
| `cas` / `C` flag | Atomic | Atomic on one node. Two nodes can each accept a conditional write against the same token; the later version wins |
| Eviction | Slab LRU | Approximate LRU per shard; eviction is local and not replicated, so nodes under memory pressure may hold different key sets |
| `flush_all` | Local | Replicated to all peers |
| `incr`/`decr` to a shorter number | Pads the stored value with trailing spaces to its old length (`10` minus 1 reads back as `9` plus a space) | Stores exactly the digits |
| Over-long key on `get` | `CLIENT_ERROR` followed by an extra blank line | `CLIENT_ERROR` with no extra line |
| Flags over 32 bits | Truncated to 32 bits | Rejected |
| Exptime over 32 bits | Truncated to 32 bits (`4294967296` becomes 0, never expires) | Read as an absolute Unix time |
| Control characters in keys | Accepted | Rejected |
| Base64 key with an invalid character inside or after the encoded text (`Zm9v!`, `Zm=v`) | Accepted | Rejected as `error decoding key` |
| Oversized value on a non-set `ms` (append, prepend, add, replace) | Removes the existing item | Keeps the existing item |
| `ma` with an over-long opaque | Applies the arithmetic, then errors | Errors with no side effect |
| `me` with a base64 key (`b` flag) | Looks up the undecoded key | Decodes the key |
| `ma` with `t` before `T` | Reports the TTL from before the update | Reports the final TTL |
| Value of exactly 1 MiB at the default item size | Rejected as too large | Accepted |
| Default listen address | All interfaces | `127.0.0.1`, because there is no authentication; set `-l` to expose it |
| `version` reply | `1.6.x` | `1.6.40-ferrited-<version>`, so clients that gate features on memcached 1.6 keep working |
| `slabs`, `lru`, `lru_crawler`, `watch`, `cache_memlimit` | Functional | `slabs automove`, `lru` and `cache_memlimit` reply `OK` and do nothing; the rest reply `ERROR` |

When an item stored with a base64 key is read with the plain key (or the reverse), the key the reply echoes may differ from memcached's.

The cross-node anomalies only arise when the same key is written on two nodes within the replication lag. With Dalli's key hashing that happens during failover and recovery, not in steady state.

## Testing

```bash
cargo test            # unit, integration, and parity tests (parity needs memcached on PATH)
tests/dalli/run.sh    # Dalli 5 smoke test, and failover between two nodes (needs Ruby 3.3+)
tests/rails/run.sh    # Rails' MemCacheStore test suite with Dalli 5 (needs Ruby 3.3+ and network)
```

`tests/rails/run.sh` clones Rails at a pinned tag into a temporary directory and runs `ActiveSupport::Cache::MemCacheStore`'s tests against a single node, a node with `--data-dir`, and node 1 of a replicating pair, each on a free port; it never connects to port 11211. Set `RAILS_TEST_MEMCACHED=1` to run them against memcached first as a baseline, and `RAILS_TEST_DIR` to keep the clone and bundle for the next run.

The parity tests fail if memcached is not installed. Set `FERRITED_SKIP_PARITY=1` to skip them.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
