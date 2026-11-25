# Running ferrited in production

This guide sets up a pair of ferrited nodes on Linux servers with systemd, beside the Rails app servers that use them, and covers running them day to day. It assumes a private network where the app servers and both nodes can reach each other. The examples use the documentation range 192.0.2.0/24: node 1 at 192.0.2.11, node 2 at 192.0.2.12, app servers elsewhere in 192.0.2.0/24. Use your own private addresses.

The files are in `deploy/`:

| File | Installed as |
|---|---|
| `ferrited.service` | `/etc/systemd/system/ferrited.service` |
| `node1.env`, `node2.env` (commented reference: `ferrited.env.example`) | `/etc/ferrited/ferrited.env` on each node |
| `ferrited.nft` | nftables rules, adjusted per node |

## Install

1. **Build.** ferrited needs a recent stable Rust (install it with rustup). On a Linux machine of the servers' architecture, ideally the servers' own distribution so the binary links against the same glibc:

   ```bash
   cargo build --release --locked
   ```

   The binary is `target/release/ferrited`. Building in a container of that distribution and architecture works too, for example from a Mac with Docker, mounting the source and running the same command inside.

2. **Install the binary** on both nodes: `install -m 0755 ferrited /usr/local/bin/ferrited`. `install` writes a new file rather than overwriting the running one in place.

3. **Create the secret** once, and put the same file on both nodes:

   ```bash
   install -d -m 0755 /etc/ferrited
   (umask 077; openssl rand -hex 32 > /etc/ferrited/repl-secret)
   ```

   Copy it to the other node over SSH, keeping it root-owned with mode 0600. ferrited reads the file at startup and strips trailing line breaks. The unit's `LoadCredential=repl-secret:/etc/ferrited/repl-secret` has systemd read it as root and hand the service a private copy under `$CREDENTIALS_DIRECTORY`, so the file never needs to be readable by the service's user. To change the secret, replace the file on both nodes and restart both, as for an upgrade below: until both use the same secret they refuse each other's links.

4. **Configure the node**: copy `deploy/node1.env` (on node 2, `deploy/node2.env`) to `/etc/ferrited/ferrited.env` and adjust the addresses and sizes (see "Sizing" below). Install the unit:

   ```bash
   install -m 0644 ferrited.service /etc/systemd/system/ferrited.service
   systemctl daemon-reload
   systemctl enable ferrited
   ```

5. **Open the firewall** for the app servers and the other node only (see "Firewall").

6. **Start the pair.** The unit is `Type=notify`: `systemctl start` returns only once the node serves clients, which a node with peers does only once it has caught up with them. A brand-new data directory has no record of when the node last served, so on the very first start each node waits for the other (or for `--sync-timeout`, 60 seconds, after which it serves alone). Start node 1 without waiting, then node 2:

   ```bash
   node1$ systemctl start --no-block ferrited
   node2$ systemctl start ferrited
   ```

   Node 2 links with node 1, each catches up with the other (both empty), and both serve; `systemctl start` on node 2 returns within a second or two, and `systemctl status ferrited` on node 1 shows `active (running)` with `Status: "serving clients on 192.0.2.11:11211"`. Started with a plain `systemctl start` on node 1 alone, that command would wait the full 60 seconds and node 1 would then serve alone, which is harmless for an empty pair but takes longer.

   After a reboot, `systemctl enable` starts each node at boot; both then find each other the same way.

## The unit

`deploy/ferrited.service` explains each setting in comments. The choices:

- **`DynamicUser=yes`**: systemd allocates an unprivileged user while the service runs, so there is no account to create. `StateDirectory=ferrited` gives that user `/var/lib/ferrited` (stored as `/var/lib/private/ferrited`) and keeps its ownership right across restarts, and `LoadCredential=` works with it. Backups and inspection are done as root, which can read the directory either way.
- **Flags from `EnvironmentFile=`**: ferrited takes flags only. `ExecStart` builds the command line from the variables in `/etc/ferrited/ferrited.env`; `FERRITED_PEERS`, `FERRITED_METRICS` and `FERRITED_EXTRA_ARGS` are unbraced, so they split into several arguments and may be empty (or unset: leaving `FERRITED_METRICS` out turns the metrics endpoint off). The data directory (`%S/ferrited`) and the secret (`${CREDENTIALS_DIRECTORY}/repl-secret`) come from systemd.
- **`Type=notify`**: ferrited sends `READY=1` once its client port accepts connections. Until then `systemctl status` shows why it is waiting (the same text it logs, such as `not serving clients yet: no peer is reachable; serving with local data alone in 42s (--sync-timeout 60)`), and `systemctl start`/`restart` wait, which is what makes rolling restarts safe. It sends `STOPPING=1` when a clean stop begins.
- **`LimitNOFILE=65536`**: ferrited raises its soft descriptor limit to cover `-c` plus a reserve for its own descriptors, but cannot pass the hard limit; this sets both, far above `-c 4096`.
- **`TimeoutStopSec=120`**: a clean stop lets connections finish for up to 5 seconds, stops replication (up to 2 seconds), then writes and syncs everything queued, and takes a snapshot of the whole store if changes were dropped from the log. That snapshot dominates: it writes every item, so allow for `-m` at the disk's write speed (4 GiB at 200 MB/s is about 20 seconds). If systemd kills the node at the timeout, the next start is treated as after a crash: the peer sends it a full sync.
- **`TimeoutStartSec=10min`**: covers recovery (loading the newest snapshot and replaying the log, which grows with the data set), waiting for the peer (up to `--sync-timeout`, 60 seconds, while no peer is reachable) and catching up, which after a crash is a full sync of the peer's whole data set. Raise it for very large data sets; with `--sync-timeout 0` (wait forever) set it to `infinity`. The sync timeout counts only time with no peer reachable: a peer that is reachable but still recovering or waiting itself (`reason` `peer_recovering`), or a catch-up that takes longer than the start timeout, keeps the node waiting past it. systemd then stops the node and, because of `Restart=on-failure`, starts it again, which begins the wait over and may make the peer send another full sync; the journal shows `ferrited.service: start operation timed out. Terminating.` and `Failed with result 'timeout'`, repeating every 10 minutes or so. If that happens, check the peer (`systemctl status ferrited` there, and the status command under "Monitoring"), and if it is only slow, raise the timeout with `systemctl edit ferrited` (add `[Service]` and `TimeoutStartSec=30min`, or `infinity` to wait as long as it takes) and start the node again.
- **`Restart=on-failure`, `RestartSec=5s`, at most 10 starts in 10 minutes**: a node that exits with an error, is killed, or crashes comes back; one that keeps failing (a full disk refuses startup, for example) stops being retried instead of making its peer send a full sync every few seconds. The limit counts every start, manual restarts included, so after many restarts in a row systemd refuses the next one (`start-limit-hit`); `systemctl reset-failed ferrited` clears the count.
- **Hardening**: no capabilities, a read-only system (`ProtectSystem=strict`) with only the state and runtime directories writable, no home directories, private `/tmp` and `/dev`, no kernel tunables, modules, logs or clock, `RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX` (TCP for clients, replication and metrics, a Unix socket for `NOTIFY_SOCKET`), `SystemCallFilter=@system-service` minus `@privileged`, `MemoryDenyWriteExecute`, and others. ferrited needs only sockets, epoll and eventfd, its data directory (write, fsync, flock, rename, unlink), `setrlimit` within the hard limit, signals and threads, all of which these allow. `systemd-analyze security ferrited` rates the unit 1.2 ("OK"); what it still flags is inherent: the service has network access and Internet sockets.
- **No Unix socket**: do not add `-s`/`--socket`. It serves clients on a Unix socket *instead of* the TCP client port, so the app servers could no longer connect, and with `UMask=0077` only root and the service itself could use the socket.

What was verified: the unit passes `systemd-analyze verify`, and two nodes ran under systemd 255 (Ubuntu 24.04) in containers on a private network with this unit, `node1.env`, `node2.env` and a shared secret, with the system call filter set to kill rather than return `EPERM` so that a blocked call would have shown: first start of a new pair, replication both ways, `systemctl stop` (exit 0, `CLEAN` written), a node restarted while its peer was down (last standing, ready after the 2-second grace), a rolling restart, a SIGKILL followed by `Restart=on-failure` and a full sync from the peer, a peer given by host name, `systemd-analyze security`, and, separately, that `--socket` works under the hardening (it is not part of this setup; see above). Not verified: real hardware, large data sets (and so the timeouts above), and other distributions or older systemd versions. The metrics endpoint (`FERRITED_METRICS`, port 11213) was added after these checks and has not yet been run under this unit or with these nftables rules; it needs nothing the hardening forbids (a TCP listener, like the client port). Two things differed in the containers from a normal host and were adjusted there only: the root mount had to be made shared (`mount --make-rshared /`; on a normally booted host it already is) for credentials to appear, and `systemctl kill -s KILL` reported an error about auxiliary processes while still killing the main process.

## Sizing

The values in `node1.env` are for nodes with about 8 GiB of memory serving a few app servers. How to choose each:

**`-m` (`FERRITED_MEMORY_MB`): cache memory.** The process uses more than `-m`: the replication backlog (`--repl-backlog`, 256 MiB here), the oplog's queue (`--oplog-bytes`, 64 MiB by default), and buffers for each connection. Tombstones count within `-m`. Leave room for the system and the page cache the log uses. On an 8 GiB machine that does nothing else, `-m 4096` leaves about 3.5 GiB; on a machine shared with app servers, take what the app servers do not need, minus about a quarter for headroom. An out-of-memory kill is a crash: the node restarts and gets a full sync.

**`-c` (`FERRITED_CONN_LIMIT`): connections.** Each Rails process (Puma worker, Sidekiq process) opens one connection to each node per Dalli client, and Rails' `MemCacheStore` pools clients (by default since Rails 7.1), so expect app servers × processes per server × pool size on each node, plus your monitoring. Past the limit a new connection gets `ERROR Too many open connections` and is closed (counted in `rejected_connections`). Connections cost little while idle; 4096 leaves ample room for a few app servers, and `LimitNOFILE` covers it.

**`-t` (`FERRITED_THREADS`): event loop threads.** The default is one per core. A request is served on one thread without waiting on disk or network, so a few threads go a long way. Other threads run beside them: replication, readiness, signals and timers on a Tokio runtime of at most 4 worker threads, plus the log thread (which writes the log and feeds replication), an acceptor thread, a compaction thread while a snapshot is written, and Tokio's blocking pool, which the cap of 4 does not cover, for full-sync copies, reads of log segments for a peer that is behind, and the expiry sweeper. Most of these are idle most of the time. On a dedicated cache machine the default is fine; on one shared with app servers, 4 keeps the client threads from competing with them.

**`--repl-backlog`: the catch-up window kept in memory.** A peer that restarts or loses its link resumes where it left off if this node still holds every change it missed; otherwise it gets a full sync of the whole data set. Each change is charged its value's length plus its key's plus 64 bytes. So the backlog should hold at least (changes per second) × (average value + key + 64 bytes) × (seconds a peer may be away). For example 1,000 sets a second of 2 KiB values is about 2 MiB a second, and 256 MiB covers about two minutes, more than a restart takes. With persistence, a sender whose peer is behind the backlog reads its own log segments from disk, which usually reach back to the older of the two snapshots it keeps, so the window is larger than the backlog (not always: a snapshot that covers lost changes or a hole in the history is kept alone, and the log then reaches back only to it); the backlog decides how much of it is served from memory. Measure the rate from `stats` (`cmd_set`, `delete_hits` and `delete_misses` over time; the average item size is `bytes` / `curr_items`). If `stats repl` shows `repeat_syncs` above 0, the backlog is too small for what is written during a full sync: raise it.

**`--tombstone-memory` and `--tombstone-ttl`: how long deletes are remembered.** A delete leaves a tombstone so that an older write arriving late cannot bring the key back; so do evicted and expired items, and, with peers, deletes of keys that are not stored (common in Rails). Tombstones live for `--tombstone-ttl` (default 3600 seconds) within `--tombstone-memory` (default a tenth of `-m`, about 400 MiB at `-m 4096`); past the cap the oldest go first. The defaults are right for most loads: at a tenth of `-m 4096` they hold a few million deletes. A peer that is down longer than the tombstones last can bring back keys deleted meanwhile; if `repl_tombstones_evicted` climbs steadily, deletes are being forgotten early, and a larger cap (up to half of `-m`) keeps them longer at the cost of item memory.

**`--fsync-interval-ms`: the power-failure window.** Each node fsyncs its log once per interval (default 1000 ms), so a power loss on one node loses at most about that much of its writes from its own disk. With a peer that is not the whole story: when the node comes back from a crash, its peer sends it a full sync, which returns every write that had reached the peer, so only writes made within the replication delay before the crash (milliseconds) are lost for good. The interval matters when both nodes lose power together. The default is a good balance; shorter intervals cost more disk syncs.

**`--sync-timeout`: how long a restarted node waits for a missing peer.** A node that was not the last one standing (for example, both lost power together) waits until it has caught up with its peer; if no peer is reachable for this long (default 60 seconds), it serves with its local data, which may lack writes the peer accepted. Shorter means less time with no node serving after a double failure; longer means less chance of serving stale data while the other node is merely slow to boot. Keep `TimeoutStartSec` above it plus recovery time.

## Firewall

The client port has no authentication, and replication is signed (with the secret) but not encrypted: anyone who can see the traffic can read every value. So:

- the client port (11211) only from the app servers;
- the replication port (11212) only from the other node;
- the metrics port (11213) only from the monitoring host (the endpoint has no authentication either, and shows peer addresses and every counter);
- all on the private network only.

`deploy/ferrited.nft` does this with nftables (shown for node 1; node 2 admits 192.0.2.11 on the replication port; the monitoring host is 192.0.2.20 in the example). It also admits connections from the node itself on `lo`, which is how the status command below arrives. Load it with `nft -f /etc/nftables.d/ferrited.nft`, or merge the rules into your ruleset; check that your distribution loads that file at boot (Debian and Ubuntu load `/etc/nftables.conf`, which can `include` it).

The file is a table of its own whose chain accepts by default, so it can only restrict: it drops what should not reach the two ports, but it cannot open them. A packet must be accepted by every table it passes through, so on a host whose own firewall drops by default (firewalld on RHEL and Fedora, ufw on Ubuntu, a typical `/etc/nftables.conf` with `policy drop`), also open the client port (11211) to the app servers, the replication port (11212) to the other node and the metrics port (11213) to the monitoring host in that firewall: for example, on node 1, `ufw allow from 192.0.2.0/24 to any port 11211 proto tcp`, `ufw allow from 192.0.2.12 to any port 11212 proto tcp` and `ufw allow from 192.0.2.20 to any port 11213 proto tcp`, or a firewalld rich rule for each. Then check from an app server that the client port answers `stats`, and on each node that `stats repl` shows both links `streaming` (see "Monitoring").

## Rails and Dalli

List both nodes on every app server, written the same way on each:

```ruby
config.cache_store = :mem_cache_store, "192.0.2.11:11211", "192.0.2.12:11211",
  { down_retry_delay: 5 }
```

Dalli hashes each key to one of the servers it is given. The hash depends on the server strings, not on their order, so an app server that names a node by IP address and another that names it by host name send the same key to different nodes; use the same strings everywhere. In steady state each key is read and written on one node, and replication keeps a copy on the other. When a node is down (or still catching up after a restart, when it refuses connections), Dalli sends that node's keys to the other node, which has them. This matters because replication is last-writer-wins: `incr`, `append` and `cas` are atomic on one node only, and two nodes that write the same key within the replication delay keep only the later write. With Dalli's hashing that happens only around a failover and a return, not in steady state. Pointing all app servers at one node and using the other only as a standby would also work, but splits no load.

`down_retry_delay` is how long Dalli waits before trying a node it found down again (30 seconds by default). A lower value moves keys back to their home node sooner once it returns; while Dalli still treats it as down, those keys are read and written on the other node, which is safe, since the returned node only began serving once it had caught up and receives those writes by replication.

## Rolling restart

One node at a time, waiting for each to serve again, and then for the app servers to use it again, before touching the other:

```bash
node1$ systemctl restart ferrited    # returns once node 1 serves again
node1$ systemctl is-active ferrited  # active
# wait down_retry_delay plus a margin: 10 seconds with down_retry_delay: 5
node2$ systemctl restart ferrited
```

`systemctl restart` stops the node cleanly and starts it; because the unit is `Type=notify`, it returns only once the node has caught up with its peer and opened its client port. Meanwhile Dalli sends the node's keys to the other one. If the restart is still waiting (`systemctl status ferrited` shows why), do not restart the other node yet.

The wait before restarting node 2 matters. Each Dalli client that found node 1 down keeps it marked down for `down_retry_delay` after its last failed attempt, which may be just before node 1 began serving again, and does not try it before then. If node 2 stops within that time, those clients have no server at all: reads miss and writes are dropped until they try node 1 again. So after node 1 serves, wait at least `down_retry_delay` plus a few seconds (with Dalli's default of 30 seconds, 40 seconds) before stopping node 2. The same applies whenever you take one node down after the other, such as for backups.

## Upgrades

**Link protocol.** Nodes of different versions may speak different link protocol versions, in which case they refuse to link (logging the version mismatch) and each serves alone. So upgrade both nodes together, in a way that keeps that window short:

1. Install the new binary on both nodes (`install -m 0755 ferrited /usr/local/bin/ferrited`); the running processes are unaffected.
2. `node2$ systemctl stop ferrited`. Clients use node 1, which has everything.
3. `node1$ systemctl restart ferrited`. Node 1 was last standing (its peer stopped while it served), so it serves again after loading its data and a 2-second grace period. During this restart neither node serves; Dalli treats both as down and Rails reads miss and writes are dropped. This lasts until each client tries node 1 again, up to `down_retry_delay` after node 1 serves.
4. `node2$ systemctl start ferrited`. Node 2 links with node 1, catches up and serves. Starting it takes nothing away from the clients, so it need not wait.

Alternatively restart node 1 first while node 2 serves: node 1 cannot link with the old node 2, so it waits up to `--sync-timeout` (60 seconds) before serving alone. Restarting node 2 while node 1 still waits leaves no node serving until node 2 is back (as last standing it serves 2 seconds after loading its data) and the clients try it again, up to `down_retry_delay` later; this gap is usually shorter than in the steps above, which include node 1's whole restart, but it is not zero. To have no gap at all, let node 1 finish waiting and serve alone (`systemctl restart` on it returns after the 60 seconds), wait `down_retry_delay` plus a margin as for a rolling restart, then restart node 2; meanwhile the two nodes serve without replicating to each other, and their data is merged by version (last writer wins) when they link.

**Data directory format.** A node refuses to start, rather than discard anything, if its data directory holds a file in a format its binary cannot read. A new version may change the format, so back up each data directory first (below) if you may need to go back; to roll back, restore the backups onto both nodes along with the old binary (see "Restore").

## Backup

ferrited is a cache, and its peer holds the same data, so backups are mainly for rolling back an upgrade. Take a consistent copy one of two ways:

- **Stop the node** (`systemctl stop ferrited`, which writes the `CLEAN` marker), copy `/var/lib/private/ferrited` (for example `tar -C /var/lib/private -czf ferrited-backup.tgz ferrited`), and start it again. The other node serves meanwhile, and the restarted node catches up. To back up both nodes this way, wait between them as for a rolling restart.
- **An atomic filesystem snapshot** (LVM, ZFS, btrfs) of a running node is what a crash at that instant would leave on disk, and recovery handles it the same way: it loads what was synced and cuts a torn last entry.

Do not copy the directory of a running node with `cp` or `rsync`. The copy is not taken at one instant: compaction writes new snapshots and deletes old log segments while it runs, so it can hold a snapshot from before and segments from after, with a gap between them that recovery reports as lost history.

## Restore

Restore a backup onto both nodes together, as a rollback, not onto one node of a running pair. A node restored from an old backup brings back keys deleted since the backup, on both nodes: the peer's full sync carries deletes only as the tombstones it still holds (for `--tombstone-ttl`, 1 hour by default, or less once `--tombstone-memory` fills), and the restored node's log is one the peer cannot resume from, so the restored node sends the peer a full sync of its old data in turn, and the peer accepts every old item whose tombstone it no longer has.

- **To rebuild one node of a pair** (damaged disk, data in doubt), do not restore a backup: stop it, empty its data directory (`find /var/lib/private/ferrited -mindepth 1 -delete`) and start it. It gets a full sync of the peer's data and serves once caught up, as under "A node replaced with an empty disk".
- **To roll back both nodes** (after an upgrade, with the old binary installed on both), stop both, restore each node's backup as below, and start the pair as for a new one (`systemctl start --no-block ferrited` on node 1, then `systemctl start ferrited` on node 2): each waits for the other, they link, catch up with each other and serve. Take the two backups close together (in the same maintenance window), so they hold the same data.
- **Onto one node of a running pair** only if the backup is younger than the peer's tombstones (well under `--tombstone-ttl`, with `repl_tombstones_evicted` on the peer not rising since the backup was taken); otherwise rebuild it empty as above.

To restore a backup onto a node, as root:

```bash
systemctl stop ferrited
find /var/lib/private/ferrited -mindepth 1 -delete
tar -C /var/lib/private -xzf ferrited-backup.tgz
rm -f /var/lib/private/ferrited/CLEAN
chown -R --reference=/var/lib/private/ferrited /var/lib/private/ferrited
systemctl start ferrited
```

Deleting `CLEAN` makes the node be treated as having crashed: it discards its replication positions and its peer sends it a full sync. The `chown` gives every restored file the owner of the directory itself. With `DynamicUser=yes`, systemd gives the directory to the service's user at start, but changes the owner of the files inside only when the directory's own owner is wrong; ferrited runs without capabilities, so it could neither read nor write files left with another owner.

## Monitoring

Logs go to the journal: `journalctl -u ferrited -f`. `systemctl status ferrited` shows the latest status line.

### With Prometheus

`node1.env` and `node2.env` turn on the metrics endpoint on each node's private address, port 11213 (`FERRITED_METRICS=--metrics-addr 192.0.2.11:11213`). It serves every value of `stats`, each link of `stats repl` and the node's readiness at `http://192.0.2.11:11213/metrics`, from before the node loads its data until it has stopped, so a node that is waiting for its peer can be scraped while its client port is closed. The README's "Metrics" section lists what it reports; each metric's help text names the `stats` value it comes from. A scrape locks each shard of the store briefly, as `stats` does, which is negligible at the interval below.

On the Prometheus server (192.0.2.20 here, which the firewall admits to port 11213):

```yaml
# prometheus.yml
scrape_configs:
  - job_name: ferrited
    scrape_interval: 15s
    static_configs:
      - targets: ["192.0.2.11:11213", "192.0.2.12:11213"]
rule_files:
  - ferrited-rules.yml
```

Check it from the Prometheus server with `curl -s http://192.0.2.11:11213/metrics | head`; `ferrited_ready 1` and `ferrited_repl_link_state{...,state="streaming"} 1` for both directions are a healthy node.

Alert rules (`ferrited-rules.yml`); adjust thresholds to your load. Prometheus evaluates rules every `evaluation_interval`, 1 minute unless `prometheus.yml` sets it, so a `for` shorter than that interval is coarse: an alert fires at the first evaluation after its `for` has passed.

```yaml
groups:
  - name: ferrited
    rules:
      - alert: FerritedDown
        expr: up{job="ferrited"} == 0
        for: 1m
        annotations:
          summary: "{{ $labels.instance }} cannot be scraped: the process is down or the host unreachable"
      # Longer than --sync-timeout (60 s) plus loading the data: a node that waits this long is
      # waiting for a peer that is reachable but not catching it up (see ferrited_waiting_reason),
      # or catching up slowly.
      - alert: FerritedNotReady
        expr: ferrited_ready == 0
        for: 3m
        annotations:
          summary: "{{ $labels.instance }} has not served clients for 3 minutes; ferrited_waiting_reason says why"
      # On a node that serves, a link that is down or syncing. A rolling restart takes the
      # surviving node's links down while its peer restarts, and a full sync shows as syncing,
      # so `for` must outlast both; raise it if a restart or a full sync of your data set takes
      # longer than 2 minutes.
      - alert: FerritedLinkNotStreaming
        expr: ferrited_repl_link_state{state="streaming"} == 0 and on(instance) ferrited_ready == 1
        for: 2m
        annotations:
          summary: "{{ $labels.instance }}: link {{ $labels.direction }} with {{ $labels.peer }} not streaming for 2 minutes"
      - alert: FerritedLagGrowing
        expr: deriv(ferrited_repl_peer_lag_entries[5m]) > 0 and ferrited_repl_peer_lag_entries > 10000
        for: 5m
        annotations:
          summary: "{{ $labels.instance }}: {{ $labels.peer }} is falling behind ({{ $value }} entries a second)"
      - alert: FerritedRepeatFullSyncs
        expr: increase(ferrited_repl_peer_repeat_syncs_total[1h]) > 0
        annotations:
          summary: "{{ $labels.instance }}: repeated full syncs to {{ $labels.peer }}; raise --repl-backlog"
      - alert: FerritedBadFrames
        expr: increase(ferrited_repl_bad_frames_total[1h]) > 0
        annotations:
          summary: "{{ $labels.instance }}: replication frames failed authentication; something changed traffic on the network"
      - alert: FerritedPersistDropped
        expr: increase(ferrited_persist_dropped_total[10m]) > 0
        annotations:
          summary: "{{ $labels.instance }}: changes dropped from the log; the disk cannot keep up"
      - alert: FerritedPersistDegraded
        expr: ferrited_persist_degraded == 1
        annotations:
          summary: "{{ $labels.instance }}: the disk is failing and changes are not being saved (see Disk full or failing)"
      - alert: FerritedRejectingConnections
        expr: increase(ferrited_rejected_connections_total[5m]) > 0
        annotations:
          summary: "{{ $labels.instance }}: connections turned away at -c; raise FERRITED_CONN_LIMIT"
      - alert: FerritedEvictingNearLimit
        expr: rate(ferrited_evictions_total[10m]) > 0 and ferrited_memory_used_bytes / ferrited_memory_limit_bytes > 0.9
        for: 30m
        annotations:
          summary: "{{ $labels.instance }}: evicting with memory at the limit; raise -m"
      # Fires as soon as tombstones were evicted within the last hour: --tombstone-memory is
      # full, so deletes are forgotten before --tombstone-ttl, and a peer that was down for
      # a while can bring back keys deleted meanwhile.
      - alert: FerritedTombstonesEvicted
        expr: increase(ferrited_repl_tombstones_evicted_total[1h]) > 0
        annotations:
          summary: "{{ $labels.instance }}: deletes forgotten early; see Sizing, --tombstone-memory"
```

The rules use `increase` rather than a counter's value, so an alert clears once the counter stops rising; `stats reset` on the client port zeroes the counters, which Prometheus treats as a reset. These rules have not been loaded into a running Prometheus or checked with `promtool check rules`; check them with it before relying on them.

### Without Prometheus

The client port answers `stats` (from the node itself, the firewall admits `lo`):

```bash
printf 'stats\r\nquit\r\n' | nc 192.0.2.11 11211
printf 'stats repl\r\nquit\r\n' | nc 192.0.2.11 11211
```

`quit` makes the node close the connection after the reply, so `nc` exits by itself; this works with both netcat-openbsd (Debian, Ubuntu) and nmap's `ncat` (RHEL, Fedora), which unlike the former has no `-q`.

What to watch in `stats`:

| Value | Meaning | Act when |
|---|---|---|
| `curr_connections`, `rejected_connections` | Open connections; connections turned away at `-c` | `rejected_connections` rises: raise `-c` |
| `evictions`, `bytes`, `limit_maxbytes` | Items evicted for memory; memory in use and the limit | Evictions rise steadily: raise `-m` |
| `repl_peers` | Links up, each direction counted: 2 with one healthy peer | Below 2 for more than a few seconds |
| `repl_full_syncs_sent`, `repl_full_syncs_received` | Full syncs; each restart after a crash, or a peer too far behind, adds one | Rise without a crash or long outage |
| `repl_bad_frames` | Links dropped because a frame failed authentication | Above 0: something on the network changed replication traffic |
| `repl_sync_timeouts` | 1 if this node began serving because no peer was reachable for `--sync-timeout` | 1: it may lack the peer's writes until the peer returns |
| `repl_tombstones_evicted` | Deletes forgotten early to stay within `--tombstone-memory` | Climbs steadily: see "Sizing" |
| `persist_degraded` | 1 while the disk is failing and changes are not being saved | 1: see "Disk full" |
| `persist_dropped`, `persist_lost` | Changes the log had no room for; changes discarded while degraded | Above 0: the disk cannot keep up or failed |
| `persist_errors` | Disk errors | Above 0 |

`stats repl` gives, for the peer, `peer:<address>:out` and `:in` (each direction's link: `down`, `syncing` or `streaming`; both should be `streaming`), `:acked` and `:lag` (entries the peer is behind; it should stay near 0 and return to it), and `:repeat_syncs` (full syncs that started within a minute of the last; above 0 means `--repl-backlog` is too small).

The replication port answers a status command that works even while the client port is closed. With a secret, it is answered only on loopback, and since the listener is on the private address, connect to it from the node itself with a loopback source address:

```bash
printf 'status\r\n' | nc -s 127.0.0.1 192.0.2.11 11212
```

The node closes the connection after its reply. Without `-s 127.0.0.1` the connection comes from 192.0.2.11 and gets `SERVER_ERROR`. `state` is `recovering`, `waiting` or `ready`; while waiting, `reason` says why (`no_peer_reachable`, `peer_recovering`, `no_inbound_link`, `catching_up`, `contacting_peers` or `grace_period`) and the `peer:node-<id>:applied` and `:target` lines show catch-up progress; once ready, `ready_by` says how (`caught_up`, `last_standing`, `sync_timeout` or `force_standalone`). The README describes every line.

## Failures

**One node down.** The other keeps serving, and Dalli sends the down node's keys to it. It holds every key that had replicated before the failure; writes made on the failed node within the replication delay before it failed are lost. When the node returns, it loads its data and keeps its client port closed until it has caught up with the survivor: after a clean stop it resumes from where it left off; after a crash or kill it gets a full sync, which also returns its own last writes that never reached its disk. `systemctl start` (or the automatic restart) completes when it serves.

**Both nodes down.** Each node records in its log which peers it was linked with while serving. If one node went down first and the other carried on alone, the survivor is "last standing": started again, it serves after its data loads and a 2-second grace period, and the other catches up from it when it starts. If both went down together (a power loss, say), neither is last standing: each waits for the other. Start both (`systemctl start --no-block` on the first, as for a new pair); they link, catch up with each other and serve. If one cannot come back, the other serves alone after `--sync-timeout` (60 seconds), logging a warning and setting `repl_sync_timeouts` to 1; writes only the missing node had are merged in when it returns.

**`--force-standalone`.** Makes a node serve as soon as its data is loaded, without waiting for its peer. Use it only when you know the peer is gone and cannot wait for `--sync-timeout` (for example when it is set to 0). The risk: the node serves without any writes the peer accepted while this node was down, and if the peer is in fact alive but unreachable, both serve and drift apart until the link returns and their data is merged by version (last writer wins). Add it to `FERRITED_EXTRA_ARGS` for one start, then remove it again, since every start with it skips the wait. A node that became ready this way logs no record of its links until it has caught up with a peer that linked to it, so its next start waits again.

**Disk full or failing.** ferrited keeps serving from memory, logs the error, discards changes rather than queue them (`persist_lost`), retries every second, and sets `persist_degraded` to 1. It tries to write a `DEGRADED` file in the data directory, which the next start reports. Once the disk works again it takes a snapshot and clears both. Free space or fix the disk; nothing needs restarting. A crash while degraded loses the discarded changes from disk (the peer still has them and sends a full sync after the crash). A node whose disk is full or read-only at startup refuses to start, naming the problem, so the automatic restarts give up after the start limit; fix the disk, then `systemctl reset-failed ferrited && systemctl start ferrited`.

**`HISTORY_LOST`.** Recovery found a hole in the node's history (a gap or damaged stretch in the log, a damaged snapshot, or changes lost to a failing disk that no snapshot covered yet). It writes `HISTORY_LOST`, takes a snapshot at once, and removes the file once that snapshot is written. While the file exists, the node never counts itself last standing, so its next start waits for its peer (or the sync timeout) rather than serving at once. No action is needed; a damaged snapshot is left as `*.damaged` for inspection, and can be deleted.

**A node replaced with an empty disk.** Install the binary, unit, env file and secret as for a new node, with the same `--node-id` and addresses (the peer learns the ID in the handshake; IDs only need to differ between nodes), and start it. With an empty data directory it has no record of having served, so it waits for its peer, receives a full sync of the peer's data, and serves once caught up. The peer, finding no position it can resume from, sends the full sync on its own. Keep both nodes' clocks synchronized (NTP), here as always: versions are based on time, and large clock skew biases which of two conflicting writes wins.
