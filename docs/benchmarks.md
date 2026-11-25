# Benchmarks

ferrited compared with memcached, and the cost of persistence, replication and frame signing.

## Running them

`scripts/bench.sh [runs] [seconds]` (defaults: 3 runs of 20 seconds) needs `memtier_benchmark` and `memcached` on `PATH`. For each run, workload and server it:

1. Starts the server on port 21311 with 1024 MB of memory.
2. Writes every key from 1 to 100,000 once with a 512-byte value, so every later get hits.
3. Runs memtier for the test time on random keys, at 1 set to 10 gets unless `RATIO` says otherwise.
4. Reads the server's total CPU time, then stops it.

Runs alternate between servers, so a change in machine load affects all of them. Results go to `target/bench`.

| Variable | Meaning | Default |
|---|---|---|
| `SERVERS` | Servers to compare: `memcached`, `ferrited`, `ferrited-persist` (with `--data-dir`), `ferrited-repl` (node 1 of a replicating pair), `ferrited-repl-secret` (the same with signed frames); `ferrited-base[-persist\|-repl\|-repl-secret]` runs `BASE_BIN` instead, for A/B comparisons of two builds | `memcached ferrited ferrited-persist` |
| `WORKLOADS` | `nopipe`: 4 threads × 50 connections, one request in flight each. `pipe16`: 4 threads × 25 connections, 16 requests in flight each | `nopipe pipe16` |
| `SERVER_THREADS`, `CLIENT_THREADS` | Server event loop threads, memtier threads | 4 each |
| `RATIO` | memtier set:get ratio | `1:10` |

Server CPU per operation is the server's user plus system CPU time divided by the operations memtier completed. Competing load disturbs it much less than throughput, so it is the number to watch when changing ferrited's speed. Throughput is only meaningful on a quiet machine; a busy one can vary by more than 2× between runs.

With 4 server and 4 memtier threads, memtier is the limit even when pipelined. To measure server capacity, saturate the server:

```bash
SERVER_THREADS=2 CLIENT_THREADS=6 WORKLOADS="nopipe pipe16" scripts/bench.sh 5 15
```

memtier speaks only the classic text protocol, so these numbers do not cover the meta commands that Dalli uses.

### In-process

`examples/hotpath` runs the pipelined workload's requests through a `server::Session`, so the kernel and memtier are not measured. Other load moves it by a few percent, so compare two builds by alternating runs:

```bash
cargo run --release --example hotpath [rounds] [classic|meta] [threads]
```

Under the saturated network benchmark about 60% of ferrited's CPU time is `writev`, `recvfrom` and `kevent`, which memcached pays too. Most of the rest is cache misses on randomly chosen items: the first read of the item and copying its value into the reply.

## macOS, Apple M1 Max

10 cores (8 performance, 2 efficiency); memtier_benchmark 2.6.1 and memcached 1.6.45 from Homebrew. Medians of 5 runs of 15 seconds, with the range in brackets. Server CPU is in µs per operation; latencies are in milliseconds.

| Workload | Server | Ops/sec | Server CPU | p50 | p99 | p99.9 |
|---|---|---|---|---|---|---|
| Saturated (2 server, 6 memtier threads), no pipelining | memcached | 135,008 [134,291–135,245] | 11.88 [11.86–11.89] | 2.22 | 2.70 [2.69–2.72] | 3.20 [3.06–3.39] |
| | ferrited | 134,754 [134,277–136,124] | 11.75 [11.70–11.78] | 2.22 | 2.69 [2.66–2.70] | 2.96 [2.88–3.36] |
| Saturated, pipelined 16 deep | memcached | 1,754,165 [1,748,194–1,764,824] | 1.10 [1.10–1.10] | 1.36 | 2.16 [2.13–2.22] | 2.62 [2.61–2.70] |
| | ferrited | 1,764,909 [1,760,764–1,774,918] | 0.99 [0.98–0.99] | 1.35 | 1.82 [1.80–1.86] | 2.38 [2.34–2.83] |
| 4 server, 4 memtier threads, no pipelining | memcached | 130,586 [129,649–131,603] | 14.84 [14.70–15.00] | 1.54 | 1.91 [1.89–1.98] | 2.29 [2.21–4.08] |
| | ferrited | 131,536 [130,977–132,107] | 14.64 [14.61–14.83] | 1.52 | 1.90 [1.88–1.90] | 2.24 [2.10–2.46] |
| 4 server, 4 memtier threads, pipelined 16 deep | memcached | 1,691,990 [1,675,994–1,705,669] | 1.45 [1.41–1.46] | 0.94 | 1.31 [1.30–1.38] | 1.63 [1.62–1.90] |
| | ferrited | 1,691,897 [1,681,600–1,698,428] | 1.25 [1.24–1.26] | 0.94 | 1.30 [1.29–1.32] | 1.78 [1.65–2.51] |

Saturated and pipelined, ferrited reaches 1.01× memcached's throughput at 0.90× its CPU per operation; unpipelined, 0.99× its CPU per operation.

### Persistence and replication

Saturated and pipelined at 1:10 (`SERVERS="ferrited ferrited-persist ferrited-repl"`):

- **Persistence** (`--data-dir`) costs about 3% of throughput and about 10% more CPU per operation, most of it the writer thread encoding and writing the log. Unpipelined, the extra CPU is under 1%.
- **Replicating** every write to a live peer costs about 6% of throughput and about 7% more CPU per operation on the writing node. The peer shared the machine, so part of the throughput cost is the peer's CPU. Writes are 1 in 11 operations here; a write-heavy workload pays more. Under this load the peer applied every set, and both nodes ended with the same item count and bytes.

### Frame signing

With `--repl-secret-file`, every replication frame carries an HMAC-SHA256. On Apple Silicon `sha2` uses the ARMv8 SHA-2 instructions.

- At 1:10, signing adds about 1% CPU per operation on the writing node and 0.03 µs per operation on the peer; throughput is within the noise.
- Per replicated 512-byte set, signing costs about 0.27 µs on the writing node and 0.29 µs on the peer.
- A signed link carries about 530,000 such sets a second, measured by sending a pair a million write-only sets (4 threads × 25 clients, pipelined 16 deep, `--repl-backlog 1500m` so the peer never needs a full sync) until the peer had acknowledged all of them. An unsigned link keeps up with nearly a million. A write rate above what the link carries makes the peer lag, and once the lag passes `--repl-backlog` the peer gets a full sync.

## Linux arm64 (VM)

Ubuntu 24.04 arm64, kernel 6.8, in a Lima VM with 8 vCPUs and 16 GiB on the same M1 Max; server, peer and load generator all ran inside the VM. Its vCPUs share the host's cores, so treat these numbers as indicative and compare servers only within this section. memtier_benchmark built from source and Ubuntu's memcached 1.6.24. Nothing here covers x86_64.

`scripts/bench.sh 3 20` (4 server threads, 4 memtier threads, 1:10). Medians of three runs; each run was within 2% of the median.

| Workload | Server | Ops/sec | Server CPU (µs/op) | p99 (ms) |
|---|---|---|---|---|
| No pipelining | memcached 1.6.24 | 540,732 | 5.68 | 0.63 |
| No pipelining | ferrited | 539,095 | 5.57 | 0.63 |
| No pipelining | ferrited, persistence | 541,705 | 5.74 | 0.65 |
| Pipelined | memcached 1.6.24 | 1,344,478 | 3.04 | 2.37 |
| Pipelined | ferrited | 3,655,185 | 0.92 | 0.75 |
| Pipelined | ferrited, persistence | 3,482,696 | 1.01 | 1.74 |

- Without pipelining memtier is the limit and the servers match; ferrited used 0.98× memcached's CPU per operation.
- Pipelined, this memcached saturated its 4 threads at about 3 µs per operation, more than twice what 1.6.45 uses on macOS, so ferrited reached 2.7× its throughput. Whether the gap comes from the older version or from Ubuntu's build is unknown, so this is not evidence that ferrited is faster than current memcached on Linux.
- Persistence costs about 3% CPU per operation unpipelined and, pipelined, 10% CPU per operation and 5% throughput, as on macOS.

### Frame signing

`sha2` uses the ARMv8 SHA-2 instructions here too, detected at run time. Saturated (`SERVERS="ferrited-repl ferrited-repl-secret" SERVER_THREADS=2 CLIENT_THREADS=4`), ranges over three runs; peer CPU is node 2's, per operation made on node 1.

| Workload | Server | Ops/sec | CPU µs/op | Peer CPU µs/op | p99 ms |
|---|---|---|---|---|---|
| No pipelining | no secret | 457,054 to 466,668 | 4.65 to 4.73 | 0.20 to 0.22 | 0.56 to 0.57 |
| No pipelining | secret (signed) | 463,601 to 467,065 | 4.66 to 4.71 | 0.23 to 0.24 | 0.55 to 0.56 |
| Pipelined | no secret | 2,880,630 to 2,965,786 | 0.78 to 0.80 | 0.12 | 0.83 to 0.89 |
| Pipelined | secret (signed) | 2,864,923 to 2,922,688 | 0.81 to 0.83 | 0.15 | 0.84 to 0.87 |

Signing adds about 4% CPU per operation pipelined and under 1% unpipelined, and about 0.27 µs per replicated 512-byte set on each side. Signed links kept up as well as unsigned ones; the replicated rate in this VM varied from 327,000 to 496,000 sets a second with or without signing, so something other than hashing limited it.
