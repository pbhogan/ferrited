#!/usr/bin/env bash
# Builds ferrited and runs the Dalli smoke test on a spare port, then the failover test.
set -euo pipefail

cd "$(dirname "$0")/../.."
port="${FERRITED_TEST_PORT:-$(ruby -rsocket -e 'server = TCPServer.new("127.0.0.1", 0); puts server.addr[1]; server.close')}"

cargo build --release
./target/release/ferrited -p "$port" &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
  if nc -z 127.0.0.1 "$port" 2>/dev/null; then break; fi
  sleep 0.1
done

FERRITED_ADDR="127.0.0.1:$port" ruby tests/dalli/smoke.rb
FERRITED_BIN="$PWD/target/release/ferrited" ruby tests/dalli/failover.rb
