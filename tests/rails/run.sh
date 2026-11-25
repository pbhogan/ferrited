#!/usr/bin/env bash
# Runs Rails' MemCacheStore test suite with Dalli 5 against ferrited: a single node, a node
# with --data-dir, and node 1 of a replicating pair.
#
#   RAILS_TEST_MEMCACHED=1  also run against memcached, as a baseline
#   RAILS_TEST_DIR=<dir>    keep and reuse the Rails clone and bundle (default: a temp dir)
#
# Never touches port 11211: servers use free ports, and tests/rails/port_guard.rb refuses and
# records any connection to 11211, where Rails' tests would otherwise fall back.
set -euo pipefail

rails_tag="v8.1.4.1"
dalli_version="5.0.6"

cd "$(dirname "$0")/../.."
repo="$PWD"
bin="$repo/target/release/ferrited"

ruby -e 'exit(RUBY_VERSION >= "3.3" ? 0 : 1)' || { echo "needs Ruby 3.3+" >&2; exit 1; }

if [ -n "${RAILS_TEST_DIR:-}" ]; then
  work="$RAILS_TEST_DIR"
  mkdir -p "$work"
else
  work="$(mktemp -d)"
fi

pids=()
cleanup() {
  for pid in ${pids[@]+"${pids[@]}"}; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [ -z "${RAILS_TEST_DIR:-}" ]; then rm -rf "$work"; fi
}
trap cleanup EXIT

free_port() {
  local port
  port="$(ruby -rsocket -e 'server = TCPServer.new("127.0.0.1", 0); puts server.addr[1]; server.close')"
  if [ "$port" = 11211 ]; then echo "refusing port 11211" >&2; exit 1; fi
  echo "$port"
}

# Patched as on Rails main: Dalli 5 has only the meta protocol, so the unavailable-server stub
# subclasses Dalli::Protocol::Meta.
rails="$work/rails-$rails_tag"
test_file="test/cache/stores/mem_cache_store_test.rb"
if [ ! -d "$rails" ]; then
  git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$rails_tag" https://github.com/rails/rails.git "$rails"
  ruby -i -pe '
    sub(/^  class UnavailableDalliServer < Dalli::Protocol::Binary$/) {
      %Q{  DALLI_PROTOCOL_NAME = Dalli::VERSION >= "5." ? :Meta : :Binary\n\n} +
        %Q{  class UnavailableDalliServer < Dalli::Protocol.const_get(DALLI_PROTOCOL_NAME)}
    }
    gsub(/(Dalli::Protocol\.send\(:remove_const), :Binary/, "\\1, DALLI_PROTOCOL_NAME")
    gsub(/(Dalli::Protocol\.const_set\():Binary/, "\\1DALLI_PROTOCOL_NAME")
  ' "$rails/activesupport/$test_file"
  if grep -q 'remove_const, :Binary\|const_set(:Binary\|< Dalli::Protocol::Binary' "$rails/activesupport/$test_file"; then
    echo "could not patch $test_file for Dalli 5" >&2
    exit 1
  fi
fi

export BUNDLE_GEMFILE="$work/Gemfile"
export BUNDLE_PATH="$work/bundle"
cat >"$BUNDLE_GEMFILE" <<EOF
source "https://rubygems.org"

gemspec path: "rails-$rails_tag/activesupport"

gem "dalli", "$dalli_version"
gem "connection_pool", "< 3"
gem "minitest", "~> 6.0"
gem "minitest-mock"
gem "msgpack", ">= 1.7.0", require: false
gem "rack", "~> 3.2"
gem "rake"
EOF
bundle install --quiet

cargo build --release

wait_ready() {
  for _ in $(seq 1 300); do
    if printf 'mn\r\n' | nc -w 1 127.0.0.1 "$1" 2>/dev/null | grep -q '^MN'; then return 0; fi
    sleep 0.1
  done
  echo "server on port $1 did not become ready" >&2
  exit 1
}

stop_servers() {
  for pid in ${pids[@]+"${pids[@]}"}; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  pids=()
}

failed=()

# Runs the suite against the server on port $2, naming the pass $1.
run_suite() {
  local name="$1" port="$2" guard_log="$work/port_guard-$1.log"
  rm -f "$guard_log"
  echo "== $name (MEMCACHE_SERVERS=127.0.0.1:$port)"
  if (
    cd "$rails/activesupport"
    env -u BUILDKITE MEMCACHE_SERVERS="127.0.0.1:$port" PORT_GUARD_LOG="$guard_log" \
      RUBYOPT="-r$repo/tests/rails/port_guard.rb" \
      ruby -Itest -I../railties/lib "$test_file"
  ); then :; else failed+=("$name"); fi
  if [ -s "$guard_log" ]; then
    cat "$guard_log" >&2
    echo "$name: the test process tried to connect to port 11211" >&2
    failed+=("$name (port guard)")
  fi
  stop_servers
}

if [ "${RAILS_TEST_MEMCACHED:-}" = 1 ]; then
  port="$(free_port)"
  memcached -p "$port" -l 127.0.0.1 -U 0 & pids+=($!)
  wait_ready "$port"
  run_suite memcached "$port"
fi

port="$(free_port)"
"$bin" -p "$port" & pids+=($!)
wait_ready "$port"
run_suite ferrited "$port"

port="$(free_port)"
"$bin" -p "$port" --data-dir "$work/data" & pids+=($!)
wait_ready "$port"
run_suite "ferrited --data-dir" "$port"
rm -rf "$work/data"

port="$(free_port)"
port2="$(free_port)"
repl1="$(free_port)"
repl2="$(free_port)"
printf 'rails-test-secret\n' >"$work/repl-secret"
"$bin" -p "$port" --node-id 1 --repl-port "$repl1" --peer "127.0.0.1:$repl2" --repl-secret-file "$work/repl-secret" & pids+=($!)
"$bin" -p "$port2" --node-id 2 --repl-port "$repl2" --peer "127.0.0.1:$repl1" --repl-secret-file "$work/repl-secret" & pids+=($!)
wait_ready "$port"
wait_ready "$port2"
run_suite "ferrited pair, node 1" "$port"

if [ ${#failed[@]} -gt 0 ]; then
  printf 'failed: %s\n' "${failed[@]}" >&2
  exit 1
fi
echo "all passes succeeded"
