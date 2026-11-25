#!/usr/bin/env bash
# Compares ferrited with memcached using memtier_benchmark on a preloaded key space.
#
# Usage: scripts/bench.sh [runs] [seconds]   (defaults: 3 runs, 20 seconds each)
#
# SERVERS (default: memcached ferrited ferrited-persist):
#   memcached, ferrited     each alone
#   ferrited-persist        with --data-dir in a fresh temporary directory
#   ferrited-repl           node 1 of a replicating pair (node 2 on the next ports)
#   ferrited-repl-secret    the same, with a shared --repl-secret-file (signed frames)
#   ferrited-base[-persist|-repl|-repl-secret]
#                           the same, running BASE_BIN, for A/B comparisons
# Replicated runs also report the peer's CPU per operation.
#
#   RATIO                           memtier set:get ratio (default 1:10)
#   WORKLOADS                       default: nopipe pipe16
#   SERVER_THREADS, CLIENT_THREADS  default: 4 each
#   OUT                             results directory (default: target/bench)
set -euo pipefail

runs=${1:-3}
seconds=${2:-20}
port=21311
keys=100000
size=512
server_threads=${SERVER_THREADS:-4}
client_threads=${CLIENT_THREADS:-4}
out=${OUT:-target/bench}
mkdir -p "$out"

cargo build --release -q

ratio=${RATIO:-1:10}

data_dir=
peer_pid=
pid=
secret_dir=

# Stops leftover servers and removes temporary directories on exit.
cleanup() {
	# A server may already be gone; a failed kill must not become the exit status.
	if [[ -n $peer_pid ]]; then kill $peer_pid 2>/dev/null || true; fi
	if [[ -n $pid ]]; then kill $pid 2>/dev/null || true; fi
	if [[ -n $data_dir ]]; then rm -rf "$data_dir"; fi
	if [[ -n $secret_dir ]]; then rm -rf "$secret_dir"; fi
	return 0
}
trap cleanup EXIT

# Starts binary $1 as node 2 on the next two ports, then as node 1 on $port, each the other's
# peer. With $2 set, both share a --repl-secret-file.
start_pair() {
	local secret=()
	if [[ -n $2 ]]; then
		secret_dir=$(mktemp -d)
		echo "bench secret" >"$secret_dir/secret"
		secret=(--repl-secret-file "$secret_dir/secret")
	fi
	"$1" -p $((port + 2)) -m 1024 -t $server_threads --node-id 2 \
		--repl-port $((port + 3)) --peer 127.0.0.1:$((port + 1)) ${secret[@]+"${secret[@]}"} >/dev/null 2>&1 &
	peer_pid=$!
	"$1" -p $port -m 1024 -t $server_threads --node-id 1 \
		--repl-port $((port + 1)) --peer 127.0.0.1:$((port + 3)) ${secret[@]+"${secret[@]}"} >/dev/null 2>&1 &
}

start_server() {
	case $1 in
		memcached) memcached -p $port -l 127.0.0.1 -U 0 -m 1024 -t $server_threads & ;;
		ferrited) ./target/release/ferrited -p $port -m 1024 -t $server_threads >/dev/null 2>&1 & ;;
		ferrited-base) "${BASE_BIN:?set BASE_BIN to the baseline binary}" -p $port -m 1024 -t $server_threads >/dev/null 2>&1 & ;;
		ferrited-persist)
			data_dir=$(mktemp -d)
			./target/release/ferrited -p $port -m 1024 -t $server_threads --data-dir "$data_dir" >/dev/null 2>&1 &
			;;
		ferrited-base-persist)
			data_dir=$(mktemp -d)
			"${BASE_BIN:?set BASE_BIN to the baseline binary}" -p $port -m 1024 -t $server_threads --data-dir "$data_dir" >/dev/null 2>&1 &
			;;
		ferrited-repl) start_pair ./target/release/ferrited "" ;;
		ferrited-repl-secret) start_pair ./target/release/ferrited secret ;;
		ferrited-base-repl) start_pair "${BASE_BIN:?set BASE_BIN to the baseline binary}" "" ;;
		ferrited-base-repl-secret) start_pair "${BASE_BIN:?set BASE_BIN to the baseline binary}" secret ;;
	esac
	pid=$!
	for _ in $(seq 50); do
		nc -z 127.0.0.1 $port 2>/dev/null && return
		sleep 0.1
	done
	echo "server $1 did not start" >&2
	exit 1
}

# Prints process $1's user + system CPU seconds. Linux's ps rounds to whole seconds, so read
# /proc there; macOS's ps prints M:SS.ss.
cpu_seconds() {
	if [[ -r /proc/$1/stat ]]; then
		# utime and stime: fields 12 and 13 after the parenthesized command name.
		local fields
		fields=$(sed 's/^.*) //' "/proc/$1/stat")
		awk -v hz="$(getconf CLK_TCK)" '{ printf "%.2f", ($12 + $13) / hz }' <<<"$fields"
	else
		ps -o time= -p "$1" | awk -F: '{ print $1 * 60 + $2 }'
	fi
}

memtier() {
	memtier_benchmark -s 127.0.0.1 -p $port -P memcache_text --hide-histogram \
		--data-size $size --key-minimum 1 --key-maximum $keys "$@"
}

for run in $(seq "$runs"); do
	for workload in ${WORKLOADS:-nopipe pipe16}; do
		for server in ${SERVERS:-memcached ferrited ferrited-persist}; do
			start_server $server
			memtier --threads 1 --clients 1 --ratio 1:0 --key-pattern P:P --requests allkeys >/dev/null 2>&1
			case $workload in
				nopipe) extra=(--clients 50 --pipeline 1) ;;
				pipe16) extra=(--clients 25 --pipeline 16) ;;
			esac
			memtier --threads $client_threads "${extra[@]}" --ratio "$ratio" --key-pattern R:R \
				--test-time "$seconds" >"$out/$workload-$server-$run.txt" 2>&1
			cpu=$(cpu_seconds $pid)
			peer_cpu=
			if [[ -n $peer_pid ]]; then
				peer_cpu=$(cpu_seconds $peer_pid)
			fi
			kill $pid
			wait $pid 2>/dev/null || true
			if [[ -n $peer_pid ]]; then
				kill $peer_pid
				wait $peer_pid 2>/dev/null || true
				peer_pid=
			fi
			if [[ -n $data_dir ]]; then
				rm -rf "$data_dir"
				data_dir=
			fi
			if [[ -n $secret_dir ]]; then
				rm -rf "$secret_dir"
				secret_dir=
			fi
			totals=$(grep '^Totals' "$out/$workload-$server-$run.txt")
			ops=$(awk '{ print $2 }' <<<"$totals")
			# CPU per operation: less disturbed by competing load than throughput.
			us_per_op=$(awk -v cpu="$cpu" -v ops="$ops" -v t="$seconds" 'BEGIN { printf "%.2f", cpu * 1e6 / (ops * t) }')
			peer=
			if [[ -n $peer_cpu ]]; then
				# The peer's CPU includes the preload's sets, which it also applied.
				peer=", peer CPU $(awk -v cpu="$peer_cpu" -v ops="$ops" -v t="$seconds" 'BEGIN { printf "%.2f", cpu * 1e6 / (ops * t) }') us/op"
			fi
			echo "$workload $server run $run: ops/s $ops, server CPU ${us_per_op} us/op${peer}, p99 $(awk '{ print $7 }' <<<"$totals") ms"
		done
	done
done
