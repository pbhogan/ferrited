# frozen_string_literal: true

# Dalli failover between two replicating nodes: the survivor serves every key, and a returning
# node serves only once caught up, so Dalli never reads stale data.
#
# Run by tests/dalli/run.sh, which sets FERRITED_BIN. Picks its own free ports.

require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "dalli", "~> 5.0"
  gem "minitest"
end

require "fileutils"
require "minitest/autorun"
require "socket"
require "tmpdir"

def monotonic
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

# Ports that were free a moment ago, all different.
def free_ports(count)
  servers = Array.new(count) { TCPServer.new("127.0.0.1", 0) }
  servers.map { |server| server.addr[1] }
ensure
  servers&.each(&:close)
end

# One ferrited process, which can be killed and started again on the same ports and data.
class Node
  # Live pids, killed however the script exits.
  RUNNING = []

  attr_reader :port

  def initialize(id:, port:, repl_port:, peer_repl_port:, dir:)
    FileUtils.mkdir_p(dir)
    @port = port
    @log = File.join(dir, "stderr.log")
    @args = [
      ENV.fetch("FERRITED_BIN"), "-p", port.to_s, "--node-id", id.to_s,
      "--repl-port", repl_port.to_s, "--peer", "127.0.0.1:#{peer_repl_port}",
      "--data-dir", File.join(dir, "data"), "--fsync-interval-ms", "50"
    ]
  end

  def start
    @pid = Process.spawn(*@args, out: File::NULL, err: [@log, "a"])
    RUNNING << @pid
  end

  def kill
    return unless @pid

    Node.kill(@pid)
    @pid = nil
  end

  def serving?
    TCPSocket.new("127.0.0.1", @port).close
    true
  rescue Errno::ECONNREFUSED, Errno::ECONNRESET
    false
  end

  def log
    File.exist?(@log) ? File.read(@log) : ""
  end

  def self.kill(pid)
    Process.kill("KILL", pid)
    Process.wait(pid)
  rescue Errno::ESRCH, Errno::ECHILD
    nil
  ensure
    RUNNING.delete(pid)
  end

  def self.kill_all
    RUNNING.dup.each { |pid| kill(pid) }
  end
end

# Minitest runs tests from at_exit, so an at_exit hook here would run first. Kill the nodes
# after the run and on a signal instead.

# Removed by teardown or, on a signal, here.
DATA_DIRS = []

Minitest.after_run { Node.kill_all }
%w[INT TERM HUP].each do |signal|
  Signal.trap(signal) do
    Node.kill_all
    FileUtils.rm_rf(DATA_DIRS)
    exit 128 + Signal.list.fetch(signal)
  end
end

class DalliFailoverTest < Minitest::Test
  KEYS = (0...200).map { |index| "key#{index}" }

  def setup
    @dir = Dir.mktmpdir("ferrited-failover")
    DATA_DIRS << @dir
    a_port, a_repl, b_port, b_repl = free_ports(4)
    @a = Node.new(id: 1, port: a_port, repl_port: a_repl, peer_repl_port: b_repl,
                  dir: File.join(@dir, "a"))
    @b = Node.new(id: 2, port: b_port, repl_port: b_repl, peer_repl_port: a_repl,
                  dir: File.join(@dir, "b"))
    # A node serves only once linked with its peer, so start both before waiting.
    [@a, @b].each(&:start)
    [@a, @b].each { |node| wait_until("node on #{node.port} serves", node) { node.serving? } }
    # Dalli retries a down server only after down_retry_delay (30 s by default).
    @cache = Dalli::Client.new(["127.0.0.1:#{a_port}", "127.0.0.1:#{b_port}"],
                               down_retry_delay: 0.5)
  end

  def teardown
    @cache&.close
    [@a, @b].each { |node| node&.kill }
    FileUtils.rm_rf(DATA_DIRS.delete(@dir)) if @dir
  end

  def wait_until(what, node = nil, timeout: 30)
    deadline = monotonic + timeout
    until yield
      flunk "timed out waiting until #{what}\n#{node&.log}" if monotonic > deadline
      sleep 0.05
    end
  end

  # A client for one node alone, to see what that node holds.
  def only(node)
    Dalli::Client.new("127.0.0.1:#{node.port}")
  end

  def test_a_node_fails_over_and_rejoins_only_once_caught_up
    KEYS.each { |key| @cache.set(key, "before-#{key}") }
    [@a, @b].each do |node|
      client = only(node)
      wait_until("node on #{node.port} has every key", node) do
        client.get_multi(*KEYS).size == KEYS.size
      end
      client.close
    end

    @b.kill
    # Node 2's keys fail over to node 1, which has them all.
    KEYS.each { |key| assert_equal "before-#{key}", @cache.get(key), key }
    KEYS.each { |key| @cache.set(key, "during-#{key}") }

    @b.start
    # Node 2 refuses connections until caught up, so its first one sees every missed write.
    client = nil
    wait_until("node 2 serves again", @b) do
      client = only(@b)
      client.get(KEYS.first)
      true
    rescue Dalli::RingError, Dalli::NetworkError
      client.close
      false
    end
    KEYS.each { |key| assert_equal "during-#{key}", client.get(key), key }
    client.close
    assert_match(/not serving clients yet/, @b.log)
    assert_match(/caught up with node 1/, @b.log)
    # Once Dalli retries node 2, its keys are read there, and none is stale.
    sleep 1
    KEYS.each { |key| assert_equal "during-#{key}", @cache.get(key), key }
  end
end
