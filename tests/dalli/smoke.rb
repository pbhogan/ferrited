# frozen_string_literal: true

require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "dalli", "~> 5.0"
  gem "minitest"
end

require "minitest/autorun"

class DalliSmokeTest < Minitest::Test
  def setup
    @cache = Dalli::Client.new(ENV.fetch("FERRITED_ADDR"))
    @cache.flush
  end

  def teardown
    @cache.close
  end

  def test_set_and_get_objects
    assert_nil @cache.get("missing")
    @cache.set("user", { "name" => "Ada", "roles" => %w[admin dev] })
    assert_equal({ "name" => "Ada", "roles" => %w[admin dev] }, @cache.get("user"))
  end

  def test_get_multi
    @cache.set("a", 1)
    @cache.set("c", 3)
    assert_equal({ "a" => 1, "c" => 3 }, @cache.get_multi("a", "b", "c"))
  end

  def test_add_and_replace
    assert @cache.add("k", "first")
    refute @cache.add("k", "second")
    assert @cache.replace("k", "third")
    refute @cache.replace("nope", "x")
    assert_equal "third", @cache.get("k")
  end

  def test_delete
    @cache.set("k", "v")
    assert @cache.delete("k")
    refute @cache.delete("k")
    assert_nil @cache.get("k")
  end

  def test_counters
    assert_nil @cache.incr("missing")
    assert_equal 10, @cache.incr("counter", 1, nil, 10)
    assert_equal 15, @cache.incr("counter", 5)
    assert_equal 12, @cache.decr("counter", 3)
    assert_equal 0, @cache.decr("counter", 100)
  end

  def test_compare_and_swap
    @cache.set("k", 1)
    assert @cache.cas("k") { |value| value + 1 }
    assert_equal 2, @cache.get("k")
    assert_nil @cache.cas("missing") { |value| value }
  end

  def test_fetch
    calls = 0
    2.times { assert_equal "computed", @cache.fetch("k") { calls += 1; "computed" } }
    assert_equal 1, calls
  end

  def test_expiry_and_touch
    @cache.set("short", "v", 1)
    @cache.set("kept", "v", 1)
    assert @cache.touch("kept", 60)
    assert_equal "v", @cache.gat("short", 1)
    sleep 2.1
    assert_nil @cache.get("short")
    assert_equal "v", @cache.get("kept")
  end

  def test_large_value
    payload = "x" * 500_000
    @cache.set("big", payload)
    assert_equal payload, @cache.get("big")
  end

  def test_quiet_pipeline
    @cache.quiet do
      100.times { |index| @cache.set("q#{index}", index) }
    end
    assert_equal 99, @cache.get("q99")
    assert_equal 100, @cache.get_multi(*(0...100).map { |index| "q#{index}" }).size
  end

  def test_flush_version_and_stats
    @cache.set("k", "v")
    @cache.flush
    assert_nil @cache.get("k")
    assert_match(/ferrited/, @cache.version.values.first)
    stats = @cache.stats.values.first
    assert stats.key?("curr_items")
  end
end
