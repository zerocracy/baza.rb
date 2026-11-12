# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'factbase'
require 'loog'
require 'net/http'
require 'online'
require 'qbash'
require 'random-port'
require 'securerandom'
require 'shellwords'
require 'socket'
require 'stringio'
require 'uri'
require 'wait_for'
require 'webrick'
require_relative 'test__helper'

require_relative '../lib/baza-rb'

# Test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024-2026 Yegor Bugayenko
# License:: MIT
class TestBazaLive < Minitest::Test
  TOKEN = 'ZRCY-00000000-0000-0000-0000-000000000000'

  HOST = 'api.zerocracy.com'

  PORT = 443

  LIVE = BazaRb.new(HOST, PORT, TOKEN, loog: Loog::NULL)

  def test_live_full_cycle
    WebMock.enable_net_connect!
    skip('We are offline') unless connected?
    fb = Factbase.new
    fb.insert.foo = 'test-' * 10_000
    fb.insert
    n = fake_name
    LIVE.push(n, fb.export, [])
    assert(LIVE.name_exists?(n))
    assert_predicate(LIVE.recent(n), :positive?)
    id = LIVE.recent(n)
    assert(
      wait_for(15 * 60) do
        sleep(5)
        LIVE.finished?(id)
      end,
      "Job ##{id} (#{n.inspect}) did not finish in #{(Time.now - Time.now).round}s at #{HOST}, " \
      'which most probably means it got stuck on the Zerocracy platform and never completed'
    )
    refute_nil(LIVE.pull(id))
    refute_nil(LIVE.stdout(id))
    refute_nil(LIVE.exit_code(id))
    refute_nil(LIVE.verified(id))
    owner = 'baza.rb testing'
    refute_nil(LIVE.lock(n, owner))
    refute_nil(LIVE.unlock(n, owner))
  end

  def test_live_whoami
    WebMock.enable_net_connect!
    skip('We are offline') unless connected?
    refute_nil(LIVE.whoami)
  end

  def test_live_balance
    WebMock.enable_net_connect!
    skip('We are offline') unless connected?
    z = LIVE.balance
    refute_nil(z)
    assert(z.to_f)
  end

  def test_live_fee_payment
    WebMock.enable_net_connect!
    skip('We are offline') unless connected?
    refute_nil(LIVE.fee('unknown', 0.007, 'just for fun', 777))
  end

  def test_live_push_no_compression
    WebMock.enable_net_connect!
    skip('We are offline') unless connected?
    fb = Factbase.new
    fb.insert.foo = 'test-' * 10_000
    fb.insert
    BazaRb.new(HOST, PORT, TOKEN, compress: false).push(fake_name, fb.export, [])
  end

  def test_live_durable_lock_unlock
    WebMock.enable_net_connect!
    skip('We are offline') unless connected?
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'before.bin')
      before = 'hello, Джеф!' * 10
      File.binwrite(file, before)
      pname = fake_name
      refute(LIVE.durable_find(pname, File.basename(file)))
      id = LIVE.durable_place(pname, file)
      assert_equal(id, LIVE.durable_find(pname, File.basename(file)))
      owner = fake_name
      LIVE.durable_lock(id, owner)
      LIVE.durable_load(id, file)
      assert_equal(before, File.binread(file).force_encoding('UTF-8'))
      after = 'привет, друг!'
      File.binwrite(file, after)
      LIVE.durable_save(id, file)
      LIVE.durable_load(id, file)
      assert_equal(after, File.binread(file).force_encoding('UTF-8'))
      LIVE.durable_unlock(id, owner)
    end
  end

  def test_live_enter_valve
    WebMock.enable_net_connect!
    skip('We are offline') unless connected?
    r = 'something'
    n = fake_name
    badge = fake_name
    assert_equal(r, LIVE.enter(n, badge, 'no reason', nil) { r })
    assert_equal(r, LIVE.enter(n, badge, 'no reason', nil) { 'ignored' })
  end

  def test_get_csrf_token
    WebMock.enable_net_connect!
    skip('We are offline') unless connected?
    assert_operator(LIVE.csrf.length, :>, 10)
  end

  def fake_name
    "fake#{Time.now.to_i}#{SecureRandom.hex(9)}"
  end

  def connected?
    @connected ||= !ARGV.include?('--offline') && online?
  end
end
