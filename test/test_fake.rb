# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require_relative 'test__helper'
require_relative '../lib/baza-rb/fake'

# Test fake object.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024-2026 Yegor Bugayenko
# License:: MIT
class TestFake < Minitest::Test
  def test_whoami
    baza = BazaRb::Fake.new
    nick = baza.whoami
    refute_nil(nick)
  end

  def test_balance
    baza = BazaRb::Fake.new
    z = baza.balance
    refute_nil(z)
  end

  def test_pull
    baza = BazaRb::Fake.new
    bin = baza.pull(42)
    refute_nil(bin)
  end

  def test_push
    baza = BazaRb::Fake.new
    id = baza.push('test-job', 'test-data', [])
    assert_equal(42, id)
  end

  def test_finished
    baza = BazaRb::Fake.new
    assert(baza.finished?(42))
  end

  def test_stdout
    baza = BazaRb::Fake.new
    output = baza.stdout(42)
    assert_equal('Fake stdout output', output)
  end

  def test_exit_code
    baza = BazaRb::Fake.new
    code = baza.exit_code(42)
    assert_equal(0, code)
  end

  def test_verified
    baza = BazaRb::Fake.new
    verdict = baza.verified(42)
    assert_equal('fake-verdict', verdict)
  end

  def test_lock_unlock
    baza = BazaRb::Fake.new
    baza.lock('test-job', 'test-owner')
    baza.unlock('test-job', 'test-owner')
  end

  def test_recent
    baza = BazaRb::Fake.new
    id = baza.recent('test-job')
    assert_equal(42, id)
  end

  def test_name_exists
    baza = BazaRb::Fake.new
    assert(baza.name_exists?('test-job'))
  end

  def test_durable_operations
    baza = BazaRb::Fake.new
    Dir.mktmpdir do |tmp|
      f = File.join(tmp, 'test.bin')
      File.write(f, 'hello')
      baza.durable_place('test-job', f)
      baza.durable_save(42, f)
      baza.durable_load(42, f)
      baza.durable_lock(42, 'test-owner')
      baza.durable_unlock(42, 'test-owner')
    end
  end

  def test_durable_find_accepts_file_name
    baza = BazaRb::Fake.new
    assert_equal(42, baza.durable_find('test-job', 'missing.txt'))
  end

  def test_durable_find_rejects_empty_file_name
    baza = BazaRb::Fake.new
    assert_raises(StandardError) { baza.durable_find('test-job', '') }
  end

  def test_transfer
    baza = BazaRb::Fake.new
    receipt_id = baza.transfer('recipient', 1.0, 'test-payment')
    assert_equal(42, receipt_id)
  end

  def test_pays_fee
    baza = BazaRb::Fake.new
    receipt_id = baza.fee('unknown', 43.0, 'for fun', 44)
    assert_equal(42, receipt_id)
  end

  def test_enter
    baza = BazaRb::Fake.new
    result =
      baza.enter('test-job', 'test-badge', 'test-reason', 42) do
        'test-result'
      end
    assert_equal('test-result', result)
  end

  def test_csrf
    baza = BazaRb::Fake.new
    token = baza.csrf
    assert_equal('fake-csrf-token', token)
  end
end
