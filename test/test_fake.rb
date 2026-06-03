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
    refute_nil(BazaRb::Fake.new.whoami)
  end

  def test_balance
    refute_nil(BazaRb::Fake.new.balance)
  end

  def test_pull
    refute_nil(BazaRb::Fake.new.pull(42))
  end

  def test_push
    assert_equal(42, BazaRb::Fake.new.push('test-job', 'test-data', []))
  end

  def test_push_accepts_chunk_size_kwarg
    assert_equal(42, BazaRb::Fake.new.push('test-job', 'test-data', [], chunk_size: 1024))
  end

  def test_push_raises_when_data_is_nil
    assert_equal(
      'The "data" of the job is nil',
      assert_raises(RuntimeError) { BazaRb::Fake.new.push('test-job', nil, []) }.message
    )
  end

  def test_finished
    assert(BazaRb::Fake.new.finished?(42))
  end

  def test_stdout
    assert_equal('Fake stdout output', BazaRb::Fake.new.stdout(42))
  end

  def test_exit_code
    assert_equal(0, BazaRb::Fake.new.exit_code(42))
  end

  def test_verified
    assert_equal('fake-verdict', BazaRb::Fake.new.verified(42))
  end

  def test_lock_unlock
    baza = BazaRb::Fake.new
    baza.lock('test-job', 'test-owner')
    baza.unlock('test-job', 'test-owner')
  end

  def test_lock_unlock_accepts_any_non_empty_owner
    baza = BazaRb::Fake.new
    baza.lock('test-job', 'Jeff Lebowski')
    baza.unlock('test-job', 'jeff@example.com')
  end

  def test_recent
    assert_equal(42, BazaRb::Fake.new.recent('test-job'))
  end

  def test_name_exists
    assert(BazaRb::Fake.new.name_exists?('test-job'))
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

  def test_durable_lock_unlock_any_owner
    baza = BazaRb::Fake.new
    baza.durable_lock(42, 'Jeff Lebowski')
    baza.durable_unlock(42, 'jeff@example.com')
  end

  def test_durable_save_accepts_chunk_size_kwarg
    baza = BazaRb::Fake.new
    Dir.mktmpdir do |tmp|
      f = File.join(tmp, 'test.bin')
      File.write(f, 'hello')
      baza.durable_save(42, f, chunk_size: 1024)
    end
  end

  def test_durable_load_accepts_nonexistent_target
    baza = BazaRb::Fake.new
    Dir.mktmpdir do |tmp|
      target = File.join(tmp, 'not-yet-written.bin')
      refute_path_exists(target)
      baza.durable_load(42, target)
    end
  end

  def test_durable_find_accepts_nonexistent_file_name
    assert_equal(42, BazaRb::Fake.new.durable_find('test-job', 'remote.bin'))
  end

  def test_durable_find_rejects_nil_file_name
    assert_equal(
      'The "file" is nil',
      assert_raises(RuntimeError) { BazaRb::Fake.new.durable_find('test-job', nil) }.message
    )
  end

  def test_durable_find_rejects_empty_file_name
    assert_equal(
      'The "file" may not be empty',
      assert_raises(RuntimeError) { BazaRb::Fake.new.durable_find('test-job', '') }.message
    )
  end

  def test_durable_load_raises_when_file_is_nil
    assert_equal(
      'The "file" of the durable is nil',
      assert_raises(RuntimeError) { BazaRb::Fake.new.durable_load(42, nil) }.message
    )
  end

  def test_transfer
    assert_equal(42, BazaRb::Fake.new.transfer('recipient', 1.0, 'test-payment'))
  end

  def test_transfer_rejects_multiline_recipient
    assert_equal(
      'The recipient "recipient\nbad value" is not valid',
      assert_raises(RuntimeError) { BazaRb::Fake.new.transfer("recipient\nbad value", 1.0, 'test-payment') }.message
    )
  end

  def test_transfer_accepts_job_kwarg
    assert_equal(42, BazaRb::Fake.new.transfer('recipient', 1.0, 'test-payment', job: 42))
  end

  def test_transfer_rejects_unknown_keyword
    assert_raises(ArgumentError) do
      BazaRb::Fake.new.transfer('recipient', 1.0, 'test-payment', other: 42)
    end
  end

  def test_transfer_rejects_invalid_job
    assert_equal(
      'The ID must be an Integer',
      assert_raises(RuntimeError) { BazaRb::Fake.new.transfer('recipient', 1.0, 'test-payment', job: '42') }.message
    )
  end

  def test_transfer_rejects_nil_recipient
    assert_equal(
      'The "recipient" is nil',
      assert_raises(RuntimeError) { BazaRb::Fake.new.transfer(nil, 1.0, 'test-payment') }.message
    )
  end

  def test_transfer_rejects_nil_amount
    assert_equal(
      'The "amount" is nil',
      assert_raises(RuntimeError) { BazaRb::Fake.new.transfer('recipient', nil, 'test-payment') }.message
    )
  end

  def test_transfer_rejects_non_float_amount
    assert_equal(
      'The "amount" must be Float',
      assert_raises(RuntimeError) { BazaRb::Fake.new.transfer('recipient', 1, 'test-payment') }.message
    )
  end

  def test_transfer_rejects_negative_amount
    assert_equal(
      'The "amount" must be positive',
      assert_raises(RuntimeError) { BazaRb::Fake.new.transfer('recipient', -1.0, 'test-payment') }.message
    )
  end

  def test_transfer_rejects_nil_summary
    assert_equal(
      'The "summary" is nil',
      assert_raises(RuntimeError) { BazaRb::Fake.new.transfer('recipient', 1.0, nil) }.message
    )
  end

  def test_pays_fee
    assert_equal(42, BazaRb::Fake.new.fee('unknown', 43.0, 'for fun', 44))
  end

  def test_fee_raises_when_amount_is_nil
    assert_equal(
      'The "amount" is nil',
      assert_raises(RuntimeError) { BazaRb::Fake.new.fee('unknown', nil, 'for fun', 44) }.message
    )
  end

  def test_fee_raises_when_amount_is_not_float
    assert_equal(
      'The "amount" must be Float',
      assert_raises(RuntimeError) { BazaRb::Fake.new.fee('unknown', 43, 'for fun', 44) }.message
    )
  end

  def test_fee_raises_when_amount_is_not_positive
    assert_equal(
      'The "amount" must be positive',
      assert_raises(RuntimeError) { BazaRb::Fake.new.fee('unknown', 0.0, 'for fun', 44) }.message
    )
  end

  def test_enter
    assert_equal('test-result', BazaRb::Fake.new.enter('test-job', 'test-badge', 'test-reason', 42) { 'test-result' })
  end

  def test_enter_rejects_multiline_name_and_badge
    baza = BazaRb::Fake.new
    assert_equal(
      'The name "test-job\nBAD" is not valid',
      assert_raises(RuntimeError) { baza.enter("test-job\nBAD", 'test-badge', 'test-reason', 42) { 'ignored' } }.message
    )
    assert_equal(
      "The badge 'test-badge\nBAD' is not valid",
      assert_raises(RuntimeError) { baza.enter('test-job', "test-badge\nBAD", 'test-reason', 42) { 'ignored' } }.message
    )
  end

  def test_durable_lock_rejects_multiline_owner
    assert_equal(
      'The owner "test-owner\nbad value" is not valid',
      assert_raises(RuntimeError) { BazaRb::Fake.new.durable_lock(42, "test-owner\nbad value") }.message
    )
  end

  def test_csrf
    assert_equal('fake-csrf-token', BazaRb::Fake.new.csrf)
  end
end
