# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'time'
require 'webmock/minitest'
require_relative '../lib/baza-rb'
require_relative 'test__helper'

class TestRetryAfter < Minitest::Test
  def test_waits_for_the_requested_retry_delay
    assert_equal([12], waits(429, '12'))
  end

  def test_honors_the_retry_date
    deadline = Time.at(Time.now.to_i + 600).utc
    seconds = waits(503, deadline.httpdate).first
    assert_operator(seconds, :>=, (deadline - Time.now).ceil)
    assert_operator(seconds, :<=, 600)
  end

  def test_keeps_local_backoff_for_an_unreadable_value
    assert_equal([2], waits(429, 'soon'))
  end

  private

  def waits(code, header)
    client = BazaRb.new('example.org', 443, 'test', retries: 1, pause: 1)
    delays = []
    stub_request(:get, 'https://example.org/whoami')
      .to_return(status: code, headers: { 'Retry-After' => header }).then.to_return(status: 200, body: 'tester')
    client.define_singleton_method(:sleep) { |seconds| delays << seconds }
    client.whoami
    delays
  end
end
