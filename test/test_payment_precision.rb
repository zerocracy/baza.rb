# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'bigdecimal'
require 'loog'
require 'uri'
require_relative 'test__helper'

require_relative '../lib/baza-rb'

class TestPaymentPrecision < Minitest::Test
  def test_transfer_rejects_amount_that_becomes_zero
    payment('/account/transfer')
    [0.0000001, BigDecimal('0.0000001')].each do |amount|
      assert_equal(
        'The "amount" must be positive',
        assert_raises(RuntimeError) { client.transfer('alice', amount, 'test') }.message
      )
    end
    assert_not_requested(:post, 'https://example.org/account/transfer')
  end

  def test_fee_rejects_amount_that_becomes_zero
    payment('/account/fee')
    [0.0000001, BigDecimal('0.0000001')].each do |amount|
      assert_equal(
        'The "amount" must be positive',
        assert_raises(RuntimeError) { client.fee('unknown', amount, 'test', 42) }.message
      )
    end
    assert_not_requested(:post, 'https://example.org/account/fee')
  end

  def test_transfer_truncates_both_types
    amounts('/account/transfer') { |amount| client.transfer('alice', amount, 'test') }
  end

  def test_fee_truncates_both_types
    amounts('/account/fee') { |amount| client.fee('unknown', amount, 'test', 42) }
  end

  private

  def payment(path)
    stub_request(:get, 'https://example.org/csrf').to_return(body: 'csrf')
    stub_request(:post, "https://example.org#{path}")
      .to_return(status: 302, headers: { 'X-Zerocracy-ReceiptId' => '42' })
  end

  def client
    WebMock.disable_net_connect!
    BazaRb.new('example.org', 443, '000', loog: Loog::NULL, retries: 0, pause: 0)
  end

  def amounts(path)
    stub_request(:get, 'https://example.org/csrf').to_return(body: 'csrf')
    [
      ['1.23456789', '1.234567', '1.234567'],
      ['0.000001', '0.000001', '0.000001'],
      ['1.25', '1.250000', '1.25']
    ].each do |input, floated, decimal|
      [[input.to_f, floated], [BigDecimal(input), decimal]].each do |amount, expected|
        sent = nil
        stub_request(:post, "https://example.org#{path}").to_return do |request|
          sent = URI.decode_www_form(request.body).to_h.fetch('amount')
          { status: 302, headers: { 'X-Zerocracy-ReceiptId' => '42' } }
        end
        assert_equal(42, yield(amount))
        assert_equal(expected, sent)
      end
    end
  end
end
