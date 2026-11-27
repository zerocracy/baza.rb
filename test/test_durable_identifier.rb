# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'loog'
require 'tmpdir'
require_relative 'test__helper'

require_relative '../lib/baza-rb'

class TestDurableIdentifier < Minitest::Test
  def test_durable_identifier_header
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'payload.txt')
      File.write(file, 'payload')
      identifiers('/durable-place', 'X-Zerocracy-DurableId') { client.durable_place('test', file) }
    end
  end

  private

  def client
    BazaRb.new('example.org', 443, '000', loog: Loog::NULL, retries: 0, pause: 0)
  end

  def identifiers(path, header, &request)
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org/csrf').to_return(body: 'csrf')
    [nil, '', '0', '-1', '42junk', '1.5', '1_2', '0x10', ' 42 '].each do |value|
      stub_request(:post, "https://example.org#{path}").to_return(
        status: 302, headers: value.nil? ? {} : { header => value }
      )
      assert_equal(
        "Invalid #{header} response header: expected a positive decimal ID",
        assert_raises(RuntimeError, &request).message
      )
    end
    ['1', '42', '9' * 100].each do |value|
      stub_request(:post, "https://example.org#{path}").to_return(status: 302, headers: { header => value })
      assert_equal(Integer(value, 10), yield)
    end
  end
end
