# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'tmpdir'
require 'webmock/minitest'
require_relative '../lib/baza-rb'
require_relative 'test__helper'

class TestDownloadEncoding < Minitest::Test
  def test_requests_identity_without_compression
    stub_request(:get, 'https://example.org/durables/42').to_return(status: 200, body: 'abcd')
    client = BazaRb.new('example.org', 443, 'test', compress: false, retries: 0)
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'sample.bin')
      client.durable_load(42, file)
      assert_equal('abcd', File.binread(file))
    end
    assert_requested(:get, 'https://example.org/durables/42', headers: { 'Accept-Encoding' => 'identity' })
  end

  def test_advertises_gzip_with_default_compression
    stub_request(:get, 'https://example.org/durables/42').to_return(status: 200, body: 'abcd')
    client = BazaRb.new('example.org', 443, 'test', retries: 0)
    Dir.mktmpdir { |dir| client.durable_load(42, File.join(dir, 'sample.bin')) }
    assert_requested(:get, 'https://example.org/durables/42', headers: { 'Accept-Encoding' => 'gzip' })
  end
end
