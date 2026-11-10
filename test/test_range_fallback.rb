# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'loog'
require 'tmpdir'
require_relative 'test__helper'

require_relative '../lib/baza-rb'

class TestRangeFallback < Minitest::Test
  def test_full_response_replaces_partial_download
    ["abcd\x00\xFFefgh".b, 'xy', ''].each do |body|
      with_partial do |file|
        stub_request(:get, 'https://example.org/durables/42')
          .with(headers: { 'Range' => 'bytes=4-' })
          .to_return(status: 200, body:)
        client.durable_load(42, file)
        assert_equal(body, File.binread(file))
      end
    end
  end

  def test_appends_partial_response
    with_partial do |file|
      stub_request(:get, 'https://example.org/durables/42')
        .with(headers: { 'Range' => 'bytes=4-' })
        .to_return(status: 206, body: 'efgh', headers: { 'Content-Range' => 'bytes 4-7/8' })
      client.durable_load(42, file)
      assert_equal('abcdefgh', File.binread(file))
    end
  end

  private

  def client
    BazaRb.new('example.org', 443, '000', loog: Loog::NULL, retries: 0, pause: 0)
  end

  def with_partial
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org/durables/42')
      .with(headers: { 'Range' => 'bytes=0-' })
      .to_return(status: 206, body: 'abcd', headers: { 'Content-Range' => 'bytes 0-3/8', 'Content-Length' => '4' })
    Dir.mktmpdir { |dir| yield(File.join(dir, 'download.bin')) }
  end
end
