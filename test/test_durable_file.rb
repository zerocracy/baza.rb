# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'tmpdir'
require_relative 'test__helper'

require_relative '../lib/baza-rb'

# Durable uploads must reject directories before entering the upload path.
class TestDurableFile < Minitest::Test
  def setup
    WebMock.reset!
    WebMock.disable_net_connect!
  end

  def test_rejects_directory
    Dir.mktmpdir do |dir|
      assert_includes(assert_raises(RuntimeError) { client.durable_save(42, dir) }.message, 'is not a regular file')
    end
    assert_not_requested(:put, endpoint)
    assert_not_requested(:get, 'https://example.org/csrf')
  end

  def test_accepts_empty_and_binary_files
    ['', "payload\x00\xff".b].each do |data|
      WebMock.reset!
      stub_request(:get, 'https://example.org/csrf').to_return(body: 'token')
      stub_request(:put, endpoint).with(body: data).to_return(status: 200)
      Dir.mktmpdir do |dir|
        file = File.join(dir, 'upload.bin')
        File.binwrite(file, data)
        client.durable_save(42, file)
        assert_requested(:put, endpoint, body: data, times: 1)
      end
    end
  end

  private

  def client
    BazaRb.new('example.org', 443, 'test-only', compress: false, retries: 0, pause: 0)
  end

  def endpoint
    'https://example.org/durables/42'
  end
end
