# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'stringio'
require 'tmpdir'
require 'zlib'
require_relative 'test__helper'

require_relative '../lib/baza-rb'

# Encoded ranges belong to one representation, not independent gzip files.
class TestGzipRanges < Minitest::Test
  def setup
    WebMock.reset!
    WebMock.disable_net_connect!
  end

  def test_reassembles_encoded_ranges_before_decoding
    data = "payload\x00\xff".b * 2000
    encoded = gzip(data)
    [5, encoded.bytesize / 2, encoded.bytesize - 4].each do |split|
      [encoded.bytesize, '*'].each do |total|
        WebMock.reset!
        ranges(encoded, split, total:)
        downloaded do |file|
          client.durable_load(42, file)
          assert_equal(data, File.binread(file))
        end
        assert_requested(:get, endpoint, times: 2)
      end
    end
  end

  def test_accepts_empty_encoded_representation
    ranges(gzip(''), 10)
    downloaded do |file|
      client.durable_load(42, file)
      assert_empty(File.binread(file))
    end
    assert_requested(:get, endpoint, times: 2)
  end

  def test_accepts_exact_uncompressed_limit
    data = 'x' * BazaRb::Compress::LIMIT_UNCOMPRESSED
    encoded = gzip(data)
    ranges(encoded, encoded.bytesize / 2)
    downloaded do |file|
      client.durable_load(42, file)
      assert_equal(data, File.binread(file))
    end
    assert_requested(:get, endpoint, times: 2)
  end

  def test_rejects_combined_uncompressed_overflow
    encoded = gzip('x' * (BazaRb::Compress::LIMIT_UNCOMPRESSED + 1))
    ranges(encoded, encoded.bytesize / 2)
    downloaded do |file|
      assert_includes(
        assert_raises(BazaRb::BadCompression) { client.durable_load(42, file) }.message,
        'exceeds limit'
      )
    end
    assert_requested(:get, endpoint, times: 2)
  end

  def test_rejects_encoding_change
    encoded = gzip('some data' * 100)
    [%w[gzip identity], %w[identity gzip]].each do |codings|
      WebMock.reset!
      ranges(encoded, 5, codings:)
      downloaded do |file|
        assert_includes(
          assert_raises(BazaRb::BadCompression) { client.durable_load(42, file) }.message,
          'Content-Encoding changed'
        )
      end
      assert_requested(:get, endpoint, times: 2)
    end
  end

  def test_checks_final_gzip_footer
    encoded = gzip('some data' * 100)
    encoded[-8, 4] = 'bad!'
    ranges(encoded, 5)
    downloaded do |file|
      assert_raises(BazaRb::BadCompression) { client.durable_load(42, file) }
    end
    assert_requested(:get, endpoint, times: 2)
  end

  private

  def endpoint
    'https://example.org/durables/42'
  end

  def client
    BazaRb.new('example.org', 443, 'test-only', retries: 0, pause: 0)
  end

  def gzip(data)
    io = StringIO.new
    writer = Zlib::GzipWriter.new(io)
    writer.write(data)
    writer.close
    io.string
  end

  def ranges(encoded, split, total: encoded.bytesize, codings: %w[GZip gzip])
    parts = [[0, split - 1, total], [split, encoded.bytesize - 1, encoded.bytesize]]
    parts.each_with_index do |(first, last, size), index|
      body = encoded.byteslice(first..last)
      stub_request(:get, endpoint).with(headers: { 'Range' => "bytes=#{first}-" }).to_return(
        status: 206,
        body:,
        headers: {
          'Content-Encoding' => codings[index],
          'Content-Length' => body.bytesize.to_s,
          'Content-Range' => "bytes #{first}-#{last}/#{size}"
        }
      )
    end
  end

  def downloaded
    Dir.mktmpdir { |dir| yield(File.join(dir, 'result.bin')) }
  end
end
