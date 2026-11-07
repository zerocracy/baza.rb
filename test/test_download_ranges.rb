# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'loog'
require 'stringio'
require 'zlib'
require_relative 'test__helper'

require_relative '../lib/baza-rb'

# A range must describe the bytes received before those bytes reach the file.
class TestDownloadRanges < Minitest::Test
  def setup
    WebMock.reset!
    WebMock.disable_net_connect!
  end

  def teardown
    WebMock.allow_net_connect!
  end

  def test_rejects_incomplete_slice
    ['', 'abc'].each do |body|
      stub_request(:get, endpoint).to_return(reply(body, '0-3/4'))
      with_file do |file|
        assert_includes(assert_raises(RuntimeError) { download(file) }.message, 'bytes 0-3/4')
        assert_empty(File.binread(file))
      end
    end
  end

  def test_rejects_oversized_slice
    stub_request(:get, endpoint).to_return(reply('abcde', '0-3/4'))
    with_file do |file|
      assert_includes(assert_raises(RuntimeError) { download(file) }.message, 'bytes 0-3/4')
      assert_empty(File.binread(file))
    end
  end

  def test_rejects_overlapping_second_slice
    stub_request(:get, endpoint).to_return(reply('abcd', '0-3/8'), reply('abcdefgh', '0-7/8'))
    with_file do |file|
      assert_includes(assert_raises(RuntimeError) { download(file) }.message, 'bytes 0-7/8')
      assert_equal('abcd', File.binread(file))
      assert_requested(:get, endpoint, times: 2)
    end
  end

  def test_rejects_gap_in_second_slice
    stub_request(:get, endpoint).to_return(reply('abcd', '0-3/8'), reply('fgh', '5-7/8'))
    with_file do |file|
      assert_includes(assert_raises(RuntimeError) { download(file) }.message, 'bytes 5-7/8')
      assert_equal('abcd', File.binread(file))
    end
  end

  def test_stops_unknown_length_repeated_range
    stub_request(:get, endpoint).to_return(
      reply('abcd', '0-3/*'), reply('abcd', '0-3/*'), { status: 200, body: 'escape' }
    )
    with_file do |file|
      assert_includes(assert_raises(RuntimeError) { download(file) }.message, 'bytes 0-3/*')
      assert_equal('abcd', File.binread(file))
      assert_requested(:get, endpoint, times: 2)
    end
  end

  def test_rejects_malformed_or_impossible_range
    ['x-3/4', '+0-3/4', '0x0-3/4', '3-2/4', '0-3/3', '0-3/0'].each do |range|
      stub_request(:get, endpoint).to_return(reply('abcd', range), { status: 200, body: 'escape' })
      with_file do |file|
        assert_includes(assert_raises(RuntimeError) { download(file) }.message, range)
        assert_empty(File.binread(file))
      end
    end
  end

  def test_reads_binary_with_unknown_total
    stub_request(:get, endpoint).with(headers: { 'Range' => 'bytes=0-' }).to_return(reply("\x00\xFF\xFE".b, '0-2/*'))
    stub_request(:get, endpoint).with(headers: { 'Range' => 'bytes=3-' }).to_return(reply("\x80\x00\x01".b, '3-5/6'))
    with_file do |file|
      download(file)
      assert_equal("\x00\xFF\xFE\x80\x00\x01".b, File.binread(file))
      assert_requested(:get, endpoint, times: 2)
    end
  end

  def test_checks_gzip_range_before_decoding
    payload = "gzip payload\x00\xFF".b
    stream = StringIO.new
    Zlib::GzipWriter.wrap(stream) { |writer| writer.write(payload) }
    compressed = stream.string
    response = reply(compressed, "0-#{compressed.bytesize - 1}/#{compressed.bytesize}")
    response[:headers]['Content-Encoding'] = 'gzip'
    stub_request(:get, endpoint).to_return(response)
    with_file do |file|
      download(file)
      assert_equal(payload, File.binread(file))
    end
  end

  private

  def endpoint
    'https://example.org/durables/42'
  end

  def reply(body, range)
    { status: 206, body:, headers: { 'Content-Range' => "bytes #{range}", 'Content-Length' => body.bytesize.to_s } }
  end

  def with_file
    Dir.mktmpdir do |dir|
      yield(File.join(dir, 'download.bin'))
    end
  end

  def download(file)
    BazaRb.new('example.org', 443, 'test-only', loog: Loog::NULL, compress: false, pause: 0, retries: 0).durable_load(
      42, file
    )
  end
end
