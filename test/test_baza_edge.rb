# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'factbase'
require 'loog'
require 'net/http'
require 'qbash'
require 'random-port'
require 'securerandom'
require 'shellwords'
require 'socket'
require 'stringio'
require 'uri'
require 'webrick'
require_relative 'test__helper'

require_relative '../lib/baza-rb'

# Edge case tests using WebMock for implementation-specific behavior.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024-2026 Yegor Bugayenko
# License:: MIT
class TestBazaRbEdge < Minitest::Test
  def test_durable_place
    WebMock.disable_net_connect!
    [fake_baza(compress: true), fake_baza(compress: false)].each do |baza|
      stub_request(:get, 'https://example.org/csrf').to_return(body: 'token')
      stub_request(:post, 'https://example.org/durable-place').to_return(
        status: 302, headers: { 'X-Zerocracy-DurableId' => '42' }
      )
      stub_request(:post, %r{https://example\.org/durables/42/lock}).to_return(status: 302)
      stub_request(:post, %r{https://example\.org/durables/42/unlock}).to_return(status: 302)
      stub_request(:put, 'https://example.org/durables/42')
        .with(headers: { 'X-Zerocracy-Chunk' => '0' })
        .to_return(status: 200)
      stub_request(:put, 'https://example.org/durables/42')
        .with(headers: { 'X-Zerocracy-Chunk' => '1' })
        .to_return(status: 200)
      stub_request(:put, 'https://example.org/durables/42')
        .with(headers: { 'X-Zerocracy-Chunk' => '2' })
        .to_return(status: 200)
      Dir.mktmpdir do |dir|
        file = File.join(dir, 'test.bin')
        File.binwrite(file, 'hello, world!')
        assert_equal(42, baza.durable_place('simple', file))
      end
    end
  end

  def test_real_http
    WebMock.enable_net_connect!
    assert_equal(
      "baza.rb #{BazaRb::VERSION}",
      with_http(200, 'yes') { |baza| baza.name_exists?('simple') }['user-agent']
    )
  end

  def test_push_with_meta
    WebMock.enable_net_connect!
    assert_equal(
      'Ym9vbSE= 0YXQtdC5IQ==',
      with_http(200, 'yes') do |baza|
        baza.push('simple', 'hello, world!', ['boom!', 'хей!'])
      end['x-zerocracy-meta']
    )
  end

  def test_push_with_big_meta
    WebMock.enable_net_connect!
    assert(
      with_http(200, 'yes') do |baza|
        baza.push(
          'simple',
          'hello, world!',
          [
            'pages_url:https://zerocracy.github.io/zerocracy.html',
            'others:https://zerocracy.github.io/zerocracy.html',
            'duration:59595'
          ]
        )
      end['x-zerocracy-meta']
    )
  end

  def test_push_compressed_content
    WebMock.enable_net_connect!
    fb = Factbase.new
    fb.insert.foo = 'test-' * 10_000
    req =
      with_http(200, 'yes') do |baza|
        baza.push('simple', fb.export, %w[meta1 meta2 meta3])
      end
    assert_equal('application/zip', req.content_type)
    assert_equal('gzip', req['content-encoding'])
    assert_equal(fb.export, Zlib::GzipReader.zcat(StringIO.new(req.body)))
  end

  def test_push_compression_disabled
    WebMock.enable_net_connect!
    fb = Factbase.new
    fb.insert.foo = 'test-' * 10_000
    req =
      with_http(200, 'yes', compress: false) do |baza|
        baza.push('simple', fb.export, %w[meta1 meta2 meta3])
      end
    assert_equal('application/octet-stream', req.content_type)
    assert_equal(fb.export, req.body)
  end

  def test_with_very_short_timeout
    WebMock.enable_net_connect!
    host = '127.0.0.1'
    RandomPort::Pool::SINGLETON.acquire do |port|
      server = TCPServer.new(host, port)
      t =
        Thread.new do
          socket = server.accept
          req = WEBrick::HTTPRequest.new(WEBrick::Config::HTTP)
          req.parse(socket)
          req.body
          sleep(0.1)
          socket.puts("HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc")
          socket.close
        end
      assert_includes(
        assert_raises(StandardError) do
          BazaRb.new(host, port, '0000', ssl: false, timeout: 0.01).push('x', 'y', [])
        end.message, 'timed out in'
      )
      t.terminate
      assert(t.join(1))
    end
  end

  def test_durable_load_in_chunks
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'loaded.txt')
      stub_request(:get, 'https://example.org:443/durables/42')
        .with(headers: { 'Range' => 'bytes=0-' })
        .to_return(status: 206, body: '', headers: { 'Content-Range' => 'bytes 0-0/*' })
      stub_request(:get, 'https://example.org:443/durables/42')
        .with(headers: { 'Range' => 'bytes=0-' })
        .to_return(status: 206, body: 'привет', headers: { 'Content-Range' => 'bytes 0-11/25' })
      stub_request(:get, 'https://example.org:443/durables/42')
        .with(headers: { 'Range' => 'bytes=12-' })
        .to_return(status: 206, body: " друг \xFF\xFE\x12", headers: { 'Content-Range' => 'bytes 12-24/25' })
      fake_baza.durable_load(42, file)
      assert_equal("привет друг \xFF\xFE\x12", File.read(file))
    end
  end

  def test_durable_load_rejects_range_without_hyphen
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'loaded.txt')
      stub_request(:get, 'https://example.org:443/durables/42')
        .with(headers: { 'Range' => 'bytes=0-' })
        .to_return(status: 206, body: 'x', headers: { 'Content-Range' => 'bytes 0/10' })
      assert_includes(
        assert_raises(RuntimeError) do
          fake_baza.durable_load(42, file)
        end.message, 'Range is not valid ("0")'
      )
    end
  end

  def test_durable_load_with_broken_compression
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'loaded.txt')
      stub_request(:get, 'https://example.org:443/durables/42').to_return(
        status: 200, body: 'this is not gzip!', headers: { 'Content-Encoding' => 'gzip' }
      )
      assert_raises(BazaRb::BadCompression) { fake_baza.durable_load(42, file) }
    end
  end

  def test_checked_with_internal_error
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org:443/test')
      .with(headers: { 'X-Zerocracy-Token' => '000' })
      .to_return(status: 500, headers: { 'X-Zerocracy-Failure' => 'Boom-500', 'X-Zerocracy-FailureMark' => 'mark-500' })
    error =
      assert_raises(BazaRb::ServerFailure) do
        fake_baza.__send__(
          :checked,
          Typhoeus.get('https://example.org:443/test', headers: { 'X-Zerocracy-Token' => '000' })
        )
      end
    assert_includes(error.message, 'Invalid response code #500')
    assert_includes(error.message, "most probably it's an internal error on the server")
    assert_includes(error.message, 'Boom-500')
    assert_includes(error.message, 'mark-500')
  end

  def test_checked_with_unavailable
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org:443/test')
      .with(headers: { 'X-Zerocracy-Token' => '000' })
      .to_return(
        status: 503,
        headers: {
          'X-Zerocracy-Failure' => 'Service unavailable',
          'X-Zerocracy-FailureMark' => 'mark-503'
        }
      )
    error =
      assert_raises(BazaRb::ServerFailure) do
        fake_baza.__send__(
          :checked,
          Typhoeus.get('https://example.org:443/test', headers: { 'X-Zerocracy-Token' => '000' })
        )
      end
    assert_includes(error.message, 'Invalid response code #503')
    assert_includes(error.message, "most probably it's an internal error on the server")
    assert_includes(error.message, 'Service unavailable')
    assert_includes(error.message, 'mark-503')
  end

  def test_checked_with_not_found
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org:443/test')
      .with(headers: { 'X-Zerocracy-Token' => '000' })
      .to_return(status: 404)
    error =
      assert_raises(BazaRb::ServerFailure) do
        fake_baza.__send__(
          :checked,
          Typhoeus.get('https://example.org:443/test', headers: { 'X-Zerocracy-Token' => '000' })
        )
      end
    assert_includes(error.message, 'Invalid response code #404')
    assert_includes(error.message, 'most probably you are trying to reach a wrong server')
  end

  def test_checked_with_zero_code
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org:443/test')
      .with(headers: { 'X-Zerocracy-Token' => '000' })
      .to_return(status: 0)
    error =
      assert_raises(BazaRb::ConnectionFailed) do
        fake_baza.__send__(
          :checked,
          Typhoeus.get('https://example.org:443/test', headers: { 'X-Zerocracy-Token' => '000' })
        )
      end
    assert_kind_of(BazaRb::TimedOut, error, 'ConnectionFailed must inherit from TimedOut so attempt retries it')
    assert_includes(error.message, 'Invalid response code #0')
    assert_includes(error.message, 'most likely a connection failure')
  end

  def test_push_without_compression
    WebMock.disable_net_connect!
    stub_request(:put, 'https://example.org:443/push/test')
      .with(
        headers: {
          'X-Zerocracy-Token' => '000',
          'Content-Type' => 'application/octet-stream',
          'Content-Length' => '4'
        },
        body: 'data'
      )
      .to_return(status: 200, body: '123')
    BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress: false).push('test', 'data', [])
  end

  def test_download_retries_on_busy_server
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'download.txt')
      attempts = 0
      stub_request(:get, 'https://example.org:443/file')
        .with(headers: { 'Range' => 'bytes=0-' })
        .to_return do |_request|
          attempts += 1
          if attempts < 2
            { status: 429, body: 'Too Many Requests', headers: {} }
          else
            { status: 200, body: 'success content', headers: {} }
          end
        end
      baza = BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress: false, timeout: 0.1, pause: 0)
      baza.__send__(:download, baza.__send__(:home).append('file'), file)
      assert_equal(2, attempts, 'Expected two HTTP calls due to 429 retries')
      assert_equal('success content', File.read(file))
    end
  end

  # Reproduces zerocracy/baza.rb#289: BazaRb#download never retries on
  # timeout because checked() is called outside attempt. After the fix,
  # a libcurl operation_timedout on the first GET re-raises BazaRb::TimedOut
  # from inside attempt, which retries up to @retries times.
  def test_download_retries_on_timeout
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'download.txt')
      stub_request(:get, 'https://example.org:443/file')
        .with(headers: { 'Range' => 'bytes=0-' })
        .to_timeout.then
        .to_return(status: 200, body: 'success content', headers: {})
      baza = BazaRb.new(
        'example.org', 443, '000',
        loog: Loog::NULL, compress: false, timeout: 0.1, retries: 2, pause: 0
      )
      baza.__send__(:download, baza.__send__(:home).append('file'), file)
      assert_equal('success content', File.read(file))
      assert_requested(:get, 'https://example.org:443/file', times: 2)
    end
  end

  def test_download_rejects_malformed_total_size
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'download.txt')
      stub_request(:get, 'https://example.org:443/file')
        .with(headers: { 'Range' => 'bytes=0-' })
        .to_return(status: 206, body: 'x', headers: { 'Content-Range' => 'bytes 0-0/*malformed' })
      assert_includes(
        assert_raises(RuntimeError) do
          fake_baza.__send__(:download, fake_baza.__send__(:home).append('file'), file)
        end.message,
        'Total size is not valid ("*malformed")'
      )
    end
  end

  def test_download_rejects_malformed_range_end
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'download.txt')
      stub_request(:get, 'https://example.org:443/file')
        .with(headers: { 'Range' => 'bytes=0-' })
        .to_return(status: 206, body: 'x', headers: { 'Content-Range' => "bytes 0-0\njunk/10" })
      assert_includes(
        assert_raises(RuntimeError) do
          fake_baza.__send__(:download, fake_baza.__send__(:home).append('file'), file)
        end.message,
        'Range is not valid'
      )
    end
  end

  def test_upload_retries_on_busy_server
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'upload.txt')
      File.write(file, 'test content')
      attempts = 0
      stub_request(:put, 'https://example.org:443/file')
        .to_return do |_request|
          attempts += 1
          if attempts < 2
            { status: 429, body: 'Too Many Requests' }
          else
            { status: 200, body: 'OK' }
          end
        end
      baza = BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress: false, timeout: 0.1, pause: 0)
      baza.__send__(:upload, baza.__send__(:home).append('file'), file)
      assert_equal(2, attempts, 'Expected 2 HTTP calls due to 429 retries')
    end
  end

  def test_post_retries_on_busy_server
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org:443/csrf').to_return(body: 'token')
    attempts = 0
    stub_request(:post, 'https://example.org:443/lock/simple')
      .to_return do |_request|
        attempts += 1
        if attempts < 2
          { status: 429, body: 'Too Many Requests' }
        else
          { status: 302, body: '' }
        end
      end
    BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress: false, timeout: 0.1, pause: 0).lock(
      'simple',
      'owner'
    )
    assert_equal(2, attempts, 'Expected 2 HTTP calls due to 429 retries on POST')
  end

  # Reproduces zerocracy/baza.rb#122: when an upstream proxy aborts an in-flight
  # request (e.g. nginx returns 499 "Client Closed Request" after a load-balancer
  # timeout), the failure is server-side from the client's point of view and
  # should be retried, just like 5xx responses.
  def test_upload_retries_on_client_close
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'upload.txt')
      File.write(file, 'test content')
      attempts = 0
      stub_request(:put, 'https://example.org:443/file')
        .to_return do |_request|
          attempts += 1
          if attempts < 2
            { status: 499, body: 'Client Closed Request' }
          else
            { status: 200, body: 'OK' }
          end
        end
      baza = BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress: false, timeout: 0.1, pause: 0)
      baza.__send__(:upload, baza.__send__(:home).append('file'), file)
      assert_equal(2, attempts, 'Expected 2 HTTP calls due to 499 retries')
    end
  end

  # Reproduces zerocracy/baza.rb#111: when libcurl reports a transport-level
  # failure such as CURLE_PARTIAL_FILE (HTTP code 0, Typhoeus return_code
  # :partial_file), BazaRb#checked must raise something that BazaRb#attempt
  # will retry, so an upload doesn't abort the whole pipeline on the first
  # transient failure.
  def test_checked_partial_file_is_retryable
    WebMock.disable_net_connect!
    fake = Typhoeus::Response.new(
      return_code: :partial_file,
      return_message: 'Transferred a partial file',
      code: 0,
      total_time: 0.01
    )
    fake.request = Typhoeus::Request.new('https://example.org:443/file', method: :put)
    error = assert_raises(BazaRb::ConnectionFailed) { fake_baza.__send__(:checked, fake) }
    assert_kind_of(BazaRb::TimedOut, error, 'must inherit from TimedOut so attempt retries it')
    assert_includes(error.message, 'r:partial_file')
    attempts = 0
    fast = BazaRb.new('example.org', 443, '000', loog: Loog::NULL, retries: 2, pause: 0)
    assert_equal(
      'simulated partial file',
      assert_raises(BazaRb::ConnectionFailed) do
        fast.__send__(:attempt) do
          attempts += 1
          raise(BazaRb::ConnectionFailed, 'simulated partial file')
        end
      end.message
    )
    assert_operator(attempts, :>, 1, 'attempt must retry on ConnectionFailed (it is a TimedOut)')
  end

  def test_durable_load_from_sinatra
    WebMock.enable_net_connect!
    Dir.mktmpdir do |dir|
      with_sinatra do |baza|
        file = File.join(dir, 'x.txt')
        baza.durable_load(42, file)
        assert_equal("Hello, \xFF\xFE\x12!", File.read(file))
      end
    end
  end

  def test_download_sticks_host
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'test.txt')
      host = 'example.org'
      other = 'server2.example.org'
      baza = BazaRb.new(host, 443, '000', loog: Loog::NULL, compress: false)
      stub_request(:get, "https://#{host}:443/file")
        .with(headers: { 'Range' => 'bytes=0-' })
        .to_return(
          status: 200,
          body: 'file content',
          headers: { 'X-Zerocracy-Host' => other }
        )
      baza.__send__(:download, baza.__send__(:home).append('file'), file)
      assert_equal('file content', File.read(file), 'File should be downloaded correctly')
      second = File.join(dir, 'test2.txt')
      stub_request(:get, "https://#{other}:443/second")
        .with(headers: { 'Range' => 'bytes=0-' })
        .to_return(
          status: 200,
          body: 'second file',
          headers: {}
        )
      baza.__send__(:download, baza.__send__(:home).append('second'), second)
      assert_equal('second file', File.read(second), 'Second request should go to new host')
    end
  end

  def test_download_switches_host_mid_range
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'chunked.txt')
      host = 'example.org'
      other = 'server2.example.org'
      baza = BazaRb.new(host, 443, '000', loog: Loog::NULL, compress: false)
      stub_request(:get, "https://#{host}:443/file")
        .with(headers: { 'Range' => 'bytes=0-' })
        .to_return(
          status: 206,
          body: 'first ',
          headers: {
            'X-Zerocracy-Host' => other,
            'Content-Range' => 'bytes 0-5/11'
          }
        )
      stub_request(:get, "https://#{other}:443/file")
        .with(headers: { 'Range' => 'bytes=6-' })
        .to_return(
          status: 200,
          body: 'chunk',
          headers: {}
        )
      baza.__send__(:download, baza.__send__(:home).append('file'), file)
      assert_equal('first chunk', File.read(file), 'All chunks should be downloaded')
    end
  end

  def test_upload_sticks_host
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'upload.txt')
      File.write(file, 'test data')
      host = 'example.org'
      other = 'server2.example.org'
      baza = BazaRb.new(host, 443, '000', loog: Loog::NULL, compress: false)
      stub_request(:put, "https://#{host}:443/file")
        .to_return(
          status: 200,
          body: 'OK',
          headers: { 'X-Zerocracy-Host' => other }
        )
      baza.__send__(:upload, baza.__send__(:home).append('file'), file)
      stub_request(:put, "https://#{other}:443/second").to_return(status: 200, body: 'OK')
      baza.__send__(:upload, baza.__send__(:home).append('second'), file)
    end
  end

  def test_lock_raises_when_owner_is_empty
    assert_equal(
      'The "owner" of the lock may not be empty',
      assert_raises(RuntimeError) { fake_baza.lock('pname', '') }.message
    )
  end

  def test_transfer_raises_when_amount_is_not_positive
    [0.0, -1.0, -0.000001].each do |amount|
      assert_equal(
        'The "amount" must be positive',
        assert_raises(RuntimeError) { fake_baza.transfer('jeff', amount, 'pay') }.message
      )
    end
  end

  def test_fee_raises_when_amount_is_not_positive
    [0.0, -1.0, -0.000001].each do |amount|
      assert_equal(
        'The "amount" must be positive',
        assert_raises(RuntimeError) { fake_baza.fee('unknown', amount, 'pay', 42) }.message
      )
    end
  end

  def test_pull_raises_when_id_is_not_integer
    assert_equal('The ID of the job must be an Integer', assert_raises(RuntimeError) { fake_baza.pull(42.5) }.message)
  end

  def test_finished_raises_when_id_is_not_integer
    assert_equal(
      'The ID of the job must be an Integer',
      assert_raises(RuntimeError) { fake_baza.finished?(42.5) }.message
    )
  end

  def test_stdout_raises_when_id_is_not_integer
    assert_equal(
      'The ID of the job must be an Integer',
      assert_raises(RuntimeError) { fake_baza.stdout(42.5) }.message
    )
  end

  def test_exit_code_raises_when_id_is_not_integer
    assert_equal(
      'The ID of the job must be an Integer',
      assert_raises(RuntimeError) { fake_baza.exit_code(42.5) }.message
    )
  end

  def test_verified_raises_when_id_is_not_integer
    assert_equal(
      'The ID of the job must be an Integer',
      assert_raises(RuntimeError) { fake_baza.verified(42.5) }.message
    )
  end

  def test_upload_switches_host_mid_chunks
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'large.txt')
      File.write(file, 'x' * 2_000_000)
      host = 'example.org'
      other = 'server2.example.org'
      baza = BazaRb.new(host, 443, '000', loog: Loog::NULL, compress: false)
      stub_request(:put, "https://#{host}:443/file")
        .with(headers: { 'X-Zerocracy-Chunk' => '0' })
        .to_return(
          status: 200,
          body: 'OK',
          headers: { 'X-Zerocracy-Host' => other }
        )
      stub_request(:put, "https://#{other}:443/file")
        .with(headers: { 'X-Zerocracy-Chunk' => '1' })
        .to_return(
          status: 200,
          body: 'OK',
          headers: {}
        )
      stub_request(:put, "https://#{other}:443/file")
        .with(headers: { 'X-Zerocracy-Chunk' => '2' })
        .to_return(
          status: 200,
          body: 'OK',
          headers: {}
        )
      baza.__send__(:upload, baza.__send__(:home).append('file'), file, {}, chunk_size: 1_000_000)
    end
  end

  # Reproduces zerocracy/baza.rb#109: when the server is rebooted in the
  # middle of a chunked upload, it loses the partial state and on the next
  # PUT replies "400 Bad Request" with an "Expecting chunk #N" hint in the
  # `X-Zerocracy-Flash` header. The client should restart the upload from
  # the chunk the server is asking for, instead of aborting the whole job.
  def test_upload_restarts_when_server_loses_chunk
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'large.txt')
      File.write(file, 'x' * 2_000_000)
      received = []
      rebooted = false
      stub_request(:put, 'https://example.org:443/file')
        .to_return do |request|
          chunk = request.headers['X-Zerocracy-Chunk']
          received << chunk
          if chunk == '1' && !rebooted
            rebooted = true
            {
              status: 400,
              headers: {
                'X-Zerocracy-Flash' => 'Expecting chunk #0 (0b are here), received #1'
              }
            }
          else
            { status: 200, body: 'OK' }
          end
        end
      baza = BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress: false, retries: 2, pause: 0)
      baza.__send__(:upload, baza.__send__(:home).append('file'), file, {}, chunk_size: 1_000_000)
      assert_equal(
        %w[0 1 0 1 2], received,
        'Expected the client to restart the upload from chunk #0 after the reboot, ' \
        'then re-send chunks 0 and 1 plus the empty terminating chunk #2'
      )
    end
  end

  # Covers the `raise if match.nil?` exit of the upload rescue block: when the
  # server replies "400 Bad Request" with a flash that does not match the
  # `Expecting chunk #N` hint (for example a generic `Server out of disk`),
  # `BazaRb#upload` must re-raise the original `BazaRb::ServerFailure` on the
  # first PUT instead of entering the rewind loop.
  def test_upload_raises_on_unmatched_flash
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'large.txt')
      File.write(file, 'x' * 2_000_000)
      received = []
      stub_request(:put, 'https://example.org:443/file')
        .to_return do |request|
          received << request.headers['X-Zerocracy-Chunk']
          {
            status: 400,
            headers: { 'X-Zerocracy-Flash' => 'Server out of disk' }
          }
        end
      baza = BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress: false, retries: 2, pause: 0)
      assert_match(
        /Server out of disk/,
        assert_raises(BazaRb::ServerFailure) do
          baza.__send__(:upload, baza.__send__(:home).append('file'), file, {}, chunk_size: 1_000_000)
        end.message
      )
      assert_equal(
        %w[0], received,
        'Expected the client to fail on the first PUT without rewinding ' \
        'when the X-Zerocracy-Flash header does not carry an "Expecting chunk #N" hint'
      )
    end
  end

  # Covers the `raise if rewinds > @retries` exit of the upload rescue block:
  # when the server keeps returning a matching `Expecting chunk #N` flash on
  # every PUT, the rewind loop must terminate after the rewind count exceeds
  # the `retries:` setting and re-raise the original `BazaRb::ServerFailure`,
  # so a stuck server does not spin the client forever.
  def test_upload_raises_when_rewinds_exceed_retries
    WebMock.disable_net_connect!
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'large.txt')
      File.write(file, 'x' * 2_000_000)
      received = []
      stub_request(:put, 'https://example.org:443/file')
        .to_return do |request|
          chunk = request.headers['X-Zerocracy-Chunk']
          received << chunk
          if chunk == '1'
            {
              status: 400,
              headers: {
                'X-Zerocracy-Flash' => 'Expecting chunk #0 (0b are here), received #1'
              }
            }
          else
            { status: 200, body: 'OK' }
          end
        end
      retries = 2
      baza = BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress: false, retries: retries, pause: 0)
      assert_match(
        /Expecting chunk #0/,
        assert_raises(BazaRb::ServerFailure) do
          baza.__send__(:upload, baza.__send__(:home).append('file'), file, {}, chunk_size: 1_000_000)
        end.message
      )
      assert_equal(
        retries + 1, received.count('1'),
        'Expected the rewind loop to stop after rewinds exceeds the retries: setting'
      )
    end
  end

  private

  def with_sinatra
    Dir.mktmpdir do |dir|
      app = File.join(dir, 'app.rb')
      File.write(
        app,
        "
        require 'rack'
        require 'sinatra'
        use Rack::Deflater
        get '/' do
          'I am alive'
        end
        get '/durables/42' do
          \"Hello, \\xFF\\xFE\\x12!\"
        end
        "
      )
      RandomPort::Pool::SINGLETON.acquire do |port|
        host = '127.0.0.1'
        qbash("bundle exec ruby #{Shellwords.escape(app)} -p #{port}", stdout: Loog::NULL, accept: nil) do
          loop do
            break if Typhoeus::Request.get("http://#{host}:#{port}").code == 200
            sleep(0.1)
          end
          yield(BazaRb.new(host, port, '0000-0000-0000', ssl: false))
        end
      end
    end
  end

  def with_http(code, response, opts = {})
    opts = { ssl: false, timeout: 1 }.merge(opts)
    WebMock.enable_net_connect!
    req = WEBrick::HTTPRequest.new(WEBrick::Config::HTTP)
    host = '127.0.0.1'
    RandomPort::Pool::SINGLETON.acquire do |port|
      server = TCPServer.new(host, port)
      t =
        Thread.new do
          socket = server.accept
          req.parse(socket)
          body = req.body
          len = req.header['content-length'].first.to_i
          if body.nil? || len == body.size
            socket.puts("HTTP/1.1 #{code} OK\r\nContent-Length: #{response.length}\r\n\r\n#{response}")
          else
            socket.puts("HTTP/1.1 400 Bad Request\r\n")
          end
          socket.close
        end
      yield(BazaRb.new(host, port, '0000', **opts))
      t.terminate
      assert(t.join(1))
    end
    req
  end

  def fake_baza(compress: true)
    BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress:)
  end
end
