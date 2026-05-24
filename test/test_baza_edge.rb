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
      stub_request(:post, %r{https://example\.org/durables/42/lock})
        .to_return(status: 302)
      stub_request(:post, %r{https://example\.org/durables/42/unlock})
        .to_return(status: 302)
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
    req =
      with_http_server(200, 'yes') do |baza|
        baza.name_exists?('simple')
      end
    assert_equal("baza.rb #{BazaRb::VERSION}", req['user-agent'])
  end

  def test_push_with_meta
    WebMock.enable_net_connect!
    req =
      with_http_server(200, 'yes') do |baza|
        baza.push('simple', 'hello, world!', ['boom!', 'хей!'])
      end
    assert_equal('Ym9vbSE= 0YXQtdC5IQ==', req['x-zerocracy-meta'])
  end

  def test_push_with_big_meta
    WebMock.enable_net_connect!
    req =
      with_http_server(200, 'yes') do |baza|
        baza.push(
          'simple',
          'hello, world!',
          [
            'pages_url:https://zerocracy.github.io/zerocracy.html',
            'others:https://zerocracy.github.io/zerocracy.html',
            'duration:59595'
          ]
        )
      end
    assert(req['x-zerocracy-meta'])
  end

  def test_push_compressed_content
    WebMock.enable_net_connect!
    fb = Factbase.new
    fb.insert.foo = 'test-' * 10_000
    req =
      with_http_server(200, 'yes') do |baza|
        baza.push('simple', fb.export, %w[meta1 meta2 meta3])
      end
    assert_equal('application/zip', req.content_type)
    assert_equal('gzip', req['content-encoding'])
    body = Zlib::GzipReader.zcat(StringIO.new(req.body))
    assert_equal(fb.export, body)
  end

  def test_push_compression_disabled
    WebMock.enable_net_connect!
    fb = Factbase.new
    fb.insert.foo = 'test-' * 10_000
    req =
      with_http_server(200, 'yes', compress: false) do |baza|
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
          sleep 0.1
          socket.puts "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc"
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

  def test_checked_with_500_error
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org:443/test')
      .with(headers: { 'X-Zerocracy-Token' => '000' })
      .to_return(status: 500, headers: { 'X-Zerocracy-Failure' => 'Boom-500', 'X-Zerocracy-FailureMark' => 'mark-500' })
    error =
      assert_raises(BazaRb::ServerFailure) do
        fake_baza.send(
          :checked,
          Typhoeus.get('https://example.org:443/test', headers: { 'X-Zerocracy-Token' => '000' })
        )
      end
    assert_includes(error.message, 'Invalid response code #500')
    assert_includes(error.message, "most probably it's an internal error on the server")
    assert_includes(error.message, 'Boom-500')
    assert_includes(error.message, 'mark-500')
  end

  def test_checked_with_503_error
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
        fake_baza.send(
          :checked,
          Typhoeus.get('https://example.org:443/test', headers: { 'X-Zerocracy-Token' => '000' })
        )
      end
    assert_includes(error.message, 'Invalid response code #503')
    assert_includes(error.message, "most probably it's an internal error on the server")
    assert_includes(error.message, 'Service unavailable')
    assert_includes(error.message, 'mark-503')
  end

  def test_checked_with_404_error
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org:443/test')
      .with(headers: { 'X-Zerocracy-Token' => '000' })
      .to_return(status: 404)
    error =
      assert_raises(BazaRb::ServerFailure) do
        fake_baza.send(
          :checked,
          Typhoeus.get('https://example.org:443/test', headers: { 'X-Zerocracy-Token' => '000' })
        )
      end
    assert_includes(error.message, 'Invalid response code #404')
    assert_includes(error.message, 'most probably you are trying to reach a wrong server')
  end

  def test_checked_with_0_error
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org:443/test')
      .with(headers: { 'X-Zerocracy-Token' => '000' })
      .to_return(status: 0)
    error =
      assert_raises(BazaRb::ConnectionFailed) do
        fake_baza.send(
          :checked,
          Typhoeus.get('https://example.org:443/test', headers: { 'X-Zerocracy-Token' => '000' })
        )
      end
    assert_kind_of(BazaRb::TimedOut, error, 'ConnectionFailed must inherit from TimedOut so retry_it retries it')
    assert_includes(error.message, 'Invalid response code #0')
    assert_includes(error.message, 'most likely a connection failure')
  end

  def test_push_without_compression
    WebMock.disable_net_connect!
    baza = BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress: false)
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
    baza.push('test', 'data', [])
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
      baza.send(:download, baza.send(:home).append('file'), file)
      assert_equal(2, attempts, 'Expected two HTTP calls due to 429 retries')
      assert_equal('success content', File.read(file))
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
      baza.send(:upload, baza.send(:home).append('file'), file)
      assert_equal(2, attempts, 'Expected 2 HTTP calls due to 429 retries')
    end
  end

  # Reproduces zerocracy/baza.rb#122: when an upstream proxy aborts an in-flight
  # request (e.g. nginx returns 499 "Client Closed Request" after a load-balancer
  # timeout), the failure is server-side from the client's point of view and
  # should be retried, just like 5xx responses.
  def test_upload_retries_on_499_failure
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
      baza.send(:upload, baza.send(:home).append('file'), file)
      assert_equal(2, attempts, 'Expected 2 HTTP calls due to 499 retries')
    end
  end

  # Reproduces zerocracy/baza.rb#111: when libcurl reports a transport-level
  # failure such as CURLE_PARTIAL_FILE (HTTP code 0, Typhoeus return_code
  # :partial_file), BazaRb#checked must raise something that BazaRb#retry_it
  # will retry, so an upload doesn't abort the whole pipeline on the first
  # transient failure.
  def test_checked_partial_file_response_is_retryable_by_retry_it
    WebMock.disable_net_connect!
    fake = Typhoeus::Response.new(
      return_code: :partial_file,
      return_message: 'Transferred a partial file',
      code: 0,
      total_time: 0.01
    )
    fake.request = Typhoeus::Request.new('https://example.org:443/file', method: :put)
    error = assert_raises(BazaRb::ConnectionFailed) { fake_baza.send(:checked, fake) }
    assert_kind_of(BazaRb::TimedOut, error, 'must inherit from TimedOut so retry_it retries it')
    assert_includes(error.message, 'r:partial_file')
    attempts = 0
    fast = BazaRb.new('example.org', 443, '000', loog: Loog::NULL, retries: 2, pause: 0)
    raised =
      assert_raises(BazaRb::ConnectionFailed) do
        fast.send(:retry_it) do
          attempts += 1
          raise BazaRb::ConnectionFailed, 'simulated partial file'
        end
      end
    assert_equal('simulated partial file', raised.message)
    assert_operator(attempts, :>, 1, 'retry_it must retry on ConnectionFailed (it is a TimedOut)')
  end

  def test_durable_load_from_sinatra
    WebMock.enable_net_connect!
    Dir.mktmpdir do |dir|
      with_sinatra_server do |baza|
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
      baza.send(:download, baza.send(:home).append('file'), file)
      assert_equal('file content', File.read(file), 'File should be downloaded correctly')
      file2 = File.join(dir, 'test2.txt')
      stub_request(:get, "https://#{other}:443/file2")
        .with(headers: { 'Range' => 'bytes=0-' })
        .to_return(
          status: 200,
          body: 'second file',
          headers: {}
        )
      baza.send(:download, baza.send(:home).append('file2'), file2)
      assert_equal('second file', File.read(file2), 'Second request should go to new host')
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
      baza.send(:download, baza.send(:home).append('file'), file)
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
      baza.send(:upload, baza.send(:home).append('file'), file)
      stub_request(:put, "https://#{other}:443/file2")
        .to_return(
          status: 200,
          body: 'OK'
        )
      baza.send(:upload, baza.send(:home).append('file2'), file)
    end
  end

  def test_lock_raises_when_owner_is_empty
    error = assert_raises(RuntimeError) { fake_baza.lock('pname', '') }
    assert_equal('The "owner" of the lock may not be empty', error.message)
  end

  def test_enter_raises_when_pname_is_nil
    assert_enter_rejects(nil, 'badge', 'why', nil, 'The "pname" is nil')
  end

  def test_enter_raises_when_pname_is_empty
    assert_enter_rejects('', 'badge', 'why', nil, 'The "pname" may not be empty')
  end

  def test_enter_raises_when_badge_is_nil
    assert_enter_rejects('pname', nil, 'why', nil, 'The "badge" is nil')
  end

  def test_enter_raises_when_badge_is_empty
    assert_enter_rejects('pname', '', 'why', nil, 'The "badge" may not be empty')
  end

  def test_enter_raises_when_why_is_nil
    assert_enter_rejects('pname', 'badge', nil, nil, 'The "why" is nil')
  end

  def test_enter_raises_when_why_is_empty
    assert_enter_rejects('pname', 'badge', '', nil, 'The "why" may not be empty')
  end

  def test_enter_raises_when_job_is_not_integer
    assert_enter_rejects('pname', 'badge', 'why', 'job', 'The "job" must be Integer')
  end

  def test_enter_raises_when_job_is_not_positive
    assert_enter_rejects('pname', 'badge', 'why', 0, 'The "job" must be a positive integer')
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
      baza.send(:upload, baza.send(:home).append('file'), file, {}, chunk_size: 1_000_000)
    end
  end

  private

  def assert_enter_rejects(pname, badge, why, job, message)
    ran = false
    error =
      assert_raises(RuntimeError) do
        fake_baza.enter(pname, badge, why, job) do
          ran = true
          'result'
        end
      end
    assert_equal(message, error.message)
    refute(
      ran,
      'Invalid enter arguments must be rejected before the block is executed'
    )
  end

  def with_sinatra_server
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
          yield BazaRb.new(host, port, '0000-0000-0000', ssl: false)
        end
      end
    end
  end

  def with_http_server(code, response, opts = {})
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
            socket.puts "HTTP/1.1 #{code} OK\r\nContent-Length: #{response.length}\r\n\r\n#{response}"
          else
            socket.puts "HTTP/1.1 400 Bad Request\r\n"
          end
          socket.close
        end
      yield BazaRb.new(host, port, '0000', **opts)
      t.terminate
      assert(t.join(1))
    end
    req
  end

  def fake_baza(compress: true)
    BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress:)
  end
end
