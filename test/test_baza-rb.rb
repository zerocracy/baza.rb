# frozen_string_literal: true

# Copyright (c) 2024 Zerocracy
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the 'Software'), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

require 'factbase'
require 'loog'
require 'minitest/autorun'
require 'net/ping'
require 'random-port'
require 'securerandom'
require 'socket'
require 'stringio'
require 'wait_for'
require 'webmock/minitest'
require 'webrick'
require_relative '../lib/baza-rb'

# Test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024 Yegor Bugayenko
# License:: MIT
class TestBazaRb < Minitest::Test
  TOKEN = '00000000-0000-0000-0000-000000000000'
  HOST = 'api.zerocracy.com'
  PORT = 443
  LIVE = BazaRb.new(HOST, PORT, TOKEN, loog: Loog::VERBOSE)

  def test_live_push
    WebMock.enable_net_connect!
    skip unless we_are_online
    fb = Factbase.new
    fb.insert.foo = 'test-' * 10_000
    fb.insert
    n = fake_name
    assert(LIVE.push(n, fb.export, []).positive?)
    assert(LIVE.name_exists?(n))
    assert(LIVE.recent(n).positive?)
    id = LIVE.recent(n)
    wait_for(60) { LIVE.finished?(id) }
    assert(!LIVE.pull(id).nil?)
    assert(!LIVE.stdout(id).nil?)
    assert(!LIVE.exit_code(id).nil?)
    assert(!LIVE.verified(id).nil?)
    owner = 'baza.rb testing'
    assert(!LIVE.lock(n, owner).nil?)
    assert(!LIVE.unlock(n, owner).nil?)
  end

  def test_live_push_no_compression
    WebMock.enable_net_connect!
    skip unless we_are_online
    fb = Factbase.new
    fb.insert.foo = 'test-' * 10_000
    fb.insert
    baza = BazaRb.new(HOST, PORT, TOKEN, compression: false)
    assert(baza.push(fake_name, fb.export, []).positive?)
  end

  def test_live_durable_lock_unlock
    WebMock.enable_net_connect!
    skip unless we_are_online
    Dir.mktmpdir do |dir|
      file = File.join(dir, "#{fake_name}.bin")
      File.binwrite(file, 'hello')
      id = LIVE.durable_place(fake_name, file)
      owner = fake_name
      LIVE.durable_lock(id, owner)
      LIVE.durable_load(id, file)
      LIVE.durable_save(id, file)
      LIVE.durable_unlock(id, owner)
    end
  end

  def test_durable_place
    WebMock.disable_net_connect!
    stub_request(:post, 'https://example.org/durables/place').to_return(
      status: 302, headers: { 'X-Zerocracy-DurableId' => '42' }
    )
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'test.bin')
      File.binwrite(file, 'hello')
      assert_equal(42, BazaRb.new('example.org', 443, '000').durable_place('simple', file))
    end
  end

  def test_simple_push
    WebMock.disable_net_connect!
    stub_request(:put, 'https://example.org/push/simple').to_return(
      status: 200, body: '42'
    )
    assert_equal(
      42,
      BazaRb.new('example.org', 443, '000').push('simple', 'hello, world!', [])
    )
  end

  def test_simple_recent_check
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org/recent/simple.txt')
      .with(body: '', headers: { 'User-Agent' => /^baza.rb .*$/ })
      .to_return(status: 200, body: '42')
    assert_equal(
      42,
      BazaRb.new('example.org', 443, '000').recent('simple')
    )
  end

  def test_simple_exists_check
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org/exists/simple').to_return(
      status: 200, body: 'yes'
    )
    assert(
      BazaRb.new('example.org', 443, '000').name_exists?('simple')
    )
  end

  def test_exit_code_check
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org/exit/42.txt').to_return(
      status: 200, body: '0'
    )
    assert(
      BazaRb.new('example.org', 443, '000').exit_code(42).zero?
    )
  end

  def test_stdout_read
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org/stdout/42.txt').to_return(
      status: 200, body: 'hello!'
    )
    assert(
      !BazaRb.new('example.org', 443, '000').stdout(42).empty?
    )
  end

  def test_simple_pull
    WebMock.disable_net_connect!
    stub_request(:get, 'https://example.org/pull/333.fb').to_return(
      status: 200, body: 'hello, world!'
    )
    assert(
      BazaRb.new('example.org', 443, '000').pull(333).start_with?('hello')
    )
  end

  def test_real_http
    req =
      with_http_server(200, 'yes') do |baza|
        baza.name_exists?('simple')
      end
    assert_equal("baza.rb #{BazaRb::VERSION}", req['user-agent'])
  end

  def test_push_with_meta
    req =
      with_http_server(200, 'yes') do |baza|
        baza.push('simple', 'hello, world!', ['boom!', 'хей!'])
      end
    assert_equal('Ym9vbSE= 0YXQtdC5IQ==', req['x-zerocracy-meta'])
  end

  def test_push_with_big_meta
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
    req =
      with_http_server(200, 'yes') do |baza|
        baza.push('simple', 'hello, world!', %w[meta1 meta2 meta3])
      end
    assert_equal('application/zip', req.content_type)
    assert_equal('gzip', req['content-encoding'])
    body = Zlib::GzipReader.zcat(StringIO.new(req.body))
    assert_equal('hello, world!', body)
  end

  def test_push_compression_disabled
    req =
      with_http_server(200, 'yes', compression: false) do |baza|
        baza.push('simple', 'hello, world!', %w[meta1 meta2 meta3])
      end
    assert_equal('application/octet-stream', req.content_type)
    assert_equal('hello, world!', req.body)
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
      assert(
        assert_raises do
          BazaRb.new(host, port, '0000', ssl: false, timeout: 0.01).push('x', 'y', [])
        end.message.include?('timed out in')
      )
      t.join
    end
  end

  private

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
          req.body
          socket.puts "HTTP/1.1 #{code} OK\r\nContent-Length: #{response.length}\r\n\r\n#{response}"
          socket.close
        end
      yield BazaRb.new(host, port, '0000', **opts)
      t.join
    end
    req
  end

  def fake_name
    "fake-#{SecureRandom.hex(8)}"
  end

  def we_are_online
    Net::Ping::External.new('8.8.8.8').ping?
  end
end
