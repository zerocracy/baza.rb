# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'base64'
require 'bigdecimal'
require 'elapsed'
require 'fileutils'
require 'iri'
require 'logger'
require 'loog'
require 'retries'
require 'stringio'
require 'tago'
require 'tempfile'
require 'typhoeus'
require 'zlib'
require_relative 'baza-rb/compress'
require_relative 'baza-rb/version'

# Ruby client for the Zerocracy API.
#
# This class provides a complete interface to interact with the Zerocracy
# platform API. Create an instance with your authentication token and use
# its methods to manage jobs, transfer funds, handle durables, and more.
#
# @example Basic usage
#   baza = BazaRb.new('api.zerocracy.com', 443, 'your-token-here')
#   puts baza.whoami        # => "your-github-username"
#   puts baza.balance       # => 100.5
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024-2026 Yegor Bugayenko
# License:: MIT
class BazaRb
  DEFAULT_CHUNK_SIZE = 1_000_000

  include Compress

  # When the server failed (503).
  class ServerFailure < StandardError; end

  # When the request times out.
  class TimedOut < StandardError; end

  # When libcurl reported a transport-level failure (HTTP code 0, e.g.
  # connection reset, partial file, SSL error). Subclasses {TimedOut} so
  # {#attempt} retries it as a transient failure.
  class ConnectionFailed < TimedOut; end

  # When the server sent incorrectly compressed data.
  class BadCompression < StandardError; end

  # Initialize a new Zerocracy API client.
  #
  # @param [String] host The API host name (e.g., 'api.zerocracy.com')
  # @param [Integer] port The TCP port to connect to (usually 443 for HTTPS)
  # @param [String] token Your Zerocracy API authentication token
  # @param [Boolean] ssl Whether to use SSL/HTTPS (default: true)
  # @param [Float] timeout Connection and request timeout in seconds (default: 30)
  # @param [Integer] retries Number of retries on connection failure (default: 5)
  # @param [Integer] pause The factor on pause (<1 means faster, >1 means slower)
  # @param [Loog] loog The logging facility (default: Loog::NULL)
  # @param [Boolean] compress Whether to use GZIP compression for requests/responses (default: true)
  def initialize(host, port, token, ssl: true, timeout: 30, retries: 5, pause: 1, loog: Loog::NULL, compress: true)
    @host = host
    @port = port
    @ssl = ssl
    @token = token
    @timeout = timeout
    @loog = loog
    @retries = retries
    @pause = ENV.fetch('BAZA_BACKOFF', pause).to_i
    @compress = compress
    @mutex = Mutex.new
  end

  # Get GitHub login name of the logged in user.
  #
  # @return [String] GitHub nickname of the authenticated user
  # @raise [ServerFailure] If authentication fails or server returns an error
  def whoami
    nick = nil
    elapsed(@loog, level: Logger::INFO) do
      nick = get(home.append('whoami')).body
      throw(:"I know that I am @#{nick}, at #{@host}")
    end
    nick
  end

  # Get current balance of the authenticated user.
  #
  # @return [Float] The balance in zents (Ƶ), where 1 Ƶ = 1 USDT
  # @raise [ServerFailure] If authentication fails or server returns an error
  def balance
    z = nil
    elapsed(@loog, level: Logger::INFO) do
      z = get(home.append('account').append('balance')).body.to_f
      throw(:"The balance is Ƶ#{z}, at #{@host}")
    end
    z
  end

  # Push factbase to the server to create a new job.
  #
  # @param [String] pname The unique name of the product on the server
  # @param [String] data The binary data to push to the server (factbase content)
  # @param [Array<String>] meta List of metadata strings to attach to the job
  # @param [Integer] chunk_size Maximum size of one chunk
  # @raise [ServerFailure] If the push operation fails
  def push(pname, data, meta, chunk_size: DEFAULT_CHUNK_SIZE)
    raise(RuntimeError, 'The "name" of the job is nil') if pname.nil?
    raise(RuntimeError, 'The "name" of the job may not be empty') if pname.empty?
    raise(RuntimeError, "The name #{pname.inspect} is not valid") unless pname.match?(/\A[a-z0-9-]+\z/)
    raise(RuntimeError, "The name #{pname.inspect} is too long") if pname.length > 32
    raise(RuntimeError, 'The "data" of the job is nil') if data.nil?
    raise(RuntimeError, 'The "data" of the job may not be empty') if data.empty?
    raise(RuntimeError, 'The "meta" of the job is nil') if meta.nil?
    raise(RuntimeError, 'The "meta" of the job must be an Array') unless meta.is_a?(Array)
    elapsed(@loog, level: Logger::INFO) do
      Tempfile.open do |file|
        File.binwrite(file.path, data)
        upload(
          home.append('push').append(pname),
          file.path,
          headers.merge(
            'X-Zerocracy-Meta' => meta.map { |v| Base64.strict_encode64(v) }.join(' ')
          ),
          chunk_size:
        )
      end
      throw(:"Pushed #{data.bytesize} bytes to #{@host}")
    end
  end

  # Pull factbase from the server for a specific job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [String] Binary data of the factbase (can be saved to file)
  # @raise [ServerFailure] If the job doesn't exist or pull fails
  def pull(id)
    raise(RuntimeError, 'The ID of the job is nil') if id.nil?
    raise(RuntimeError, 'The ID of the job must be an Integer') unless id.is_a?(Integer)
    raise(RuntimeError, 'The ID of the job must be a positive integer') unless id.positive?
    data = ''
    elapsed(@loog, level: Logger::INFO) do
      Tempfile.open do |file|
        download(home.append('pull').append("#{id}.fb"), file.path)
        data = File.binread(file)
        throw(:"Pulled #{data.bytesize} bytes of job ##{id} factbase at #{@host}")
      end
    end
    data
  end

  # Check if the job with this ID is finished already.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [Boolean] TRUE if the job has completed execution, FALSE otherwise
  # @raise [ServerFailure] If the job doesn't exist
  def finished?(id)
    raise(RuntimeError, 'The ID of the job is nil') if id.nil?
    raise(RuntimeError, 'The ID of the job must be an Integer') unless id.is_a?(Integer)
    raise(RuntimeError, 'The ID of the job must be a positive integer') unless id.positive?
    fin = false
    elapsed(@loog, level: Logger::INFO) do
      ret = get(home.append('finished').append(id))
      fin = ret.body == 'yes'
      throw(:"The job ##{id} is #{'not yet ' unless fin}finished at #{@host}#{" (#{ret.body.inspect})" unless fin}")
    end
    fin
  end

  # Read and return the stdout of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [String] The stdout, as a text
  # @raise [ServerFailure] If the job doesn't exist or retrieval fails
  def stdout(id)
    raise(RuntimeError, 'The ID of the job is nil') if id.nil?
    raise(RuntimeError, 'The ID of the job must be an Integer') unless id.is_a?(Integer)
    raise(RuntimeError, 'The ID of the job must be a positive integer') unless id.positive?
    stdout = ''
    elapsed(@loog, level: Logger::INFO) do
      stdout = get(home.append('stdout').append("#{id}.txt")).body
      throw(:"The stdout of the job ##{id} has #{stdout.split("\n").count} lines")
    end
    stdout
  end

  # Read and return the exit code of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [Integer] The exit code
  # @raise [ServerFailure] If the job doesn't exist or retrieval fails
  def exit_code(id)
    raise(RuntimeError, 'The ID of the job is nil') if id.nil?
    raise(RuntimeError, 'The ID of the job must be an Integer') unless id.is_a?(Integer)
    raise(RuntimeError, 'The ID of the job must be a positive integer') unless id.positive?
    code = 0
    elapsed(@loog, level: Logger::INFO) do
      code = get(home.append('exit').append("#{id}.txt")).body.to_i
      throw(:"The exit code of the job ##{id} is #{code}")
    end
    code
  end

  # Read and return the verification verdict of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [String] The verdict
  # @raise [ServerFailure] If the job doesn't exist or retrieval fails
  def verified(id)
    raise(RuntimeError, 'The ID of the job is nil') if id.nil?
    raise(RuntimeError, 'The ID of the job must be an Integer') unless id.is_a?(Integer)
    raise(RuntimeError, 'The ID of the job must be a positive integer') unless id.positive?
    verdict = ''
    elapsed(@loog, level: Logger::INFO) do
      verdict = get(home.append('jobs').append(id).append('verified.txt')).body
      throw(:"The verdict of the job ##{id} is #{verdict.inspect}")
    end
    verdict
  end

  # Lock the name.
  #
  # @param [String] pname The name of the product on the server
  # @param [String] owner The owner of the lock (any string)
  # @raise [RuntimeError] If the name is already locked
  # @raise [ServerFailure] If the lock operation fails
  def lock(pname, owner)
    raise(RuntimeError, 'The "pname" of the product is nil') if pname.nil?
    raise(RuntimeError, 'The "pname" of the product may not be empty') if pname.empty?
    raise(RuntimeError, "The name #{pname.inspect} is not valid") unless pname.match?(/\A[a-z0-9-]+\z/)
    raise(RuntimeError, "The name #{pname.inspect} is too long") if pname.length > 32
    raise(RuntimeError, 'The "owner" of the lock is nil') if owner.nil?
    raise(RuntimeError, 'The "owner" of the lock may not be empty') if owner.empty?
    elapsed(@loog, level: Logger::INFO) do
      throw(:"Product name #{pname.inspect} locked at #{@host}") if post(
        home.append('lock').append(pname),
        { 'owner' => owner }, [302, 409]
      ).code == 302
      raise(RuntimeError, "Failed to lock #{pname.inspect} product at #{@host}, it's already locked")
    end
  end

  # Unlock the name.
  #
  # @param [String] pname The name of the job on the server
  # @param [String] owner The owner of the lock (any string)
  # @raise [ServerFailure] If the unlock operation fails
  def unlock(pname, owner)
    raise(RuntimeError, 'The "pname" of the job is nil') if pname.nil?
    raise(RuntimeError, 'The "pname" of the job may not be empty') if pname.empty?
    raise(RuntimeError, "The name #{pname.inspect} is not valid") unless pname.match?(/\A[a-z0-9-]+\z/)
    raise(RuntimeError, "The name #{pname.inspect} is too long") if pname.length > 32
    raise(RuntimeError, 'The "owner" of the lock is nil') if owner.nil?
    raise(RuntimeError, 'The "owner" of the lock may not be empty') if owner.empty?
    elapsed(@loog, level: Logger::INFO) do
      post(home.append('unlock').append(pname), { 'owner' => owner })
      throw(:"Job name #{pname.inspect} unlocked at #{@host}")
    end
  end

  # Get the ID of the job by the name.
  #
  # @param [String] name The name of the job on the server
  # @return [Integer] The ID of the job on the server
  # @raise [ServerFailure] If the job doesn't exist or retrieval fails
  def recent(name)
    raise(RuntimeError, 'The "name" of the job is nil') if name.nil?
    raise(RuntimeError, 'The "name" of the job may not be empty') if name.empty?
    raise(RuntimeError, "The name #{name.inspect} is not valid") unless name.match?(/\A[a-z0-9-]+\z/)
    raise(RuntimeError, "The name #{name.inspect} is too long") if name.length > 32
    job = nil
    elapsed(@loog, level: Logger::INFO) do
      job = get(home.append('recent').append("#{name}.txt")).body.to_i
      throw(:"The recent \"#{name}\" job's ID is ##{job} at #{@host}")
    end
    job
  end

  # Check whether the name of the job exists on the server.
  #
  # @param [String] pname The name of the product on the server
  # @return [Boolean] TRUE if such name exists
  def name_exists?(pname)
    raise(RuntimeError, 'The "pname" of the product is nil') if pname.nil?
    raise(RuntimeError, 'The "pname" of the product may not be empty') if pname.empty?
    raise(RuntimeError, "The name #{pname.inspect} is not valid") unless pname.match?(/\A[a-z0-9-]+\z/)
    raise(RuntimeError, "The name #{pname.inspect} is too long") if pname.length > 32
    exists = false
    elapsed(@loog, level: Logger::INFO) do
      exists = get(home.append('exists').append(pname)).body == 'yes'
      throw(:"The name #{pname.inspect} #{exists ? 'exists' : "doesn't exist"} at #{@host}")
    end
    exists
  end

  # Place a single durable file on the server.
  #
  # The file provided will only be uploaded to the server if the durable
  # is currently absent. If the durable is present, the file will be
  # ignored. It is expected to use only small placeholder files, not real
  # data.
  #
  # @param [String] pname The name of the product on the server
  # @param [String] file The path to the file to upload
  # @return [Integer] The ID of the created durable
  # @raise [ServerFailure] If the upload fails
  def durable_place(pname, file)
    raise(RuntimeError, 'The "pname" of the durable is nil') if pname.nil?
    raise(RuntimeError, 'The "pname" of the durable may not be empty') if pname.empty?
    raise(RuntimeError, "The name #{pname.inspect} is not valid") unless pname.match?(/\A[a-z0-9-]+\z/)
    raise(RuntimeError, "The name #{pname.inspect} is too long") if pname.length > 32
    raise(RuntimeError, 'The "file" of the durable is nil') if file.nil?
    raise(RuntimeError, "The file '#{file}' is absent") unless File.exist?(file)
    if File.size(file) > 1024
      raise(
        RuntimeError,
        "The file '#{file}' is too big (#{File.size(file)} bytes) for durable_place(), use durable_save() instead"
      )
    end
    id = nil
    elapsed(@loog, level: Logger::INFO) do
      id = post(
        home.append('durable-place'),
        {
          'pname' => pname,
          'file' => File.basename(file),
          'zip' => File.open(file, 'rb')
        }
      ).headers['X-Zerocracy-DurableId'].to_i
      throw(:"Durable ##{id} (#{file}, #{File.size(file)} bytes) placed for job \"#{pname}\" at #{@host}")
    end
    id
  end

  # Save a single durable from local file to server.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] file The file to upload
  # @param [Integer] chunk_size Maximum size of one chunk
  # @raise [ServerFailure] If the save operation fails
  def durable_save(id, file, chunk_size: DEFAULT_CHUNK_SIZE)
    raise(RuntimeError, 'The ID of the durable is nil') if id.nil?
    raise(RuntimeError, 'The ID of the durable must be an Integer') unless id.is_a?(Integer)
    raise(RuntimeError, 'The ID of the durable must be a positive integer') unless id.positive?
    raise(RuntimeError, 'The "file" of the durable is nil') if file.nil?
    raise(RuntimeError, "The file '#{file}' is absent") unless File.exist?(file)
    elapsed(@loog, level: Logger::INFO) do
      upload(home.append('durables').append(id), file, chunk_size:)
      throw(:"Durable ##{id} saved #{File.size(file)} bytes to #{@host}")
    end
  end

  # Load a single durable from server to local file.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] file The local file path to save the downloaded durable
  # @raise [ServerFailure] If the load operation fails
  def durable_load(id, file)
    raise(RuntimeError, 'The ID of the durable is nil') if id.nil?
    raise(RuntimeError, 'The ID of the durable must be an Integer') unless id.is_a?(Integer)
    raise(RuntimeError, 'The ID of the durable must be a positive integer') unless id.positive?
    raise(RuntimeError, 'The "file" of the durable is nil') if file.nil?
    elapsed(@loog, level: Logger::INFO) do
      download(home.append('durables').append(id), file)
      throw(:"Durable ##{id} loaded #{File.size(file)} bytes from #{@host}")
    end
  end

  # Lock a single durable.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] owner The owner of the lock
  # @raise [ServerFailure] If the lock operation fails
  def durable_lock(id, owner)
    raise(RuntimeError, 'The ID of the durable is nil') if id.nil?
    raise(RuntimeError, 'The ID of the durable must be an Integer') unless id.is_a?(Integer)
    raise(RuntimeError, 'The ID of the durable must be a positive integer') unless id.positive?
    raise(RuntimeError, 'The "owner" of the lock is nil') if owner.nil?
    raise(RuntimeError, 'The "owner" of the lock may not be empty') if owner.empty?
    elapsed(@loog, level: Logger::INFO) do
      post(home.append('durables').append(id).append('lock'), { 'owner' => owner })
      throw(:"Durable ##{id} locked at #{@host}")
    end
  end

  # Unlock a single durable.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] owner The owner of the lock
  # @raise [ServerFailure] If the unlock operation fails
  def durable_unlock(id, owner)
    raise(RuntimeError, 'The ID of the durable is nil') if id.nil?
    raise(RuntimeError, 'The ID of the durable must be an Integer') unless id.is_a?(Integer)
    raise(RuntimeError, 'The ID of the durable must be a positive integer') unless id.positive?
    raise(RuntimeError, 'The "owner" of the lock is nil') if owner.nil?
    raise(RuntimeError, 'The "owner" of the lock may not be empty') if owner.empty?
    elapsed(@loog, level: Logger::INFO) do
      post(home.append('durables').append(id).append('unlock'), { 'owner' => owner })
      throw(:"Durable ##{id} unlocked at #{@host}")
    end
  end

  # Find a durable by job name and file name.
  #
  # @param [String] pname The name of the job
  # @param [String] file The file name
  # @return [Integer, nil] The ID of the durable if found, nil if not found
  def durable_find(pname, file)
    raise(RuntimeError, 'The "pname" is nil') if pname.nil?
    raise(RuntimeError, 'The "pname" may not be empty') if pname.empty?
    raise(RuntimeError, "The name #{pname.inspect} is not valid") unless pname.match?(/\A[a-z0-9-]+\z/)
    raise(RuntimeError, "The name #{pname.inspect} is too long") if pname.length > 32
    raise(RuntimeError, 'The "file" is nil') if file.nil?
    raise(RuntimeError, 'The "file" may not be empty') if file.empty?
    id = nil
    elapsed(@loog, level: Logger::INFO) do
      ret = get(home.append('durable-find').add(file:, pname:), [200, 404])
      if ret.code == 200
        id = ret.body.to_i
        throw(:"Found durable ##{id} for job \"#{pname}\" file \"#{file}\" at #{@host}")
      else
        throw(:"Durable not found for job \"#{pname}\" file \"#{file}\" at #{@host}")
      end
    end
    id
  end

  # Transfer funds to another user.
  #
  # @param [String] recipient GitHub username of the recipient (e.g. "yegor256")
  # @param [Float, BigDecimal] amount The amount to transfer in Ƶ (zents)
  # @param [String] summary The description/reason for the payment
  # @param [Integer] job Optional job ID to associate with this transfer
  # @param [String] badge Optional idempotency key; the server dedups on it,
  #   collapsing a repeated payment into a no-op
  # @return [Integer] Receipt ID for the transaction
  # @raise [ServerFailure] If the transfer fails
  def transfer(recipient, amount, summary, job: nil, badge: nil)
    raise(RuntimeError, 'The "recipient" is nil') if recipient.nil?
    raise(RuntimeError, "The recipient #{recipient.inspect} is not valid") unless recipient.match?(/\A[a-zA-Z0-9-]+\z/)
    raise(RuntimeError, 'The "amount" is nil') if amount.nil?
    unless amount.is_a?(Float) || amount.is_a?(BigDecimal)
      raise(RuntimeError, 'The "amount" must be Float or BigDecimal')
    end
    raise(RuntimeError, 'The "amount" must be positive') unless amount.positive?
    raise(RuntimeError, 'The "summary" is nil') if summary.nil?
    raise(RuntimeError, "The summary #{summary.inspect} is empty") if summary.empty?
    unless job.nil?
      raise(RuntimeError, 'The ID must be an Integer') unless job.is_a?(Integer)
      raise(RuntimeError, 'The ID must be positive') unless job.positive?
    end
    amt = amount.is_a?(BigDecimal) ? amount.truncate(6).to_s('F') : format('%0.6f', amount)
    id = nil
    body = { 'human' => recipient, 'amount' => amt, 'summary' => summary }
    body['job'] = job unless job.nil?
    body['badge'] = badge unless badge.nil?
    elapsed(@loog, level: Logger::INFO) do
      id = post(home.append('account').append('transfer'), body).headers['X-Zerocracy-ReceiptId'].to_i
      throw(:"Transferred Ƶ#{amt} to @#{recipient} at #{@host}")
    end
    id
  end

  # Pay a fee associated with a job.
  #
  # @param [String] tab The category/type of the fee (use "unknown" if not sure)
  # @param [Float, BigDecimal] amount The fee amount in Ƶ (zents)
  # @param [String] summary The description/reason for the fee
  # @param [Integer] job The ID of the job this fee is for
  # @return [Integer] Receipt ID for the fee payment
  # @raise [ServerFailure] If the payment fails
  def fee(tab, amount, summary, job)
    raise(RuntimeError, 'The "tab" is nil') if tab.nil?
    raise(RuntimeError, 'The "tab" may not be empty') if tab.empty?
    raise(RuntimeError, 'The "amount" is nil') if amount.nil?
    unless amount.is_a?(Float) || amount.is_a?(BigDecimal)
      raise(RuntimeError, 'The "amount" must be Float or BigDecimal')
    end
    raise(RuntimeError, 'The "amount" must be positive') unless amount.positive?
    raise(RuntimeError, 'The "job" is nil') if job.nil?
    raise(RuntimeError, 'The "job" must be Integer') unless job.is_a?(Integer)
    raise(RuntimeError, 'The "job" must be positive') unless job.positive?
    raise(RuntimeError, 'The "summary" is nil') if summary.nil?
    raise(RuntimeError, "The summary #{summary.inspect} is empty") if summary.empty?
    amt = amount.is_a?(BigDecimal) ? amount.truncate(6).to_s('F') : format('%0.6f', amount)
    id = nil
    elapsed(@loog, level: Logger::INFO) do
      id = post(
        home.append('account').append('fee'),
        {
          'amount' => amt,
          'job' => job.to_s,
          'summary' => summary,
          'tab' => tab
        }
      ).headers['X-Zerocracy-ReceiptId'].to_i
      throw(:"Fee Ƶ#{amt} paid at #{@host}")
    end
    id
  end

  # Enter a valve to cache or retrieve a computation result.
  #
  # Valves prevent duplicate computations by caching results. If a result
  # for the given badge already exists, it's returned. Otherwise, the block
  # is executed and its result is cached.
  #
  # @param [String] pname Name of the product
  # @param [String] badge Unique identifier for this valve/computation
  # @param [String] why The reason/description for entering this valve
  # @param [nil|Integer] job Optional job ID to associate with this valve
  # @yield Block that computes the result if not cached
  # @return [String] The cached result or newly computed result from the block
  # @raise [ServerFailure] If the valve operation fails
  def enter(pname, badge, why, job)
    raise(RuntimeError, 'The "pname" is nil') if pname.nil?
    raise(RuntimeError, 'The "pname" may not be empty') if pname.empty?
    raise(RuntimeError, 'The "badge" is nil') if badge.nil?
    raise(RuntimeError, 'The "badge" may not be empty') if badge.empty?
    raise(RuntimeError, 'The "why" is nil') if why.nil?
    raise(RuntimeError, 'The "why" may not be empty') if why.empty?
    raise(RuntimeError, 'The "job" must be an Integer') unless job.nil? || job.is_a?(Integer)
    raise(RuntimeError, 'The "job" must be positive') unless job.nil? || job.positive?
    elapsed(@loog, good: "Entered valve #{badge} to #{pname}") do
      ret = get(home.append('result').add(badge:), [200, 204])
      return ret.body if ret.code == 200
      r = yield
      uri = home.append('valves')
      uri = uri.add(job:) unless job.nil?
      post(uri, { 'badge' => badge, 'pname' => pname, 'result' => r.to_s, 'why' => why })
      r
    end
  end

  # Get CSRF token from the server for authenticated requests.
  #
  # The CSRF token is required for POST requests to prevent cross-site
  # request forgery attacks.
  #
  # @return [String] The CSRF token for the authenticated user
  # @raise [ServerFailure] If token retrieval fails
  def csrf
    token = nil
    elapsed(@loog, level: Logger::INFO) do
      token = get(home.append('csrf')).body
      throw(:"CSRF token retrieved (#{token.length} chars)")
    end
    token
  end

  private

  # Calculate exponential backoff in seconds for a given attempt number.
  #
  # @param [Integer] attempt The attempt number (1-based)
  # @return [Integer] The number of seconds to sleep
  def backoff(attempt)
    @pause * (2**attempt)
  end

  # Stick host from X-Zerocracy-Host header if present.
  #
  # @param [Typhoeus::Response] ret The HTTP response containing headers
  # @param [Iri] uri The current URI object to update
  # @return [Iri] The updated URI object (or original if no valid header present)
  # @note Invalid hostnames are logged as warnings and ignored
  def rehost(ret, uri)
    sticky = ret.headers && ret.headers['X-Zerocracy-Host']
    return uri unless sticky
    return uri unless hostname?(sticky)
    host = sticky.strip.chomp('.')
    @mutex.synchronize do
      if host != @host
        @loog.debug("Switching host from #{@host} to #{host} as per X-Zerocracy-Host")
        @host = host
      end
      uri.host(@host)
    end
  end

  # Validate hostname format according to RFC 1123.
  #
  # @param [String] name The hostname to validate
  # @return [Boolean] True if valid, false otherwise
  def hostname?(name)
    name = name.strip
    return false if name.empty? || name.bytesize > 253
    name.match?(/\A[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\.?\z/)
  end

  # Get the user agent string for HTTP requests.
  #
  # @return [String] The user agent string
  def agent
    "baza.rb #{BazaRb::VERSION}"
  end

  # Get default headers for HTTP requests.
  #
  # @return [Hash] The default headers including User-Agent, Connection, and authentication token
  def headers
    {
      'User-Agent' => agent,
      'Connection' => 'close',
      'X-Zerocracy-Token' => @token
    }
  end

  # Build the base URI for API requests.
  #
  # @return [Iri] The base URI object
  def home
    Iri.new('')
      .host(@host)
      .port(@port)
      .scheme(@ssl ? 'https' : 'http')
  end

  # Execute a block with retries on timeout.
  #
  # @yield The block to execute with retries
  # @return [Object] The result of the block execution
  def attempt(&)
    with_retries(max_tries: @retries + 1, rescue: TimedOut, &)
  end

  # Retry the HTTP request on rate-limiting (429) and server errors (>= 499).
  #
  # Retries on any HTTP status 429 or >= 499. The 499 code ("Client Closed
  # Request") is emitted by upstream proxies such as nginx when they abort
  # an in-flight request, typically because of a load-balancer or upstream
  # timeout. From the client's point of view this is a transient server-side
  # failure and should be retried just like 5xx responses.
  #
  # @yield The block to execute with retries
  # @return [Object] The result of the block execution
  def retry_on(&)
    attempt = 0
    loop do
      ret = yield
      if (ret.code == 429 || ret.code >= 499) && attempt < @retries
        attempt += 1
        seconds = backoff(attempt)
        if ret.code == 429
          @loog.info("Server seems to be busy, will sleep for #{seconds} (attempt no.#{attempt})...")
        else
          @loog.info("Server seems to be in trouble (#{ret.code}), sleep #{seconds}s (attempt no.#{attempt})...")
        end
        sleep(seconds) unless @pause.zero?
        next
      end
      return ret
    end
  end

  # Check the HTTP response and return it.
  #
  # @param [Typhoeus::Response] ret The response
  # @param [Array<Integer>] allowed List of acceptable HTTP codes
  # @return [Typhoeus::Response] The same response
  def checked(ret, allowed = [200])
    allowed = [allowed] unless allowed.is_a?(Array)
    mtd = (ret.request.original_options[:method] || '???').upcase
    url = ret.effective_url
    if ret.return_code == :operation_timedout
      msg = "#{mtd} #{url} timed out in #{ret.total_time}s"
      @loog.error(msg)
      raise(TimedOut, msg)
    end
    log = "#{mtd} #{url} -> #{ret.code} (#{format('%0.2f', ret.total_time)}s)"
    if allowed.include?(ret.code)
      @loog.debug(log)
      return ret
    end
    headers = ret.headers || {}
    if headers.any?
      @loog.error(
        "#{log}\n" +
        headers.map { |k, v| "  #{k}: #{v}" }.join("\n")
      )
    else
      @loog.error("#{log}\n  (no headers returned)")
    end
    details = [
      ("Flash: #{headers['X-Zerocracy-Flash']}" if headers['X-Zerocracy-Flash']),
      ("Failure: #{headers['X-Zerocracy-Failure']}" if headers['X-Zerocracy-Failure']),
      ("FailureMark: #{headers['X-Zerocracy-FailureMark']}" if headers['X-Zerocracy-FailureMark'])
    ].compact
    msg = "Invalid response code ##{ret.code} at #{mtd} #{url}"
    msg += " (#{details.join(', ')})" unless details.empty?
    msg += hint(ret)
    @loog.error(msg)
    raise(ret.code.zero? ? ConnectionFailed : ServerFailure, msg)
  end

  # Build a human hint explaining a failing HTTP response code.
  #
  # @param [Typhoeus::Response] ret The response
  # @return [String] A suffix to append to the error message
  def hint(ret)
    case ret.code
    when 500, 503
      ", most probably it's an internal error on the server, " \
      'please report this to https://github.com/zerocracy/baza.rb'
    when 404
      ", most probably you are trying to reach a wrong server, which doesn't " \
      'have the URL that it is expected to have'
    when 0
      ', most likely a connection failure, timeout, or SSL error ' \
      "(r:#{ret.return_code}, m:#{ret.return_message})"
    else
      ''
    end
  end

  # Make a GET request.
  #
  # @param [Iri] uri The URI to send the request to
  # @param [Array<Integer>] allowed List of allowed HTTP response codes
  # @return [Typhoeus::Response] The HTTP response
  # @raise [ServerFailure] If the response code is not in the allowed list
  def get(uri, allowed = [200])
    attempt do
      checked(
        retry_on do
          Typhoeus::Request.get(uri.to_s, headers:, connecttimeout: @timeout, timeout: @timeout)
        end,
        allowed
      )
    end
  end

  # Make a POST request.
  #
  # @param [Iri] uri The URI to send the request to
  # @param [Hash] params The request parameters to send in the body
  # @param [Array<Integer>] allowed List of allowed HTTP response codes
  # @return [Typhoeus::Response] The HTTP response
  # @raise [ServerFailure] If the response code is not in the allowed list
  def post(uri, params, allowed = [302])
    attempt do
      checked(
        retry_on do
          Typhoeus::Request.post(
            uri.to_s,
            body: params.merge('_csrf' => csrf).sort.to_h,
            headers:,
            connecttimeout: @timeout,
            timeout: @timeout
          )
        end,
        allowed
      )
    end
  end

  # Download file via GET, using range requests for large files.
  #
  # @param [Iri] uri The URI to download from
  # @param [String] file The local file path to save to
  # @raise [ServerFailure] If the download fails
  def download(uri, file)
    FileUtils.mkdir_p(File.dirname(file))
    FileUtils.rm_f(file)
    FileUtils.touch(file)
    chunk = 0
    blanks = [204, 302]
    elapsed(@loog, level: Logger::INFO) do
      loop do
        slice = ''
        ret =
          attempt do
            checked(
              retry_on do
                slice = ''
                request = Typhoeus::Request.new(
                  uri.to_s,
                  method: :get,
                  headers: headers.merge(
                    'Accept' => '*/*',
                    'Accept-Encoding' => 'gzip',
                    'Range' => "bytes=#{File.size(file)}-"
                  ),
                  connecttimeout: @timeout,
                  timeout: @timeout
                )
                request.on_body do |data|
                  slice += data
                end
                request.run
                request.response
              end,
              [200, 206, 204, 302]
            )
          end
        rheaders = ret.headers || {}
        msg = [
          "GET #{uri.to_uri.path} #{ret.code}",
          "#{slice.bytesize} bytes",
          ('in gzip' if rheaders['Content-Encoding'] == 'gzip'),
          ("ranged as #{rheaders['Content-Range'].inspect}" if rheaders['Content-Range'])
        ]
        uri = rehost(ret, uri)
        if blanks.include?(ret.code)
          sleep(2)
          next
        end
        if rheaders['Content-Encoding'] == 'gzip'
          begin
            slice = unzip(slice)
            msg << "unzipped to #{slice.bytesize} bytes"
          rescue BazaRb::BadCompression => e
            raise(BazaRb::BadCompression, "#{msg.compact.join(', ')} (#{e.message})")
          end
        end
        File.open(file, 'ab') do |f|
          msg << "added to existing #{File.size(file)} bytes"
          f.write(slice)
        end
        @loog.debug(msg.compact.join(', '))
        break if ret.code == 200
        range, total = crange(rheaders)
        raise(RuntimeError, "Total size is not valid (#{total.inspect})") unless total.match?(/\A(?:\*|[0-9]+)\z/)
        _b, e = range.split('-', 2)
        raise(RuntimeError, "Range is not valid (#{range.inspect})") if e.nil?
        raise(RuntimeError, "Range is not valid (#{range.inspect})") unless e.match?(/\A[0-9]+\z/)
        break if e.to_i == total.to_i - 1
        break if total == '0'
        chunk += 1
        sleep(1) if rheaders['Content-Length'].to_i.zero?
      end
      throw(:"Downloaded #{File.size(file)} bytes in #{chunk + 1} chunks from #{uri}")
    end
  end

  def crange(headers)
    crange = headers['Content-Range']
    raise(RuntimeError, 'Content-Range header is missing') if crange.nil?
    _, value = crange.split
    raise(RuntimeError, "Content-Range is not valid (#{crange.inspect})") if value.nil?
    range, total = value.split('/', 2)
    raise(RuntimeError, "Content-Range is not valid (#{crange.inspect})") if total.nil?
    [range, total]
  end

  # Upload file via PUT, using chunked uploads for large files.
  #
  # If the server replies with a 4xx that contains an "Expecting chunk #N"
  # hint (e.g. `X-Zerocracy-Flash: Expecting chunk #0 (0b are here),
  # received #25`), the upload is rewound to chunk N and retried, instead
  # of failing the whole pipeline. This typically happens after a server
  # reboot in the middle of a multi-chunk upload (see #109). The number of
  # such rewinds is bounded by the same `retries` setting as transport
  # failures.
  #
  # @param [Iri] uri The URI to upload to
  # @param [String] file The local file path to upload from
  # @param [Hash] extra Hash of extra HTTP headers to include
  # @param [Integer] chunk_size Maximum size of each chunk in bytes
  # @raise [ServerFailure] If the upload fails
  def upload(uri, file, extra = {}, chunk_size: DEFAULT_CHUNK_SIZE)
    params = {
      connecttimeout: @timeout,
      timeout: @timeout,
      headers: headers.merge(extra).merge('Content-Type' => 'application/octet-stream')
    }
    total = File.size(file)
    chunk = 0
    sent = 0
    rewinds = 0
    elapsed(@loog, level: Logger::INFO) do
      loop do
        slice =
          if total > chunk_size
            File.open(file, 'rb') do |f|
              params[:headers]['X-Zerocracy-Chunk'] = chunk.to_s
              f.seek(chunk_size * chunk)
              f.read(chunk_size) || ''
            end
          else
            File.binread(file)
          end
        params[:body] = slice
        params[:headers]['Content-Length'] = slice.bytesize.to_s
        params = zipped(params) if @compress
        begin
          ret =
            attempt do
              checked(
                retry_on do
                  Typhoeus::Request.put(uri.to_s, params)
                end
              )
            end
        rescue BazaRb::ServerFailure => e
          match = e.message.match(/Expecting chunk #(\d+)/)
          raise if match.nil?
          rewinds += 1
          raise if rewinds > @retries
          target = match[1].to_i
          @loog.info(
            "Server lost upload state at chunk ##{chunk}, " \
            "restarting from chunk ##{target} (rewind no.#{rewinds})"
          )
          chunk = target
          sent = 0
          next
        end
        uri = rehost(ret, uri)
        sent += params[:body].bytesize
        @loog.debug(
          [
            "PUT #{uri.to_uri.path} #{ret.code}",
            ("gzipped #{slice.bytesize} bytes" if params[:headers]['Content-Encoding'] == 'gzip'),
            "sent #{params[:body].bytesize} bytes",
            ("chunk ##{chunk}" if params[:headers]['X-Zerocracy-Chunk']),
            ('no chunks' unless params[:headers]['X-Zerocracy-Chunk'])
          ].compact.join(', ')
        )
        break if slice.empty?
        break if total <= chunk_size
        chunk += 1
      end
      throw(:"Uploaded #{sent} bytes to #{uri}#{" in #{chunk + 1} chunks" if chunk.positive?}")
    end
  end
end
