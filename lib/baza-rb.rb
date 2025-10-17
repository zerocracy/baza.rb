# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2025 Zerocracy
# SPDX-License-Identifier: MIT

require 'base64'
require 'elapsed'
require 'fileutils'
require 'iri'
require 'loog'
require 'retries'
require 'stringio'
require 'tago'
require 'tempfile'
require 'typhoeus'
require 'zlib'
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
# Copyright:: Copyright (c) 2024-2025 Yegor Bugayenko
# License:: MIT
class BazaRb
  # How big are the chunks we send, by default, in bytes.
  # Numbers larger than 1Mb may lead to problems with the server,
  # since sending time will be too long and the server may drop
  # connections. Better keep it as is: 1Mb.
  DEFAULT_CHUNK_SIZE = 1_000_000

  # When the server failed (503).
  class ServerFailure < StandardError; end

  # When request timeout.
  class TimedOut < StandardError; end

  # Unexpected response arrived from the server.
  class BadResponse < StandardError; end

  # When server sent incorrectly compressed data.
  class BadCompression < StandardError; end

  # Initialize a new Zerocracy API client.
  #
  # @param [String] host The API host name (e.g., 'api.zerocracy.com')
  # @param [Integer] port The TCP port to connect to (usually 443 for HTTPS)
  # @param [String] token Your Zerocracy API authentication token
  # @param [Boolean] ssl Whether to use SSL/HTTPS (default: true)
  # @param [Float] timeout Connection and request timeout in seconds (default: 30)
  # @param [Integer] retries Number of retries on connection failure (default: 3)
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
    @pause = pause
    @compress = compress
  end

  # Get GitHub login name of the logged in user.
  #
  # @return [String] GitHub nickname of the authenticated user
  # @raise [ServerFailure] If authentication fails or server returns an error
  def whoami
    nick = nil
    elapsed(@loog) do
      ret = get(home.append('whoami'))
      nick = ret.body
      throw :"I know that I am @#{nick}, at #{@host}"
    end
    nick
  end

  # Get current balance of the authenticated user.
  #
  # @return [Float] The balance in zents (Ƶ), where 1 Ƶ = 1 USDT
  # @raise [ServerFailure] If authentication fails or server returns an error
  def balance
    z = nil
    elapsed(@loog) do
      ret = get(home.append('account').append('balance'))
      z = ret.body.to_f
      throw :"The balance is Ƶ#{z}, at #{@host}"
    end
    z
  end

  # Push factbase to the server to create a new job.
  #
  # @param [String] name The unique name of the product on the server
  # @param [String] data The binary data to push to the server (factbase content)
  # @param [Array<String>] meta List of metadata strings to attach to the job
  # @param [Integer] chunk_size Maximum size of one chunk
  # @raise [ServerFailure] If the push operation fails
  def push(pname, data, meta, chunk_size: DEFAULT_CHUNK_SIZE)
    raise 'The "name" of the job is nil' if pname.nil?
    raise 'The "name" of the job may not be empty' if pname.empty?
    raise 'The "data" of the job is nil' if data.nil?
    raise 'The "meta" of the job is nil' if meta.nil?
    elapsed(@loog) do
      Tempfile.open do |file|
        File.binwrite(file.path, data)
        upload(
          home.append('push').append(pname),
          file.path,
          headers.merge(
            'X-Zerocracy-Meta' => meta.map { |v| Base64.encode64(v).delete("\n") }.join(' ')
          ),
          chunk_size:
        )
      end
      throw :"Pushed #{data.bytesize} bytes to #{@host}"
    end
  end

  # Pull factbase from the server for a specific job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [String] Binary data of the factbase (can be saved to file)
  # @raise [ServerFailure] If the job doesn't exist or pull fails
  def pull(id)
    raise 'The ID of the job is nil' if id.nil?
    raise 'The ID of the job must be a positive integer' unless id.positive?
    data = ''
    elapsed(@loog) do
      Tempfile.open do |file|
        download(home.append('pull').append("#{id}.fb"), file.path)
        data = File.binread(file)
        throw :"Pulled #{data.bytesize} bytes of job ##{id} factbase at #{@host}"
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
    raise 'The ID of the job is nil' if id.nil?
    raise 'The ID of the job must be a positive integer' unless id.positive?
    fin = false
    elapsed(@loog) do
      ret = get(home.append('finished').append(id))
      fin = ret.body == 'yes'
      throw :"The job ##{id} is #{'not yet ' unless fin}finished at #{@host}#{" (#{ret.body.inspect})" unless fin}"
    end
    fin
  end

  # Read and return the stdout of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [String] The stdout, as a text
  # @raise [ServerFailure] If the job doesn't exist or retrieval fails
  def stdout(id)
    raise 'The ID of the job is nil' if id.nil?
    raise 'The ID of the job must be a positive integer' unless id.positive?
    stdout = ''
    elapsed(@loog) do
      ret = get(home.append('stdout').append("#{id}.txt"))
      stdout = ret.body
      throw :"The stdout of the job ##{id} has #{stdout.split("\n").count} lines"
    end
    stdout
  end

  # Read and return the exit code of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [Integer] The exit code
  # @raise [ServerFailure] If the job doesn't exist or retrieval fails
  def exit_code(id)
    raise 'The ID of the job is nil' if id.nil?
    raise 'The ID of the job must be a positive integer' unless id.positive?
    code = 0
    elapsed(@loog) do
      ret = get(home.append('exit').append("#{id}.txt"))
      code = ret.body.to_i
      throw :"The exit code of the job ##{id} is #{code}"
    end
    code
  end

  # Read and return the verification verdict of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [String] The verdict
  # @raise [ServerFailure] If the job doesn't exist or retrieval fails
  def verified(id)
    raise 'The ID of the job is nil' if id.nil?
    raise 'The ID of the job must be a positive integer' unless id.positive?
    verdict = ''
    elapsed(@loog) do
      ret = get(home.append('jobs').append(id).append('verified.txt'))
      verdict = ret.body
      throw :"The verdict of the job ##{id} is #{verdict.inspect}"
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
    raise 'The "pname" of the product is nil' if pname.nil?
    raise 'The "pname" of the product may not be empty' if pname.empty?
    raise 'The "owner" of the lock is nil' if owner.nil?
    elapsed(@loog) do
      ret = post(
        home.append('lock').append(pname),
        { 'owner' => owner },
        [302, 409]
      )
      throw :"Product name #{pname.inspect} locked at #{@host}" if ret.code == 302
      raise "Failed to lock #{pname.inspect} product at #{@host}, it's already locked"
    end
  end

  # Unlock the name.
  #
  # @param [String] pname The name of the job on the server
  # @param [String] owner The owner of the lock (any string)
  # @raise [ServerFailure] If the unlock operation fails
  def unlock(pname, owner)
    raise 'The "pname" of the job is nil' if pname.nil?
    raise 'The "pname" of the job may not be empty' if pname.empty?
    raise 'The "owner" of the lock is nil' if owner.nil?
    raise 'The "owner" of the lock may not be empty' if owner.empty?
    elapsed(@loog) do
      post(
        home.append('unlock').append(pname),
        { 'owner' => owner }
      )
      throw :"Job name #{pname.inspect} unlocked at #{@host}"
    end
  end

  # Get the ID of the job by the name.
  #
  # @param [String] name The name of the job on the server
  # @return [Integer] The ID of the job on the server
  # @raise [ServerFailure] If the job doesn't exist or retrieval fails
  def recent(name)
    raise 'The "name" of the job is nil' if name.nil?
    raise 'The "name" of the job may not be empty' if name.empty?
    job = nil
    elapsed(@loog) do
      ret = get(home.append('recent').append("#{name}.txt"))
      job = ret.body.to_i
      throw :"The recent \"#{name}\" job's ID is ##{job} at #{@host}"
    end
    job
  end

  # Check whether the name of the job exists on the server.
  #
  # @param [String] name The name of the product on the server
  # @return [Boolean] TRUE if such name exists
  def name_exists?(pname)
    raise 'The "pname" of the product is nil' if pname.nil?
    raise 'The "pname" of the product may not be empty' if pname.empty?
    exists = false
    elapsed(@loog) do
      ret = get(home.append('exists').append(pname))
      exists = ret.body == 'yes'
      throw :"The name #{pname.inspect} #{exists ? 'exists' : "doesn't exist"} at #{@host}"
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
    raise 'The "pname" of the durable is nil' if pname.nil?
    raise 'The "pname" of the durable may not be empty' if pname.empty?
    raise 'The "file" of the durable is nil' if file.nil?
    raise "The file '#{file}' is absent" unless File.exist?(file)
    if File.size(file) > 1024
      raise "The file '#{file}' is too big (#{File.size(file)} bytes) for durable_place(), use durable_save() instead"
    end
    id = nil
    elapsed(@loog) do
      ret = post(
        home.append('durables').append('place'),
        {
          'pname' => pname,
          'jname' => pname,
          'file' => File.basename(file),
          'zip' => File.open(file, 'rb')
        }
      )
      id = ret.headers['X-Zerocracy-DurableId'].to_i
      throw :"Durable ##{id} (#{file}, #{File.size(file)} bytes) placed for job \"#{pname}\" at #{@host}"
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
    raise 'The ID of the durable is nil' if id.nil?
    raise 'The ID of the durable must be an Integer' unless id.is_a?(Integer)
    raise 'The ID of the durable must be a positive integer' unless id.positive?
    raise 'The "file" of the durable is nil' if file.nil?
    raise "The file '#{file}' is absent" unless File.exist?(file)
    elapsed(@loog) do
      upload(home.append('durables').append(id), file, chunk_size:)
      throw :"Durable ##{id} saved #{File.size(file)} bytes to #{@host}"
    end
  end

  # Load a single durable from server to local file.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] file The local file path to save the downloaded durable
  # @raise [ServerFailure] If the load operation fails
  def durable_load(id, file)
    raise 'The ID of the durable is nil' if id.nil?
    raise 'The ID of the durable must be an Integer' unless id.is_a?(Integer)
    raise 'The ID of the durable must be a positive integer' unless id.positive?
    raise 'The "file" of the durable is nil' if file.nil?
    elapsed(@loog) do
      download(home.append('durables').append(id), file)
      throw :"Durable ##{id} loaded #{File.size(file)} bytes from #{@host}"
    end
  end

  # Lock a single durable.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] owner The owner of the lock
  # @raise [ServerFailure] If the lock operation fails
  def durable_lock(id, owner)
    raise 'The ID of the durable is nil' if id.nil?
    raise 'The ID of the durable must be an Integer' unless id.is_a?(Integer)
    raise 'The ID of the durable must be a positive integer' unless id.positive?
    raise 'The "owner" of the lock is nil' if owner.nil?
    raise 'The "owner" of the lock may not be empty' if owner.empty?
    elapsed(@loog) do
      post(
        home.append('durables').append(id).append('lock'),
        { 'owner' => owner }
      )
      throw :"Durable ##{id} locked at #{@host}"
    end
  end

  # Unlock a single durable.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] owner The owner of the lock
  # @raise [ServerFailure] If the unlock operation fails
  def durable_unlock(id, owner)
    raise 'The ID of the durable is nil' if id.nil?
    raise 'The ID of the durable must be an Integer' unless id.is_a?(Integer)
    raise 'The ID of the durable must be a positive integer' unless id.positive?
    raise 'The "owner" of the lock is nil' if owner.nil?
    raise 'The "owner" of the lock may not be empty' if owner.empty?
    elapsed(@loog) do
      post(
        home.append('durables').append(id).append('unlock'),
        { 'owner' => owner }
      )
      throw :"Durable ##{id} unlocked at #{@host}"
    end
  end

  # Find a durable by job name and file name.
  #
  # @param [String] pname The name of the job
  # @param [String] file The file name
  # @return [Integer, nil] The ID of the durable if found, nil if not found
  def durable_find(pname, file)
    raise 'The "pname" is nil' if pname.nil?
    raise 'The "pname" may not be empty' if pname.empty?
    raise 'The "file" is nil' if file.nil?
    raise 'The "file" may not be empty' if file.empty?
    id = nil
    elapsed(@loog) do
      ret = get(home.append('durables').append('find').add(jname: pname, pname:, file:), [200, 404])
      if ret.code == 200
        id = ret.body.to_i
        throw :"Found durable ##{id} for job \"#{pname}\" file \"#{file}\" at #{@host}"
      else
        throw :"Durable not found for job \"#{pname}\" file \"#{file}\" at #{@host}"
      end
    end
    id
  end

  # Transfer funds to another user.
  #
  # @param [String] recipient GitHub username of the recipient (e.g. "yegor256")
  # @param [Float] amount The amount to transfer in Ƶ (zents)
  # @param [String] summary The description/reason for the payment
  # @param [Integer] job Optional job ID to associate with this transfer
  # @return [Integer] Receipt ID for the transaction
  # @raise [ServerFailure] If the transfer fails
  def transfer(recipient, amount, summary, job: nil)
    raise 'The "recipient" is nil' if recipient.nil?
    raise 'The "amount" is nil' if amount.nil?
    raise 'The "amount" must be Float' unless amount.is_a?(Float)
    raise 'The "summary" is nil' if summary.nil?
    id = nil
    body = {
      'human' => recipient,
      'amount' => format('%0.6f', amount),
      'summary' => summary
    }
    body['job'] = job unless job.nil?
    elapsed(@loog) do
      ret = post(
        home.append('account').append('transfer'),
        body
      )
      id = ret.headers['X-Zerocracy-ReceiptId'].to_i
      throw :"Transferred Ƶ#{format('%0.6f', amount)} to @#{recipient} at #{@host}"
    end
    id
  end

  # Pay a fee associated with a job.
  #
  # @param [String] tab The category/type of the fee (use "unknown" if not sure)
  # @param [Float] amount The fee amount in Ƶ (zents)
  # @param [String] summary The description/reason for the fee
  # @param [Integer] job The ID of the job this fee is for
  # @return [Integer] Receipt ID for the fee payment
  # @raise [ServerFailure] If the payment fails
  def fee(tab, amount, summary, job)
    raise 'The "tab" is nil' if tab.nil?
    raise 'The "amount" is nil' if amount.nil?
    raise 'The "amount" must be Float' unless amount.is_a?(Float)
    raise 'The "job" is nil' if job.nil?
    raise 'The "job" must be Integer' unless job.is_a?(Integer)
    raise 'The "summary" is nil' if summary.nil?
    id = nil
    elapsed(@loog) do
      ret = post(
        home.append('account').append('fee'),
        {
          'tab' => tab,
          'amount' => format('%0.6f', amount),
          'summary' => summary,
          'job' => job.to_s
        }
      )
      id = ret.headers['X-Zerocracy-ReceiptId'].to_i
      throw :"Fee Ƶ#{format('%0.6f', amount)} paid at #{@host}"
    end
    id
  end

  # Pop the next available job from the server's queue.
  #
  # @param [String] owner Identifier of who is taking the job (any descriptive text)
  # @param [String] zip The local file path where the job's ZIP archive will be saved
  # @return [Boolean] TRUE if a job was successfully popped, FALSE if queue is empty
  # @raise [ServerFailure] If the pop operation fails
  def pop(owner, zip)
    success = false
    elapsed(@loog) do
      uri = home.append('pop').add(owner:)
      ret = get(uri, [204, 302])
      if ret.code == 204
        FileUtils.rm_f(zip)
        throw :"Nothing to pop at #{uri}"
      end
      job = ret.headers['X-Zerocracy-JobId']
      raise 'Job ID is not returned in X-Zerocracy-JobId' if job.nil?
      raise "Job ID returned in X-Zerocracy-JobId is not valid (#{job.inspect})" unless job.match?(/^[0-9]+$/)
      download(uri.add(job:), zip)
      success = true
      throw :"Popped #{File.size(zip)} bytes in ZIP archive at #{@host}"
    end
    success
  end

  # Submit a ZIP archive to finish a previously popped job.
  #
  # @param [Integer] id The ID of the job to finish
  # @param [String] zip The path to the ZIP file containing job results
  # @raise [ServerFailure] If the submission fails
  def finish(id, zip)
    raise 'The ID of the job is nil' if id.nil?
    raise 'The ID of the job must be a positive integer' unless id.positive?
    raise 'The "zip" of the job is nil' if zip.nil?
    raise "The 'zip' file is absent: #{zip}" unless File.exist?(zip)
    elapsed(@loog) do
      upload(home.append('finish').add(id:), zip)
      throw :"Pushed #{File.size(zip)} bytes to #{@host}, finished job ##{id}"
    end
  end

  # Enter a valve to cache or retrieve a computation result.
  #
  # Valves prevent duplicate computations by caching results. If a result
  # for the given badge already exists, it's returned. Otherwise, the block
  # is executed and its result is cached.
  #
  # @param [String] name Name of the product
  # @param [String] badge Unique identifier for this valve/computation
  # @param [String] why The reason/description for entering this valve
  # @param [nil|Integer] job Optional job ID to associate with this valve
  # @yield Block that computes the result if not cached
  # @return [String] The cached result or newly computed result from the block
  # @raise [ServerFailure] If the valve operation fails
  def enter(pname, badge, why, job)
    elapsed(@loog, good: "Entered valve #{badge} to #{pname}") do
      retry_it do
        ret = get(home.append('valves').append('result').add(badge:), [200, 204])
        return ret.body if ret.code == 200
        r = yield
        uri = home.append('valves').append('add')
        uri = uri.add(job:) unless job.nil?
        post(
          uri,
          {
            'name' => pname,
            'pname' => pname,
            'badge' => badge,
            'why' => why,
            'result' => r.to_s
          }
        )
        r
      end
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
    elapsed(@loog) do
      token = get(home.append('csrf')).body
      throw :"CSRF token retrieved (#{token.length} chars)"
    end
    token
  end

  private

  # Get the user agent string for HTTP requests.
  #
  # @return [String] The user agent string
  def user_agent
    "baza.rb #{BazaRb::VERSION}"
  end

  # Get default headers for HTTP requests.
  #
  # @return [Hash] The default headers including User-Agent, Connection, and authentication token
  def headers
    {
      'User-Agent' => user_agent,
      'Connection' => 'close',
      'X-Zerocracy-Token' => @token
    }
  end

  # Decompress gzipped data.
  #
  # @param [String] data The gzipped data to decompress
  # @return [String] The decompressed data
  def unzip(data)
    io = StringIO.new(data)
    gz = Zlib::GzipReader.new(io)
    gz.read
  rescue Zlib::GzipFile::Error => e
    raise BadCompression, "Failed to unzip #{data.bytesize} bytes: #{e.message}"
  end

  # Compress request parameters with gzip.
  #
  # @param [Hash] params The request parameters with :body and :headers keys
  # @return [Hash] The modified parameters with compressed body and updated headers
  def zipped(params)
    io = StringIO.new
    gz = Zlib::GzipWriter.new(io)
    gz.write(params.fetch(:body))
    gz.close
    body = io.string
    headers = params
      .fetch(:headers)
      .merge(
        {
          'Content-Type' => 'application/zip',
          'Content-Encoding' => 'gzip',
          'Content-Length' => body.bytesize
        }
      )
    params.merge(body:, headers:)
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
  def retry_it(&)
    with_retries(max_tries: @retries, rescue: TimedOut, &)
  end

  # Execute a block with retries on 429 status codes.
  #
  # @yield The block to execute with retries
  # @return [Object] The result of the block execution
  def retry_if_server_busy(&)
    attempt = 0
    loop do
      ret = yield
      if ret.code == 429 && attempt < @retries
        attempt += 1
        seconds = @pause * (2**attempt)
        @loog.info("Server seems to be busy, will sleep for #{seconds} (attempt no.#{attempt})...")
        sleep(seconds)
        next
      end
      return ret
    end
  end

  # Execute a block with retries on 500 status codes.
  #
  # @yield The block to execute with retries
  # @return [Object] The result of the block execution
  def retry_if_server_failed(&)
    attempt = 0
    loop do
      ret = yield
      if ret.code.between?(429, 599) && attempt < @retries
        attempt += 1
        seconds = 2 * @pause * attempt
        @loog.info("Server seems to be in trouble, will sleep for #{seconds} (attempt no.#{attempt})...")
        sleep(seconds)
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
      raise TimedOut, msg
    end
    log = "#{mtd} #{url} -> #{ret.code} (#{format('%0.2f', ret.total_time)}s)"
    if allowed.include?(ret.code)
      @loog.debug(log)
      return ret
    end
    @loog.debug("#{log}\n  #{(ret.headers || {}).map { |k, v| "#{k}: #{v}" }.join("\n  ")}")
    headers = ret.headers || {}
    msg = [
      "Invalid response code ##{ret.code} ",
      "at #{mtd} #{url}",
      headers['X-Zerocracy-Flash'] ? " (#{headers['X-Zerocracy-Flash'].inspect})" : ''
    ].join
    case ret.code
    when 500
      msg +=
        ", most probably it's an internal error on the server, " \
        'please report this to https://github.com/zerocracy/baza.rb'
    when 503
      msg +=
        ", most probably it's an internal error on the server (#{headers['X-Zerocracy-Failure'].inspect}), " \
        'please report this to https://github.com/zerocracy/baza.rb'
    when 404
      msg +=
        ", most probably you are trying to reach a wrong server, which doesn't " \
        'have the URL that it is expected to have'
    when 0
      msg +=
        ', most likely a connection failure, timeout, or SSL error ' \
        "(r:#{ret.return_code}, m:#{ret.return_message})"
    end
    @loog.error(msg)
    raise ServerFailure, msg
  end

  # Make a GET request.
  #
  # @param [Iri] uri The URI to send the request to
  # @param [Array<Integer>] allowed List of allowed HTTP response codes
  # @return [Typhoeus::Response] The HTTP response
  # @raise [ServerFailure] If the response code is not in the allowed list
  def get(uri, allowed = [200])
    retry_it do
      checked(
        retry_if_server_failed do
          retry_if_server_busy do
            Typhoeus::Request.get(
              uri.to_s,
              headers:,
              connecttimeout: @timeout,
              timeout: @timeout
            )
          end
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
    retry_it do
      checked(
        retry_if_server_failed do
          Typhoeus::Request.post(
            uri.to_s,
            body: params.merge('_csrf' => csrf),
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
    elapsed(@loog) do
      loop do
        slice = ''
        ret = nil
        retry_if_server_busy do
          retry_if_server_failed do
            slice = ''
            request = Typhoeus::Request.new(
              uri.to_s,
              method: :get,
              headers: headers.merge(
                'Accept' => '*',
                'Accept-Encoding' => 'gzip',
                'Range' => "bytes=#{File.size(file)}-"
              ),
              connecttimeout: @timeout,
              timeout: @timeout
            )
            request.on_body do |data|
              slice += data
            end
            retry_it do
              request.run
            end
            ret = request.response
          end
        end
        msg = [
          "GET #{uri.to_uri.path} #{ret.code}",
          "#{slice.bytesize} bytes",
          ('in gzip' if ret.headers['Content-Encoding'] == 'gzip'),
          ("ranged as #{ret.headers['Content-Range'].inspect}" if ret.headers['Content-Range'])
        ]
        ret = checked(ret, [200, 206, 204, 302])
        if blanks.include?(ret.code)
          sleep(2)
          next
        end
        if ret.headers['Content-Encoding'] == 'gzip'
          begin
            slice = unzip(slice)
            msg << "unzipped to #{slice.bytesize} bytes"
          rescue BazaRb::BadCompression => e
            raise BazaRb::BadCompression, "#{msg.compact.join(', ')} (#{e.message})"
          end
        end
        File.open(file, 'ab') do |f|
          msg << "added to existed #{File.size(file)} bytes"
          f.write(slice)
        end
        @loog.debug(msg.compact.join(', '))
        break if ret.code == 200
        _, v = ret.headers['Content-Range'].split
        range, total = v.split('/')
        raise "Total size is not valid (#{total.inspect})" unless total.match?(/^\*|[0-9]+$/)
        _b, e = range.split('-')
        raise "Range is not valid (#{range.inspect})" unless e.match?(/^[0-9]+$/)
        len = ret.headers['Content-Length'].to_i
        break if e.to_i == total.to_i - 1
        break if total == '0'
        chunk += 1
        sleep(1) if len.zero?
      end
      throw :"Downloaded #{File.size(file)} bytes in #{chunk + 1} chunks from #{uri}"
    end
  end

  # Upload file via PUT, using chunked uploads for large files.
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
      headers: headers.merge(extra).merge(
        'Content-Type' => 'application/octet-stream'
      )
    }
    total = File.size(file)
    chunk = 0
    sent = 0
    elapsed(@loog) do
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
        params[:headers]['Content-Length'] = slice.bytesize
        params = zipped(params) if @compress
        ret =
          retry_it do
            checked(
              retry_if_server_failed do
                Typhoeus::Request.put(
                  uri.to_s,
                  params
                )
              end
            )
          end
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
      throw :"Uploaded #{sent} bytes to #{uri}#{" in #{chunk + 1} chunks" if chunk.positive?}"
    end
  end
end
