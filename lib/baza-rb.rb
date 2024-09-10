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

require 'base64'
require 'elapsed'
require 'fileutils'
require 'iri'
require 'loog'
require 'retries'
require 'tago'
require 'typhoeus'
require_relative 'baza-rb/version'

# Interface to the API of zerocracy.com.
#
# You make an instance of this class and then call one of its methods.
# The object will make HTTP request to api.zerocracy.com and interpret the
# results returned.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024 Yegor Bugayenko
# License:: MIT
class BazaRb
  # When the server failed (503).
  class ServerFailure < StandardError; end

  # When request timeout.
  class TimedOut < StandardError; end

  # Unexpected response arrived from the server.
  class BadResponse < StandardError; end

  # Ctor.
  #
  # @param [String] host Host name
  # @param [Integer] port TCP port
  # @param [String] token Secret token of zerocracy.com
  # @param [Boolean] ssl Should we use SSL?
  # @param [Float] timeout Connect timeout and session timeout, in seconds
  # @param [Integer] retries How many times to retry on connection failure?
  # @param [Loog] loog The logging facility
  # @param [Boolean] compress Set to TRUE if need to use GZIP while pulling and sending
  def initialize(host, port, token, ssl: true, timeout: 30, retries: 3, loog: Loog::NULL, compress: true)
    @host = host
    @port = port
    @ssl = ssl
    @token = token
    @timeout = timeout
    @loog = loog
    @retries = retries
    @compress = compress
  end

  # Push factbase to the server.
  #
  # @param [String] name The name of the job on the server
  # @param [Bytes] data The data to push to the server (binary)
  # @param [Array<String>] meta List of metas, possibly empty
  # @return [Integer] Job ID on the server
  def push(name, data, meta)
    raise 'The "name" of the job is nil' if name.nil?
    raise 'The "name" of the job may not be empty' if name.empty?
    raise 'The "data" of the job is nil' if data.nil?
    raise 'The "meta" of the job is nil' if meta.nil?
    id = 0
    hdrs = headers.merge(
      'Content-Type' => 'application/octet-stream',
      'Content-Length' => data.size
    )
    unless meta.empty?
      hdrs = hdrs.merge('X-Zerocracy-Meta' => meta.map { |v| Base64.encode64(v).gsub("\n", '') }.join(' '))
    end
    params = {
      connecttimeout: @timeout,
      timeout: @timeout,
      body: data,
      headers: hdrs
    }
    elapsed(@loog) do
      ret =
        with_retries(max_tries: @retries, rescue: TimedOut) do
          checked(
            Typhoeus::Request.put(
              home.append('push').append(name).to_s,
              @compress ? zipped(params) : params
            )
          )
        end
      id = ret.body.to_i
      throw :"Pushed #{data.size} bytes to #{@host}, job ID is ##{id}"
    end
    id
  end

  # Pull factbase from the server.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [Bytes] Binary data pulled
  def pull(id)
    raise 'The ID of the job is nil' if id.nil?
    raise 'The ID of the job must be a positive integer' unless id.positive?
    data = 0
    elapsed(@loog) do
      Tempfile.open do |file|
        File.open(file, 'wb') do |f|
          request = Typhoeus::Request.new(
            home.append('pull').append("#{id}.fb").to_s,
            method: :get,
            headers: headers.merge(
              'Accept' => 'application/zip, application/factbase'
            ),
            accept_encoding: 'gzip',
            connecttimeout: @timeout,
            timeout: @timeout
          )
          request.on_body do |chunk|
            f.write(chunk)
          end
          with_retries(max_tries: @retries, rescue: TimedOut) do
            request.run
          end
          checked(request.response)
        end
        data = File.binread(file)
        throw :"Pulled #{data.size} bytes of job ##{id} factbase at #{@host}"
      end
    end
    data
  end

  # The job with this ID is finished already?
  #
  # @param [Integer] id The ID of the job on the server
  # @return [Boolean] TRUE if the job is already finished
  def finished?(id)
    raise 'The ID of the job is nil' if id.nil?
    raise 'The ID of the job must be a positive integer' unless id.positive?
    finished = false
    elapsed(@loog) do
      ret =
        with_retries(max_tries: @retries, rescue: TimedOut) do
          checked(
            Typhoeus::Request.get(
              home.append('finished').append(id).to_s,
              headers:
            )
          )
        end
      finished = ret.body == 'yes'
      throw :"The job ##{id} is #{finished ? '' : 'not yet '}finished at #{@host}"
    end
    finished
  end

  # Read and return the stdout of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [String] The stdout, as a text
  def stdout(id)
    raise 'The ID of the job is nil' if id.nil?
    raise 'The ID of the job must be a positive integer' unless id.positive?
    stdout = ''
    elapsed(@loog) do
      ret =
        with_retries(max_tries: @retries, rescue: TimedOut) do
          checked(
            Typhoeus::Request.get(
              home.append('stdout').append("#{id}.txt").to_s,
              headers:
            )
          )
        end
      stdout = ret.body
      throw :"The stdout of the job ##{id} has #{stdout.split("\n").count} lines"
    end
    stdout
  end

  # Read and return the exit code of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [Integer] The exit code
  def exit_code(id)
    raise 'The ID of the job is nil' if id.nil?
    raise 'The ID of the job must be a positive integer' unless id.positive?
    code = 0
    elapsed(@loog) do
      ret =
        with_retries(max_tries: @retries, rescue: TimedOut) do
          checked(
            Typhoeus::Request.get(
              home.append('exit').append("#{id}.txt").to_s,
              headers:
            )
          )
        end
      code = ret.body.to_i
      throw :"The exit code of the job ##{id} is #{code}"
    end
    code
  end

  # Read and return the verification verdict of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [String] The verdict
  def verified(id)
    raise 'The ID of the job is nil' if id.nil?
    raise 'The ID of the job must be a positive integer' unless id.positive?
    verdict = 0
    elapsed(@loog) do
      ret =
        with_retries(max_tries: @retries, rescue: TimedOut) do
          checked(
            Typhoeus::Request.get(
              home.append('jobs').append(id).append('verified.txt').to_s,
              headers:
            )
          )
        end
      verdict = ret.body
      throw :"The verdict of the job ##{id} is #{verdict.inspect}"
    end
    verdict
  end

  # Lock the name.
  #
  # @param [String] name The name of the job on the server
  # @param [String] owner The owner of the lock (any string)
  def lock(name, owner)
    raise 'The "name" of the job is nil' if name.nil?
    raise 'The "name" of the job may not be empty' if name.empty?
    raise 'The "owner" of the lock is nil' if owner.nil?
    elapsed(@loog) do
      with_retries(max_tries: @retries, rescue: TimedOut) do
        checked(
          Typhoeus::Request.get(
            home.append('lock').append(name).add(owner:).to_s,
            headers:
          ),
          302
        )
      end
      throw :"Job name '#{name}' locked at #{@host}"
    end
  end

  # Unlock the name.
  #
  # @param [String] name The name of the job on the server
  # @param [String] owner The owner of the lock (any string)
  def unlock(name, owner)
    raise 'The "name" of the job is nil' if name.nil?
    raise 'The "name" of the job may not be empty' if name.empty?
    raise 'The "owner" of the lock is nil' if owner.nil?
    elapsed(@loog) do
      with_retries(max_tries: @retries, rescue: TimedOut) do
        checked(
          Typhoeus::Request.get(
            home.append('unlock').append(name).add(owner:).to_s,
            headers:
          ),
          302
        )
      end
      throw :"Job name '#{name}' unlocked at #{@host}"
    end
  end

  # Get the ID of the job by the name.
  #
  # @param [String] name The name of the job on the server
  # @return [Integer] The ID of the job on the server
  def recent(name)
    raise 'The "name" of the job is nil' if name.nil?
    raise 'The "name" of the job may not be empty' if name.empty?
    job = 0
    elapsed(@loog) do
      ret =
        with_retries(max_tries: @retries, rescue: TimedOut) do
          checked(
            Typhoeus::Request.get(
              home.append('recent').append("#{name}.txt").to_s,
              headers:
            )
          )
        end
      job = ret.body.to_i
      throw :"The recent \"#{name}\" job's ID is ##{job} at #{@host}"
    end
    job
  end

  # Check whether the name of the job exists on the server.
  #
  # @param [String] name The name of the job on the server
  # @return [Boolean] TRUE if such name exists
  def name_exists?(name)
    raise 'The "name" of the job is nil' if name.nil?
    raise 'The "name" of the job may not be empty' if name.empty?
    exists = 0
    elapsed(@loog) do
      ret =
        with_retries(max_tries: @retries, rescue: TimedOut) do
          checked(
            Typhoeus::Request.get(
              home.append('exists').append(name).to_s,
              headers:
            )
          )
        end
      exists = ret.body == 'yes'
      throw :"The name \"#{name}\" #{exists ? 'exists' : "doesn't exist"} at #{@host}"
    end
    exists
  end

  # Place a single durable.
  #
  # @param [String] jname The name of the job on the server
  # @param [String] file The file name
  def durable_place(jname, file)
    raise 'The "jname" of the durable is nil' if jname.nil?
    raise 'The "jname" of the durable may not be empty' if jname.empty?
    raise 'The "file" of the durable is nil' if file.nil?
    raise "The file '#{file}' is absent" unless File.exist?(file)
    id = nil
    elapsed(@loog) do
      ret =
        with_retries(max_tries: @retries, rescue: TimedOut) do
          checked(
            Typhoeus::Request.post(
              home.append('durables').append('place').to_s,
              body: {
                'jname' => jname,
                'file' => File.basename(file),
                'zip' => File.open(file, 'rb')
              },
              headers:,
              connecttimeout: @timeout,
              timeout: @timeout
            ),
            302
          )
        end
      id = ret.headers['X-Zerocracy-DurableId'].to_i
      throw :"Durable ##{id} (#{file}) placed for job \"#{jname}\" at #{@host}"
    end
    id
  end

  # Save a single durable from local file to server.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] file The file to upload
  def durable_save(id, file)
    raise 'The ID of the durable is nil' if id.nil?
    raise 'The ID of the durable must be a positive integer' unless id.positive?
    raise 'The "file" of the durable is nil' if file.nil?
    raise "The file '#{file}' is absent" unless File.exist?(file)
    elapsed(@loog) do
      with_retries(max_tries: @retries, rescue: TimedOut) do
        checked(
          Typhoeus::Request.put(
            home.append('durables').append(id).to_s,
            body: File.binread(file),
            headers:,
            connecttimeout: @timeout,
            timeout: @timeout
          )
        )
      end
      throw :"Durable ##{id} saved #{File.size(file)} bytes to #{@host}"
    end
  end

  # Load a single durable from server to local file.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] file The file to upload
  def durable_load(id, file)
    raise 'The ID of the durable is nil' if id.nil?
    raise 'The ID of the durable must be a positive integer' unless id.positive?
    raise 'The "file" of the durable is nil' if file.nil?
    FileUtils.mkdir_p(File.dirname(file))
    elapsed(@loog) do
      File.open(file, 'wb') do |f|
        request = Typhoeus::Request.new(
          home.append('durables').append(id).to_s,
          method: :get,
          headers: headers.merge(
            'Accept' => 'application/octet-stream'
          ),
          connecttimeout: @timeout,
          timeout: @timeout
        )
        request.on_body do |chunk|
          f.write(chunk)
        end
        with_retries(max_tries: @retries, rescue: TimedOut) do
          request.run
        end
        checked(request.response)
      end
      throw :"Durable ##{id} loaded #{File.size(file)} bytes from #{@host}"
    end
  end

  # Lock a single durable.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] owner The owner of the lock
  def durable_lock(id, owner)
    raise 'The ID of the durable is nil' if id.nil?
    raise 'The ID of the durable must be a positive integer' unless id.positive?
    raise 'The "owner" of the lock is nil' if owner.nil?
    raise 'The "owner" of the lock may not be empty' if owner.empty?
    elapsed(@loog) do
      with_retries(max_tries: @retries, rescue: TimedOut) do
        checked(
          Typhoeus::Request.get(
            home.append('durables').append(id).append('lock').add(owner:).to_s,
            headers:
          ),
          302
        )
      end
      throw :"Durable ##{id} locked at #{@host}"
    end
  end

  # Unlock a single durable.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] owner The owner of the lock
  def durable_unlock(id, owner)
    raise 'The ID of the durable is nil' if id.nil?
    raise 'The ID of the durable must be a positive integer' unless id.positive?
    raise 'The "owner" of the lock is nil' if owner.nil?
    raise 'The "owner" of the lock may not be empty' if owner.empty?
    elapsed(@loog) do
      with_retries(max_tries: @retries, rescue: TimedOut) do
        checked(
          Typhoeus::Request.get(
            home.append('durables').append(id).append('unlock').add(owner:).to_s,
            headers:
          ),
          302
        )
      end
      throw :"Durable ##{id} unlocked at #{@host}"
    end
  end

  # Pop job from the server.
  #
  # @param [String] owner Who is acting (could be any text)
  # @param [String] zip The path to ZIP archive to take
  # @return [Boolean] TRUE if job taken, otherwise false
  def pop(owner, zip)
    raise 'The "zip" of the job is nil' if zip.nil?
    success = false
    FileUtils.rm_f(zip)
    elapsed(@loog) do
      File.open(zip, 'wb') do |f|
        request = Typhoeus::Request.new(
          home.append('pop').add(owner:).to_s,
          method: :get,
          headers: headers.merge(
            'Accept' => 'application/octet-stream'
          ),
          connecttimeout: @timeout,
          timeout: @timeout
        )
        request.on_body do |chunk|
          f.write(chunk)
        end
        with_retries(max_tries: @retries, rescue: TimedOut) do
          request.run
        end
        ret = request.response
        checked(ret, [200, 204])
        success = ret.code == 200
      end
      unless success
        FileUtils.rm_f(zip)
        throw :"Nothing to pop at #{@host}"
      end
      throw :"Popped #{File.size(zip)} bytes in ZIP archive at #{@host}"
    end
    success
  end

  # Submit a ZIP archvie to finish a job.
  #
  # @param [Integer] id The ID of the job on the server
  # @param [String] zip The path to the ZIP file with the content of the archive
  def finish(id, zip)
    raise 'The "id" of the job is nil' if id.nil?
    raise 'The "id" of the job must be an integer' unless id.is_a?(Integer)
    raise 'The "zip" of the job is nil' if zip.nil?
    raise "The 'zip' file is absent: #{zip}" unless File.exist?(zip)
    elapsed(@loog) do
      with_retries(max_tries: @retries, rescue: TimedOut) do
        checked(
          Typhoeus::Request.put(
            home.append('finish').add(id:).to_s,
            connecttimeout: @timeout,
            timeout: @timeout,
            body: File.binread(zip),
            headers: headers.merge(
              'Content-Type' => 'application/octet-stream',
              'Content-Length' => File.size(zip)
            )
          )
        )
      end
      throw :"Pushed #{File.size(zip)} bytes to #{@host}, finished job ##{id}"
    end
  end

  private

  def headers
    {
      'User-Agent' => "baza.rb #{BazaRb::VERSION}",
      'Connection' => 'close',
      'X-Zerocracy-Token' => @token
    }
  end

  def zipped(params)
    body = gzip(params.fetch(:body))
    headers = params
      .fetch(:headers)
      .merge(
        {
          'Content-Type' => 'application/zip',
          'Content-Encoding' => 'gzip',
          'Content-Length' => body.size
        }
      )
    params.merge(body:, headers:)
  end

  def gzip(data)
    ''.dup.tap do |result|
      io = StringIO.new(result)
      gz = Zlib::GzipWriter.new(io)
      gz.write(data)
      gz.close
    end
  end

  def home
    Iri.new('')
      .host(@host)
      .port(@port)
      .scheme(@ssl ? 'https' : 'http')
  end

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
        ', most probably it\'s an internal error on the server, ' \
        'please report this to https://github.com/zerocracy/baza'
    when 503
      msg +=
        ", most probably it's an internal error on the server (#{headers['X-Zerocracy-Failure'].inspect}), " \
        'please report this to https://github.com/zerocracy/baza.rb'
    when 404
      msg +=
        ', most probably you are trying to reach a wrong server, which doesn\'t ' \
        'have the URL that it is expected to have'
    when 0
      msg += ', most likely an internal error'
    end
    @loog.error(msg)
    raise ServerFailure, msg
  end
end
