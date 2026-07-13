# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'stringio'
require 'zlib'

# Compression helpers for BazaRb.
#
# Provides gzip compression and decompression for request bodies
# and response data.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024-2026 Yegor Bugayenko
# License:: MIT
class BazaRb
  # Compression helpers.
  module Compress
    LIMIT_UNCOMPRESSED = 10_485_760

    # Decompress gzipped data.
    #
    # @param [String] data The gzipped data to decompress
    # @return [String] The decompressed data
    # @raise [BadCompression] If the data is not valid gzip
    def unzip(data)
      unzipped = StringIO.new
      Zlib::GzipReader.new(StringIO.new(data)).each(4096) do |chunk|
        unzipped.write(chunk)
        next unless unzipped.length > LIMIT_UNCOMPRESSED
        raise(BadCompression, "Uncompressed size #{unzipped.length} exceeds limit #{LIMIT_UNCOMPRESSED}")
      end
      unzipped.string
    rescue Zlib::GzipFile::Error => e
      raise(BadCompression, "Failed to unzip #{data.bytesize} bytes: #{e.message}")
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
      params.merge(
        body:,
        headers: params.fetch(:headers).merge({ 'Content-Encoding' => 'gzip', 'Content-Length' => body.bytesize })
      )
    end
  end
end
