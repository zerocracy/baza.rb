# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'tmpdir'
require 'webmock/minitest'
require_relative '../lib/baza-rb'
require_relative 'test__helper'

class TestUploadFinalChunk < Minitest::Test
  def test_resumes_at_the_terminating_chunk
    [4, 5].each do |length|
      received = []
      last = (length - 1) / 2
      stub_request(:put, 'https://example.org/durables/42').to_return do |request|
        chunk = Integer(request.headers.fetch('X-Zerocracy-Chunk'), 10)
        received << [chunk, request.body]
        if chunk == last
          {
            status: 400,
            headers: { 'X-Zerocracy-Flash' => "Expecting chunk ##{last + 1} (#{length}b are here), received ##{chunk}" }
          }
        else
          { status: 200 }
        end
      end
      client = BazaRb.new('example.org', 443, 'test', compress: false, retries: 1, pause: 0)
      Dir.mktmpdir do |dir|
        file = File.join(dir, 'sample.txt')
        File.write(file, 'x' * length)
        client.durable_save(42, file, chunk_size: 2)
      end
      assert_equal((0..(last + 1)).to_a, received.map(&:first))
      assert_equal('', received.last.last)
    end
  end
end
