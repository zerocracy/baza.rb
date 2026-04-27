# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'tmpdir'
require_relative 'test__helper'
require_relative '../lib/baza-rb'

# Repro for zerocracy/baza.rb issue #109.
# When the server loses chunk state mid-upload (e.g. after a reboot),
# it answers subsequent chunk PUTs with HTTP 400
# "Expecting chunk #0 (0b are here), received #N".
# Current client raises BazaRb::ServerFailure instead of resetting and
# resuming the upload from chunk #0.
class TestBazaRbUploadRestart < Minitest::Test
  def test_upload_resumes_after_server_state_loss
    WebMock.disable_net_connect!
    baza = BazaRb.new('example.org', 443, '000', loog: Loog::NULL, compress: false)
    # Chunk #0 is accepted on the first try.
    stub_request(:put, 'https://example.org/durables/42')
      .with(headers: { 'X-Zerocracy-Chunk' => '0' }).to_return(status: 200)
    # Server reboots between chunks. Chunk #1 first hits the production
    # error from issue #109; on the replay (after the client resets to
    # chunk #0) it succeeds.
    stub_request(:put, 'https://example.org/durables/42')
      .with(headers: { 'X-Zerocracy-Chunk' => '1' })
      .to_return(
        {
          status: 400,
          headers: { 'X-Zerocracy-Failure' => 'Expecting chunk #0 (0b are here), received #1' }
        },
        { status: 200 }
      )
    # The upload loop sends a trailing empty PUT after the last data chunk
    # as a finalization signal (the server treats it as commit).
    stub_request(:put, 'https://example.org/durables/42')
      .with(headers: { 'X-Zerocracy-Chunk' => '2' })
      .to_return(status: 200)
    Dir.mktmpdir do |dir|
      file = File.join(dir, 'big.bin')
      File.binwrite(file, 'x' * (BazaRb::DEFAULT_CHUNK_SIZE + 100)) # 2 chunks
      # Desired behaviour: client reads the "Expecting chunk #0" hint,
      # resets its chunk counter, and resumes the upload from the start.
      # Today this assertion fails because durable_save propagates
      # BazaRb::ServerFailure from the rejected PUT.
      baza.durable_save(42, file)
    end
    # Confirm the client actually restarted from chunk #0 (sent it twice:
    # once in the original attempt, once after reset).
    assert_requested :put, 'https://example.org/durables/42',
                     headers: { 'X-Zerocracy-Chunk' => '0' }, times: 2
  end
end
