# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'factbase'
require_relative '../baza-rb'
require_relative 'version'

# Fake implementation of the Zerocracy API client for testing.
#
# This class implements the same public interface as BazaRb but doesn't
# make any network connections. Instead, it returns predefined fake values
# and validates inputs to help catch errors during testing.
#
# @example Using in tests
#   baza = BazaRb::Fake.new
#   assert_equal 'torvalds', baza.whoami
#   assert_equal 42, baza.push('test-job', 'data', [])
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024-2026 Yegor Bugayenko
# License:: MIT
class BazaRb::Fake
  include BazaRb::Validation

  # Get GitHub login name of the logged in user.
  #
  # @return [String] Always returns 'torvalds' for testing
  def whoami
    'torvalds'
  end

  # Push factbase to the server.
  #
  # @param [String] name The unique name of the job on the server
  # @param [String] data The binary data to push to the server
  # @param [Array<String>] meta List of metadata strings to attach to the job
  # @param [Integer] chunk_size Size of each chunk in bytes (accepted for signature parity with BazaRb#push)
  # @return [Integer] Always returns 42 as the fake job ID
  def push(name, data, meta, chunk_size: BazaRb::DEFAULT_CHUNK_SIZE) # rubocop:disable Lint/UnusedMethodArgument
    valname(name)
    valdata(data)
    valarray(meta, context: 'meta')
    42
  end

  # Pull factbase from the server.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [String] Returns an empty factbase export for testing
  def pull(id)
    valnum(id)
    Factbase.new.export
  end

  # Check if the job with this ID is finished already.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [Boolean] Always returns TRUE for testing
  def finished?(id)
    valnum(id)
    true
  end

  # Read and return the stdout of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [String] The stdout, as a text
  def stdout(id)
    valnum(id)
    'Fake stdout output'
  end

  # Read and return the exit code of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [Integer] The exit code
  def exit_code(id)
    valnum(id)
    0
  end

  # Read and return the verification verdict of the job.
  #
  # @param [Integer] id The ID of the job on the server
  # @return [String] The verdict
  def verified(id)
    valnum(id)
    'fake-verdict'
  end

  # Lock the name.
  #
  # @param [String] name The name of the job on the server
  # @param [String] owner The owner of the lock (any string)
  def lock(name, owner)
    valname(name)
    valowner(owner)
  end

  # Unlock the name.
  #
  # @param [String] name The name of the job on the server
  # @param [String] owner The owner of the lock (any string)
  def unlock(name, owner)
    valname(name)
    valowner(owner)
  end

  # Get the ID of the job by the name.
  #
  # @param [String] name The name of the job on the server
  # @return [Integer] The ID of the job on the server
  def recent(name)
    valname(name)
    42
  end

  # Check whether the name of the job exists on the server.
  #
  # @param [String] name The name of the job on the server
  # @return [Boolean] TRUE if such name exists
  def name_exists?(name)
    valname(name)
    true
  end

  # Find a single durable.
  #
  # @param [String] pname The name of the job on the server
  # @param [String] file The file name
  # @return [Integer] Always returns 42 as the fake durable ID
  def durable_find(pname, file)
    valname(pname)
    raise(BazaRb::ValidationError, 'The "file" is nil') if file.nil?
    raise(BazaRb::ValidationError, 'The "file" may not be empty') if file.empty?
    42
  end

  # Place a single durable file on the server.
  #
  # @param [String] pname The name of the job on the server
  # @param [String] file The path to the file to upload
  # @return [Integer] Always returns 42 as the fake durable ID
  def durable_place(pname, file)
    valname(pname)
    valfile(file, must_exist: true)
    42
  end

  # Save a single durable from local file to server.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] file The file to upload
  # @param [Integer] chunk_size Size of each chunk in bytes (accepted for signature parity with BazaRb#durable_save)
  def durable_save(id, file, chunk_size: BazaRb::DEFAULT_CHUNK_SIZE) # rubocop:disable Lint/UnusedMethodArgument
    valnum(id, context: 'durable')
    valfile(file, must_exist: true)
  end

  # Load a single durable from server to local file.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] file The local file path to save the downloaded durable
  def durable_load(id, file)
    valnum(id, context: 'durable')
    valfile(file)
  end

  # Lock a single durable.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] owner The owner of the lock
  def durable_lock(id, owner)
    valnum(id, context: 'durable')
    valowner(owner)
  end

  # Unlock a single durable.
  #
  # @param [Integer] id The ID of the durable
  # @param [String] owner The owner of the lock
  def durable_unlock(id, owner)
    valnum(id, context: 'durable')
    valowner(owner)
  end

  # Get current balance of the authenticated user.
  #
  # @return [Float] Always returns 3.14 zents for testing
  def balance
    3.14
  end

  # Transfer funds to another user.
  #
  # @param [String] recipient GitHub username of the recipient
  # @param [Float] amount The amount to transfer in ƶ (zents)
  # @param [String] summary The description/reason for the payment
  # @param [Integer] job Optional job ID to associate with this transfer
  # @param [String] badge Optional idempotency key for deduping the payment
  # @return [Integer] Always returns 42 as the fake receipt ID
  def transfer(recipient, amount, summary, job: nil, badge: nil)
    raise(BazaRb::ValidationError, 'The "recipient" is nil') if recipient.nil?
    unless recipient.match?(/\A[a-zA-Z0-9-]+\z/)
      raise(BazaRb::ValidationError, "The recipient #{recipient.inspect} is not valid")
    end
    raise(BazaRb::ValidationError, 'The "amount" is nil') if amount.nil?
    unless amount.is_a?(Float) || amount.is_a?(BigDecimal)
      raise(BazaRb::ValidationError, 'The "amount" must be Float or BigDecimal')
    end
    raise(BazaRb::ValidationError, 'The "amount" must be positive') unless amount.positive?
    raise(BazaRb::ValidationError, 'The "summary" is nil') if summary.nil?
    raise(BazaRb::ValidationError, "The summary #{summary.inspect} is empty") if summary.empty?
    valnum(job) unless job.nil?
    raise(BazaRb::ValidationError, 'The "badge" must be a String') if !badge.nil? && !badge.is_a?(String)
    42
  end

  # Pay a fee associated with a job.
  #
  # @param [String] tab The category/type of the fee
  # @param [Float, BigDecimal] amount The fee amount in ƶ (zents)
  # @param [String] summary The description/reason for the fee
  # @param [Integer] job The ID of the job this fee is for
  # @return [Integer] Always returns 42 as the fake receipt ID
  def fee(tab, amount, summary, job)
    raise(BazaRb::ValidationError, 'The "tab" is nil') if tab.nil?
    raise(BazaRb::ValidationError, 'The "amount" is nil') if amount.nil?
    unless amount.is_a?(Float) || amount.is_a?(BigDecimal)
      raise(BazaRb::ValidationError, 'The "amount" must be Float or BigDecimal')
    end
    raise(BazaRb::ValidationError, 'The "amount" must be positive') unless amount.positive?
    raise(BazaRb::ValidationError, 'The "job" is nil') if job.nil?
    raise(BazaRb::ValidationError, 'The "job" must be Integer') unless job.is_a?(Integer)
    raise(BazaRb::ValidationError, 'The "job" must be positive') unless job.positive?
    raise(BazaRb::ValidationError, 'The "summary" is nil') if summary.nil?
    42
  end

  # Enter a valve to cache or retrieve a computation result.
  #
  # @param [String] name Name of the job
  # @param [String] badge Unique identifier for this valve
  # @param [String] why The reason/description for entering this valve
  # @param [nil|Integer] job Optional job ID to associate with this valve
  # @yield Block that computes the result
  # @return [String] Always executes and returns the block's result
  def enter(name, badge, why, job)
    valname(name)
    raise(BazaRb::ValidationError, "The badge '#{badge}' is not valid") unless badge.match?(/\A[a-zA-Z0-9_-]+\z/)
    raise(BazaRb::ValidationError, 'The reason cannot be empty') if why.empty?
    valnum(job) unless job.nil?
    yield
  end

  # Get CSRF token from the server for authenticated requests.
  #
  # @return [String] Always returns 'fake-csrf-token' for testing
  def csrf
    'fake-csrf-token'
  end
end
