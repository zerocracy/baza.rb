# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT
# Copyright:: Copyright (c) 2024-2026 Yegor Bugayenko

# Shared validation methods for BazaRb and BazaRb::Fake.
#
# Every method here raises BazaRb::ValidationError on invalid input.
# By using the same module in both real and fake implementations, we
# guarantee they reject the same set of invalid inputs.
class BazaRb
  # Validation helpers.
  module Validation
    # Validate a product/job/durable name (pname).
    #
    # @param [String, nil] name The name to validate
    # @param [String] context Human-readable field name for error messages
    # @raise [BazaRb::ValidationError] If validation fails
    def valname(name, context: 'name')
      raise(BazaRb::ValidationError, "The \"#{context}\" is nil") if name.nil?
      raise(BazaRb::ValidationError, "The \"#{context}\" must be a String") unless name.is_a?(String)
      raise(BazaRb::ValidationError, "The \"#{context}\" may not be empty") if name.empty?
      unless name.match?(/\A[a-z0-9-]+\z/)
        raise(BazaRb::ValidationError, "The \"#{context}\" #{name.inspect} is not valid")
      end
      raise(BazaRb::ValidationError, "The \"#{context}\" #{name.inspect} is too long") if name.length > 32
    end

    # Validate a job/durable ID (a positive Integer).
    #
    # @param [Integer, nil] id The ID to validate
    # @param [String] context Human-readable noun for error messages (default 'job')
    # @raise [BazaRb::ValidationError] If validation fails
    def valnum(id, context: 'job')
      raise(BazaRb::ValidationError, "The \"#{context}\" is nil") if id.nil?
      raise(BazaRb::ValidationError, "The \"#{context}\" must be an Integer") unless id.is_a?(Integer)
      raise(BazaRb::ValidationError, "The \"#{context}\" must be positive") unless id.positive?
    end

    # Validate a `push` payload (the data string).
    #
    # @param [String, nil] data The data payload to validate
    # @param [String] context Human-readable field name for error messages
    # @raise [BazaRb::ValidationError] If validation fails
    def valdata(data, context: 'data')
      raise(BazaRb::ValidationError, "The \"#{context}\" of the job is nil") if data.nil?
      raise(BazaRb::ValidationError, "The \"#{context}\" of the job may not be empty") if data.empty?
    end

    # Validate a `push` meta-array.
    #
    # @param [Array, nil] array The meta-array to validate
    # @param [String] context Human-readable field name for error messages
    # @raise [BazaRb::ValidationError] If validation fails
    def valarray(array, context: 'array')
      raise(BazaRb::ValidationError, "The \"#{context}\" of the job is nil") if array.nil?
      raise(BazaRb::ValidationError, "The \"#{context}\" of the job must be an Array") unless array.is_a?(Array)
    end

    # Validate a file path.
    #
    # @param [String, nil] file The file path to validate
    # @param [Boolean] must_exist Whether to check that the file exists on disk
    # @param [String] context Human-readable field name for error messages
    # @raise [BazaRb::ValidationError] If validation fails
    def valfile(file, must_exist: false, context: 'file')
      raise(BazaRb::ValidationError, "The \"#{context}\" is nil") if file.nil?
      raise(BazaRb::ValidationError, "The \"#{context}\" must be a String") unless file.is_a?(String)
      raise(BazaRb::ValidationError, "The \"#{context}\" may not be empty") if file.empty?
      raise(BazaRb::ValidationError, "The file '#{file}' is absent") if must_exist && !File.exist?(file)
      raise(BazaRb::ValidationError, "The file '#{file}' must be non-empty") if must_exist && File.empty?(file)
    end

    # Validate an owner string.
    #
    # @param [String, nil] owner The owner to validate
    # @param [String] context Human-readable field name for error messages
    # @raise [BazaRb::ValidationError] If validation fails
    def valowner(owner, context: 'owner')
      raise(BazaRb::ValidationError, "The \"#{context}\" is nil") if owner.nil?
      raise(BazaRb::ValidationError, "The \"#{context}\" must be a String") unless owner.is_a?(String)
      raise(BazaRb::ValidationError, "The \"#{context}\" may not be empty") if owner.empty?
      raise(BazaRb::ValidationError, "The \"#{context}\" #{owner.inspect} is not valid") unless owner.match?(/\A.+\z/)
    end
  end
end
