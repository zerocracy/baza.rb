# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

$stdout.sync = true

require 'simplecov'
require 'simplecov-cobertura'
unless Coverage.running? || ARGV.include?('--no-cov')
  SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter.new(
    [
      SimpleCov::Formatter::HTMLFormatter,
      SimpleCov::Formatter::CoberturaFormatter
    ]
  )
  SimpleCov.minimum_coverage(90)
  SimpleCov.coverage(:line) { minimum 95, per: :file }
  SimpleCov.start do
    skip('test/')
    skip('lib/baza-rb/version.rb')
    skip('vendor/')
    skip('target/')
    cover('lib/**/*.rb')
  end
end

require 'minitest/reporters'
Minitest::Reporters.use!([Minitest::Reporters::SpecReporter.new])
Minitest.load(:minitest_reporter)

require 'minitest/autorun'
require 'tempfile'
require 'timeout'
require 'webmock/minitest'

ENV['RACK_ENV'] = 'test'

# Cancels any test that runs longer than one minute.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024-2026 Yegor Bugayenko
# License:: MIT
module Deadline
  def run
    max = 180
    Timeout.timeout(max, nil, "test #{name} took longer than #{max} seconds") { super }
  end
end
Minitest::Test.prepend(Deadline)
