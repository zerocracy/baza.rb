# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

$stdout.sync = true

require 'simplecov'
require 'simplecov-cobertura'
unless SimpleCov.running || ARGV.include?('--no-cov')
  SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter.new(
    [
      SimpleCov::Formatter::HTMLFormatter,
      SimpleCov::Formatter::CoberturaFormatter
    ]
  )
  SimpleCov.minimum_coverage(90)
  SimpleCov.minimum_coverage_by_file(95)
  SimpleCov.start do
    add_filter('test/')
    add_filter('lib/baza-rb/version.rb')
    add_filter('vendor/')
    add_filter('target/')
    track_files('lib/**/*.rb')
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
