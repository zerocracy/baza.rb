# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'yaml'
require_relative 'test__helper'

# Coverage must run on pull requests and must not swallow upload errors.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024-2026 Yegor Bugayenko
# License:: MIT
class TestCoverageWorkflow < Minitest::Test
  FILE = '.github/workflows/codecov.yml'

  def test_runs_on_push_and_pull_request
    triggers = YAML.load_file(FILE)['on']
    assert_equal(%w[master], triggers['push']['branches'], 'the push trigger is gone')
    assert_equal(%w[master], triggers['pull_request']['branches'], 'coverage no longer runs on a pull request')
  end

  def test_does_not_swallow_upload_errors
    assert(upload['with']['fail_ci_if_error'], 'a failed upload still leaves the check green')
  end

  def test_takes_the_token_from_the_secret
    assert_match(/\A\${{ secrets\.\w+ }}\z/, upload['with']['token'], 'the upload is not authenticated')
  end

  private

  def upload
    YAML.load_file(FILE)['jobs']['codecov']['steps']
      .find { |step| step['uses'].to_s.start_with?('codecov/codecov-action@') }
  end
end
