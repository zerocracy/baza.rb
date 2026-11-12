# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'yaml'
require_relative 'test__helper'

# Workflow actions must be pinned to immutable refs.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024-2026 Yegor Bugayenko
# License:: MIT
class TestWorkflows < Minitest::Test
  def test_all_actions_are_pinned
    Dir['.github/workflows/*.yml'].each do |file|
      YAML.load_file(file)['jobs'].each do |name, job|
        job['steps'].each do |step|
          next unless step['uses']
          assert_match(
            /\A(v?\d+(\.\d+)*|[0-9a-f]{40})\z/, step['uses'].split('@').last,
            "#{file}: job '#{name}' runs #{step['uses']} from a moving ref"
          )
        end
      end
    end
  end

  def test_pdd_and_xcop_are_pinned
    assert_match(/pdd-action@[0-9a-f]{40}/, File.read('.github/workflows/pdd.yml'))
    assert_match(/xcop-action@v\d+\.\d+/, File.read('.github/workflows/xcop.yml'))
  end
end
