# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

require 'yaml'
require_relative 'test__helper'

# Checks that GitHub workflows declare least-privilege token permissions.
class TestWorkflows < Minitest::Test
  def test_every_workflow_declares_permissions
    files = Dir['.github/workflows/*.yml']
    refute_empty(files, 'no workflows found')
    files.each do |file|
      assert_equal(
        { 'contents' => 'read' },
        YAML.safe_load_file(file)['permissions'],
        "#{file}: expected permissions: contents: read"
      )
    end
  end
end
