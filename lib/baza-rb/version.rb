# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2024-2026 Zerocracy
# SPDX-License-Identifier: MIT

# Just a version.
#
# We keep this file separate from the "baza-rb.rb" in order to have an
# ability to include it from the ".gemspec" script, without including all
# other packages (thus failing the build).
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2024-2026 Yegor Bugayenko
# License:: MIT
class BazaRb
  VERSION = '0.15.0'
end
