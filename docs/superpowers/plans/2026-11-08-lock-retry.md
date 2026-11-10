# Lock retry false conflict Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `lock` whose reply was lost and whose retry gets 409 succeeds
instead of raising "already locked", while a genuine conflict still raises.

**Architecture:** Pass an `on_retry` callback from `lock` through `post` to
`retry_on`; the callback fires only for retries that may already have applied
the request (transport re-entry, 499+), never for 429. `lock` accepts a 409
only when it fired.

**Tech Stack:** Ruby, Typhoeus, WebMock/Minitest, `retries` gem.

**Spec:** `docs/superpowers/specs/2026-11-08-lock-retry-design.md`

## Global Constraints

- `post(uri, params, allowed = [302], on_retry: nil)`;
  `retry_on(on_retry = nil, &)`; both new arguments optional.
- 429 retries are NOT ambiguous; 499 and 5xx retries are.
- Failure message stays exactly
  `Failed to lock <pname.inspect> product at <@host>, it's already locked`.
- `lock` allowed codes stay `[302, 409]`.
- Existing `test_fails_to_lock` (`test/test_baza.rb:466`) and
  `test_post_retries_on_busy_server` (`test/test_baza_edge.rb:707`) must pass
  unchanged.
- No HTTP response mutation, no thread-local state, no server changes.

## Review Focus

1. Fresh 409 (no retry) must still raise — `test_lock_raises_when_conflict_is_not_a_retry`.
2. 429 then 409 must still raise (429 means not applied) — `test_lock_raises_after_busy_retry`.
3. Timeout then 409 must succeed — `test_lock_succeeds_after_timeout_retry`.
4. 503 then 409 must succeed (5xx may have applied the write) — `test_lock_succeeds_after_server_error_retry`.
5. Other `post` callers (`unlock`, `durable_*`, `transfer`, `fee`, `enter`) and
   `get`/`download`/`upload` retry behavior stay unchanged — full suite;
   existing retry tests.

---

### Task 1: Accept 409 after an ambiguous lock retry

**Files:**

- Modify: `lib/baza-rb.rb:236-250` (`lock`), `lib/baza-rb.rb:658-675`
  (`retry_on`), `lib/baza-rb.rb:761-776` (`post`)
- Test: `test/test_baza_edge.rb` (append next to
  `test_post_retries_on_busy_server`, line ~730)

**Interfaces:**

- Consumes: `with_retries` passes the attempt number to its block (`retries`
  gem, `retries.rb` `return block.call(attempts)`).
- Produces: `post(..., on_retry: nil)` and `retry_on(on_retry = nil, &)`;
  `lock` uses the callback to treat 409 as success.

- [ ] **Step 1: Write the failing tests**

Append to `test/test_baza_edge.rb`:

```ruby
# Reproduces zerocracy/baza.rb#490: the server took the lock and the reply was
# lost, so the retried POST gets 409. The caller holds the lock, not somebody
# else, and must not be told otherwise.
def test_lock_succeeds_after_timeout_retry
  WebMock.disable_net_connect!
  stub_request(:get, 'https://example.org:443/csrf').to_return(body: 'token')
  stub_request(:post, 'https://example.org:443/lock/simple')
    .to_timeout.then
    .to_return(status: 409, body: '')
  BazaRb.new(
    'example.org', 443, '000',
    loog: Loog::NULL, compress: false,
    timeout: 0.1, retries: 2, pause: 0
  ).lock('simple', 'owner')
  assert_requested(:post, 'https://example.org:443/lock/simple', times: 2)
end

def test_lock_succeeds_after_server_error_retry
  WebMock.disable_net_connect!
  stub_request(:get, 'https://example.org:443/csrf').to_return(body: 'token')
  stub_request(:post, 'https://example.org:443/lock/simple')
    .to_return({ status: 503, body: '' }, { status: 409, body: '' })
  BazaRb.new(
    'example.org', 443, '000',
    loog: Loog::NULL, compress: false,
    timeout: 0.1, retries: 2, pause: 0
  ).lock('simple', 'owner')
  assert_requested(:post, 'https://example.org:443/lock/simple', times: 2)
end

def test_lock_raises_when_conflict_is_not_a_retry
  WebMock.disable_net_connect!
  stub_request(:get, 'https://example.org:443/csrf').to_return(body: 'token')
  stub_request(:post, 'https://example.org:443/lock/simple')
    .to_return(status: 409, body: '')
  error = assert_raises(RuntimeError) do
    BazaRb.new(
      'example.org', 443, '000',
      loog: Loog::NULL, compress: false,
      timeout: 0.1, pause: 0
    ).lock('simple', 'owner')
  end
  assert_includes(error.message, "it's already locked")
  assert_requested(:post, 'https://example.org:443/lock/simple', times: 1)
end

def test_lock_raises_after_busy_retry
  WebMock.disable_net_connect!
  stub_request(:get, 'https://example.org:443/csrf').to_return(body: 'token')
  stub_request(:post, 'https://example.org:443/lock/simple')
    .to_return({ status: 429, body: '' }, { status: 409, body: '' })
  error = assert_raises(RuntimeError) do
    BazaRb.new(
      'example.org', 443, '000',
      loog: Loog::NULL, compress: false,
      timeout: 0.1, retries: 2, pause: 0
    ).lock('simple', 'owner')
  end
  assert_includes(error.message, "it's already locked")
  assert_requested(:post, 'https://example.org:443/lock/simple', times: 2)
end
```

- [ ] **Step 2: Run tests to verify RED**

Run: `bundle exec ruby -Ilib -Itest test/test_baza_edge.rb -n '/test_lock_(succeeds_after_timeout_retry|succeeds_after_server_error_retry|raises_when_conflict_is_not_a_retry|raises_after_busy_retry)/'`

Expected: the two `succeeds_*` FAIL with `RuntimeError: Failed to lock "simple"
product at example.org, it's already locked`; the two `raises_*` PASS. Record
the output.

- [ ] **Step 3: Implement the callback**

In `lib/baza-rb.rb`, `retry_on` becomes:

```ruby
def retry_on(on_retry = nil, &)
  attempt = 0
  loop do
    ret = yield
    if (ret.code == 429 || ret.code >= 499) && attempt < @retries
      attempt += 1
      on_retry&.call if ret.code >= 499
      seconds = backoff(attempt)
      if ret.code == 429
        @loog.info("Server seems to be busy, will sleep for #{seconds} (attempt no.#{attempt})...")
      else
        @loog.info(
          "Server seems to be in trouble (#{ret.code}), " \
          "sleep #{seconds}s (attempt no.#{attempt})..."
        )
      end
      sleep(seconds) unless @pause.zero?
      next
    end
    return ret
  end
end
```

`post` becomes:

```ruby
def post(uri, params, allowed = [302], on_retry: nil)
  sent = false
  attempt do |try|
    on_retry&.call if try > 1 && sent
    body = params.merge('_csrf' => csrf).sort.to_h
    checked(
      retry_on(on_retry) do
        sent = true
        Typhoeus::Request.post(
          uri.to_s, body:, headers:,
          connecttimeout: @timeout, timeout: @timeout
        )
      end,
      allowed
    )
  end
end
```

`lock` becomes:

```ruby
    elapsed(@loog, level: Logger::INFO) do
      retried = false
      ret = post(
        home.append('lock').append(pname),
        { 'owner' => owner }, [302, 409],
        on_retry: -> { retried = true }
      )
      if ret.code == 302 || (ret.code == 409 && retried)
        throw(:"Product name #{pname.inspect} locked at #{@host}")
      end
      raise(
        RuntimeError,
        "Failed to lock #{pname.inspect} product at #{@host}, " \
        "it's already locked"
      )
    end
```

Known limit: the `lock` docblock carries a PDD puzzle (30min) because a plain
`# shortcut:` comment trips `Elegant/NoComments`.

- [ ] **Step 4: Run GREEN**

Run: `bundle exec ruby -Ilib -Itest test/test_baza_edge.rb`
Expected: 0 failures; the four new tests pass.

Run: `bundle exec rake test`
Expected: 0 failures, 0 errors.

- [ ] **Step 5: Commit**

```bash
git add lib/baza-rb.rb test/test_baza_edge.rb \
  docs/superpowers/specs/2026-11-08-lock-retry-design.md \
  docs/superpowers/plans/2026-11-08-lock-retry.md
git commit -m "fix(#490): treat 409 after an ambiguous lock retry as success"
```
