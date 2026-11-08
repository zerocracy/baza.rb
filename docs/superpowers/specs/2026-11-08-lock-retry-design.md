# Lock retry false conflict

**Issue:** zerocracy/baza.rb#490

## Intent

`BazaRb#lock` POSTs `/lock/<pname>` and treats any non-302 as "somebody else owns
it". The POST is retried by `attempt` (timed out) and `retry_on` (429, 499+). When
the server took the lock but the reply was lost, the next attempt gets 409 and the
caller is told the name is taken although the caller is the holder.

## Goals

- A 409 that follows an ambiguous retry of the same `lock` call is success.
- A 409 on the first attempt, or after a 429 retry (server did not apply it), still
  raises `Failed to lock ... it's already locked`.

## Non-goals

- Server changes. `zerocracy/baza` is private; no owner echo in the 409 exists in
  any fixture (`BazaRb-Zerocracy.json:787` has no body/headers).
- Idempotency for `unlock`, `durable_*`, `transfer`, `fee`, `enter`. Only `lock`
  declares a conflict code that a retry can turn into success.

## Constraints

- `retry_on`/`attempt`/`post` are shared by `get`, `download`, `upload`; their
  signatures must stay backward compatible.
- `Typhoeus::Response` must not be mutated; no thread-local state.
- Existing pact `test_fails_to_lock` (`test/test_baza.rb:466`) stubs a 409 on the
  first attempt and must keep raising.
- `test_post_retries_on_busy_server` (`test/test_baza_edge.rb:707`) pins 429→302
  as two attempts; unchanged.

## Approaches

1. **Ambiguous-retry flag on `post` (chosen).** `attempt` already receives the
   attempt number from the `retries` gem; `retry_on` knows the status. `lock` gets
   `on_retry:` and treats 409 as success when an earlier attempt may have applied
   the request: transport error, 499, or 5xx. A 429 retry is not ambiguous.
2. **Re-check ownership after 409.** No client method and no documented endpoint
   returns the lock owner; the 409 fixture carries no body or header. Rejected.
3. **Never retry `lock`.** Turns the lost reply into `TimedOut` and fails the
   issue's acceptance criterion; also drops the 429 retry that is safe. Rejected.
4. **Treat every 409 as success.** Hides genuine conflicts on the first attempt.
   Rejected.

## Chosen design

- `attempt` keeps `with_retries` but forwards its attempt number to the block.
- `post(uri, params, allowed = [302], on_retry: nil)` calls `on_retry` when a
  retry means the request may already have been applied: block attempt `> 1`
  (transport), or `retry_on` status `>= 499`.
- `lock` passes `on_retry: -> { retried = true }` and accepts 409 when `retried`.

Known limit: if an ambiguous attempt was not applied and another process locked
the name before the retry, the client reports success. No client-visible owner
check exists; documented with a `shortcut:` comment.

## Files

- `lib/baza-rb.rb` — `lock`, `post`, `retry_on`, `attempt`.
- `test/test_baza_edge.rb` — three lock-retry tests.

## Risks

- Concurrency: wrong success under a lock race (limit above).
- Shared retry helpers: a signature slip breaks `get`/`download`/`upload`; the
  full suite and existing retry tests cover them.
