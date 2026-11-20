# NotificationSystem

Guide for anyone, human or agent, working in this repository: what the system
is, the decisions behind it, and the conventions the code follows.

## Conventions

- Explicit over implicit. Simple over complex. Flat over nested.
- Write the smallest thing that makes the current change work. No field,
  parameter, lock, injection seam or abstraction before the code that uses it
  exists. An abstraction needs a second concrete caller.
- A column, field, parameter, interface or config value appears with the code
  that first uses it. Schema changes arrive as a new goose migration.
- Fix the bug that exists, not one that might exist later.
- Errors never pass silently.
- No comments unless necessary. "Go tooling" lists the allowed ones.
- If the implementation is hard to explain, it is a bad idea.
- Look up every library API in context7 or the library's own docs before
  using it. Do not guess.
- Prefer `jq` and `yq` over scripts for JSON and YAML.

## The assignment

"Event-Driven Notification System" for a marketing-technology company. Send
SMS, email and push notifications through an external provider. Millions per
day, burst traffic, reliable delivery, real-time status.

Required:

- API: create a notification (recipient, channel, content, priority); batch
  create up to 1000; status by ID or batch ID; cancel pending; list with
  filters (status, channel, date range) and pagination.
- Processing: asynchronous workers; at most 100 messages per second per
  channel; priorities high, normal, low; content validation; idempotency.
- Delivery and retry logic: left to us to design.
- Observability: metrics (queue depth, success and failure rates, latency),
  structured logs with correlation IDs, health check.
- Provider: `POST {url}` with `{"to","channel","content"}`; it answers 202
  with `{"messageId","status","timestamp"}`.

Bonus: failure handling, scheduled notifications, templates, WebSocket status
updates, tracing, CI.

Deliverables: README (setup, architecture, API examples), one-command
docker compose, OpenAPI spec, versioned migrations, tests runnable with one
command, clean commit history.

## Decisions

These are settled. Raise it before changing one.

### Architecture

- The queue is the Postgres `notifications` table. Workers claim rows with
  `FOR UPDATE SKIP LOCKED`. There is no broker. Reason: sending is capped at
  300 per second in total, so the job is managing a backlog, and one
  transaction should own the truth.
- Redis has two jobs only: the shared rate limiter, and pub/sub of status
  events.
- Binaries: `cmd/api`, `cmd/worker`, `cmd/migrate`, `cmd/fakeprovider`.
- Delivery is at-least-once. Every state change is one conditional UPDATE
  (compare-and-swap).
- No job-queue framework (River, Asynq, Watermill). No ORM.

### Stack

- Go 1.27. The `go` line in `go.mod` is the single version: the Dockerfile
  matches it and CI reads it. Module path:
  `github.com/kaancolakoglu/NotificationSystem`.
- HTTP: `net/http` with oapi-codegen, spec-first, strict server, standard
  library router. Spec at `api/openapi.yaml`. The spec covers `/v1` only.
  `/healthz`, `/readyz`, `/metrics` and `/openapi.yaml` are plain routes on
  the API's mux: the first three serve operators, the last is the spec
  itself.
- Postgres through `pgx/v5` and `pgxpool`. Queries with sqlc in pgx/v5 mode.
- Migrations: goose SQL files in `migrations/`, applied by `cmd/migrate`
  using goose as a library with embedded files. A run holds a Postgres
  advisory lock. Reason: two runs at once both tried the same migration, and
  the loser exited with an error. A run waiting for the lock checks again
  every second, for at most 4 minutes. `cmd/migrate` gives up after 5
  minutes. Reason: a database that accepts the connection and never answers
  would hang it forever. The lock wait is the shorter one, so the error
  says which of the two happened.
- Redis: `go-redis/v9`. Limiter: `redis_rate` (GCRA).
- Circuit breaker: `sony/gobreaker/v2`.
- IDs: UUIDv7 from `google/uuid`, generated in the API.
- Logs: `log/slog` JSON. Metrics: Prometheus `client_golang`.
- Tests: standard `testing` and `httptest`; `testify/require` allowed;
  `testcontainers-go` (Postgres and Redis modules) for integration tests.

### Behaviour

- Priority is strict. Stored as smallint (0 high, 1 normal, 2 low). The API
  uses the words; default is normal.
- Status is text with a CHECK: `pending`, `processing`, `sent`, `failed`,
  `cancelled`.
- Channel and priority have a CHECK too: `sms`, `email`, `push`, and 0, 1,
  2. Reason: workers claim by channel and priority, so a row with any other
  value would stay `pending` forever without an error.
- Scheduling and retry delay share one column, `next_attempt_at`.
- Validation runs at the API, in `internal/validate`, as a map from channel
  to function. Limits are named constants:
  - sms: recipient in E.164; content 1 to 1600 characters.
  - email: recipient parses with `net/mail` and is the bare address, with no
    display name; content 1 byte to 100 kB.
  - push: recipient token not empty; content 1 to 4096 bytes.
  - every channel: no NUL character in recipient or content. Reason:
    Postgres text cannot store one.
- A create request body over 1 MB is 413. Reason: the largest valid body is
  100 kB of email content, which JSON escaping can grow to 600 kB. A batch
  body over 10 MB is 413. Reason: that fits 1000 items of about 10 kB each,
  and a batch of larger emails can be sent in parts.
- A request body with a field the endpoint does not define is 400. Reason:
  a mistyped name such as `priorty` would otherwise be dropped and the
  default used. In a batch, one such field rejects the whole batch, like a
  field of the wrong type. Field names are matched without regard to case,
  as `encoding/json` does.
- Batch is partial success. Validate each item, insert all valid items in one
  transaction, answer per index. 202 if at least one is accepted, 422 if none.
  More than 1000 items, or no items, is 422. An accepted item answers with
  its id, a rejected one with the reason. Rejected items are not stored. A
  batch is the set of rows that share a `batch_id`: there is no batches
  table, and its status is the count of those rows by status.
- Idempotency: optional `Idempotency-Key` header on both create endpoints.
  Table `idempotency_keys (key primary key, request_hash, response,
  created_at)`. Insert the key in the same transaction as the notifications
  with `ON CONFLICT DO NOTHING`. On conflict return the stored response. The
  same key with a different request hash is 409. The hash is SHA-256 of the
  body after defaults are applied, so field order and a spelled-out default
  do not matter. A key is 1 to 255 bytes, else 400. Only an accepted request
  stores its key: a rejected one can be corrected and sent again with the
  same key.
- Pagination is keyset on `id` descending with an opaque cursor. Limit is 1
  to 200, default 50, else 400.
- List filters: status, channel, batch_id, created from and to. Every invalid
  query parameter is 400. So is one sent more than once, and one the
  endpoint does not define, on every endpoint. Reason: a mistyped filter
  name would otherwise return the unfiltered list. The created range is matched against the time
  inside the id, not `created_at`. Reason: the id is the sort key, so the
  range and the cursor become one id range on the primary key and need no
  index. `created_at` is the database clock and can differ from the id time
  by milliseconds.
- The list adds no index. Status and channel are filters on the primary key
  scan. With `batch_id` a second query reads the batch index and applies the
  id range as a filter. Reason: as an index condition the planner combined
  it with a scan of the whole primary key.
- Cancel works only from `pending`: 200 on success, 409 otherwise, 404 if
  unknown.
- Provider call: JSON `{"to","channel","content"}`, header
  `Idempotency-Key: <notification id>`, timeout 5 s. Success is 202. A 202
  whose body has no readable `messageId` is still a success, and is logged.
  Reason: the provider has accepted it, so failing it would report it wrong
  and retrying would send it twice.
- Error classes: retry on transport errors, timeouts, 408, 429 and 5xx. Any
  other answer that is not 202 goes to `failed` at once: another 4xx, a 2xx
  such as 200, and a redirect, which is not followed. Reason: Go follows a
  301, 302 or 303 with a GET that has no body, and a 202 to that GET would
  count as sent. For 429, wait the larger of `Retry-After` and the backoff.
  `Retry-After` is read as seconds or as an HTTP date, and only on a 429.
- Backoff is full jitter: `random(0, 1s * 2^attempts)`, where `attempts` is
  the row's value after the failed send, so 1 after the first try. Maximum
  5 attempts, so the ceilings are 2, 4, 8 and 16 s.
- A claim increments `attempts`, so an attempt in progress is counted.
  Every provider call whose result is stored is one row in
  `delivery_attempts (notification_id, attempt, error, created_at)`, written
  in the same statement as the result, so the two cannot disagree. A result
  that could not be stored leaves no row and is only logged. `error` is
  null for the accepted attempt. `last_error` on the notification is the
  error of the last failed attempt, and stays after a later success.
- Every notification the API returns has `attempts` and `last_error`.
  `GET /v1/notifications/{id}` also lists the delivery attempts.
- Rate limit per channel: rate 90 per second, burst 5, so we release at
  most 95 in any one-second window. The limit of 100 is counted at the
  provider, and the spare 5 absorbs requests bunching on the way there. On
  deny, wait the delay the limiter returns. Every provider call takes a
  token, retries included.
- If Redis is unreachable, do not send. Release the row, and do not claim
  for that channel until Redis answers. Log and count it.
- Worker: per channel, a pool of 20 goroutines fed by a Go channel. Poll
  every 200 ms. Claim at most the pool's free capacity. Query once per
  priority, high first.
- The worker gives each database call 5 s, the ping at start included.
  Reason: with a database that stopped answering, a pool froze and logged
  nothing. A call that times out is logged, a result write with the provider
  message id. The statement may still finish on the server, so a claim that
  timed out can leave rows `processing` that no sender holds.
- The provider client keeps one idle connection per sender, 60 in total.
  Reason: Go keeps 2 per host, and each poll starts up to 20 sends at once
  per channel. Measured with 2: 1443 connections for 1500 sends.
- Lease: a claim sets `locked_until = now() + 60s` and increments `attempts`.
  Result writes are conditional on `id`, `status = 'processing'` and the
  `attempts` value read at claim.
- Release: a row given up without a provider call goes back to `pending`
  with its attempt refunded. This covers a breaker refusal, an unreachable
  limiter and a shutdown before the send starts. The write is conditional
  like a result write.
- Sweeper: every 15 s, `processing` rows with an expired lease go back to
  `pending`, or to `failed` if attempts are used up.
- Breaker: one per channel. Opens after 5 consecutive failures for 30 s; 3
  trial calls when half-open. Only transport errors, timeouts and 5xx count.
  While open, the poll loop does not claim for that channel. A row the
  breaker refuses is released.
- Shutdown on SIGTERM: stop polling, release rows whose send has not
  started, let in-flight sends finish (deadline 30 s), exit.
- Compose uses `cmd/fakeprovider` by default. `PROVIDER_URL` switches to a
  webhook.site URL. Reason: free webhook.site URLs stop after 50 to 100
  requests. The README says so. The fake provider answers 202. A
  notification whose content starts with `script:` gets the listed statuses
  instead, one per attempt, then 202: `script:500,429` fails twice. Its 429
  carries `Retry-After: 1`. The worker exits at start if `PROVIDER_URL`
  is not an http or https URL. Reason: otherwise every notification would
  fail one by one.

## Design reference

### Layout

```
api/openapi.yaml
cmd/api  cmd/worker  cmd/migrate  cmd/fakeprovider
internal/api        generated server and handlers
internal/validate   per-channel rules
internal/store      sqlc queries and generated code
internal/provider   HTTP client, error classes
internal/worker     poll loop, pools, retry, sweeper
internal/ratelimit
internal/obs        logging, metrics
migrations/
```

### States

| State | Meaning | Leaves when |
| --- | --- | --- |
| pending | waiting for its time | a worker claims it, or the client cancels |
| processing | a worker holds a lease | result is written, or the lease expires |
| sent | provider answered 202 | never |
| failed | permanent error or attempts used up | never |
| cancelled | cancelled before a claim | never |

### Claim query

```sql
WITH due AS (
  SELECT id FROM notifications
  WHERE status = 'pending'
    AND channel = $1
    AND priority = $2
    AND next_attempt_at <= now()
  ORDER BY next_attempt_at
  LIMIT $3
  FOR UPDATE SKIP LOCKED
)
UPDATE notifications
SET status = 'processing',
    attempts = attempts + 1,
    locked_until = now() + interval '60 seconds'
FROM due
WHERE notifications.id = due.id
RETURNING notifications.*;
```

Backed by a partial index on `(channel, priority, next_attempt_at) WHERE
status = 'pending'`. The query must contain the literal `'pending'`.

The rows to claim are picked in a CTE, not in `WHERE id IN (SELECT ...)`.
Reason: a CTE runs once. As a subquery the planner may run the SELECT once
per row, and each run skips the rows this statement already updated and
picks the next ones. Measured with such a plan forced: a limit of 5 claimed
all 2000 pending rows.

### Endpoints

```
POST /v1/notifications
POST /v1/notifications/batch
GET  /v1/notifications/{id}
GET  /v1/notifications
POST /v1/notifications/{id}/cancel
GET  /v1/batches/{id}
GET  /healthz   GET /readyz   GET /metrics   GET /openapi.yaml
```

## Go tooling

- Make targets: `fmt`, `lint`, `test` (everything, with `-race`),
  `test-short` (unit only), `test-isolated`, `check` (lint and test),
  `generate`, `up`, `down`.
- Integration tests skip under `-short`. No build tags.
- Database tests in one package share one Postgres container, and each
  test gets its own database in it. Reason: a container per test took 1 s
  per test. `make test-isolated` gives every test its own container again,
  to check that no test passes only because the server is shared.
- golangci-lint v2: default linters plus `bodyclose`, `noctx`, `errorlint`,
  `gosec`, `misspell`. Formatters `gofumpt` and `goimports`. The gosec
  check for weak random numbers (G404) is off in `internal/worker/worker.go`.
  Reason: the retry delay only spreads retries out, so `math/rand/v2` is
  enough, and `crypto/rand` would add an error path.
- oapi-codegen, sqlc and golangci-lint are each pinned as a `tool` in a
  module of their own, `tools/<name>/go.mod`, so their dependencies stay
  out of the app's `go.mod`. Make targets hide the `-modfile` flag. CI
  checks that every `go.mod` is tidy.
- Necessary comments are allowed: generator directives, `//go:embed`,
  `-- name:` lines in sqlc files and goose markers. Nothing else.
- Wrap errors with `%w`. Never ignore an error.
- `context.Context` is the first parameter. Config comes from environment
  variables read in `main`.

