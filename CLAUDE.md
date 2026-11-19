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
  library router. Spec at `api/openapi.yaml`.
- Postgres through `pgx/v5` and `pgxpool`. Queries with sqlc in pgx/v5 mode.
- Migrations: goose SQL files in `migrations/`, applied by `cmd/migrate`
  using goose as a library with embedded files.
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
- Scheduling and retry delay share one column, `next_attempt_at`.
- Validation runs at the API, in `internal/validate`, as a map from channel
  to function. Limits are named constants:
  - sms: recipient in E.164; content 1 to 1600 characters.
  - email: recipient parses with `net/mail`; content at most 100 kB.
  - push: recipient token not empty; content at most 4096 bytes.
- Batch is partial success. Validate each item, insert all valid items in one
  transaction, answer per index. 202 if at least one is accepted, 422 if none.
  More than 1000 items is 422.
- Idempotency: optional `Idempotency-Key` header on both create endpoints.
  Table `idempotency_keys (key primary key, request_hash, response,
  created_at)`. Insert the key in the same transaction as the notifications
  with `ON CONFLICT DO NOTHING`. On conflict return the stored response. The
  same key with a different request hash is 409.
- Pagination is keyset on `id` descending with an opaque cursor.
- Cancel works only from `pending`: 200 on success, 409 otherwise, 404 if
  unknown.
- Provider call: JSON `{"to","channel","content"}`, header
  `Idempotency-Key: <notification id>`, timeout 5 s. Success is 202.
- Error classes: retry on transport errors, timeouts, 408, 429 and 5xx. Any
  other 4xx goes to `failed` at once. For 429, wait the larger of
  `Retry-After` and the backoff.
- Backoff is full jitter: `random(0, 1s * 2^attempts)`, where `attempts` is
  the row's value after the failed send, so 1 after the first try. Maximum
  5 attempts, so the ceilings are 2, 4, 8 and 16 s.
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
  requests. The README says so.

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
UPDATE notifications
SET status = 'processing',
    attempts = attempts + 1,
    locked_until = now() + interval '60 seconds'
WHERE id IN (
  SELECT id FROM notifications
  WHERE status = 'pending'
    AND channel = $1
    AND priority = $2
    AND next_attempt_at <= now()
  ORDER BY next_attempt_at
  LIMIT $3
  FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

Backed by a partial index on `(channel, priority, next_attempt_at) WHERE
status = 'pending'`. The query must contain the literal `'pending'`.

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
  `test-short` (unit only), `check` (lint and test), `generate`, `up`,
  `down`.
- Integration tests skip under `-short`. No build tags.
- golangci-lint v2: default linters plus `bodyclose`, `noctx`, `errorlint`,
  `gosec`, `misspell`. Formatters `gofumpt` and `goimports`.
- oapi-codegen, sqlc and golangci-lint are each pinned as a `tool` in a
  module of their own, `tools/<name>/go.mod`, so their dependencies stay
  out of the app's `go.mod`. Make targets hide the `-modfile` flag. CI
  checks that every `go.mod` is tidy.
- Necessary comments are allowed: generator directives, `//go:embed`,
  `-- name:` lines in sqlc files and goose markers. Nothing else.
- Wrap errors with `%w`. Never ignore an error.
- `context.Context` is the first parameter. Config comes from environment
  variables read in `main`.

