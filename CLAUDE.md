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
  itself. The worker serves `/metrics` only, on port 8082.
- WebSocket: `coder/websocket`. The two stream endpoints are in the spec
  for documentation and are left out of the generated server
  (`exclude-operation-ids`). They are plain handlers on the `/v1` mux.
  Reason: a strict handler returns a response object and cannot take over
  the connection.
- Postgres through `pgx/v5` and `pgxpool`. Queries with sqlc in pgx/v5 mode.
- Migrations: goose SQL files in `migrations/`, applied by `cmd/migrate`
  using goose as a library with embedded files. A run holds a Postgres
  advisory lock. Reason: two runs at once both tried the same migration, and
  the loser exited with an error. A run waiting for the lock checks again
  every second, for at most 4 minutes. `cmd/migrate` gives up after 5
  minutes. Reason: a database that accepts the connection and never answers
  would hang it forever. The wait for the advisory lock is the shorter one,
  so the error says which of the two happened. A run sets `lock_timeout`
  to 3 s: a statement gives up after waiting that long for one lock on a
  table or row. Reason: while it waits for a table, every later request
  for that table waits behind it, and the worker gives a database call
  5 s. The run then fails with a lock timeout error. Each migration runs
  in a transaction, so the one it was applying is rolled back, and the run
  can be started again. This limits the wait only. A migration that has
  the lock keeps it until it commits, so one that checks or indexes a
  large table still blocks that table for as long as it runs.
- Redis: `go-redis/v9`. Limiter: `redis_rate` (GCRA).
- Circuit breaker: `sony/gobreaker/v2`.
- IDs: UUIDv7 from `google/uuid`, generated in the API.
- Logs: `log/slog` JSON. Metrics: Prometheus `client_golang`.
- Tracing: OpenTelemetry (`otel` and `otel/sdk`), exported over OTLP on
  HTTP (`otlptracehttp`). Compose runs Jaeger, which shows the traces at
  `localhost:16686`.
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
- Scheduling and retry delay share one column, `next_attempt_at`. An
  optional `scheduled_at` on a create request, or on an item of a batch,
  sets it. Without one, or with a time in the past, the notification is
  due at once and the column holds the database time of the insert.
  Reason: a time in the past would put the row first in the claim order
  and make the oldest pending age wrong. A batch asks the database for
  its time once. Reason: its rows are written with COPY, which cannot
  apply `now()` per row, and the API's clock may be ahead of the
  database's. Without an `Idempotency-Key` that query and the COPY are
  two statements, so a batch row can be due a moment before its
  `created_at`. `scheduled_at` is RFC 3339, else 400. In a batch, one
  that is not rejects the whole batch, like a field of the wrong type.
  It is part of the idempotency hash as it was written. So with one key,
  the same instant in another time zone is a different request, and so
  are a time in the past and no time. Reason: turned into UTC first, a
  time at the ends of the year range could not be hashed, and the
  request failed. The API does not return it. Reason: it is stored only
  in `next_attempt_at`, which a retry overwrites. A scheduled
  notification stays `pending` until its time, so it counts in
  `notifications_pending` and can be cancelled. The cost:
  `notification_delivery_duration_seconds` includes the scheduled wait,
  so a scheduled notification looks slow.
- Templates: `POST /v1/templates` stores a text with placeholders and
  answers 201 with its id. A template belongs to no channel. A
  placeholder is `{{name}}`: ASCII letters, digits and underscores, no
  spaces. Content that is empty, has a NUL character, or has `{{` or `}}`
  outside a placeholder is 422. Reason: `{{ name }}` would otherwise
  reach the recipient as it is written. The cost: a template cannot hold
  a literal `{{`. These checks are in `internal/api`, next to the
  rendering. Reason: both use one placeholder pattern. A template has no
  size limit of its own, so the body limit of 1 MB applies. No endpoint
  reads, lists, changes or deletes a template, and the create endpoint
  takes no `Idempotency-Key`. Reason for no change: the idempotency hash
  of a notification covers `template_id`, not the text, so a changed
  template would let one key stand for two texts.
- Creating from a template: a create request, or an item of a batch,
  names a template with `template_id` and gives `variables`, an object
  of strings, in place of `content`. Rejected with 422, or as that
  item's error in a batch: `content` that is not empty next to
  `template_id`; `variables` that are not empty without `template_id`; a
  template that does not exist; a placeholder without a variable, where
  the message lists the missing names. A variable that is null counts as
  missing. Reason: it was rendered as an empty text. A variable without a
  placeholder is not used. Reason: one set of variables can serve several
  templates. A `template_id` that is not a UUID is 400 and rejects a
  whole batch, like a field of the wrong type. The API renders at create
  time, in one pass, so a value that holds a placeholder is not rendered
  again. Rendering stops with a 422 once the text passes 100 kB, the
  largest content any channel allows. Reason: a 1 MB template of one
  placeholder repeated, with a large value, made the API build
  gigabytes. The rendered text is validated like `content` and is what
  the row stores. The row does not keep the template id. The idempotency
  hash covers `template_id` and `variables`, not the rendered text, so
  the same text sent as `content` is a different request. A request
  reads its templates with one query, a batch too, before anything is
  stored.
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
- Idempotency: optional `Idempotency-Key` header on the two endpoints
  that create notifications.
  Table `idempotency_keys (key primary key, request_hash, response,
  created_at)`. Insert the key in the same transaction as the notifications
  with `ON CONFLICT DO NOTHING`. On conflict return the stored response. The
  same key with a different request hash is 409. The hash is SHA-256 of the
  body after defaults are applied, so field order and a spelled-out default
  do not matter. A key is 1 to 255 bytes, else 400. Only an accepted request
  stores its key: a rejected one can be corrected and sent again with the
  same key.
- A key is kept for 24 hours, then deleted. Every worker runs the DELETE
  once a minute, and a key still replays until that run. Reason: a key is
  only useful while the client retries, and the stored responses would
  grow the table forever. A request with a deleted key is handled as a
  new one. Backed by an index on `created_at`. Reason: without it every
  run read the whole table, 2.4 GB at 5 million keys. A run deletes at
  most 100,000 keys. Reason: the DELETE gets the worker's 5 s like any
  database call. Without a limit, a backlog of about 2 million keys on an
  idle database would not fit in 5 s, so every run would fail and the
  backlog would never shrink. 100,000 keys take about 1 s. The query
  picks the keys with `ARRAY(SELECT ...)`, not with `DELETE ... USING` a
  subquery. Reason: with `USING` the planner read the whole table on
  every run, 1.4 s to delete 38 keys. The cost: a worker deletes at most
  100,000 keys a minute, 144 million a day. If clients store keys faster
  than that, the table grows. A run that deletes 100,000 keys logs a
  warning, so a warning every minute means the expiry is falling behind.
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
  and retrying would send it twice. A `messageId` with a NUL character
  counts as not readable. Reason: Postgres text cannot store one, so the
  result write failed and the notification was sent again.
- The error of a failed call is stored in `last_error` and shown by the API.
  For an answer that is not 202 it holds the status and the first 200 bytes
  of the answer, without NUL characters, with invalid UTF-8 replaced and
  with white space at the ends removed.
  Reason: Postgres text cannot store those, so the result write would fail.
  For a call that got no answer it holds the cause without the provider URL.
  Reason: a webhook.site URL contains the token. The host can still appear.
- Error classes: retry on transport errors, timeouts, 408, 429 and 5xx. Any
  other answer that is not 202 goes to `failed` at once: another 4xx, a 2xx
  such as 200, and a redirect, which is not followed. Reason: Go follows a
  301, 302 or 303 with a GET that has no body, and a 202 to that GET would
  count as sent. For 429, wait the larger of `Retry-After` and the backoff.
  `Retry-After` is read as seconds or as an HTTP date, and only on a 429.
  It counts for at most 60 s. Reason: the header could ask for years, and
  the notification would sit `pending` all that time. The cost: a provider
  that asks for longer is tried again after 60 s, and that try uses up an
  attempt.
- Backoff is full jitter: `random(0, 1s * 2^attempts)`, where `attempts` is
  the row's value after the failed send, so 1 after the first try. Maximum
  5 attempts, so the ceilings are 2, 4, 8 and 16 s.
- A claim increments `attempts`, so an attempt in progress is counted.
  Every finished attempt is one row in
  `delivery_attempts (notification_id, attempt, error, created_at)`. A
  stored result writes its row in the same statement, so the two cannot
  disagree. An attempt whose result was not stored gets its row from the
  sweeper, and the result itself is only logged. `error` is null for the
  accepted attempt. `last_error` on the notification is the error of the
  last failed or lost attempt, and stays after a later success.
- Every notification the API returns has `attempts` and `last_error`.
  `GET /v1/notifications/{id}` also lists the delivery attempts.
- Rate limit per channel: rate 90 per second, burst 5, so we send at
  most 95 in any one-second window. The limit of 100 is counted at the
  provider, and the spare 5 absorbs requests bunching on the way there. On
  deny, wait the delay the limiter returns. Every provider call takes a
  token, retries included. A sender takes the token after the claim, so a
  claimed row waits for it inside its lease. The wait is short: each worker
  holds at most 20 rows per channel and 90 tokens come every second, so
  even with 10 workers a row waits about 2 s.
- If Redis is unreachable, do not send. Release the row, and do not claim
  for that channel until Redis answers. Log and count it. The same goes
  for any limiter call that fails, such as a Redis that refuses writes.
  After a failed call, the pool takes a token itself at each poll instead
  of claiming, and claims again once it gets one. Reason: a check that
  writes nothing, such as a ping or a request for zero tokens, passed on a
  Redis that refuses writes, so the pool would claim and release its rows
  at every poll. The cost: each time a pool recovers, one token is taken
  and not used. A limiter call uses go-redis's default timeouts. Measured:
  with Redis stopped a call fails within 6 s. With a Redis that stopped
  answering, 60 senders had all released their rows after 25 s, inside
  the 60 s lease. The worker starts without Redis and exits only if
  `REDIS_URL` does not parse.
- Worker: per channel, a pool of 20 goroutines fed by a Go channel. Poll
  every 200 ms. Claim at most the pool's free capacity. Query once per
  priority, high first.
- The worker gives each database call 5 s, the ping at start included.
  Reason: with a database that stopped answering, a pool froze and logged
  nothing. A call that times out is logged, a result write with the provider
  message id. The statement may still finish on the server, so a claim that
  timed out can leave rows `processing` that no sender holds. A result
  write that failed leaves its row `processing` too. The sweeper takes both
  once the lease has expired, and the lost attempt counts.
- The provider client keeps one idle connection per sender, 60 in total.
  Reason: Go keeps 2 per host, and each poll starts up to 20 sends at once
  per channel. Measured with 2: 1443 connections for 1500 sends.
- Lease: a claim sets `locked_until = now() + 60s` and increments `attempts`.
  Result writes are conditional on `id`, `status = 'processing'` and the
  `attempts` value read at claim. Reason: a sender that stalled past its
  lease must not overwrite the attempt that took the row after it. Its
  result is logged and dropped. The provider may then get the notification
  twice, with the same `Idempotency-Key`. A CHECK requires `locked_until`
  on every `processing` row. Reason: the sweeper matches
  `locked_until < now()`, which a null never meets, so the row would stay
  `processing` forever without an error. A worker from before the lease
  that claims during a deploy now gets an error instead.
- Release: a row given up without a provider call goes back to `pending`
  with its attempt refunded. This covers a breaker refusal, an unreachable
  limiter and a shutdown before the send starts. The write is conditional
  like a result write.
- Sweeper: every 15 s, `processing` rows with an expired lease go back to
  `pending`, or to `failed` if attempts are used up. A row sent back is due
  at once. The lost attempt gets a `delivery_attempts` row with the error
  `lease expired before a result was stored`, which is also the row's
  `last_error`. A lost attempt may never have called the provider, or the
  provider may have accepted it. So a row the sweeper fails may have been
  delivered. Every worker runs a sweeper; two sweeping at once are safe,
  because the sweep is one statement and its UPDATE is conditional. Backed
  by a partial index on
  `(locked_until) WHERE status = 'processing'`. Reason: the sweep must not
  scan the whole table every 15 s, and few rows are `processing` at a time.
- Breaker: one per channel in each worker. Workers do not share it: each
  counts its own failures and makes its own trial calls. It opens after 5
  failures in a row and stays open for 30 s. Then it is half-open and lets
  3 trial calls through. If all 3 succeed, it closes. If one fails, it
  opens again for 30 s. Only transport errors, timeouts and 5xx answers
  count as failures. Every other answer counts as a success and ends the
  run of failures: 202, every 4xx including 408 and 429, a 200 and a
  redirect. Reason: the provider answered, so it is up. While open, the
  poll loop does not claim for that channel. While half-open it claims
  only for trial calls that have not started: 3, less the calls the
  breaker has let through since it went half-open, less the rows the pool
  holds. Reason: the breaker refuses every call after the third, also
  while the 3 are still waiting for their answer, so more rows would be
  claimed and released at every poll. Measured with two slow trial
  calls: 98 rows claimed and released in 500 ms. A row the breaker
  refuses is released. Every change of state is logged. The cost: a
  trial call that fails uses up an attempt like any call, so in a long
  outage up to 3 notifications per worker lose an attempt every 30 s.
- Shutdown on SIGTERM or SIGINT: stop polling, release rows whose send has
  not started, let in-flight sends finish (deadline 30 s), exit with
  status 0. A row still in the pool's queue, or waiting for its rate-limit
  token, has not started, so it is released. A send in flight
  still gets its result written. Reason: claims, sends, releases and
  result writes run on a context that the signal does not cancel. The
  sweeper and the key expiry finish a run in progress, at most 5 s, then
  stop. Reason: a run cut off by the signal logged an error although
  nothing had failed, and a sweep that still finished on the server
  logged none of its rows. The sweeper then still publishes the events
  of that run, which can add up to 5 s. The deadline counts from the
  signal. When
  it passes, each pool logs an error, cancels what is still running and
  returns once its senders have. Such a row stays `processing` until the
  sweeper takes it, and the attempt counts as lost. A send cut off this
  way does not count for the breaker. The deadline is a safety net: a
  provider call takes at most 5 s and a result write 5 s. Compose gives
  the worker 40 s before it kills it. Reason: the default is 10 s,
  shorter than the deadline. After the deadline a last publish can take
  5 s and the export of the spans 2 s.
- Compose uses `cmd/fakeprovider` by default. `PROVIDER_URL` switches to a
  webhook.site URL. Reason: free webhook.site URLs stop after 50 to 100
  requests. The README says so. The fake provider answers 202. A
  notification whose content starts with `script:` gets the listed statuses
  instead, one per attempt, then 202: `script:500,429` fails twice. Its 429
  carries `Retry-After: 1`. The worker exits at start if `PROVIDER_URL`
  is not an http or https URL. Reason: otherwise every notification would
  fail one by one.
- Correlation id: every `/v1` request has one. The API takes it from
  `X-Correlation-ID`, or generates a UUIDv7 if the header is not sent. A
  sent id is 1 to 128 printable ASCII characters without spaces, sent
  once, else 400. Reason: it is stored as Postgres text and returned in a
  header, and a caller that sends an id expects to find that id in the
  logs. That 400 carries a generated id. Every `/v1` response carries the
  id in the same header, also a 400, 404 or 405 answered before a handler
  runs. The one exception is the router's redirect for a path that is not
  clean, such as `/v1//notifications`. The id is stored on each
  notification the request creates, in `correlation_id`, so a batch
  shares one. The column is null for rows from before it existed. A
  replay with an `Idempotency-Key` creates nothing, so the row keeps the
  id of the first request, and the replay's answer carries the replay's
  own id. The API does not return the id in a body.
- Logs: the logger adds `correlation_id` to a line when the context
  carries one. In the API that is the request. In the worker it is the
  notification being sent, released or swept, so the lines of a delivery
  carry the id of the request that created it, the provider client's
  lines included. A row with a null id is logged without one. The API
  logs one line per `/v1` request: method, path, status and duration.
  Routes outside `/v1` get no request line and no id. Reason: compose
  calls `/healthz` every 2 s. Every worker line has `worker`, the host
  name. Reason: with several workers the lines of one can be told apart.
  A failed send logs what happens next: `status` `pending` with
  `delay_seconds` at warn level, or `status` `failed` at error level.
  The line is written before the result is stored. If that write fails,
  a `write result` or `result not written` line follows. The sweeper
  logs an expired lease the same way: warn for `pending`, error for
  `failed`. A notification sent without a problem is not logged. Reason:
  at 300 per second that is 26 million lines a day, and a count belongs
  in metrics. go-redis's own messages are written as JSON lines at warn
  level. Reason: it printed plain text to stderr. The messages of the
  API's HTTP server itself, such as a handler that panicked, are JSON
  lines at error level, without a correlation id. Reason: it printed
  plain text to stderr too.
- Worker metrics: every series has a `channel` label and exists at 0 from
  the start, for every channel. Reason: `increase()` cannot see the first
  event of a series that did not exist before it.
  - `notifications_sent_total`, `notifications_failed_total` and
    `notifications_retried_total` count stored results. `retried` is a
    failed or lost attempt whose row went back to `pending`. A sender
    counts after its result write, and only if the write changed the row.
    The sweeper counts each row it moves. A released row is not counted.
    Reason: counted before the write, a result that was not stored was
    counted again when the sweeper took the row. The cost: a write that
    timed out but still finished on the server is not counted.
  - `provider_call_duration_seconds`: histogram of every provider call,
    whatever its result. A call the breaker refused is not a call.
  - `notification_delivery_duration_seconds`: histogram of the worker's
    time when the `sent` result was stored, minus `created_at`, which the
    database set. So it includes the retry delays, the wait of a
    scheduled notification, and any difference between the two clocks. Buckets from 0.1 s to 1 hour.
  - `circuit_breaker_state`: 0 closed, 1 half-open, 2 open. Each worker
    reports its own breaker.
  - `rate_limiter_failures_total`: limiter calls that failed. That
    includes the pool's own check at each poll, so with Redis down it
    grows by about 5 a second per channel in each worker.
- Queue metrics: the API reports `notifications_pending` and
  `notifications_oldest_pending_age_seconds`, by `channel` and `priority`,
  0 for an empty queue. Reason for the API: the queue stays visible when
  no worker runs. The count includes rows that are not due yet. The age
  is the time since the earliest `next_attempt_at` among the pending
  rows, 0 if that is in the future. Reason: it says how far behind the
  workers are, and a row waiting for its retry is not late. Both come
  from one query at each scrape, read from the claim index. Measured with
  5 million pending rows: 0.2 s after a vacuum, 1 s before. The query
  gets 5 s. If it fails, the scrape answers with every other series, the
  error is logged, and `promhttp_metric_handler_errors_total` counts it.
- Both `/metrics` endpoints include the Go runtime and process series.
  Reason: memory, goroutines and open files are the first things to look
  at under load. Compose does not publish the worker's port. Reason:
  several workers cannot share one host port. The worker exits at start
  if it cannot listen on the port. If its metrics server stops later, the
  worker logs that and keeps sending.
- Readiness: `GET /readyz` on the API pings Postgres, then Redis, each
  within 1 s. 200 if both answer, else 503 and an error line that names
  the first one that failed. Reason for Redis: without it nothing is
  sent. The cost: the API reports not ready although it can still accept
  requests. A ping goes on when the caller gives up. Reason: cut off, it
  was logged as a failed dependency. The API's Redis client obeys the
  deadline of a call (`ContextTimeoutEnabled`). Reason: by default
  go-redis waits 3 s for an answer whatever the deadline. The API starts
  without Redis and exits only if `REDIS_URL` does not parse. Readiness
  does not look at the subscription for status events, which comes back
  a second after Redis does.
- Tracing: a delivery is one trace with three kinds of span. The API
  starts a server span for every `/v1` request. Its name is the route,
  such as `POST /v1/notifications`, or the method alone when no route
  matched. It carries the method, the status and the correlation id, and
  a status of 500 or more marks it as an error. A `traceparent` header
  from the caller continues the caller's trace, and one that is not
  valid is ignored. `tracestate` is not read. The span's context is
  stored on each notification the request creates, in `trace_context`,
  as a W3C `traceparent` text, so a batch shares one. It is stored only
  when the span is recorded. So the column is null when tracing is off,
  also if the caller sent a `traceparent`, when the caller did not
  sample its trace, and for rows from before it existed. Reason: the
  worker would start spans under a parent that nobody recorded. A
  replay with an `Idempotency-Key` creates
  nothing, so the row keeps the context of the first request. The worker
  reads the context of a row it claimed and starts two spans as children
  of the request span: `claim`, which covers the claim query, and
  `provider call`, a client span around the request to the provider.
  Both carry the notification id, the channel and the attempt. A failed
  provider call marks its span as an error with the text that is stored
  as `last_error`. A retry adds another pair. A row that is released has
  a `claim` span and no `provider call`. A row without a trace context
  gets no span. Reason: each span would be a trace of its own. The
  sweeper is not traced. The request to the provider carries no
  `traceparent`. Reason: the provider is another company's service.
  Tracing is on when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, in the API
  and in the worker each. It is the base URL of an OTLP endpoint on
  HTTP, and `/v1/traces` is added to it. A value that is not an http or
  https URL with a host makes the binary exit at start. Without it
  nothing is traced and no context is stored. Each binary logs at start
  whether tracing is on. A trace the API starts is always sampled. One
  that continues a caller's trace follows the caller's sampled flag. The
  sampler is set in the code, so `OTEL_TRACES_SAMPLER` does not change
  it.
  At 90 sends a second per channel that is about 540 worker spans a
  second. Spans are exported in batches of up to 512, at least every
  5 s. If the endpoint is down, a batch is tried again for up to 30 s,
  and each failed export is one JSON line at warn level. The messages of
  the OpenTelemetry libraries themselves are JSON lines too, their
  errors at error level. Reason: both printed plain text to stderr.
  Meanwhile 2048 spans wait
  in memory, and spans beyond that are dropped without a line. A request
  or a send never waits for an export. When the worker stops, after its
  pools have returned, it waits at most 2 s to export the spans it still
  holds. Spans not sent by then are lost and an error is logged. The API
  does not handle a stop signal, so it loses the spans of its last
  seconds. The OpenTelemetry libraries read other `OTEL_` variables
  themselves, such as `OTEL_EXPORTER_OTLP_HEADERS` and the `OTEL_BSP_`
  ones. The span of a status stream
  lasts as long as the stream. In compose the API and the worker do not
  wait for Jaeger, and Jaeger keeps the traces in memory.
- Status events: every status change a worker stores is published on the
  Redis channel `notifications:status`, as JSON with `notification_id`,
  `batch_id` if the row has one, `status` and `attempts`. A pool publishes
  the rows of each claim in one round trip, before its senders get them,
  so within one worker `processing` comes before the result. A sender
  publishes after its
  result or release write, and only if the write changed the row, like
  the counters. The sweeper publishes the rows it moved. The API
  publishes a cancel. Reason: it is a status change too, and a client
  that watches a batch would not learn of it. Creating a notification is
  not an event. Events are best effort: one that cannot be published is
  logged at error level with the notification ids and lost, and the
  delivery goes on. A publish
  gets 2 s and goes on when its caller is cancelled. A cancel answers
  after its publish, so a Redis that does not answer delays it by up to
  2 s. The worker's Redis
  client does not obey a deadline while it waits on the socket, so there
  a publish to a Redis that never answers takes 5 s. Measured with such a
  Redis: 60 claimed rows were all released after 10 s, inside the 60 s
  lease. Redis does not keep pub/sub apart by database number, so every
  worker and API on one Redis server share the channel.
- Status streams: `GET /v1/notifications/{id}/events` and
  `GET /v1/batches/{id}/events` are WebSocket endpoints. After the
  upgrade the API sends every event of that notification, or of any
  notification of that batch, as a text message, as it was published.
  The stream is subscribed before the API answers 101, so no event after
  the upgrade is missed.
  Each API process keeps one Redis subscription and passes the events on
  to its streams in memory. Reason: a subscription per stream is a Redis
  connection per client. The cost: every API process receives every
  event, about 600 a second at the full sending rate. A stream has no
  history and no first message. So a client connects first, then fetches
  the status, and fetches it again after every reconnect. The events of
  one notification can arrive out of order. Reason: each is published
  after its own write, on its own connection, and a retry can be claimed
  by another worker at once. `sent`, `failed` and `cancelled` are final,
  so a client ignores what arrives after one of them. The API closes
  a stream with code 1013 when the stream may have missed events: when
  the Redis subscription failed, and when the stream holds 256 events
  the client has not read. A write to a client gets 5 s. Then the API
  drops the connection without a close frame, so a client that stopped
  reading sees code 1006. While the API has no subscription, a new
  stream is refused with 503. After a failed subscription the API waits
  a second and opens a new one, and logs an error for each try that
  fails. Reason for a new one: after an error that Redis answered, such
  as a refused permission, go-redis keeps the connection, and the old
  subscription waited on it forever. The subscription has no ping. A
  Redis host that
  vanished is found by TCP keep-alive after about 45 s, and until then
  the streams stay open and get nothing. An unknown id is 404, found
  with the queries of the status endpoints. A query parameter is 400. A
  data message from the client closes the stream with code 1008. A
  request whose `Origin` header names another host or port than its
  `Host` header is 403. Reason: it is the library's default, and it
  keeps a page of another site from opening a stream in a visitor's
  browser. A handshake the library refuses is answered in plain text,
  not with an error object. The streams have no ping either. Reason:
  TCP keep-alive finds a client that vanished, within minutes. The
  request line of a stream is logged when the stream
  ends, with status 101 and the lifetime of the stream as its duration.

## Design reference

### Layout

```
api/openapi.yaml
cmd/api  cmd/worker  cmd/migrate  cmd/fakeprovider
internal/api        generated server and handlers
internal/validate   per-channel rules
internal/store      sqlc queries and generated code
internal/provider   HTTP client, error classes
internal/worker     poll loop, pools, retry, sweeper, key expiry
internal/ratelimit
internal/obs        logging, metrics, tracing
internal/events     status events: publisher, hub of the streams
migrations/
```

### States

| State | Meaning | Leaves when |
| --- | --- | --- |
| pending | waiting for its time | a worker claims it, or the client cancels |
| processing | claimed, with a lease | result is written, or the sweeper finds the lease expired |
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
POST /v1/templates
GET  /v1/notifications/{id}/events   WebSocket
GET  /v1/batches/{id}/events         WebSocket
GET  /healthz   GET /readyz   GET /metrics   GET /openapi.yaml
```

## Go tooling

- Make targets: `fmt`, `lint`, `test` (everything, with `-race`),
  `test-short` (unit only), `test-isolated`, `check` (lint and test),
  `generate`, `up`, `down`.
- Integration tests skip under `-short`. No build tags.
- Database tests in one package share one Postgres container, and each
  test gets its own database in it. Reason: a container per test took 1 s
  per test. Redis tests share one Redis container the same way, and each
  test gets its own database number in it. `make test-isolated` gives every
  test its own containers again, to check that no test passes only because
  the server is shared.
- golangci-lint v2: default linters plus `bodyclose`, `noctx`, `errorlint`,
  `gosec`, `misspell`. Formatters `gofumpt` and `goimports`. The gosec
  check for weak random numbers (G404) is off in `internal/worker/worker.go`.
  Reason: the retry delay only spreads retries out, so `math/rand/v2` is
  enough, and `crypto/rand` would add an error path. The `bodyclose`
  check is off on lines that call `websocket.Dial`. Reason: `Dial`
  closes the response body itself, also after a handshake that failed.
- oapi-codegen, sqlc and golangci-lint are each pinned as a `tool` in a
  module of their own, `tools/<name>/go.mod`, so their dependencies stay
  out of the app's `go.mod`. Make targets hide the `-modfile` flag. CI
  checks that every `go.mod` is tidy.
- Necessary comments are allowed: generator directives, `//go:embed`,
  `-- name:` lines in sqlc files and goose markers. Nothing else.
- Wrap errors with `%w`. Never ignore an error.
- `context.Context` is the first parameter. Config comes from environment
  variables read in `main`.

