# NotificationSystem

Sends SMS, email and push notifications through an external provider:
an HTTP API takes them in, workers send them, and every notification can
be followed from the request to the provider's answer.

## Why the queue is a table

The provider accepts 100 messages a second per channel. That one number
decides the design.

| | Per second | |
| --- | --- | --- |
| The provider accepts | 300 | 3 channels x 100. At most 26 million a day. |
| The API takes in, one per request | 4,000 | measured on one laptop, 13 times the provider |
| The API takes in, batches of 1,000 | 80,000 | measured on one laptop, about 270 times the provider |

So a large burst cannot be sent as it arrives. It waits:

```
1,000,000 SMS from a flash sale / 90 a second = 3 hours 5 minutes
```

(The workers send 90 a second per channel, not 100. "Limits" says why.)

The hard part is not moving messages fast. It is keeping a backlog for
hours and still doing the right thing with every message in it:

- send the urgent ones first,
- let the client cancel one that has not been sent,
- hold one back until its scheduled time, or until its retry is due,
- answer "where is my notification" and "how far is my batch" at any moment,
- never lose one, and never create one twice when a client retries.

Each of these is a query on rows. A broker would still need those rows in
a database, and then the two would have to agree on every state change.
Here the row is the queue entry:

- A worker claims rows with `FOR UPDATE SKIP LOCKED`. Every state change
  is one conditional `UPDATE`.
- The queue only has to hand out 270 rows a second. Postgres did that at
  16% of one CPU in the load test.
- A notification and its idempotency key are stored in one transaction. A
  result and its attempt record are written in one statement. There is
  nothing to keep in sync.

The cost: workers poll every 200 ms, so a notification waits 100 ms on
average before it is sent. And one Postgres is the ceiling. At 270 a
second that ceiling is far away.

Redis has two small jobs: the shared rate limiter, and passing status
events to the WebSocket streams.

## Run it

Needs Docker.

```sh
docker compose up --build --wait
```

| What | Where |
| --- | --- |
| API | http://localhost:8080 |
| API documentation (Swagger UI, read only) | http://localhost:8081 |
| OpenAPI spec | http://localhost:8080/openapi.yaml, or `api/openapi.yaml` |
| Traces (Jaeger) | http://localhost:16686 |
| Metrics: queue depth | http://localhost:8080/metrics |
| Metrics: sent, failed, latency | on the worker, see "Observability" |
| Health, readiness | http://localhost:8080/healthz, http://localhost:8080/readyz |

Stop it and delete its data with `docker compose down --volumes`.

By default the stack sends to a fake provider of its own, which answers
202 like the real one. To send to webhook.site, create a URL there, set
its response to status 202 with a JSON body that has a `messageId`, and
start the stack with it. Its default answer, 200, counts as a failure.

```sh
PROVIDER_URL=https://webhook.site/<your-uuid> docker compose up --build --wait
```

A free webhook.site URL stops answering after 50 to 100 requests. That is
why it is not the default.

Tested against webhook.site on 2026-11-20: one notification per channel, all three sent.

The fake provider can fail on purpose. A notification whose content starts
with `script:` gets the listed statuses, one per attempt, then 202. So
`script:500,429` fails twice and is sent on the third attempt.

### Tests

Needs Go 1.27 and Docker. The tests start their own Postgres and Redis.

```sh
make check        # lint and every test, with the race detector
make test-short   # unit tests only, no Docker
```

CI runs the same on every push (`.github/workflows/ci.yml`).

## Try it

Create a notification. The API answers 202:

```sh
curl -s -X POST localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-42-sms' \
  -d '{"recipient":"+905551234567","channel":"sms","content":"Your order is on its way","priority":"high"}'
```

```json
{"attempts":0,"channel":"sms","content":"Your order is on its way","created_at":"2026-11-20T20:08:33.061657Z","id":"01a1fa93-321f-7bd2-a3de-f5b010002f37","priority":"high","recipient":"+905551234567","status":"pending"}
```

Send the same request again with the same `Idempotency-Key`: the answer
is the same and nothing new is created.

Look at it, with its delivery attempts:

```sh
curl -s localhost:8080/v1/notifications/<id>
```

Create a batch. Each item is accepted or rejected on its own:

```sh
curl -s -X POST localhost:8080/v1/notifications/batch \
  -H 'Content-Type: application/json' \
  -d '{"notifications":[
        {"recipient":"+905551234567","channel":"sms","content":"hello"},
        {"recipient":"user@example.com","channel":"email","content":"script:500"},
        {"recipient":"not a phone number","channel":"sms","content":"hello"}
      ]}'
```

```json
{"batch_id":"01a1fa93-3253-78d4-a884-297d1d30ffce","results":[{"id":"01a1fa93-3253-7905-827c-c820f2fc6723","index":0},{"id":"01a1fa93-3253-796d-87a5-f73ce9bfedfa","index":1},{"error":"recipient must be a phone number in E.164 format, for example +905551234567","index":2}]}
```

Follow the batch:

```sh
curl -s localhost:8080/v1/batches/<batch_id>
curl -s 'localhost:8080/v1/notifications?batch_id=<batch_id>&status=sent&limit=50'
```

More:

```sh
# send later, and cancel before it is sent
curl -s -X POST localhost:8080/v1/notifications -H 'Content-Type: application/json' \
  -d '{"recipient":"device-token-1","channel":"push","content":"Sale starts now","scheduled_at":"2030-01-01T09:00:00Z"}'
curl -s -X POST localhost:8080/v1/notifications/<id>/cancel

# a template, and a notification from it
curl -s -X POST localhost:8080/v1/templates -H 'Content-Type: application/json' \
  -d '{"content":"Hello {{name}}, your code is {{code}}"}'
curl -s -X POST localhost:8080/v1/notifications -H 'Content-Type: application/json' \
  -d '{"recipient":"+905551234567","channel":"sms","template_id":"<template_id>","variables":{"name":"Ada","code":"4821"}}'

# list with filters; the answer has next_cursor when there is another page
curl -s 'localhost:8080/v1/notifications?channel=sms&status=failed&created_from=2026-11-20&limit=20'
```

Watch status changes as they happen, over WebSocket. With
[websocat](https://github.com/vi/websocat):

```sh
websocat ws://localhost:8080/v1/batches/<batch_id>/events
```

```json
{"notification_id":"01a1fa93-3253-7905-827c-c820f2fc6723","batch_id":"01a1fa93-3253-78d4-a884-297d1d30ffce","status":"processing","attempts":1}
{"notification_id":"01a1fa93-3253-7905-827c-c820f2fc6723","batch_id":"01a1fa93-3253-78d4-a884-297d1d30ffce","status":"sent","attempts":1}
```

`/v1/notifications/<id>/events` does the same for one notification. A
stream has no history: connect first, then fetch the status.

Every `/v1` request takes an `X-Correlation-ID`, or gets one. It is in the
response header, in every log line of the request, and in the log lines of
the worker that delivers what the request created. The same request is
one trace in Jaeger: the API request, each claim by a worker, each call to
the provider.

All endpoints, with every field and error, are in the Swagger UI.

## Architecture

```
              HTTP,
            WebSocket   +----------+    insert, read      +------------+
 client <-------------->|   api    |--------------------->|            |
                        +----------+                      |  Postgres  |
                             ^                            |            |
                             | status events (pub/sub)    | the queue  |
                             v                            | is the     |
                        +----------+                      | table      |
                        |  Redis   |                      |            |
                        +----------+                      +------------+
                             ^                                  ^
                             | rate limit, status events        |
                             |                                  |
                        +----------+    claim, result           |
                        |  worker  |----------------------------+
                        |          |    POST, 202         +------------+
                        |          |--------------------->|  provider  |
                        +----------+                      +------------+
```

| Binary | Job |
| --- | --- |
| `cmd/api` | The HTTP API. Validates, stores, answers. Streams status events. |
| `cmd/worker` | Claims due notifications, sends them, writes the result. Also takes back notifications whose worker died, and deletes old idempotency keys. Run several. |
| `cmd/migrate` | Applies the migrations in `migrations/`. Compose runs it once before the API and the worker start. |
| `cmd/fakeprovider` | Stands in for the provider. |

A notification is always in one of five states:

| State | Meaning | Leaves when |
| --- | --- | --- |
| `pending` | waiting for its time | a worker claims it, or the client cancels it |
| `processing` | a worker holds it, under a lease of 60 s | the result is written, the worker gives it back unsent, or the sweeper finds the lease expired |
| `sent` | the provider answered 202 | never |
| `failed` | a permanent error, or 5 attempts used up | never |
| `cancelled` | cancelled before a worker claimed it | never |

How a worker sends:

1. Every 200 ms, per channel, it claims as many due notifications as it
   has free senders (20 per channel): high priority first, then normal,
   then low.
2. Each sender takes a token from the rate limiter in Redis, calls the
   provider, and writes the result.
3. A failure that may pass (no answer, a timeout, 408, 429, 5xx) puts
   the notification back to `pending` with a delay, until its 5 attempts
   are used up. Any other answer that is not 202 ends it as `failed`.

`CLAUDE.md` holds every design decision with its reason and its cost.

## Delivery

**At least once.** A notification is sent, or it ends as `failed` with
the reason. It is not lost when a worker dies: its lease runs out after
60 s, a sweeper puts it back within 15 s, and any worker takes it.

**The duplicate window.** The provider can get the same notification
twice when it accepted the message and the worker did not learn that, or
could not store it:

- The provider's answer came after the 5 s timeout, or was lost on the
  way. The worker retries. The window is the whole call.
- The worker died, or lost the database, between the provider's 202 and
  the result write. The window is that one write, a few milliseconds.
- The worker stalled past its 60 s lease while its call was under way,
  or was stopped by force. The notification is sent again.

Every call carries the notification id as `Idempotency-Key`, so a
provider that honours the header drops the copy. Without that, a copy
reaches the recipient.

For the same reason a notification that ends as `failed` after its lease
ran out may in fact have been delivered. Its attempt history says so:
`lease expired before a result was stored`.

**Duplicates from the client.** A client that retries a create request
should send an `Idempotency-Key`. With the same key and the same body,
the API answers with the first response and creates nothing. With the
same key and another body it answers 409. A key is kept for at least
24 hours.

**Retries.** Up to 5 attempts. The delay before attempt n+1 is a random
time between 0 and 2^n seconds (2, 4, 8, 16 s at most), so retries do not
arrive together. For a 429 the worker waits at least as long as
`Retry-After` asks, up to 60 s.

**When the provider is down.** After 5 calls in a row with no answer, a
timeout or a 5xx, a worker stops calling the provider for that channel
for 30 s, then tries 3 calls. If they succeed it goes on. While it
waits, notifications stay `pending` and lose no attempt. A 429 does not
count: the provider is up, and the notification is retried.

**When Redis is down.** Nothing is sent, because the rate could not be
kept. Notifications stay `pending` and lose no attempt. The API keeps
accepting requests, but `/readyz` answers 503, and status streams are
closed and refused until Redis is back.

**Priority.** Strict. A worker claims every due high-priority
notification before a normal one, and every normal one before a low one.
Measured with 9,000 low-priority SMS waiting: a high-priority one was
sent after 0.2 s at the median and 0.5 s at the 99th percentile. The
low-priority ones waited 54 s at the median, and the last one 105 s.

**Stopping a worker.** On SIGTERM it claims nothing more, gives back what
it has not started, and lets the sends in flight finish, for at most 30 s.
Compose gives it 40 s.

## Decisions and their cost

| Decision | Why | Cost |
| --- | --- | --- |
| The queue is the `notifications` table | See the top of this page | Polling adds 100 ms on average. One Postgres is the ceiling. |
| Rate limit of 90 a second with a burst of 5, in Redis, shared by all workers | Adding workers must never send more than the provider accepts | Without Redis nothing is sent |
| A claim is a lease of 60 s | A worker that dies must not keep its notifications | The duplicate window above |
| Ids are UUIDv7, made by the API | They sort by time, so the list and its date filter need no extra index | The date filter uses the API's clock, `created_at` the database's. They can differ by milliseconds. |
| Batches are partial: valid items are stored, invalid ones are reported per index | One bad phone number should not reject 999 good ones | The client has to read the per-item results |
| Templates are rendered when the notification is created | The stored content is what is sent, and what was validated | Changing a template does not change notifications that already exist |
| Status events are best effort | They are a convenience on top of the status endpoints, which are always right | A client must fetch the status after it connects |
| Spec first: the server code is generated from `api/openapi.yaml` | The generated server cannot drift from the spec, and CI fails if the generated code is out of date | A WebSocket endpoint does not fit the generator and is written by hand |

## Observability

- **Metrics**, in Prometheus format. The API reports the queue:
  `notifications_pending` and `notifications_oldest_pending_age_seconds`,
  by channel and priority. Each worker reports
  `notifications_sent_total`, `notifications_failed_total`,
  `notifications_retried_total`, `provider_call_duration_seconds`,
  `notification_delivery_duration_seconds`, `circuit_breaker_state` and
  `rate_limiter_failures_total`, by channel, on port 8082 inside the
  compose network. Compose does not publish that port, because several
  workers cannot share one host port. Read a worker's metrics with:

  ```sh
  docker compose exec worker wget -qO- localhost:8082/metrics
  ```

  With several workers, `--index=2` picks the second one.
- **Logs** are JSON lines. A line that belongs to a request or a
  delivery has its `correlation_id`.
- **Traces** go to Jaeger over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is
  set, which compose does.
- **Health**: `/healthz` says the API runs. `/readyz` says it reaches
  Postgres and Redis.

## Limits

- **The rate limiter is set below 100 on purpose.** It lets 90 a second
  through, with a burst of 5, so at most 95 in any one second. The
  provider counts 100 at its end, and requests can bunch up on the way.
  The spare 5 absorbs that.
- An idempotency key works for at least 24 hours. A worker deletes it
  within about a minute after that, and then the same key creates a new
  notification. Rarely, a replay that arrives in that minute gets a 500.
  Nothing is created, and the next try is handled as a new request.
- `scheduled_at` in the past means "send now".
- Status events of one notification can arrive out of order. `sent`,
  `failed` and `cancelled` are final.
- A request body is at most 1 MB, a batch body 10 MB.

What the system does under load, and where it gets slow, is in
`docs/load-test.md`.

## Not built

- Authentication, and anything per customer: quotas, separate queues.
- A way to send a `failed` notification again.
- Reading, changing or deleting a template.
- Deleting or archiving old notifications. The table only grows.
