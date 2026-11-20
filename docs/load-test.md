# Load test

Run on 2026-11-20 with k6 v2.4.0. Every number here was measured on one
laptop, and each one comes from a single run. So read them as an order of
magnitude, not as a capacity plan.

## What it answers

| Question | Answer |
| --- | --- |
| How fast does the API take notifications in? | 4,000 single creates a second, or 80,000 a second in batches of 1,000, with no errors and a 99th percentile under 160 ms |
| How fast does a channel drain? | 90 a second, with one worker and with three |
| How long does a high-priority notification wait behind a low-priority backlog? | 0.2 s typical, 0.5 s at the 99th percentile, while the low-priority ones wait up to 105 s |
| Are status events real time? | A client gets the `sent` event a few milliseconds after the result is stored, with 1,000 streams open |

The provider accepts 100 a second per channel, 300 in total. The API takes
in 13 times that with single creates and about 270 times that with batches. So a
burst always becomes a backlog, and the work is to drain it in the right
order. That is what the other three measurements check.

## Setup

- Apple M1, 8 cores, 16 GB. Docker Desktop 28.0.4 with 8 CPUs and 11.7 GiB.
- The compose stack as it is: one API, one worker, Postgres 18.7, Redis
  8.10.3, the fake provider, Jaeger with tracing on.
- k6 runs as a container in the same compose network. So k6, the API, the
  worker and the databases share the 8 CPUs.
- The fake provider answers 202 at once. A real provider is slower. A
  worker has 20 senders per channel, and a sender gets its next
  notification at the next poll, every 200 ms. So one worker stays at 90
  a second only while a call is much shorter than that. This was not
  measured.
- The API's database pool has the pgx default size, 8 connections here.
- Each run starts from empty notification tables.
- The API test is a best case: every request is the same small SMS with
  no `Idempotency-Key`, k6 reuses its connections, and the worker has
  nothing to send.

## How to run it

`make loadtest` empties the notification tables of the compose database
first. A run exits with an error if a request failed, if a notification
was not sent, or if a channel did not drain.

```sh
make up

make loadtest script=api RATE=4000 DURATION=30s
make loadtest script=api RATE=80 BATCH=1000 DURATION=20s

make loadtest script=drain ROWS=6000
docker compose up -d --scale worker=3 --no-recreate
make loadtest script=drain ROWS=6000
docker compose up -d --scale worker=1 --no-recreate

make loadtest script=priority BACKLOG=0 RATE=7 DURATION=60s
make loadtest-latency
make loadtest script=priority BACKLOG=9000 RATE=7 DURATION=60s
make loadtest-latency

make loadtest script=streams STREAMS=1000 DUE_AFTER=30
make loadtest-latency
```

| Script | What it does |
| --- | --- |
| `loadtest/api.js` | Sends creates at a fixed rate. The notifications are scheduled for 2099, so the worker does not touch them. |
| `loadtest/drain.js` | Creates `ROWS` notifications per channel in batches, then reads the `sent` count of every batch once a second until all are sent. |
| `loadtest/priority.js` | Creates `BACKLOG` low-priority SMS, then high-priority SMS at a fixed rate. Each one opens its status stream and waits for `sent`. |
| `loadtest/streams.js` | Opens `STREAMS` streams, one per notification, all due at the same moment. Reads the API's goroutines and memory once a second. |
| `loadtest/latency.sql` | Delivery times as the database stored them, by priority. It counts only what is `sent`, so run it once the queue is empty. |

The tables below come from repeating the two `api` commands with `RATE`
from 500 to 12,000, and from 5 to 160 for batches.

## 1. API throughput

The rate is fixed: k6 does not slow down when the API does. "Dropped" are
requests k6 did not start on time because it had no free virtual user at
that moment. They are not in the latency numbers, so from 6,000 up the
latencies look better than they are.

Single create, 30 s per rate:

| Asked per second | Done per second | Median | 95th | 99th | Max | Dropped | Failed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 500 | 500 | 0.9 ms | 2.3 ms | 9.9 ms | 69 ms | 0 | 0 |
| 1,000 | 1,000 | 0.6 ms | 1.0 ms | 1.7 ms | 16 ms | 0 | 0 |
| 2,000 | 2,000 | 0.5 ms | 0.9 ms | 3.3 ms | 23 ms | 0 | 0 |
| 4,000 | 4,000 | 0.6 ms | 2.6 ms | 19 ms | 56 ms | 0 | 0 |
| 6,000 | 5,782 | 1.7 ms | 204 ms | 489 ms | 974 ms | 3.6% | 0 |
| 8,000 | 7,660 | 12 ms | 240 ms | 471 ms | 758 ms | 3.1% | 0 |
| 12,000 | 3,756 | 2.4 s | 4.7 s | 5.0 s | 5.0 s | 66% | 0 |

Batch create, 1,000 notifications per request, 20 s per rate:

| Batches per second | Notifications per second | Median | 95th | 99th | Max | Dropped | Failed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 5 | 5,000 | 25 ms | 33 ms | 61 ms | 87 ms | 0 | 0 |
| 10 | 10,000 | 23 ms | 42 ms | 77 ms | 78 ms | 0 | 0 |
| 20 | 20,000 | 14 ms | 43 ms | 250 ms | 286 ms | 0 | 0 |
| 40 | 40,000 | 10 ms | 43 ms | 146 ms | 257 ms | 0 | 0 |
| 80 | 80,000 | 9 ms | 22 ms | 151 ms | 241 ms | 0 | 0 |
| 160 | 157,700 | 21 ms | 251 ms | 453 ms | 596 ms | 19 requests | 0 |

- Clean up to 4,000 single creates a second. Between 6,000 and 8,000 the
  tail grows. At 12,000 the stack stops keeping up: requests queue for
  seconds, two thirds are never started, and it finishes about as many
  as at 4,000 (120,769 against 120,001).
- Part of that is k6 itself. At 12,000 it runs 12,000 virtual users on
  the same 8 CPUs as the API.
- No request failed at any rate. An overloaded API gets slow. It does not
  answer 500.
- After the 12,000 run `docker stats` showed 923 MiB for the API, against
  37 MiB when idle. Nothing limits how many requests wait at once.
- At the highest rates the tracer drops spans, which is by design.
- The 99th percentile of a batch rate rests on few requests: 101 at 5 a
  second. That is why 20 a second looks worse than 40 and 80.

## 2. Drain rate per channel

6,000 notifications per channel, 18,000 in total. At 90 a second a channel
needs 66.7 s.

| Workers | sms | email | push | Until a channel was all sent: sms, email, push |
| --- | --- | --- | --- | --- |
| 1 | 89.9 a second | 89.8 a second | 89.9 a second | 67.5 s, 67.2 s, 67.0 s |
| 3 | 90.0 a second | 90.0 a second | 90.0 a second | 66.8 s, 66.7 s, 67.6 s |

- The rate is the average of the one-second readings of the `sent`
  count. The time runs from the first create of a channel to the reading
  that shows all 6,000 sent, so it is good to about a second.
- All 18,000 ended as `sent` with one attempt each, in both runs.
- Three workers do not drain faster. They share one limit of 90 a second
  with a burst of 5 per channel.
- This test reads the database, not the provider. The highest single
  reading was 97.2 a second with one worker and 92.7 with three.
- The limiter is the cost. Redis counted 11.5 limiter calls for each send
  with one worker and 50.7 with three (`evalsha` in `INFO commandstats`).
  Reason: every sender that waits for a token asks again at the same
  moment, and one of them gets it. Redis handled that at 12% of one CPU
  with one worker.

## 3. High priority during a low-priority backlog

High-priority SMS at 7 a second for 60 s, 421 in each run. "Client" is the
time from the create request until the `sent` event arrives on the status
stream, or until a read right after the stream opens shows `sent`.
"Database" is `sent_at` minus `created_at`.

| | Median | 95th | 99th | Max |
| --- | --- | --- | --- | --- |
| High, no backlog, client | 119 ms | 207 ms | 211 ms | 225 ms |
| High, no backlog, database | 115 ms | 201 ms | 206 ms | |
| High, 9,000 low waiting, client | 216 ms | 370 ms | 511 ms | 566 ms |
| High, 9,000 low waiting, database | 213 ms | 368 ms | 510 ms | |
| Low, in that backlog, database | 54.1 s | 99.5 s | 103.5 s | 104.7 s |

- Without a backlog the wait is the time until the next poll: 0 to
  200 ms, 100 ms on average.
- With the backlog a high-priority notification waits about 100 ms more
  at the median. It is claimed first at the next poll. Then it competes
  for each rate-limit token with up to 19 low-priority ones the worker
  already holds. A token comes every 11 ms.
- It never waits for the backlog itself. The low-priority notifications
  around it waited 54 s at the median, and the last one 105 s. The low
  row was read after the whole backlog was sent.

## 4. Status streams

1,000 notifications, due at the same moment, each with its own open
stream. They are sent at 270 a second, so the last one is sent 3.7 s after
it is due. The API was started fresh for this run.

| | Result |
| --- | --- |
| Streams open at once | 1,000 |
| Streams that got their `sent` event | 1,000 |
| Due until the event at the client: median, 95th, 99th | 1.95 s, 3.61 s, 3.76 s |
| Due until `sent_at` in the database: median, 95th, 99th | 1.95 s, 3.62 s, 3.76 s |
| API goroutines: idle, with the streams, after k6 left | 9, 3,013 and 11 |
| API resident memory: idle, with the streams, after k6 left | 25 MB, 133 MB and 133 MB |

- The client and the database times are close at every percentile: within
  6 ms in measurement 3 and within 10 ms here, where k6 prints no finer
  than 10 ms. So a `sent`
  event reaches the client a few milliseconds after the result is
  stored. This compares percentiles, not single events. In measurement 3
  the client time also holds the create request.
- With the streams open the API had 3 more goroutines per client. One of
  them is the client's idle HTTP connection: after the streams closed and
  before k6 left, 1,011 were still there. So a stream costs 2.
- Memory grew by about 110 kB per stream. Go did not give it back to the
  system by the end of the run. Goroutines went back to idle, so nothing
  leaked.

## Other things looked at

| Check | Result |
| --- | --- |
| `/metrics` on the API with 3.2 million pending notifications | 0.3 to 0.6 s per scrape |
| List with a filter that matches nothing, over 3.2 million rows (`status=failed`) | 0.18 s |
| List `status=pending`, same table | 3 ms |
| CPU while one worker drains three channels at 270 a second in total (`docker stats`, 10 readings) | Postgres 16% of one CPU, Redis 12%, the worker 38%, the API 4% |
| Connections one worker opened to the provider for 6,000 sends | 18, at most 13 open at once. Read from the provider's `/proc/net/tcp6` twice a second. |
| A batch of 1,000 emails from one template of 100 kB | 0.33 to 0.46 s. The request body is 181 kB and the API renders 100 MB from it. Its memory went from 113 to 283 MiB after three of them. |

## What this did not measure

- A provider with real latency, or one that fails. The fake one answers at
  once.
- More than one API process, and anything over a real network.
- A run longer than a few minutes, and the same run twice. There is no
  spread to show.
- Why the slowest request at 12,000 a second took exactly 5.0 s.
- The largest number of streams the API can hold. 1,000 was the only size
  tried.
