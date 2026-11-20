package worker

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
	"github.com/kaancolakoglu/NotificationSystem/internal/ratelimit"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

func unreadMetrics() *obs.WorkerMetrics {
	return obs.NewWorkerMetrics(prometheus.NewRegistry(), nil)
}

type metricReader struct {
	handler http.Handler
}

func newMetrics() (*obs.WorkerMetrics, metricReader) {
	registry := prometheus.NewRegistry()
	return obs.NewWorkerMetrics(registry, []string{"sms"}), metricReader{obs.MetricsHandler(registry, slog.New(slog.DiscardHandler))}
}

func (r metricReader) value(t *testing.T, series string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	r.handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))
	for line := range strings.Lines(rec.Body.String()) {
		text, found := strings.CutPrefix(line, series+" ")
		if !found {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
		if err != nil {
			t.Fatalf("value of %s: %v", series, err)
		}
		return value
	}
	t.Fatalf("metrics have no series %s:\n%s", series, rec.Body)
	return 0
}

func (r metricReader) check(t *testing.T, want map[string]float64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		wrong := map[string]float64{}
		for series, wantValue := range want {
			if got := r.value(t, series); got != wantValue {
				wrong[series] = got
			}
		}
		if len(wrong) == 0 {
			return
		}
		if time.Now().After(deadline) {
			for series, got := range wrong {
				t.Errorf("%s = %v, want %v", series, got, want[series])
			}
			return
		}
		time.Sleep(pollInterval)
	}
}

func TestSendResultsAreCounted(t *testing.T) {
	tests := []struct {
		name       string
		answer     func(w http.ResponseWriter) error
		wantStatus string
		want       map[string]float64
	}{
		{
			name:       "accepted",
			answer:     accept,
			wantStatus: "sent",
			want: map[string]float64{
				`notifications_sent_total{channel="sms"}`:                     1,
				`notifications_retried_total{channel="sms"}`:                  0,
				`notifications_failed_total{channel="sms"}`:                   0,
				`provider_call_duration_seconds_count{channel="sms"}`:         1,
				`notification_delivery_duration_seconds_count{channel="sms"}`: 1,
			},
		},
		{
			name:       "accepted at the second attempt",
			answer:     inOrder(status(http.StatusInternalServerError), accept),
			wantStatus: "sent",
			want: map[string]float64{
				`notifications_sent_total{channel="sms"}`:                     1,
				`notifications_retried_total{channel="sms"}`:                  1,
				`notifications_failed_total{channel="sms"}`:                   0,
				`provider_call_duration_seconds_count{channel="sms"}`:         2,
				`notification_delivery_duration_seconds_count{channel="sms"}`: 1,
			},
		},
		{
			name:       "permanent failure",
			answer:     status(http.StatusBadRequest),
			wantStatus: "failed",
			want: map[string]float64{
				`notifications_sent_total{channel="sms"}`:                     0,
				`notifications_retried_total{channel="sms"}`:                  0,
				`notifications_failed_total{channel="sms"}`:                   1,
				`provider_call_duration_seconds_count{channel="sms"}`:         1,
				`notification_delivery_duration_seconds_count{channel="sms"}`: 0,
			},
		},
		{
			name:       "attempts used up",
			answer:     status(http.StatusTooManyRequests),
			wantStatus: "failed",
			want: map[string]float64{
				`notifications_sent_total{channel="sms"}`:                     0,
				`notifications_retried_total{channel="sms"}`:                  maxAttempts - 1,
				`notifications_failed_total{channel="sms"}`:                   1,
				`provider_call_duration_seconds_count{channel="sms"}`:         maxAttempts,
				`notification_delivery_duration_seconds_count{channel="sms"}`: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testdb.New(t)
			queries := store.New(db)
			_, client := newProvider(t, tt.answer)
			created := insert(t, queries, "sms", 1)
			metrics, reader := newMetrics()

			runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.DiscardHandler), Metrics: metrics, Channel: "sms", Size: 2, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
			waitFor(t, "the notification is "+tt.wantStatus, func() bool { return get(t, queries, created.ID).Status == tt.wantStatus })

			reader.check(t, tt.want)
		})
	}
}

func TestDeliveryDurationCountsFromTheCreation(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	_, client := newProvider(t, accept)
	created := insert(t, queries, "sms", 1)
	if _, err := db.Exec(t.Context(), "UPDATE notifications SET created_at = now() - interval '45 seconds' WHERE id = $1", created.ID); err != nil {
		t.Fatalf("make the notification 45 seconds old: %v", err)
	}
	metrics, reader := newMetrics()

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.DiscardHandler), Metrics: metrics, Channel: "sms", Size: 2, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "the notification is sent", func() bool { return get(t, queries, created.ID).Status == "sent" })

	reader.check(t, map[string]float64{
		`notification_delivery_duration_seconds_bucket{channel="sms",le="30"}`: 0,
		`notification_delivery_duration_seconds_bucket{channel="sms",le="60"}`: 1,
	})
	if sum := reader.value(t, `notification_delivery_duration_seconds_sum{channel="sms"}`); sum < 45 || sum > 60 {
		t.Errorf("delivery duration = %v s, want 45 to 60 for a notification created 45 s ago", sum)
	}
	if sum := reader.value(t, `provider_call_duration_seconds_sum{channel="sms"}`); sum <= 0 || sum >= 1 {
		t.Errorf("provider call duration = %v s, want above 0 and below 1 for a provider that answers at once", sum)
	}
}

func TestSweptNotificationsAreCounted(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	for _, attempts := range []int16{maxAttempts, maxAttempts - 1, 1} {
		hold(t, db, insert(t, queries, "sms", 1).ID, attempts, "-1 second")
	}
	metrics, reader := newMetrics()

	Sweeper{Queries: queries, Logger: slog.New(slog.DiscardHandler), Metrics: metrics, Events: unreadEvents(t), Interval: pollInterval, QueryTimeout: queryTimeout}.sweep(t.Context())

	reader.check(t, map[string]float64{
		`notifications_failed_total{channel="sms"}`:  1,
		`notifications_retried_total{channel="sms"}`: 2,
		`notifications_sent_total{channel="sms"}`:    0,
	})
}

func TestEveryFailedRateLimiterCallIsCounted(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	_, client := newProvider(t, accept)
	insert(t, queries, "sms", 1)
	redisClient := redis.NewClient(&redis.Options{Dialer: func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("redis is down")
	}})
	t.Cleanup(func() {
		if err := redisClient.Close(); err != nil {
			t.Errorf("close redis client: %v", err)
		}
	})
	var logs syncBuffer
	metrics, reader := newMetrics()

	stop, stopped := runPool(t, Pool{Queries: queries, Provider: client, Limiter: ratelimit.New(redisClient, sendsPerSecond, sendBurst), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Metrics: metrics, Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "the pool has checked the rate limiter twice instead of claiming", func() bool {
		return logs.linesWith(`"msg":"rate limiter still fails, not claiming"`) >= 2
	})
	stop()
	waitToReceive(t, "the pool stops", stopped)

	released := logs.linesWith(`"msg":"rate limiter failed, releasing the notification"`)
	checks := logs.linesWith(`"msg":"rate limiter still fails, not claiming"`)
	if released != 1 {
		t.Fatalf("logs have %d lines for a notification released because the rate limiter failed, want 1", released)
	}
	if got := reader.value(t, `rate_limiter_failures_total{channel="sms"}`); got != float64(released+checks) {
		t.Errorf("rate limiter failures = %v, want %d: 1 by the sender and %d by the poll loop", got, released+checks, checks)
	}
	reader.check(t, map[string]float64{
		`notifications_retried_total{channel="sms"}`:          0,
		`provider_call_duration_seconds_count{channel="sms"}`: 0,
	})
}

func TestBreakerStateIsAGauge(t *testing.T) {
	const (
		rows  = breakerFailures + breakerTrialCalls
		state = `circuit_breaker_state{channel="sms"}`
	)
	db := testdb.New(t)
	queries := store.New(db)
	var healthy atomic.Bool
	arrived, finish := make(chan struct{}, rows), make(chan struct{})
	fake, client := newProvider(t, func(w http.ResponseWriter) error {
		if !healthy.Load() {
			return status(http.StatusInternalServerError)(w)
		}
		arrived <- struct{}{}
		<-finish
		return accept(w)
	})
	for range rows {
		insert(t, queries, "sms", 1)
	}
	metrics, reader := newMetrics()
	var logs syncBuffer
	oneCallEvery50ms := ratelimit.New(testredis.New(t), 20, 1)

	reader.check(t, map[string]float64{state: 0})
	runPool(t, Pool{Queries: queries, Provider: client, Limiter: oneCallEvery50ms, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Metrics: metrics, Channel: "sms", Size: rows, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: 2 * time.Second})
	t.Cleanup(func() {
		select {
		case <-finish:
		default:
			close(finish)
		}
	})
	waitFor(t, "the breaker is open and every notification is pending again", func() bool {
		return reader.value(t, state) == 2 && countByStatus(t, db, "pending") == rows
	})
	if !logs.lineWith(breakerRefused) {
		t.Fatal("logs have no line for a notification the open breaker refused, want one to check that it is not timed")
	}
	if timed, calls := reader.value(t, `provider_call_duration_seconds_count{channel="sms"}`), len(fake.keys()); timed != float64(calls) {
		t.Errorf("%v provider calls timed, want %d, the requests the provider got: a call the breaker refused is not a provider call", timed, calls)
	}
	healthy.Store(true)
	for range breakerTrialCalls {
		waitToReceive(t, "the provider gets a trial call", arrived)
	}
	reader.check(t, map[string]float64{state: 1})
	close(finish)
	waitFor(t, "the breaker is closed", func() bool { return reader.value(t, state) == 0 })
}
