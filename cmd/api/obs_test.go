package main

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

func unreachablePostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), "postgres://postgres@127.0.0.1:1/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("create database pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func unreachableRedis(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close redis client: %v", err)
		}
	})
	return client
}

func silentServer(t *testing.T) string {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	})
	return listener.Addr().String()
}

func silentPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), "postgres://postgres@"+silentServer(t)+"/postgres?sslmode=disable")
	if err != nil {
		t.Fatalf("create database pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func silentRedis(t *testing.T) *redis.Client {
	t.Helper()
	options, err := parseRedisURL("redis://" + silentServer(t))
	if err != nil {
		t.Fatalf("parse redis url: %v", err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close redis client: %v", err)
		}
	})
	return client
}

func get(t *testing.T, handler http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, target, nil))
	return rec
}

func metricValue(t *testing.T, metrics, series string) float64 {
	t.Helper()
	for line := range strings.Lines(metrics) {
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
	t.Fatalf("metrics have no series %s:\n%s", series, metrics)
	return 0
}

func TestRunRejectsABadRedisURL(t *testing.T) {
	for _, redisURL := range []string{"", "http://redis:6379", "redis://redis:port"} {
		err := run(t.Context(), slog.New(slog.DiscardHandler), "postgres://postgres@127.0.0.1:1/postgres", redisURL)

		if err == nil || !strings.Contains(err.Error(), "REDIS_URL") {
			t.Errorf("REDIS_URL %q: error = %v, want one about REDIS_URL", redisURL, err)
		}
	}
}

func TestReadyz(t *testing.T) {
	tests := []struct {
		name           string
		postgres       func(t *testing.T) *pgxpool.Pool
		redis          func(t *testing.T) *redis.Client
		callerGaveUp   bool
		wantStatus     int
		wantDependency string
	}{
		{name: "postgres and redis answer", postgres: testdb.New, redis: testredis.New, wantStatus: http.StatusOK},
		{name: "postgres is down", postgres: unreachablePostgres, redis: testredis.New, wantStatus: http.StatusServiceUnavailable, wantDependency: "postgres"},
		{name: "redis is down", postgres: testdb.New, redis: unreachableRedis, wantStatus: http.StatusServiceUnavailable, wantDependency: "redis"},
		{name: "postgres never answers", postgres: silentPostgres, redis: testredis.New, wantStatus: http.StatusServiceUnavailable, wantDependency: "postgres"},
		{name: "redis never answers", postgres: testdb.New, redis: silentRedis, wantStatus: http.StatusServiceUnavailable, wantDependency: "redis"},
		{name: "caller gave up", postgres: testdb.New, redis: testredis.New, callerGaveUp: true, wantStatus: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			handler := newHandler(tt.postgres(t), tt.redis(t), obs.NewLogger(&logs))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.callerGaveUp {
				cancel()
			}
			rec := httptest.NewRecorder()
			started := time.Now()

			handler.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodGet, "/readyz", nil))

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, logs %s", rec.Code, tt.wantStatus, logs.String())
			}
			if took := time.Since(started); took > 2*time.Second {
				t.Errorf("readyz took %s, want at most 2 s: 1 s for each dependency", took)
			}
			if id := rec.Header().Get("X-Correlation-ID"); id != "" {
				t.Errorf("X-Correlation-ID of the response = %q, want none", id)
			}
			if tt.wantDependency == "" && logs.Len() > 0 {
				t.Errorf("logs = %q, want none", logs.String())
			}
			if tt.wantDependency != "" && (strings.Count(logs.String(), "\n") != 1 || !strings.Contains(logs.String(), `"level":"ERROR","msg":"not ready","dependency":"`+tt.wantDependency+`"`)) {
				t.Errorf("logs = %q, want one error line that names %s", logs.String(), tt.wantDependency)
			}
		})
	}
}

func TestMetricsReportTheQueue(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	insert := func(channel string, priority int16, change string) {
		t.Helper()
		row, err := queries.InsertNotification(t.Context(), store.InsertNotificationParams{ID: uuid.Must(uuid.NewV7()), Recipient: "recipient", Channel: channel, Content: "hello", Priority: priority})
		if err != nil {
			t.Fatalf("insert notification: %v", err)
		}
		if change == "" {
			return
		}
		if _, err := db.Exec(t.Context(), "UPDATE notifications SET "+change+" WHERE id = $1", row.ID); err != nil {
			t.Fatalf("change the notification: %v", err)
		}
	}
	insert("sms", 0, "")
	insert("sms", 0, "next_attempt_at = now() - interval '90 seconds'")
	insert("sms", 0, "status = 'sent'")
	insert("sms", 0, "status = 'processing', locked_until = now() + interval '60 seconds'")
	insert("sms", 1, "next_attempt_at = now() + interval '1 hour'")
	insert("email", 2, "")
	var logs bytes.Buffer

	rec := get(t, newHandler(db, nil, obs.NewLogger(&logs)), "/metrics")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusOK, rec.Body)
	}
	metrics := rec.Body.String()
	for series, want := range map[string]float64{
		`notifications_pending{channel="sms",priority="high"}`:                      2,
		`notifications_pending{channel="sms",priority="normal"}`:                    1,
		`notifications_pending{channel="sms",priority="low"}`:                       0,
		`notifications_pending{channel="email",priority="low"}`:                     1,
		`notifications_pending{channel="push",priority="high"}`:                     0,
		`notifications_oldest_pending_age_seconds{channel="sms",priority="normal"}`: 0,
		`notifications_oldest_pending_age_seconds{channel="push",priority="low"}`:   0,
	} {
		if got := metricValue(t, metrics, series); got != want {
			t.Errorf("%s = %v, want %v", series, got, want)
		}
	}
	if age := metricValue(t, metrics, `notifications_oldest_pending_age_seconds{channel="sms",priority="high"}`); age < 90 || age > 120 {
		t.Errorf("oldest pending age of sms high = %v s, want 90 to 120 for a notification due 90 s ago", age)
	}
	if age := metricValue(t, metrics, `notifications_oldest_pending_age_seconds{channel="email",priority="low"}`); age < 0 || age > 30 {
		t.Errorf("oldest pending age of email low = %v s, want 0 to 30 for a notification created just now", age)
	}
	for _, name := range []string{"notifications_pending{", "notifications_oldest_pending_age_seconds{"} {
		if series := strings.Count(metrics, "\n"+name); series != 9 {
			t.Errorf("metrics have %d series of %s, want 9: one per channel and priority", series, name)
		}
	}
	if !strings.Contains(metrics, "\ngo_goroutines ") || !strings.Contains(metrics, "\nprocess_") {
		t.Error("metrics have no Go runtime or process series")
	}
	if logs.Len() > 0 {
		t.Errorf("logs = %q, want none", logs.String())
	}
	if id := rec.Header().Get("X-Correlation-ID"); id != "" {
		t.Errorf("X-Correlation-ID of the response = %q, want none", id)
	}
}

func TestMetricsAreServedWithoutTheQueueWhenTheDatabaseIsDown(t *testing.T) {
	var logs bytes.Buffer
	handler := newHandler(unreachablePostgres(t), nil, obs.NewLogger(&logs))

	first := get(t, handler, "/metrics")
	second := get(t, handler, "/metrics")

	if first.Code != http.StatusOK || second.Code != http.StatusOK {
		t.Fatalf("statuses = %d and %d, want %d twice", first.Code, second.Code, http.StatusOK)
	}
	if !strings.Contains(first.Body.String(), "\ngo_goroutines ") {
		t.Error("metrics have no Go runtime series, want every series but the queue")
	}
	if strings.Contains(first.Body.String(), "notifications_pending{") {
		t.Error("metrics have a pending count, want none when it cannot be read")
	}
	if got := metricValue(t, second.Body.String(), `promhttp_metric_handler_errors_total{cause="gathering"}`); got != 1 {
		t.Errorf("gathering errors at the second scrape = %v, want 1 for the first", got)
	}
	if lines := strings.Count(logs.String(), `"level":"ERROR"`); lines != 2 || !strings.Contains(logs.String(), "count pending notifications") {
		t.Errorf("logs = %q, want one error line per scrape about the pending count", logs.String())
	}
}
