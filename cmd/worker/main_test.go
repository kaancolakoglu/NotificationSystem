package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

func TestRunRejectsABadProviderURL(t *testing.T) {
	tests := []struct {
		name        string
		providerURL string
	}{
		{name: "not set", providerURL: ""},
		{name: "no scheme", providerURL: "fakeprovider:8081"},
		{name: "host only", providerURL: "webhook.site/token"},
		{name: "not http", providerURL: "ftp://fakeprovider:8081/"},
		{name: "no host", providerURL: "http:///token"},
		{name: "not a URL", providerURL: "http://[::1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(t.Context(), slog.New(slog.DiscardHandler), "postgres://postgres@127.0.0.1:1/postgres", "redis://127.0.0.1:1", tt.providerURL, time.Second)

			if err == nil || !strings.Contains(err.Error(), "PROVIDER_URL") {
				t.Errorf("error = %v, want one about PROVIDER_URL", err)
			}
		})
	}
}

func TestRunRejectsABadRedisURL(t *testing.T) {
	for _, redisURL := range []string{"", "http://redis:6379", "redis://redis:port", "redis://redis:6379/first"} {
		err := run(t.Context(), slog.New(slog.DiscardHandler), "postgres://postgres@127.0.0.1:1/postgres", redisURL, "http://fakeprovider:8081/", time.Second)

		if err == nil || !strings.Contains(err.Error(), "REDIS_URL") {
			t.Errorf("REDIS_URL %q: error = %v, want one about REDIS_URL", redisURL, err)
		}
	}
}

func TestCheckProviderURLAcceptsHTTPAndHTTPS(t *testing.T) {
	for _, providerURL := range []string{"http://fakeprovider:8081/", "https://webhook.site/token"} {
		if err := checkProviderURL(providerURL); err != nil {
			t.Errorf("check %s: %v", providerURL, err)
		}
	}
}

func TestRunStopsWhenTheDatabaseNeverAnswers(t *testing.T) {
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

	err = run(t.Context(), slog.New(slog.DiscardHandler), "postgres://postgres@"+listener.Addr().String()+"/postgres?sslmode=disable", "redis://127.0.0.1:1", "http://fakeprovider:8081/", 200*time.Millisecond)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want a deadline exceeded error", err)
	}
}

func TestRunKeepsTheProviderLimit(t *testing.T) {
	const (
		rows          = 150
		providerLimit = 100
	)
	db := testdb.New(t)
	queries := store.New(db)
	var mu sync.Mutex
	var arrivals []time.Time
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(provider.Close)
	for range rows {
		if _, err := queries.InsertNotification(t.Context(), store.InsertNotificationParams{ID: uuid.Must(uuid.NewV7()), Recipient: "+905551234567", Channel: "sms", Content: "hello", Priority: 1}); err != nil {
			t.Fatalf("insert notification: %v", err)
		}
	}

	redisOptions := testredis.New(t).Options()
	redisURL := fmt.Sprintf("redis://%s/%d", redisOptions.Addr, redisOptions.DB)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() {
		stopped <- run(ctx, slog.New(slog.DiscardHandler), db.Config().ConnString(), redisURL, provider.URL, 5*time.Second)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		mu.Lock()
		got := len(arrivals)
		mu.Unlock()
		if got == rows {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("provider got %d requests, want %d", got, rows)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-stopped; err != nil {
		t.Errorf("run returned %v after it was told to stop, want no error", err)
	}

	most, end := 0, 0
	for start := range arrivals {
		for end < len(arrivals) && arrivals[end].Sub(arrivals[start]) < time.Second {
			end++
		}
		most = max(most, end-start)
	}
	if most > providerLimit {
		t.Errorf("busiest second at the provider had %d requests, want at most %d", most, providerLimit)
	}
	if took := arrivals[rows-1].Sub(arrivals[0]); took < 1400*time.Millisecond || took > 4*time.Second {
		t.Errorf("%d requests took %s, want about 1.6 s at 90 per second after a burst of 5", rows, took)
	}
}

func TestRunLetsASendInFlightFinishAfterItIsToldToStop(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	arrived, stopSeen := make(chan struct{}, 1), make(chan struct{})
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		arrived <- struct{}{}
		<-stopSeen
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(provider.Close)
	unblockProvider := sync.OnceFunc(func() { close(stopSeen) })
	t.Cleanup(unblockProvider)
	row, err := queries.InsertNotification(t.Context(), store.InsertNotificationParams{ID: uuid.Must(uuid.NewV7()), Recipient: "+905551234567", Channel: "sms", Content: "hello", Priority: 1})
	if err != nil {
		t.Fatalf("insert notification: %v", err)
	}

	redisOptions := testredis.New(t).Options()
	redisURL := fmt.Sprintf("redis://%s/%d", redisOptions.Addr, redisOptions.DB)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() {
		stopped <- run(ctx, slog.New(slog.DiscardHandler), db.Config().ConnString(), redisURL, provider.URL, 5*time.Second)
	}()
	select {
	case <-arrived:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting until the provider gets the notification")
	}
	cancel()
	unblockProvider()

	if err := <-stopped; err != nil {
		t.Errorf("run returned %v after it was told to stop, want no error", err)
	}
	stored, err := queries.GetNotification(t.Context(), row.ID)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	if stored.Status != "sent" {
		t.Errorf("notification in flight at the stop is %s, want sent", stored.Status)
	}
}

func TestRunRetriesFailedSendsAfterTheBackoff(t *testing.T) {
	const rows = 20
	db := testdb.New(t)
	queries := store.New(db)
	var mu sync.Mutex
	failed := map[string]bool{}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		mu.Lock()
		first := !failed[key]
		failed[key] = true
		mu.Unlock()
		if first {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(provider.Close)
	ids := make([]uuid.UUID, rows)
	for i := range ids {
		row, err := queries.InsertNotification(t.Context(), store.InsertNotificationParams{ID: uuid.Must(uuid.NewV7()), Recipient: "+905551234567", Channel: "sms", Content: "hello", Priority: 1})
		if err != nil {
			t.Fatalf("insert notification: %v", err)
		}
		ids[i] = row.ID
	}

	redisOptions := testredis.New(t).Options()
	redisURL := fmt.Sprintf("redis://%s/%d", redisOptions.Addr, redisOptions.DB)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() {
		stopped <- run(ctx, slog.New(slog.DiscardHandler), db.Config().ConnString(), redisURL, provider.URL, 5*time.Second)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for _, id := range ids {
		for {
			row, err := queries.GetNotification(t.Context(), id)
			if err != nil {
				t.Fatalf("get notification: %v", err)
			}
			if row.Status == "sent" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("notification is %s with %d attempts, want sent", row.Status, row.Attempts)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	cancel()
	if err := <-stopped; err != nil {
		t.Errorf("run returned %v after it was told to stop, want no error", err)
	}

	var longest time.Duration
	for _, id := range ids {
		row, err := queries.GetNotification(t.Context(), id)
		if err != nil {
			t.Fatalf("get notification: %v", err)
		}
		history, err := queries.ListDeliveryAttempts(t.Context(), id)
		if err != nil {
			t.Fatalf("list delivery attempts: %v", err)
		}
		if row.Attempts != 2 || len(history) != 2 {
			t.Fatalf("attempts = %d, delivery attempts = %+v, want 2 and two of them", row.Attempts, history)
		}
		delay := row.NextAttemptAt.Sub(history[0].CreatedAt)
		if delay < 0 || delay >= 2*time.Second {
			t.Errorf("retry delay after the first attempt = %s, want 0 up to 2 s", delay)
		}
		longest = max(longest, delay)
	}
	if longest < time.Second {
		t.Errorf("longest of %d retry delays after the first attempt = %s, want some above 1 s", rows, longest)
	}
}

type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func TestRunLogsJSONLinesWithTheWorkerAndTheCorrelationID(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	if _, err := queries.InsertNotification(t.Context(), store.InsertNotificationParams{ID: uuid.Must(uuid.NewV7()), Recipient: "+905551234567", Channel: "sms", Content: "hello", Priority: 1, CorrelationID: pgtype.Text{String: "order-42", Valid: true}}); err != nil {
		t.Fatalf("insert notification: %v", err)
	}
	var logs syncBuffer

	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() {
		stopped <- run(ctx, obs.NewLogger(&logs).With("worker", "worker-1"), db.Config().ConnString(), "redis://127.0.0.1:1", "http://127.0.0.1:1/", 5*time.Second)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for !strings.Contains(logs.String(), `"msg":"rate limiter failed, releasing the notification"`) {
		if time.Now().After(deadline) {
			t.Fatalf("logs = %s, want a line for the notification released because Redis is down", logs.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-stopped; err != nil {
		t.Errorf("run returned %v after it was told to stop, want no error", err)
	}

	released, fromRedis := false, false
	for line := range strings.Lines(logs.String()) {
		var decoded struct {
			Msg           string `json:"msg"`
			Worker        string `json:"worker"`
			CorrelationID string `json:"correlation_id"`
		}
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("log line %s is not JSON: %v", line, err)
		}
		if decoded.Worker != "worker-1" {
			t.Errorf("log line %s has the worker %q, want the one of the logger that run was given, %q", line, decoded.Worker, "worker-1")
		}
		released = released || (decoded.Msg == "rate limiter failed, releasing the notification" && decoded.CorrelationID == "order-42")
		fromRedis = fromRedis || decoded.Msg == "redis client"
	}
	if !released {
		t.Error("logs have no line with the correlation id for the released notification")
	}
	if !fromRedis {
		t.Error("logs have no line from the Redis client, want its own messages as JSON lines")
	}
}

func TestRunSendsANotificationOnEveryChannel(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(provider.Close)
	recipients := map[string]string{"sms": "+905551234567", "email": "user@example.com", "push": "device-token-1"}
	var ids []uuid.UUID
	for channel, recipient := range recipients {
		row, err := queries.InsertNotification(t.Context(), store.InsertNotificationParams{
			ID:        uuid.Must(uuid.NewV7()),
			Recipient: recipient,
			Channel:   channel,
			Content:   "hello",
			Priority:  1,
		})
		if err != nil {
			t.Fatalf("insert %s notification: %v", channel, err)
		}
		ids = append(ids, row.ID)
	}

	redisOptions := testredis.New(t).Options()
	redisURL := fmt.Sprintf("redis://%s/%d", redisOptions.Addr, redisOptions.DB)
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() {
		stopped <- run(ctx, slog.New(slog.DiscardHandler), db.Config().ConnString(), redisURL, provider.URL, 5*time.Second)
	}()

	deadline := time.Now().Add(20 * time.Second)
	for _, id := range ids {
		for {
			row, err := queries.GetNotification(t.Context(), id)
			if err != nil {
				t.Fatalf("get notification: %v", err)
			}
			if row.Status == "sent" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s notification is %s, want sent", row.Channel, row.Status)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	cancel()
	if err := <-stopped; err != nil {
		t.Errorf("run returned %v after it was told to stop, want no error", err)
	}
}
