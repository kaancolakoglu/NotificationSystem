package main

import (
	"context"
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
	if err := <-stopped; !errors.Is(err, context.Canceled) {
		t.Errorf("run returned %v, want the cancelled context error", err)
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
	if took := arrivals[rows-1].Sub(arrivals[0]); took < 1500*time.Millisecond || took > 4*time.Second {
		t.Errorf("%d requests took %s, want about 1.6 s at 90 per second after a burst of 5", rows, took)
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
	if err := <-stopped; !errors.Is(err, context.Canceled) {
		t.Errorf("run returned %v, want the cancelled context error", err)
	}
}
