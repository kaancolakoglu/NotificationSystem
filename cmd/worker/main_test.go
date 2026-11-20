package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
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
			err := run(t.Context(), slog.New(slog.DiscardHandler), "postgres://postgres@127.0.0.1:1/postgres", tt.providerURL, time.Second)

			if err == nil || !strings.Contains(err.Error(), "PROVIDER_URL") {
				t.Errorf("error = %v, want one about PROVIDER_URL", err)
			}
		})
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

	err = run(t.Context(), slog.New(slog.DiscardHandler), "postgres://postgres@"+listener.Addr().String()+"/postgres?sslmode=disable", "http://fakeprovider:8081/", 200*time.Millisecond)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want a deadline exceeded error", err)
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

	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() {
		stopped <- run(ctx, slog.New(slog.DiscardHandler), db.Config().ConnString(), provider.URL, 5*time.Second)
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
