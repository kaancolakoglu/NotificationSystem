package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

func TestRunExportsTheSpansOfADeliveryBeforeItReturns(t *testing.T) {
	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	db := testdb.New(t)
	queries := store.New(db)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(provider.Close)
	var mu sync.Mutex
	var exported [][]byte
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil || r.URL.Path != "/v1/traces" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		exported = append(exported, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(collector.Close)
	id := uuid.Must(uuid.NewV7())
	if _, err := queries.InsertNotification(t.Context(), store.InsertNotificationParams{ID: id, Recipient: "+905551234567", Channel: "sms", Content: "hello", Priority: 1, TraceContext: pgtype.Text{String: traceParent, Valid: true}}); err != nil {
		t.Fatalf("insert notification: %v", err)
	}
	redisOptions := testredis.New(t).Options()
	redisURL := fmt.Sprintf("redis://%s/%d", redisOptions.Addr, redisOptions.DB)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := make(chan error, 1)

	go func() {
		stopped <- run(ctx, slog.New(slog.DiscardHandler), db.Config().ConnString(), redisURL, provider.URL, collector.URL, "127.0.0.1:0", 5*time.Second)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		row, err := queries.GetNotification(t.Context(), id)
		if err != nil {
			t.Fatalf("get notification: %v", err)
		}
		if row.Status == "sent" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("notification is %s, want sent", row.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-stopped; err != nil {
		t.Fatalf("run returned %v after it was told to stop, want no error", err)
	}

	mu.Lock()
	all := bytes.Join(exported, nil)
	mu.Unlock()
	for _, want := range []string{"notification-worker", "claim", "provider call", id.String()} {
		if !strings.Contains(string(all), want) {
			t.Errorf("the spans exported before run returned do not contain %q", want)
		}
	}
}

func TestRunRejectsATraceEndpointThatIsNotAURL(t *testing.T) {
	err := run(t.Context(), slog.New(slog.DiscardHandler), "postgres://postgres@127.0.0.1:1/postgres", "redis://127.0.0.1:1", "http://fakeprovider:8081/", "jaeger:4318", "127.0.0.1:0", time.Second)

	if err == nil || !strings.Contains(err.Error(), "OTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Errorf("run = %v, want an error that names OTEL_EXPORTER_OTLP_ENDPOINT", err)
	}
}

func TestRunDoesNotWaitLongForAnExportThatGetsNoAnswer(t *testing.T) {
	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	db := testdb.New(t)
	queries := store.New(db)
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(provider.Close)
	answer := make(chan struct{})
	silent := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-answer
	}))
	t.Cleanup(silent.Close)
	t.Cleanup(func() { close(answer) })
	id := uuid.Must(uuid.NewV7())
	if _, err := queries.InsertNotification(t.Context(), store.InsertNotificationParams{ID: id, Recipient: "+905551234567", Channel: "sms", Content: "hello", Priority: 1, TraceContext: pgtype.Text{String: traceParent, Valid: true}}); err != nil {
		t.Fatalf("insert notification: %v", err)
	}
	redisOptions := testredis.New(t).Options()
	redisURL := fmt.Sprintf("redis://%s/%d", redisOptions.Addr, redisOptions.DB)
	var logs syncBuffer
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := make(chan error, 1)

	go func() {
		stopped <- run(ctx, slog.New(slog.NewJSONHandler(&logs, nil)), db.Config().ConnString(), redisURL, provider.URL, silent.URL, "127.0.0.1:0", 5*time.Second)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		row, err := queries.GetNotification(t.Context(), id)
		if err != nil {
			t.Fatalf("get notification: %v", err)
		}
		if row.Status == "sent" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("notification is %s, want sent", row.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	told := time.Now()
	cancel()
	err := <-stopped
	if err != nil {
		t.Errorf("run returned %v after it was told to stop, want no error: spans that are not exported do not fail the worker", err)
	}
	if took := time.Since(told); took < traceFlushTimeout || took > traceFlushTimeout+3*time.Second {
		t.Errorf("run returned %s after it was told to stop, want about the %s it gives the export", took, traceFlushTimeout)
	}
	if !strings.Contains(logs.String(), `"level":"ERROR","msg":"flush traces"`) {
		t.Errorf("logs = %s, want an error line for the spans that were not exported", logs.String())
	}
}
