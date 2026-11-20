package api_test

import (
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace/noop"

	"github.com/kaancolakoglu/NotificationSystem/internal/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/events"
)

type brokenWriter struct {
	header   http.Header
	statuses []int
}

func (w *brokenWriter) Header() http.Header {
	return w.header
}

func (w *brokenWriter) WriteHeader(status int) {
	w.statuses = append(w.statuses, status)
}

func (w *brokenWriter) Write([]byte) (int, error) {
	return 0, errors.New("connection reset")
}

func TestFailedResponseWriteIsNotAnsweredAgain(t *testing.T) {
	var logs bytes.Buffer
	mux := http.NewServeMux()
	api.Register(mux, nil, nil, events.Publisher{}, untraced, slog.New(slog.NewJSONHandler(&logs, nil)))

	w := &brokenWriter{header: http.Header{}}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/notifications", strings.NewReader(`{"recipient":"0555","channel":"sms","content":"hello"}`))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(w, req)

	if want := []int{http.StatusUnprocessableEntity}; !slices.Equal(w.statuses, want) {
		t.Errorf("statuses written = %v, want %v", w.statuses, want)
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[1], `"msg":"request handled"`) {
		t.Fatalf("logged %d lines, want the failed write and the request: %s", len(lines), logs.String())
	}
	for _, want := range []string{`"msg":"write response"`, `"method":"POST"`, `"path":"/v1/notifications"`, `"status":422`, `"error":"connection reset"`} {
		if !strings.Contains(lines[0], want) {
			t.Errorf("log line = %s, want it to contain %s", lines[0], want)
		}
	}
}

var untraced = noop.NewTracerProvider().Tracer("")
