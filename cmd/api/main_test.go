package main

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	spec "github.com/kaancolakoglu/NotificationSystem/api"
)

func TestHealthz(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil)

	newHandler(nil, slog.New(slog.DiscardHandler)).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestOpenAPISpec(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/openapi.yaml", nil)

	newHandler(nil, slog.New(slog.DiscardHandler)).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/yaml" {
		t.Errorf("content type = %q, want %q", got, "application/yaml")
	}
	if !bytes.Equal(rec.Body.Bytes(), spec.Spec) {
		t.Error("body is not the embedded spec")
	}
}

func TestBodyOverLimitIsRejected(t *testing.T) {
	body := `{"recipient":"user@example.com","channel":"email","content":"` + strings.Repeat("a", maxBodyBytes) + `"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/notifications", strings.NewReader(body))

	newHandler(nil, slog.New(slog.DiscardHandler)).ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
	if !strings.Contains(rec.Body.String(), "request body must be at most 1048576 bytes") {
		t.Errorf("body = %s, want it to mention the size limit", rec.Body)
	}
}
