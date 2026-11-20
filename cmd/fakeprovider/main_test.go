package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHealthz(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/healthz", nil)

	newHandler(slog.New(slog.DiscardHandler)).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestAnswersAccepted(t *testing.T) {
	handler := newHandler(slog.New(slog.DiscardHandler))
	post := func(t *testing.T) map[string]string {
		t.Helper()
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(`{"to":"+905551234567","channel":"sms","content":"hello"}`))
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
		}
		res := rec.Result()
		if err := res.Body.Close(); err != nil {
			t.Errorf("close answer: %v", err)
		}
		if got := res.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content type = %q, want %q", got, "application/json")
		}
		var answer map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &answer); err != nil {
			t.Fatalf("body %s is not a JSON object of strings: %v", rec.Body, err)
		}
		return answer
	}

	first := post(t)
	if len(first) != 3 {
		t.Errorf("answer = %v, want exactly messageId, status and timestamp", first)
	}
	if first["messageId"] == "" {
		t.Error("messageId is empty")
	}
	if first["status"] != "accepted" {
		t.Errorf("status = %q, want %q", first["status"], "accepted")
	}
	if _, err := time.Parse(time.RFC3339, first["timestamp"]); err != nil {
		t.Errorf("timestamp %q is not RFC 3339: %v", first["timestamp"], err)
	}

	if second := post(t); second["messageId"] == first["messageId"] {
		t.Errorf("two answers share the messageId %q", first["messageId"])
	}
}
