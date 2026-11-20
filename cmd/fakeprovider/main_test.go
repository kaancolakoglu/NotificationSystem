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
	accepted := func(t *testing.T) map[string]string {
		t.Helper()
		rec := post(t, handler, "key", `{"to":"+905551234567","channel":"sms","content":"hello"}`)

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

	first := accepted(t)
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

	if second := accepted(t); second["messageId"] == first["messageId"] {
		t.Errorf("two answers share the messageId %q", first["messageId"])
	}
}

func post(t *testing.T, handler http.Handler, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rec, req)
	return rec
}

func TestScriptedAnswers(t *testing.T) {
	const scripted = `{"to":"+905551234567","channel":"sms","content":"script:500,429,202,400"}`
	handler := newHandler(slog.New(slog.DiscardHandler))

	for _, key := range []string{"first", "second"} {
		for i, want := range []int{http.StatusInternalServerError, http.StatusTooManyRequests, http.StatusAccepted, http.StatusBadRequest, http.StatusAccepted, http.StatusAccepted} {
			rec := post(t, handler, key, scripted)

			if rec.Code != want {
				t.Fatalf("%s notification, request %d: status = %d, want %d", key, i+1, rec.Code, want)
			}
			wantRetryAfter := map[bool]string{true: "1", false: ""}[want == http.StatusTooManyRequests]
			if got := rec.Header().Get("Retry-After"); got != wantRetryAfter {
				t.Errorf("%s notification, request %d: Retry-After = %q, want %q", key, i+1, got, wantRetryAfter)
			}
			var answer map[string]string
			if want != http.StatusAccepted {
				if rec.Body.Len() != 0 {
					t.Errorf("%s notification, request %d: body = %s, want none", key, i+1, rec.Body)
				}
				continue
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &answer); err != nil || answer["messageId"] == "" {
				t.Errorf("%s notification, request %d: body = %s, error %v, want a messageId", key, i+1, rec.Body, err)
			}
		}
	}

	if rec := post(t, handler, "first", `{"to":"+905551234567","channel":"sms","content":"hello"}`); rec.Code != http.StatusAccepted {
		t.Errorf("request without a script: status = %d, want %d", rec.Code, http.StatusAccepted)
	}
}

func TestRejectsWhatItCannotRead(t *testing.T) {
	handler := newHandler(slog.New(slog.DiscardHandler))
	tests := []struct {
		name string
		body string
	}{
		{name: "not JSON", body: `hello`},
		{name: "empty script", body: `{"content":"script:"}`},
		{name: "not a number", body: `{"content":"script:500,later"}`},
		{name: "below 200", body: `{"content":"script:199"}`},
		{name: "above 599", body: `{"content":"script:600"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if rec := post(t, handler, "key", tt.body); rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusBadRequest)
			}
		})
	}

	if rec := post(t, handler, "key", `{"content":"script:500"}`); rec.Code != http.StatusInternalServerError {
		t.Errorf("first request with a script after rejected ones: status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}
