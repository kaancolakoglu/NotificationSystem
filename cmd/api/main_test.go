package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	spec "github.com/kaancolakoglu/NotificationSystem/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
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

func TestServerWritesItsOwnMessagesAsJSONLines(t *testing.T) {
	var logs bytes.Buffer
	server := newServer(nil, obs.NewLogger(&logs))
	if server.ErrorLog == nil {
		t.Fatal("the server has no error log, so net/http prints plain text to stderr")
	}

	server.ErrorLog.Print("http: panic serving 127.0.0.1:1: boom")

	var line struct {
		Level string `json:"level"`
		Msg   string `json:"msg"`
	}
	if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
		t.Fatalf("logs = %q, want one JSON line: %v", logs.String(), err)
	}
	if line.Level != "ERROR" || line.Msg != "http: panic serving 127.0.0.1:1: boom" {
		t.Errorf("line = %+v, want level ERROR and the message as written", line)
	}
}

func TestOnlyV1RequestsAreLoggedAndGetACorrelationID(t *testing.T) {
	tests := []struct {
		target     string
		wantStatus int
		wantLogged bool
	}{
		{target: "/v1/notifications?limit=0", wantStatus: http.StatusBadRequest, wantLogged: true},
		{target: "/healthz", wantStatus: http.StatusOK},
		{target: "/openapi.yaml", wantStatus: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			var logs bytes.Buffer
			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.target, nil)

			newHandler(nil, obs.NewLogger(&logs)).ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, tt.wantStatus, rec.Body)
			}
			id := rec.Header().Get("X-Correlation-ID")
			if (id != "") != tt.wantLogged {
				t.Errorf("X-Correlation-ID of the response = %q, want one = %t", id, tt.wantLogged)
			}
			if tt.wantLogged && (!strings.Contains(logs.String(), `"msg":"request handled"`) || !strings.Contains(logs.String(), `"correlation_id":"`+id+`"`)) {
				t.Errorf("logs = %q, want a request line with the correlation id %q", logs.String(), id)
			}
			if !tt.wantLogged && logs.Len() > 0 {
				t.Errorf("logs = %q, want none", logs.String())
			}
		})
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

func TestBatchBodyLimit(t *testing.T) {
	post := func(t *testing.T, contentBytes int) *httptest.ResponseRecorder {
		t.Helper()
		body := `{"notifications":[{"recipient":"user@example.com","channel":"email","content":"` + strings.Repeat("a", contentBytes) + `"}]}`
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/notifications/batch", strings.NewReader(body))
		newHandler(nil, slog.New(slog.DiscardHandler)).ServeHTTP(rec, req)
		return rec
	}

	t.Run("body over the single limit is read", func(t *testing.T) {
		rec := post(t, maxBodyBytes)

		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusUnprocessableEntity, rec.Body)
		}
		if !strings.Contains(rec.Body.String(), "no notification in the batch is valid") {
			t.Errorf("body = %s, want the batch to be rejected by validation", rec.Body)
		}
	})

	t.Run("body over the batch limit is 413", func(t *testing.T) {
		rec := post(t, maxBatchBodyBytes)

		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
		}
		if !strings.Contains(rec.Body.String(), "request body must be at most 10485760 bytes") {
			t.Errorf("body = %s, want it to mention the size limit", rec.Body)
		}
	})
}

func TestQueryParametersAreCheckedPerRoute(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		target      string
		wantMessage string
	}{
		{name: "known on the list", method: http.MethodGet, target: "/v1/notifications?limit=0", wantMessage: "limit must be 1 to 200"},
		{name: "unknown on the list", method: http.MethodGet, target: "/v1/notifications?size=10", wantMessage: `parameter \"size\" is not known`},
		{name: "list parameter on another route", method: http.MethodGet, target: "/v1/batches/019a0000-0000-7000-8000-000000000000?limit=10", wantMessage: `parameter \"limit\" is not known`},
		{name: "list parameter on the batch route", method: http.MethodPost, target: "/v1/notifications/batch?limit=10", wantMessage: `parameter \"limit\" is not known`},
		{name: "list parameter on the cancel route", method: http.MethodPost, target: "/v1/notifications/019a0000-0000-7000-8000-000000000000/cancel?limit=10", wantMessage: `parameter \"limit\" is not known`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), tt.method, tt.target, nil)

			newHandler(nil, slog.New(slog.DiscardHandler)).ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusBadRequest, rec.Body)
			}
			if !strings.Contains(rec.Body.String(), tt.wantMessage) {
				t.Errorf("body = %s, want it to contain %s", rec.Body, tt.wantMessage)
			}
		})
	}
}
