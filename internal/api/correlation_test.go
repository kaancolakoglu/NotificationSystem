package api_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kaancolakoglu/NotificationSystem/internal/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
)

const (
	correlationIDHeader  = "X-Correlation-ID"
	correlationIDMessage = "X-Correlation-ID must be sent once and be 1 to 128 printable ASCII characters without spaces"
	smsBody              = `{"recipient":"+905551234567","channel":"sms","content":"hello"}`
)

func newLoggedServer(t *testing.T) (*http.ServeMux, *pgxpool.Pool, *bytes.Buffer) {
	t.Helper()
	pool := testdb.New(t)
	var logs bytes.Buffer
	mux := http.NewServeMux()
	api.Register(mux, pool, obs.NewLogger(&logs))
	return mux, pool, &logs
}

func send(t *testing.T, mux *http.ServeMux, method, path, body string, header map[string][]string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for name, values := range header {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	mux.ServeHTTP(rec, req)
	return rec
}

func wantGeneratedID(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	id := rec.Header().Get(correlationIDHeader)
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.Version() != 7 {
		t.Fatalf("%s of the response = %q, want a generated UUIDv7", correlationIDHeader, id)
	}
	return id
}

func wantLogLine(t *testing.T, logs *bytes.Buffer, parts ...string) {
	t.Helper()
	for line := range strings.Lines(logs.String()) {
		found := true
		for _, part := range parts {
			found = found && strings.Contains(line, part)
		}
		if found {
			return
		}
	}
	t.Errorf("logs = %s, want a line with %v", logs, parts)
}

func wantRequestLine(t *testing.T, logs *bytes.Buffer, parts ...string) {
	t.Helper()
	wantLogLine(t, logs, append(parts, `"level":"INFO"`, `"msg":"request handled"`, `"duration_seconds":`)...)
}

func TestCorrelationIDIsGeneratedWhenNotSent(t *testing.T) {
	mux, pool, logs := newLoggedServer(t)

	rec := send(t, mux, http.MethodPost, "/v1/notifications", smsBody, nil)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
	}
	id := wantGeneratedID(t, rec)
	row, err := store.New(pool).GetNotification(t.Context(), decodeBody[api.Notification](t, rec).Id)
	if err != nil {
		t.Fatalf("get stored row: %v", err)
	}
	if !row.CorrelationID.Valid || row.CorrelationID.String != id {
		t.Errorf("stored correlation id = %+v, want %q", row.CorrelationID, id)
	}
	wantRequestLine(t, logs, `"method":"POST"`, `"path":"/v1/notifications"`, `"status":202`, `"correlation_id":"`+id+`"`)

	if second := wantGeneratedID(t, send(t, mux, http.MethodPost, "/v1/notifications", smsBody, nil)); second == id {
		t.Errorf("two requests got the correlation id %q, want one each", id)
	}
}

func TestSentCorrelationIDIsUsed(t *testing.T) {
	mux, pool, logs := newLoggedServer(t)
	for _, id := range []string{"order-42", "!~", strings.Repeat("a", 128)} {
		logs.Reset()

		rec := send(t, mux, http.MethodPost, "/v1/notifications", smsBody, map[string][]string{correlationIDHeader: {id}})

		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
		}
		if got := rec.Header().Get(correlationIDHeader); got != id {
			t.Errorf("%s of the response = %q, want %q", correlationIDHeader, got, id)
		}
		row, err := store.New(pool).GetNotification(t.Context(), decodeBody[api.Notification](t, rec).Id)
		if err != nil {
			t.Fatalf("get stored row: %v", err)
		}
		if row.CorrelationID.String != id {
			t.Errorf("stored correlation id = %+v, want %q", row.CorrelationID, id)
		}
		wantRequestLine(t, logs, `"status":202`, `"correlation_id":"`+id+`"`)
	}
}

func TestBatchStoresTheCorrelationIDOnEveryNotification(t *testing.T) {
	mux, pool, _ := newLoggedServer(t)

	rec := send(t, mux, http.MethodPost, "/v1/notifications/batch", batchBody(smsBody, smsBody, smsBody), map[string][]string{correlationIDHeader: {"import-7"}})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
	}
	var stored int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM notifications WHERE correlation_id = 'import-7'").Scan(&stored); err != nil {
		t.Fatalf("count notifications with the correlation id: %v", err)
	}
	if stored != 3 {
		t.Errorf("%d notifications have the correlation id, want 3", stored)
	}
}

func TestBadCorrelationIDIsRejected(t *testing.T) {
	tests := []struct {
		name string
		sent []string
	}{
		{name: "empty", sent: []string{""}},
		{name: "too long", sent: []string{strings.Repeat("a", 129)}},
		{name: "space", sent: []string{"order 42"}},
		{name: "tab", sent: []string{"order\t42"}},
		{name: "delete character", sent: []string{"order\x7f42"}},
		{name: "not ASCII", sent: []string{"sipariş-42"}},
		{name: "not UTF-8", sent: []string{"order\xff42"}},
		{name: "sent twice", sent: []string{"order-42", "order-43"}},
	}
	mux, pool, logs := newLoggedServer(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs.Reset()

			rec := send(t, mux, http.MethodPost, "/v1/notifications", smsBody, map[string][]string{correlationIDHeader: tt.sent})

			wantError(t, rec, http.StatusBadRequest, correlationIDMessage)
			id := wantGeneratedID(t, rec)
			wantRequestLine(t, logs, `"status":400`, `"correlation_id":"`+id+`"`)
			if stored := countNotifications(t, pool); stored != 0 {
				t.Errorf("%d notifications stored, want none", stored)
			}
		})
	}
}

func TestRequestRejectedBeforeItsHandlerHasACorrelationID(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{name: "id that is not a UUID", method: http.MethodGet, path: "/v1/notifications/not-a-uuid", wantStatus: http.StatusBadRequest},
		{name: "unknown query parameter", method: http.MethodGet, path: "/v1/notifications?size=10", wantStatus: http.StatusBadRequest},
		{name: "unknown path", method: http.MethodGet, path: "/v1/unknown", wantStatus: http.StatusNotFound},
		{name: "wrong method", method: http.MethodDelete, path: "/v1/notifications", wantStatus: http.StatusMethodNotAllowed},
	}
	mux, _, logs := newLoggedServer(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs.Reset()

			rec := send(t, mux, tt.method, tt.path, "", map[string][]string{correlationIDHeader: {"early-1"}})

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, tt.wantStatus, rec.Body)
			}
			if got := rec.Header().Get(correlationIDHeader); got != "early-1" {
				t.Errorf("%s of the response = %q, want %q", correlationIDHeader, got, "early-1")
			}
			path, _, _ := strings.Cut(tt.path, "?")
			wantRequestLine(t, logs, `"method":"`+tt.method+`"`, `"path":"`+path+`"`, fmt.Sprintf(`"status":%d`, tt.wantStatus), `"correlation_id":"early-1"`)
		})
	}
}

func TestReplayKeepsTheCorrelationIDOfTheFirstRequest(t *testing.T) {
	mux, pool, _ := newLoggedServer(t)
	header := func(id string) map[string][]string {
		return map[string][]string{correlationIDHeader: {id}, "Idempotency-Key": {"replay"}}
	}

	first := send(t, mux, http.MethodPost, "/v1/notifications", smsBody, header("first"))
	second := send(t, mux, http.MethodPost, "/v1/notifications", smsBody, header("second"))

	if first.Code != http.StatusAccepted || second.Code != http.StatusAccepted {
		t.Fatalf("statuses = %d and %d, want %d twice", first.Code, second.Code, http.StatusAccepted)
	}
	if got := second.Header().Get(correlationIDHeader); got != "second" {
		t.Errorf("%s of the replay = %q, want its own, %q", correlationIDHeader, got, "second")
	}
	row, err := store.New(pool).GetNotification(t.Context(), decodeBody[api.Notification](t, second).Id)
	if err != nil {
		t.Fatalf("get stored row: %v", err)
	}
	if row.CorrelationID.String != "first" {
		t.Errorf("stored correlation id = %+v, want %q", row.CorrelationID, "first")
	}
}

func TestErrorLineOfAFailedRequestHasItsCorrelationID(t *testing.T) {
	mux, pool, logs := newLoggedServer(t)
	pool.Close()

	rec := send(t, mux, http.MethodGet, "/v1/notifications", "", map[string][]string{correlationIDHeader: {"failed-1"}})

	wantError(t, rec, http.StatusInternalServerError, "internal error")
	wantLogLine(t, logs, `"level":"ERROR"`, `"msg":"request failed"`, `"correlation_id":"failed-1"`)
	wantRequestLine(t, logs, `"status":500`, `"correlation_id":"failed-1"`)
}
