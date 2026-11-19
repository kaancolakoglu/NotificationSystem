package api_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kaancolakoglu/NotificationSystem/internal/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
)

func newTestServer(t *testing.T) (*http.ServeMux, *pgxpool.Pool) {
	t.Helper()
	pool := testdb.New(t)
	mux := http.NewServeMux()
	api.Register(mux, pool, slog.New(slog.DiscardHandler))
	return mux, pool
}

func TestCreateNotification(t *testing.T) {
	mux, pool := newTestServer(t)
	queries := store.New(pool)

	post := func(t *testing.T, body string) *httptest.ResponseRecorder {
		t.Helper()
		return postNotification(t, mux, body)
	}

	accepted := []struct {
		name         string
		body         string
		want         api.Notification
		wantPriority int16
	}{
		{
			name:         "default priority",
			body:         `{"recipient":"+905551234567","channel":"sms","content":"hello sms"}`,
			want:         api.Notification{Recipient: "+905551234567", Channel: api.Sms, Content: "hello sms", Priority: api.Normal},
			wantPriority: 1,
		},
		{
			name:         "high priority",
			body:         `{"recipient":"user@example.com","channel":"email","content":"hello email","priority":"high"}`,
			want:         api.Notification{Recipient: "user@example.com", Channel: api.Email, Content: "hello email", Priority: api.High},
			wantPriority: 0,
		},
		{
			name:         "low priority",
			body:         `{"recipient":"device-token-1","channel":"push","content":"hello push","priority":"low"}`,
			want:         api.Notification{Recipient: "device-token-1", Channel: api.Push, Content: "hello push", Priority: api.Low},
			wantPriority: 2,
		},
	}
	for _, tt := range accepted {
		t.Run(tt.name, func(t *testing.T) {
			rec := post(t, tt.body)

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
			}
			var got api.Notification
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if got.Id.Version() != 7 {
				t.Errorf("id version = %d, want 7", got.Id.Version())
			}

			want := tt.want
			want.Id = got.Id
			want.Status = api.Pending
			want.CreatedAt = got.CreatedAt
			if got != want {
				t.Errorf("response = %+v, want %+v", got, want)
			}

			row, err := queries.GetNotification(t.Context(), got.Id)
			if err != nil {
				t.Fatalf("get stored row: %v", err)
			}
			wantRow := store.Notification{
				ID:        got.Id,
				Recipient: tt.want.Recipient,
				Channel:   string(tt.want.Channel),
				Content:   tt.want.Content,
				Priority:  tt.wantPriority,
				Status:    "pending",
				CreatedAt: row.CreatedAt,
			}
			if row != wantRow {
				t.Errorf("stored row = %+v, want %+v", row, wantRow)
			}
		})
	}

	rejected := []struct {
		name        string
		body        string
		wantStatus  int
		wantMessage string
	}{
		{"invalid phone number", `{"recipient":"05551234567","channel":"sms","content":"hello"}`, http.StatusUnprocessableEntity, "E.164"},
		{"unknown channel", `{"recipient":"+905551234567","channel":"fax","content":"hello"}`, http.StatusUnprocessableEntity, "channel must be one of"},
		{"unknown priority", `{"recipient":"+905551234567","channel":"sms","content":"hello","priority":"urgent"}`, http.StatusUnprocessableEntity, "priority must be one of"},
		{"missing content", `{"recipient":"+905551234567","channel":"sms"}`, http.StatusUnprocessableEntity, "content must not be empty"},
		{"nul in content", `{"recipient":"+905551234567","channel":"sms","content":"a\u0000b"}`, http.StatusUnprocessableEntity, "NUL"},
		{"empty object", `{}`, http.StatusUnprocessableEntity, "channel must be one of"},
		{"email with display name", `{"recipient":"Bob <user@example.com>","channel":"email","content":"hello"}`, http.StatusUnprocessableEntity, "plain email address"},
		{"malformed json", `{"recipient":`, http.StatusBadRequest, "request body must be a valid JSON object"},
		{"body is not an object", `5`, http.StatusBadRequest, "request body must be a valid JSON object"},
		{"wrong field type", `{"recipient":"+905551234567","channel":"sms","content":5}`, http.StatusBadRequest, `field "content" has the wrong type`},
	}
	for _, tt := range rejected {
		t.Run(tt.name, func(t *testing.T) {
			before := countNotifications(t, pool)

			rec := post(t, tt.body)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, tt.wantStatus, rec.Body)
			}
			var got api.Error
			if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if !strings.Contains(got.Message, tt.wantMessage) {
				t.Errorf("message = %q, want it to contain %q", got.Message, tt.wantMessage)
			}
			if after := countNotifications(t, pool); after != before {
				t.Errorf("rows went from %d to %d, want no insert", before, after)
			}
		})
	}

	t.Run("database failure", func(t *testing.T) {
		pool.Close()

		rec := post(t, `{"recipient":"+905551234567","channel":"sms","content":"hello"}`)

		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusInternalServerError, rec.Body)
		}
		var got api.Error
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if got.Message != "internal error" {
			t.Errorf("message = %q, want %q", got.Message, "internal error")
		}
	})
}

func TestCreateNotificationWithIdempotencyKey(t *testing.T) {
	mux, pool := newTestServer(t)
	const body = `{"recipient":"+905551234567","channel":"sms","content":"hello"}`

	wantAccepted := func(t *testing.T, rec *httptest.ResponseRecorder) {
		t.Helper()
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
		}
	}
	wantRejected := func(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantMessage string) {
		t.Helper()
		if rec.Code != wantStatus {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, wantStatus, rec.Body)
		}
		var got api.Error
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if got.Message != wantMessage {
			t.Errorf("message = %q, want %q", got.Message, wantMessage)
		}
	}
	wantInserted := func(t *testing.T, before, want int) {
		t.Helper()
		if got := countNotifications(t, pool) - before; got != want {
			t.Errorf("inserted %d rows, want %d", got, want)
		}
	}

	t.Run("replay returns the first response and creates one row", func(t *testing.T) {
		before := countNotifications(t, pool)

		first := postNotification(t, mux, body, "replay")
		second := postNotification(t, mux, body, "replay")

		wantAccepted(t, first)
		wantAccepted(t, second)
		if first.Body.String() != second.Body.String() {
			t.Errorf("replay = %s, want %s", second.Body, first.Body)
		}
		wantInserted(t, before, 1)
	})

	t.Run("default priority spelled out is the same request", func(t *testing.T) {
		before := countNotifications(t, pool)

		first := postNotification(t, mux, body, "spelled-out")
		second := postNotification(t, mux, `{"priority":"normal","content":"hello","channel":"sms","recipient":"+905551234567"}`, "spelled-out")

		wantAccepted(t, first)
		wantAccepted(t, second)
		if first.Body.String() != second.Body.String() {
			t.Errorf("replay = %s, want %s", second.Body, first.Body)
		}
		wantInserted(t, before, 1)
	})

	t.Run("simultaneous requests create one notification", func(t *testing.T) {
		before := countNotifications(t, pool)
		responses := make([]*httptest.ResponseRecorder, 20)

		var wg sync.WaitGroup
		for i := range responses {
			wg.Go(func() {
				responses[i] = postNotification(t, mux, body, "simultaneous")
			})
		}
		wg.Wait()

		for _, rec := range responses {
			wantAccepted(t, rec)
			if rec.Body.String() != responses[0].Body.String() {
				t.Errorf("response = %s, want %s", rec.Body, responses[0].Body)
			}
		}
		wantInserted(t, before, 1)
	})

	t.Run("different body is 409", func(t *testing.T) {
		before := countNotifications(t, pool)

		first := postNotification(t, mux, body, "different-body")
		second := postNotification(t, mux, `{"recipient":"+905551234567","channel":"sms","content":"goodbye"}`, "different-body")

		wantAccepted(t, first)
		wantRejected(t, second, http.StatusConflict, "Idempotency-Key was already used with a different request body")
		wantInserted(t, before, 1)
	})

	t.Run("different keys create two notifications", func(t *testing.T) {
		before := countNotifications(t, pool)

		wantAccepted(t, postNotification(t, mux, body, "key-one"))
		wantAccepted(t, postNotification(t, mux, body, "key-two"))

		wantInserted(t, before, 2)
	})

	t.Run("invalid request does not use up the key", func(t *testing.T) {
		before := countNotifications(t, pool)

		invalid := postNotification(t, mux, `{"recipient":"0555","channel":"sms","content":"hello"}`, "invalid-first")
		valid := postNotification(t, mux, body, "invalid-first")

		if invalid.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want %d, body %s", invalid.Code, http.StatusUnprocessableEntity, invalid.Body)
		}
		wantAccepted(t, valid)
		wantInserted(t, before, 1)
	})

	t.Run("key length", func(t *testing.T) {
		before := countNotifications(t, pool)

		wantAccepted(t, postNotification(t, mux, body, strings.Repeat("k", 255)))
		wantRejected(t, postNotification(t, mux, body, strings.Repeat("k", 256)), http.StatusBadRequest, "Idempotency-Key must be 1 to 255 bytes long")
		wantRejected(t, postNotification(t, mux, body, ""), http.StatusBadRequest, "Idempotency-Key must be 1 to 255 bytes long")
		wantInserted(t, before, 1)
	})

	t.Run("key sent twice is 400", func(t *testing.T) {
		before := countNotifications(t, pool)

		rec := postNotification(t, mux, body, "one", "two")

		wantRejected(t, rec, http.StatusBadRequest, `parameter "Idempotency-Key" must be sent once`)
		wantInserted(t, before, 0)
	})
}

func postNotification(t *testing.T, mux *http.ServeMux, body string, idempotencyKeys ...string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/notifications", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for _, key := range idempotencyKeys {
		req.Header.Add("Idempotency-Key", key)
	}
	mux.ServeHTTP(rec, req)
	return rec
}

func TestGetNotification(t *testing.T) {
	mux, pool := newTestServer(t)
	queries := store.New(pool)

	get := func(t *testing.T, id string) *httptest.ResponseRecorder {
		t.Helper()
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/notifications/"+id, nil)
		mux.ServeHTTP(rec, req)
		return rec
	}
	wantError := func(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantMessage string) {
		t.Helper()
		if rec.Code != wantStatus {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, wantStatus, rec.Body)
		}
		var got api.Error
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if got.Message != wantMessage {
			t.Errorf("message = %q, want %q", got.Message, wantMessage)
		}
	}

	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("generate id: %v", err)
	}
	row, err := queries.InsertNotification(t.Context(), store.InsertNotificationParams{
		ID:        id,
		Recipient: "user@example.com",
		Channel:   "email",
		Content:   "hello",
		Priority:  0,
	})
	if err != nil {
		t.Fatalf("insert notification: %v", err)
	}

	t.Run("found", func(t *testing.T) {
		rec := get(t, id.String())

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusOK, rec.Body)
		}
		var got api.Notification
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if !got.CreatedAt.Equal(row.CreatedAt) {
			t.Errorf("created_at = %s, want %s", got.CreatedAt, row.CreatedAt)
		}
		want := api.Notification{
			Id:        id,
			Recipient: "user@example.com",
			Channel:   api.Email,
			Content:   "hello",
			Priority:  api.High,
			Status:    api.Pending,
			CreatedAt: got.CreatedAt,
		}
		if got != want {
			t.Errorf("response = %+v, want %+v", got, want)
		}
	})

	t.Run("unknown id", func(t *testing.T) {
		unknown, err := uuid.NewV7()
		if err != nil {
			t.Fatalf("generate id: %v", err)
		}

		wantError(t, get(t, unknown.String()), http.StatusNotFound, "notification not found")
	})

	t.Run("malformed id", func(t *testing.T) {
		wantError(t, get(t, "not-a-uuid"), http.StatusBadRequest, `parameter "id" is not valid`)
	})

	t.Run("database failure", func(t *testing.T) {
		pool.Close()

		wantError(t, get(t, id.String()), http.StatusInternalServerError, "internal error")
	})
}

func countNotifications(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM notifications").Scan(&count); err != nil {
		t.Fatalf("count notifications: %v", err)
	}
	return count
}
