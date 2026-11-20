package api_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
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
		wantError(t, second, http.StatusConflict, "Idempotency-Key was already used with a different request body")
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
		wantError(t, postNotification(t, mux, body, strings.Repeat("k", 256)), http.StatusBadRequest, "Idempotency-Key must be 1 to 255 bytes long")
		wantError(t, postNotification(t, mux, body, ""), http.StatusBadRequest, "Idempotency-Key must be 1 to 255 bytes long")
		wantInserted(t, before, 1)
	})

	t.Run("key sent twice is 400", func(t *testing.T) {
		before := countNotifications(t, pool)

		rec := postNotification(t, mux, body, "one", "two")

		wantError(t, rec, http.StatusBadRequest, `parameter "Idempotency-Key" must be sent once`)
		wantInserted(t, before, 0)
	})
}

func TestCreateBatch(t *testing.T) {
	mux, pool := newTestServer(t)
	queries := store.New(pool)

	const (
		validSMS        = `{"recipient":"+905551234567","channel":"sms","content":"hello sms"}`
		validEmail      = `{"recipient":"user@example.com","channel":"email","content":"hello email","priority":"high"}`
		invalidPhone    = `{"recipient":"0555","channel":"sms","content":"hello"}`
		unknownPriority = `{"recipient":"+905551234567","channel":"sms","content":"hello","priority":"urgent"}`
	)

	wantInserted := func(t *testing.T, before, want int) {
		t.Helper()
		if got := countNotifications(t, pool) - before; got != want {
			t.Errorf("inserted %d rows, want %d", got, want)
		}
	}
	wantAccepted := func(t *testing.T, rec *httptest.ResponseRecorder, wantErrors ...string) api.BatchCreated {
		t.Helper()
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
		}
		got := decodeBody[api.BatchCreated](t, rec)
		if got.BatchId.Version() != 7 {
			t.Errorf("batch id version = %d, want 7", got.BatchId.Version())
		}
		wantBatchResults(t, got.Results, wantErrors)
		return got
	}

	t.Run("mixed batch answers per index", func(t *testing.T) {
		before := countNotifications(t, pool)

		rec := postBatch(t, mux, batchBody(validSMS, invalidPhone, validEmail, unknownPriority))

		got := wantAccepted(t, rec, "", "E.164", "", "priority must be one of")
		wantInserted(t, before, 2)

		wantRows := map[int]store.Notification{
			0: {Recipient: "+905551234567", Channel: "sms", Content: "hello sms", Priority: 1},
			2: {Recipient: "user@example.com", Channel: "email", Content: "hello email", Priority: 0},
		}
		for index, want := range wantRows {
			id := *got.Results[index].Id
			if id.Version() != 7 {
				t.Errorf("result %d: id version = %d, want 7", index, id.Version())
			}
			row, err := queries.GetNotification(t.Context(), id)
			if err != nil {
				t.Fatalf("result %d: get stored row: %v", index, err)
			}
			want.ID = id
			want.Status = "pending"
			want.CreatedAt = row.CreatedAt
			want.BatchID = pgtype.UUID{Bytes: got.BatchId, Valid: true}
			if row != want {
				t.Errorf("result %d: stored row = %+v, want %+v", index, row, want)
			}
		}
	})

	t.Run("none valid is 422", func(t *testing.T) {
		before := countNotifications(t, pool)

		rec := postBatch(t, mux, batchBody(invalidPhone, unknownPriority))

		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusUnprocessableEntity, rec.Body)
		}
		got := decodeBody[api.BatchRejected](t, rec)
		if got.Message != "no notification in the batch is valid" {
			t.Errorf("message = %q, want %q", got.Message, "no notification in the batch is valid")
		}
		if got.Results == nil {
			t.Fatalf("body %s has no results", rec.Body)
		}
		wantBatchResults(t, *got.Results, []string{"E.164", "priority must be one of"})
		wantInserted(t, before, 0)
	})

	t.Run("batch size", func(t *testing.T) {
		before := countNotifications(t, pool)
		const wantMessage = "a batch must hold 1 to 1000 notifications"

		wantAccepted(t, postBatch(t, mux, batchBody(slices.Repeat([]string{validSMS}, 1000)...)), make([]string, 1000)...)
		wantError(t, postBatch(t, mux, batchBody(slices.Repeat([]string{validSMS}, 1001)...)), http.StatusUnprocessableEntity, wantMessage)
		wantError(t, postBatch(t, mux, batchBody()), http.StatusUnprocessableEntity, wantMessage)
		wantError(t, postBatch(t, mux, `{}`), http.StatusUnprocessableEntity, wantMessage)
		wantInserted(t, before, 1000)
	})

	t.Run("replay with a key returns the first response and creates nothing", func(t *testing.T) {
		before := countNotifications(t, pool)
		body := batchBody(validSMS, invalidPhone, validEmail)

		first := postBatch(t, mux, body, "batch-replay")
		second := postBatch(t, mux, body, "batch-replay")

		wantAccepted(t, first, "", "E.164", "")
		wantAccepted(t, second, "", "E.164", "")
		if first.Body.String() != second.Body.String() {
			t.Errorf("replay = %s, want %s", second.Body, first.Body)
		}
		wantInserted(t, before, 2)
	})

	t.Run("default priority spelled out is the same batch", func(t *testing.T) {
		before := countNotifications(t, pool)

		first := postBatch(t, mux, batchBody(validSMS), "batch-spelled-out")
		second := postBatch(t, mux, batchBody(`{"priority":"normal","content":"hello sms","channel":"sms","recipient":"+905551234567"}`), "batch-spelled-out")

		wantAccepted(t, first, "")
		wantAccepted(t, second, "")
		if first.Body.String() != second.Body.String() {
			t.Errorf("replay = %s, want %s", second.Body, first.Body)
		}
		wantInserted(t, before, 1)
	})

	t.Run("key with a different batch is 409", func(t *testing.T) {
		before := countNotifications(t, pool)

		first := postBatch(t, mux, batchBody(validSMS, validEmail), "batch-different")
		second := postBatch(t, mux, batchBody(validEmail, validSMS), "batch-different")

		wantAccepted(t, first, "", "")
		wantError(t, second, http.StatusConflict, "Idempotency-Key was already used with a different request body")
		wantInserted(t, before, 2)
	})

	t.Run("key from a single create is 409", func(t *testing.T) {
		before := countNotifications(t, pool)

		single := postNotification(t, mux, validSMS, "shared-key")
		batch := postBatch(t, mux, batchBody(validSMS), "shared-key")

		if single.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d, body %s", single.Code, http.StatusAccepted, single.Body)
		}
		wantError(t, batch, http.StatusConflict, "Idempotency-Key was already used with a different request body")
		wantInserted(t, before, 1)
	})

	t.Run("batch with no valid notification does not use up the key", func(t *testing.T) {
		before := countNotifications(t, pool)

		invalid := postBatch(t, mux, batchBody(invalidPhone), "batch-invalid-first")
		valid := postBatch(t, mux, batchBody(validSMS), "batch-invalid-first")

		if invalid.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status = %d, want %d, body %s", invalid.Code, http.StatusUnprocessableEntity, invalid.Body)
		}
		wantAccepted(t, valid, "")
		wantInserted(t, before, 1)
	})

	t.Run("malformed requests are 400", func(t *testing.T) {
		before := countNotifications(t, pool)

		wantError(t, postBatch(t, mux, `{"notifications":`), http.StatusBadRequest, "request body must be a valid JSON object")
		wantError(t, postBatch(t, mux, `{"notifications":[{"recipient":"+905551234567","channel":"sms","content":5}]}`), http.StatusBadRequest, `field "notifications.0.content" has the wrong type`)
		wantError(t, postBatch(t, mux, batchBody(validSMS), strings.Repeat("k", 256)), http.StatusBadRequest, "Idempotency-Key must be 1 to 255 bytes long")
		wantError(t, postBatch(t, mux, batchBody(validSMS), "one", "two"), http.StatusBadRequest, `parameter "Idempotency-Key" must be sent once`)
		wantInserted(t, before, 0)
	})

	t.Run("database failure", func(t *testing.T) {
		pool.Close()

		wantError(t, postBatch(t, mux, batchBody(validSMS)), http.StatusInternalServerError, "internal error")
	})
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

func TestGetBatch(t *testing.T) {
	mux, pool := newTestServer(t)

	get := func(t *testing.T, id string) *httptest.ResponseRecorder {
		t.Helper()
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/batches/"+id, nil)
		mux.ServeHTTP(rec, req)
		return rec
	}

	statuses := []string{
		"pending",
		"processing", "processing",
		"sent", "sent", "sent",
		"failed", "failed", "failed", "failed",
		"cancelled", "cancelled", "cancelled", "cancelled", "cancelled",
	}
	items := []string{`{"recipient":"0555","channel":"sms","content":"hello"}`}
	wantErrors := []string{"E.164"}
	for range statuses {
		items = append(items, `{"recipient":"+905551234567","channel":"sms","content":"hello"}`)
		wantErrors = append(wantErrors, "")
	}
	rec := postBatch(t, mux, batchBody(items...))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
	}
	created := decodeBody[api.BatchCreated](t, rec)
	wantBatchResults(t, created.Results, wantErrors)

	t.Run("new batch is all pending", func(t *testing.T) {
		rec := get(t, created.BatchId.String())

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusOK, rec.Body)
		}
		want := api.Batch{Id: created.BatchId, Total: 15, Pending: 15}
		if got := decodeBody[api.Batch](t, rec); got != want {
			t.Errorf("response = %+v, want %+v", got, want)
		}
	})

	t.Run("counts follow each status", func(t *testing.T) {
		for i, status := range statuses {
			tag, err := pool.Exec(t.Context(), "UPDATE notifications SET status = $1 WHERE id = $2", status, *created.Results[i+1].Id)
			if err != nil {
				t.Fatalf("set status: %v", err)
			}
			if tag.RowsAffected() != 1 {
				t.Fatalf("set status changed %d rows, want 1", tag.RowsAffected())
			}
		}

		rec := get(t, created.BatchId.String())

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusOK, rec.Body)
		}
		want := api.Batch{Id: created.BatchId, Total: 15, Pending: 1, Processing: 2, Sent: 3, Failed: 4, Cancelled: 5}
		if got := decodeBody[api.Batch](t, rec); got != want {
			t.Errorf("response = %+v, want %+v", got, want)
		}
	})

	t.Run("unknown id", func(t *testing.T) {
		unknown, err := uuid.NewV7()
		if err != nil {
			t.Fatalf("generate id: %v", err)
		}

		wantError(t, get(t, unknown.String()), http.StatusNotFound, "batch not found")
	})

	t.Run("notification id is not a batch id", func(t *testing.T) {
		wantError(t, get(t, created.Results[1].Id.String()), http.StatusNotFound, "batch not found")
	})

	t.Run("malformed id", func(t *testing.T) {
		wantError(t, get(t, "not-a-uuid"), http.StatusBadRequest, `parameter "id" is not valid`)
	})

	t.Run("database failure", func(t *testing.T) {
		pool.Close()

		wantError(t, get(t, created.BatchId.String()), http.StatusInternalServerError, "internal error")
	})
}

func postNotification(t *testing.T, mux *http.ServeMux, body string, idempotencyKeys ...string) *httptest.ResponseRecorder {
	t.Helper()
	return postJSON(t, mux, "/v1/notifications", body, idempotencyKeys)
}

func postBatch(t *testing.T, mux *http.ServeMux, body string, idempotencyKeys ...string) *httptest.ResponseRecorder {
	t.Helper()
	return postJSON(t, mux, "/v1/notifications/batch", body, idempotencyKeys)
}

func postJSON(t *testing.T, mux *http.ServeMux, path, body string, idempotencyKeys []string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for _, key := range idempotencyKeys {
		req.Header.Add("Idempotency-Key", key)
	}
	mux.ServeHTTP(rec, req)
	return rec
}

func batchBody(notifications ...string) string {
	return `{"notifications":[` + strings.Join(notifications, ",") + `]}`
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var got T
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response %s: %v", rec.Body, err)
	}
	return got
}

func wantError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantMessage string) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d, body %s", rec.Code, wantStatus, rec.Body)
	}
	if got := decodeBody[api.Error](t, rec); got.Message != wantMessage {
		t.Errorf("message = %q, want %q", got.Message, wantMessage)
	}
}

func wantBatchResults(t *testing.T, results []api.BatchItemResult, wantErrors []string) {
	t.Helper()
	if len(results) != len(wantErrors) {
		t.Fatalf("got %d results, want %d", len(results), len(wantErrors))
	}
	for i, result := range results {
		if result.Index != i {
			t.Fatalf("result %d: index = %d, want %d", i, result.Index, i)
		}
		if wantErrors[i] == "" {
			if result.Id == nil || result.Error != nil {
				t.Fatalf("result %d: want an id and no error", i)
			}
			continue
		}
		if result.Id != nil || result.Error == nil {
			t.Fatalf("result %d: want an error and no id", i)
		}
		if !strings.Contains(*result.Error, wantErrors[i]) {
			t.Fatalf("result %d: error = %q, want it to contain %q", i, *result.Error, wantErrors[i])
		}
	}
}
