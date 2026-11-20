package api_test

import (
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kaancolakoglu/NotificationSystem/internal/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/events"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

func newTestServer(t *testing.T) (*http.ServeMux, *pgxpool.Pool) {
	t.Helper()
	pool := testdb.New(t)
	logger := slog.New(slog.DiscardHandler)
	mux := http.NewServeMux()
	api.Register(mux, pool, nil, events.Publisher{Client: testredis.New(t), Logger: logger}, untraced, logger)
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
				ID:            got.Id,
				Recipient:     tt.want.Recipient,
				Channel:       string(tt.want.Channel),
				Content:       tt.want.Content,
				Priority:      tt.wantPriority,
				Status:        "pending",
				CreatedAt:     row.CreatedAt,
				NextAttemptAt: row.CreatedAt,
				CorrelationID: pgtype.Text{String: rec.Header().Get("X-Correlation-ID"), Valid: true},
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
		{"unknown field", `{"recipient":"+905551234567","channel":"sms","content":"hello","priorty":"high"}`, http.StatusBadRequest, `field "priorty" is not known`},
		{"unknown field with a quote in its name", `{"recipient":"+905551234567","channel":"sms","content":"hello","a\"b":1}`, http.StatusBadRequest, `field "a\"b" is not known`},
		{"unknown field in an invalid notification", `{"recipient":"0555","channel":"sms","content":"hello","priorty":"high"}`, http.StatusBadRequest, `field "priorty" is not known`},
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

	t.Run("query parameter", func(t *testing.T) {
		before := countNotifications(t, pool)

		rec := postJSON(t, mux, "/v1/notifications?dry_run=true", `{"recipient":"+905551234567","channel":"sms","content":"hello"}`, nil)

		wantError(t, rec, http.StatusBadRequest, `parameter "dry_run" is not known`)
		if after := countNotifications(t, pool); after != before {
			t.Errorf("rows went from %d to %d, want no insert", before, after)
		}
	})

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
			want.NextAttemptAt = row.NextAttemptAt
			if row.NextAttemptAt.After(row.CreatedAt) || row.CreatedAt.Sub(row.NextAttemptAt) > 5*time.Second {
				t.Errorf("result %d: due at %s and created at %s, want it due when it is created", index, row.NextAttemptAt, row.CreatedAt)
			}
			want.BatchID = pgtype.UUID{Bytes: got.BatchId, Valid: true}
			want.CorrelationID = pgtype.Text{String: rec.Header().Get("X-Correlation-ID"), Valid: true}
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
		wantError(t, postBatch(t, mux, batchBody(validSMS, `{"recipient":"+905551234567","channel":"sms","content":"hello","priorty":"high"}`)), http.StatusBadRequest, `field "priorty" is not known`)
		wantError(t, postBatch(t, mux, `{"notifications":[`+validSMS+`],"dry_run":true}`), http.StatusBadRequest, `field "dry_run" is not known`)
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
		if !strings.Contains(rec.Body.String(), `"delivery_attempts":[]`) {
			t.Errorf("body = %s, want an empty delivery_attempts list", rec.Body)
		}
	})

	t.Run("with delivery attempts", func(t *testing.T) {
		const failure = "provider answered 500, want 202"
		retried, err := queries.InsertNotification(t.Context(), store.InsertNotificationParams{
			ID:        uuid.Must(uuid.NewV7()),
			Recipient: "device-token-1",
			Channel:   "push",
			Content:   "hello",
			Priority:  2,
		})
		if err != nil {
			t.Fatalf("insert notification: %v", err)
		}
		claim := func() {
			t.Helper()
			claimed, err := queries.ClaimNotifications(t.Context(), store.ClaimNotificationsParams{Channel: "push", Priority: 2, RowLimit: 1})
			if err != nil || len(claimed) != 1 || claimed[0].ID != retried.ID {
				t.Fatalf("claim = %+v, error %v, want the notification", claimed, err)
			}
		}
		claim()
		if changed, err := queries.ScheduleRetry(t.Context(), store.ScheduleRetryParams{ID: retried.ID, Attempts: 1, Error: failure, Delay: pgtype.Interval{Valid: true}}); err != nil || changed != 1 {
			t.Fatalf("schedule retry: changed %d rows, error %v", changed, err)
		}
		claim()
		if changed, err := queries.MarkSent(t.Context(), store.MarkSentParams{ID: retried.ID, Attempts: 2, ProviderMessageID: pgtype.Text{String: "message-1", Valid: true}}); err != nil || changed != 1 {
			t.Fatalf("mark sent: changed %d rows, error %v", changed, err)
		}

		rec := get(t, retried.ID.String())

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusOK, rec.Body)
		}
		got := decodeBody[api.NotificationDetail](t, rec)
		if got.Status != api.Sent || got.Attempts != 2 || got.LastError == nil || *got.LastError != failure {
			t.Errorf("status = %s, attempts = %d, last_error = %v, want sent, 2 and %q", got.Status, got.Attempts, got.LastError, failure)
		}
		if len(got.DeliveryAttempts) != 2 {
			t.Fatalf("delivery_attempts = %+v, want 2 of them", got.DeliveryAttempts)
		}
		first, second := got.DeliveryAttempts[0], got.DeliveryAttempts[1]
		if first.Attempt != 1 || first.Error == nil || *first.Error != failure {
			t.Errorf("first attempt = %+v, want number 1 with the error %q", first, failure)
		}
		if second.Attempt != 2 || second.Error != nil {
			t.Errorf("second attempt = %+v, want number 2 with no error", second)
		}
		if first.CreatedAt.Before(retried.CreatedAt) || second.CreatedAt.Before(first.CreatedAt) {
			t.Errorf("attempt times = %s then %s, want them in order after the creation at %s", first.CreatedAt, second.CreatedAt, retried.CreatedAt)
		}
	})

	t.Run("found in a batch", func(t *testing.T) {
		batchID, err := uuid.NewV7()
		if err != nil {
			t.Fatalf("generate batch id: %v", err)
		}
		inBatch, err := uuid.NewV7()
		if err != nil {
			t.Fatalf("generate id: %v", err)
		}
		insertNotification(t, pool, inBatch, "sms", "pending", pgtype.UUID{Bytes: batchID, Valid: true})

		rec := get(t, inBatch.String())

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusOK, rec.Body)
		}
		got := decodeBody[api.Notification](t, rec)
		if got.BatchId == nil || *got.BatchId != batchID {
			t.Errorf("batch_id = %v, want %s", got.BatchId, batchID)
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

	t.Run("query parameter", func(t *testing.T) {
		wantError(t, get(t, id.String()+"?status=pending"), http.StatusBadRequest, `parameter "status" is not known`)
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

func TestCancelNotification(t *testing.T) {
	mux, pool := newTestServer(t)
	queries := store.New(pool)

	const notPendingMessage = "only a pending notification can be cancelled"

	cancel := func(t *testing.T, id string) *httptest.ResponseRecorder {
		t.Helper()
		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/notifications/"+id+"/cancel", nil)
		mux.ServeHTTP(rec, req)
		return rec
	}
	insert := func(t *testing.T, status string) store.Notification {
		t.Helper()
		id, err := uuid.NewV7()
		if err != nil {
			t.Fatalf("generate id: %v", err)
		}
		insertNotification(t, pool, id, "sms", status, pgtype.UUID{})
		row, err := queries.GetNotification(t.Context(), id)
		if err != nil {
			t.Fatalf("get inserted row: %v", err)
		}
		return row
	}
	wantStored := func(t *testing.T, want store.Notification) {
		t.Helper()
		got, err := queries.GetNotification(t.Context(), want.ID)
		if err != nil {
			t.Fatalf("get stored row: %v", err)
		}
		if got != want {
			t.Errorf("stored row = %+v, want %+v", got, want)
		}
	}

	t.Run("pending is cancelled", func(t *testing.T) {
		row := insert(t, "pending")
		other := insert(t, "pending")

		rec := cancel(t, row.ID.String())

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusOK, rec.Body)
		}
		got := decodeBody[api.Notification](t, rec)
		if !got.CreatedAt.Equal(row.CreatedAt) {
			t.Errorf("created_at = %s, want %s", got.CreatedAt, row.CreatedAt)
		}
		want := api.Notification{
			Id:        row.ID,
			Recipient: "+905551234567",
			Channel:   api.Sms,
			Content:   "hello",
			Priority:  api.Normal,
			Status:    api.Cancelled,
			CreatedAt: got.CreatedAt,
		}
		if got != want {
			t.Errorf("response = %+v, want %+v", got, want)
		}
		row.Status = "cancelled"
		wantStored(t, row)
		wantStored(t, other)
	})

	for _, status := range []string{"processing", "sent", "failed", "cancelled"} {
		t.Run(status+" is 409", func(t *testing.T) {
			row := insert(t, status)

			wantError(t, cancel(t, row.ID.String()), http.StatusConflict, notPendingMessage)
			wantStored(t, row)
		})
	}

	t.Run("second cancel is 409", func(t *testing.T) {
		row := insert(t, "pending")

		first := cancel(t, row.ID.String())
		second := cancel(t, row.ID.String())

		if first.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body %s", first.Code, http.StatusOK, first.Body)
		}
		wantError(t, second, http.StatusConflict, notPendingMessage)
		row.Status = "cancelled"
		wantStored(t, row)
	})

	t.Run("simultaneous cancels: one wins", func(t *testing.T) {
		row := insert(t, "pending")
		responses := make([]*httptest.ResponseRecorder, 20)

		var wg sync.WaitGroup
		for i := range responses {
			wg.Go(func() {
				responses[i] = cancel(t, row.ID.String())
			})
		}
		wg.Wait()

		won := 0
		for _, rec := range responses {
			if rec.Code == http.StatusOK {
				won++
				continue
			}
			wantError(t, rec, http.StatusConflict, notPendingMessage)
		}
		if won != 1 {
			t.Errorf("%d cancels answered 200, want 1", won)
		}
		row.Status = "cancelled"
		wantStored(t, row)
	})

	t.Run("unknown id", func(t *testing.T) {
		unknown, err := uuid.NewV7()
		if err != nil {
			t.Fatalf("generate id: %v", err)
		}

		wantError(t, cancel(t, unknown.String()), http.StatusNotFound, "notification not found")
	})

	t.Run("malformed id", func(t *testing.T) {
		wantError(t, cancel(t, "not-a-uuid"), http.StatusBadRequest, `parameter "id" is not valid`)
	})

	t.Run("query parameter", func(t *testing.T) {
		row := insert(t, "pending")

		rec := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/notifications/"+row.ID.String()+"/cancel?force=true", nil)
		mux.ServeHTTP(rec, req)

		wantError(t, rec, http.StatusBadRequest, `parameter "force" is not known`)
		wantStored(t, row)
	})

	t.Run("database failure", func(t *testing.T) {
		row := insert(t, "pending")
		pool.Close()

		wantError(t, cancel(t, row.ID.String()), http.StatusInternalServerError, "internal error")
	})
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
			tag, err := pool.Exec(t.Context(), "UPDATE notifications SET status = $1, locked_until = now() + interval '1 minute' WHERE id = $2", status, *created.Results[i+1].Id)
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

func TestListNotifications(t *testing.T) {
	mux, pool := newTestServer(t)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	batchID := idAt(base, 100)
	inBatch := pgtype.UUID{Bytes: batchID, Valid: true}
	rows := []struct {
		channel string
		status  string
		batchID pgtype.UUID
	}{
		{channel: "sms", status: "pending"},
		{channel: "email", status: "sent"},
		{channel: "push", status: "failed"},
		{channel: "sms", status: "sent", batchID: inBatch},
		{channel: "email", status: "pending", batchID: inBatch},
		{channel: "sms", status: "cancelled"},
	}
	ids := make([]uuid.UUID, len(rows))
	for i, row := range rows {
		ids[i] = idAt(base.Add(time.Duration(i)*time.Hour), byte(i))
		insertNotification(t, pool, ids[i], row.channel, row.status, row.batchID)
	}
	pick := func(indexes ...int) []uuid.UUID {
		picked := make([]uuid.UUID, 0, len(indexes))
		for _, index := range indexes {
			picked = append(picked, ids[index])
		}
		return picked
	}
	at := func(hours int) string {
		return base.Add(time.Duration(hours) * time.Hour).Format(time.RFC3339)
	}
	batch := "batch_id=" + batchID.String()

	filters := []struct {
		name  string
		query string
		want  []uuid.UUID
	}{
		{name: "no filter", query: "", want: pick(5, 4, 3, 2, 1, 0)},
		{name: "status", query: "status=sent", want: pick(3, 1)},
		{name: "channel", query: "channel=sms", want: pick(5, 3, 0)},
		{name: "status and channel", query: "status=sent&channel=sms", want: pick(3)},
		{name: "created from is inclusive", query: "created_from=" + at(2), want: pick(5, 4, 3, 2)},
		{name: "created to is exclusive", query: "created_to=" + at(2), want: pick(1, 0)},
		{name: "created range", query: "created_from=" + at(1) + "&created_to=" + at(4), want: pick(3, 2, 1)},
		{name: "created from with an offset", query: "created_from=2026-01-01T05:00:00%2B03:00", want: pick(5, 4, 3, 2)},
		{name: "created from between two milliseconds", query: "created_from=2026-01-01T02:00:00.0005Z", want: pick(5, 4, 3)},
		{name: "created to between two milliseconds", query: "created_to=2026-01-01T02:00:00.0005Z", want: pick(2, 1, 0)},
		{name: "created from as a date is midnight utc", query: "created_from=2026-01-01", want: pick(5, 4, 3, 2, 1, 0)},
		{name: "created to as a date is midnight utc", query: "created_to=2026-01-01", want: pick()},
		{name: "created from before 1970", query: "created_from=1900-01-01T00:00:00Z", want: pick(5, 4, 3, 2, 1, 0)},
		{name: "created to before 1970", query: "created_to=1900-01-01T00:00:00Z", want: pick()},
		{name: "batch", query: batch, want: pick(4, 3)},
		{name: "batch and status", query: batch + "&status=sent", want: pick(3)},
		{name: "batch and channel", query: batch + "&channel=email", want: pick(4)},
		{name: "batch and created from", query: batch + "&created_from=" + at(4), want: pick(4)},
		{name: "batch and created to", query: batch + "&created_to=" + at(4), want: pick(3)},
		{name: "unknown batch", query: "batch_id=" + idAt(base, 101).String(), want: pick()},
		{name: "nothing matches", query: "status=processing", want: pick()},
		{name: "full page is the last page", query: "limit=6", want: pick(5, 4, 3, 2, 1, 0)},
	}
	for _, tt := range filters {
		t.Run(tt.name, func(t *testing.T) {
			rec := list(t, mux, tt.query)

			page := wantPage(t, rec, tt.want)
			if page.NextCursor != nil {
				t.Errorf("next_cursor = %q, want none", *page.NextCursor)
			}
			if len(tt.want) == 0 && !strings.Contains(rec.Body.String(), `"notifications":[]`) {
				t.Errorf("body = %s, want an empty array", rec.Body)
			}
		})
	}

	t.Run("notification fields", func(t *testing.T) {
		page := wantPage(t, list(t, mux, batch+"&status=sent"), pick(3))

		got := page.Notifications[0]
		if got.BatchId == nil || *got.BatchId != batchID {
			t.Fatalf("batch_id = %v, want %s", got.BatchId, batchID)
		}
		want := api.Notification{
			Id:        ids[3],
			Recipient: "+905551234567",
			Channel:   api.Sms,
			Content:   "hello",
			Priority:  api.Normal,
			Status:    api.Sent,
			CreatedAt: got.CreatedAt,
			BatchId:   got.BatchId,
		}
		if got != want {
			t.Errorf("notification = %+v, want %+v", got, want)
		}
	})

	pages := []struct {
		name  string
		query string
		want  [][]uuid.UUID
	}{
		{name: "pages follow the cursor", query: "limit=2", want: [][]uuid.UUID{pick(5, 4), pick(3, 2), pick(1, 0)}},
		{name: "pages keep the filter", query: "channel=sms&limit=1", want: [][]uuid.UUID{pick(5), pick(3), pick(0)}},
		{name: "pages of a batch", query: batch + "&limit=1", want: [][]uuid.UUID{pick(4), pick(3)}},
		{name: "pages inside a created range", query: "created_from=" + at(1) + "&created_to=" + at(5) + "&limit=3", want: [][]uuid.UUID{pick(4, 3, 2), pick(1)}},
	}
	for _, tt := range pages {
		t.Run(tt.name, func(t *testing.T) {
			query := tt.query
			for i, want := range tt.want {
				page := wantPage(t, list(t, mux, query), want)

				last := i == len(tt.want)-1
				if last != (page.NextCursor == nil) {
					t.Fatalf("page %d: next_cursor = %v, last page = %t", i, page.NextCursor, last)
				}
				if !last {
					query = tt.query + "&cursor=" + *page.NextCursor
				}
			}
		})
	}

	t.Run("cursor and created to: the older one wins", func(t *testing.T) {
		afterFour := wantPage(t, list(t, mux, "limit=2"), pick(5, 4)).NextCursor
		afterTwo := wantPage(t, list(t, mux, "limit=4"), pick(5, 4, 3, 2)).NextCursor
		if afterFour == nil || afterTwo == nil {
			t.Fatal("want a next_cursor on both pages")
		}

		wantPage(t, list(t, mux, "created_to="+at(3)+"&cursor="+*afterFour), pick(2, 1, 0))
		wantPage(t, list(t, mux, "created_to="+at(5)+"&cursor="+*afterTwo), pick(1, 0))
	})

	invalid := []struct {
		name        string
		query       string
		wantMessage string
	}{
		{name: "unknown status", query: "status=done", wantMessage: "status must be one of pending, processing, sent, failed, cancelled"},
		{name: "unknown channel", query: "channel=fax", wantMessage: "channel must be one of sms, email, push"},
		{name: "limit zero", query: "limit=0", wantMessage: "limit must be 1 to 200"},
		{name: "limit over the maximum", query: "limit=201", wantMessage: "limit must be 1 to 200"},
		{name: "limit not a number", query: "limit=ten", wantMessage: `parameter "limit" is not valid`},
		{name: "created from not a time", query: "created_from=yesterday", wantMessage: `parameter "created_from" is not valid`},
		{name: "created to not a time", query: "created_to=noon", wantMessage: `parameter "created_to" is not valid`},
		{name: "created from empty", query: "created_from=", wantMessage: `parameter "created_from" is not valid`},
		{name: "created to empty", query: "created_to=", wantMessage: `parameter "created_to" is not valid`},
		{name: "status empty", query: "status=", wantMessage: "status must be one of pending, processing, sent, failed, cancelled"},
		{name: "channel empty", query: "channel=", wantMessage: "channel must be one of sms, email, push"},
		{name: "limit empty", query: "limit=", wantMessage: `parameter "limit" is not valid`},
		{name: "limit sent twice", query: "limit=2&limit=3", wantMessage: `parameter "limit" is not valid`},
		{name: "created from sent twice", query: "created_from=2026-01-01&created_from=2026-01-02", wantMessage: `parameter "created_from" must be sent once`},
		{name: "created to sent twice", query: "created_to=2026-01-01&created_to=2026-01-02", wantMessage: `parameter "created_to" must be sent once`},
		{name: "created from after created to", query: "created_from=2026-01-02&created_to=2026-01-01", wantMessage: "created_from must be before created_to"},
		{name: "created from equal to created to", query: "created_from=2026-01-01&created_to=2026-01-01", wantMessage: "created_from must be before created_to"},
		{name: "unknown parameter", query: "batchId=" + batchID.String(), wantMessage: `parameter "batchId" is not known`},
		{name: "two unknown parameters name the first by name", query: "zeta=1&alpha=1", wantMessage: `parameter "alpha" is not known`},
		{name: "bad escape in the query string", query: "status=%zz", wantMessage: "query string is not valid"},
		{name: "semicolon in the query string", query: "status=sent;channel=sms", wantMessage: "query string is not valid"},
		{name: "batch id empty", query: "batch_id=", wantMessage: `parameter "batch_id" is not valid`},
		{name: "cursor empty", query: "cursor=", wantMessage: "cursor is not valid"},
		{name: "batch id not a uuid", query: "batch_id=1", wantMessage: `parameter "batch_id" is not valid`},
		{name: "cursor not base64", query: "cursor=***", wantMessage: "cursor is not valid"},
		{name: "cursor of the wrong length", query: "cursor=YWJj", wantMessage: "cursor is not valid"},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			wantError(t, list(t, mux, tt.query), http.StatusBadRequest, tt.wantMessage)
		})
	}

	t.Run("database failure", func(t *testing.T) {
		pool.Close()

		wantError(t, list(t, mux, ""), http.StatusInternalServerError, "internal error")
		wantError(t, list(t, mux, batch), http.StatusInternalServerError, "internal error")
	})
}

func TestListNotificationsPaging(t *testing.T) {
	mux, pool := newTestServer(t)

	const existing = 205
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	inserts := make([]store.InsertBatchNotificationsParams, existing)
	newestFirst := make([]uuid.UUID, existing)
	for i := range existing {
		id := idAt(base.Add(time.Duration(i/3)*time.Second), byte(i%3))
		inserts[i] = store.InsertBatchNotificationsParams{ID: id, Recipient: "+905551234567", Channel: "sms", Content: "hello", Priority: 1}
		newestFirst[existing-1-i] = id
	}
	if _, err := store.New(pool).InsertBatchNotifications(t.Context(), inserts); err != nil {
		t.Fatalf("insert notifications: %v", err)
	}

	t.Run("default limit is 50", func(t *testing.T) {
		page := wantPage(t, list(t, mux, ""), newestFirst[:50])

		if page.NextCursor == nil {
			t.Error("want a next_cursor")
		}
	})

	t.Run("largest limit is 200", func(t *testing.T) {
		first := wantPage(t, list(t, mux, "limit=200"), newestFirst[:200])
		if first.NextCursor == nil {
			t.Fatal("want a next_cursor")
		}

		rest := wantPage(t, list(t, mux, "limit=200&cursor="+*first.NextCursor), newestFirst[200:])
		if rest.NextCursor != nil {
			t.Errorf("next_cursor = %q, want none", *rest.NextCursor)
		}
	})

	t.Run("pages stay stable while rows are inserted", func(t *testing.T) {
		var seen, added []uuid.UUID
		query := "limit=50"
		for {
			page := decodeBody[api.NotificationList](t, list(t, mux, query))
			for _, notification := range page.Notifications {
				seen = append(seen, notification.Id)
			}
			if page.NextCursor == nil {
				break
			}
			query = "limit=50&cursor=" + *page.NextCursor

			rec := postNotification(t, mux, `{"recipient":"+905551234567","channel":"sms","content":"hello"}`)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
			}
			added = append(added, decodeBody[api.Notification](t, rec).Id)
		}

		if !slices.Equal(seen, newestFirst) {
			t.Errorf("pages held %d ids, want the %d ids that existed at the first page, in order", len(seen), existing)
		}
		if len(added) != 4 {
			t.Fatalf("added %d rows between pages, want 4", len(added))
		}
		slices.Reverse(added)
		wantPage(t, list(t, mux, "limit=4"), added)
	})

	t.Run("an id made by the api is matched by its own time", func(t *testing.T) {
		rec := postNotification(t, mux, `{"recipient":"+905551234567","channel":"sms","content":"hello"}`)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
		}
		id := decodeBody[api.Notification](t, rec).Id
		sec, nsec := id.Time().UnixTime()
		created := time.Unix(sec, nsec).UTC()
		contains := func(query string) bool {
			page := decodeBody[api.NotificationList](t, list(t, mux, query))
			return slices.ContainsFunc(page.Notifications, func(n api.Notification) bool { return n.Id == id })
		}

		if !contains("created_from=" + created.Format(time.RFC3339Nano)) {
			t.Error("created_from at the id time must include the notification")
		}
		if contains("created_from=" + created.Add(time.Millisecond).Format(time.RFC3339Nano)) {
			t.Error("created_from after the id time must exclude the notification")
		}
		if contains("created_to=" + created.Format(time.RFC3339Nano)) {
			t.Error("created_to at the id time must exclude the notification")
		}
		if !contains("created_to=" + created.Add(time.Millisecond).Format(time.RFC3339Nano)) {
			t.Error("created_to after the id time must include the notification")
		}
	})
}

func idAt(t time.Time, n byte) uuid.UUID {
	var id uuid.UUID
	binary.BigEndian.PutUint64(id[:8], uint64(t.UnixMilli())<<16)
	id[6] |= 0x70
	id[8] = 0x80
	id[15] = n
	return id
}

func insertNotification(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, channel, status string, batchID pgtype.UUID) {
	t.Helper()
	_, err := pool.Exec(
		t.Context(),
		"INSERT INTO notifications (id, recipient, channel, content, priority, status, batch_id, locked_until) VALUES ($1, '+905551234567', $2, 'hello', 1, $3, $4, now() + interval '1 minute')",
		id, channel, status, batchID,
	)
	if err != nil {
		t.Fatalf("insert notification: %v", err)
	}
}

func list(t *testing.T, mux *http.ServeMux, query string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/notifications?"+query, nil)
	mux.ServeHTTP(rec, req)
	return rec
}

func wantPage(t *testing.T, rec *httptest.ResponseRecorder, want []uuid.UUID) api.NotificationList {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusOK, rec.Body)
	}
	page := decodeBody[api.NotificationList](t, rec)
	got := make([]uuid.UUID, 0, len(page.Notifications))
	for _, notification := range page.Notifications {
		got = append(got, notification.Id)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("ids = %v, want %v", got, want)
	}
	return page
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
