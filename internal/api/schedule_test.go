package api_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kaancolakoglu/NotificationSystem/internal/api"
)

const badScheduledAtMessage = "scheduled_at must be a date and time in RFC 3339, for example 2026-01-02T15:04:05Z"

func dueAt(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) (due, created time.Time) {
	t.Helper()
	if err := pool.QueryRow(t.Context(), "SELECT next_attempt_at, created_at FROM notifications WHERE id = $1", id).Scan(&due, &created); err != nil {
		t.Fatalf("read when the notification is due: %v", err)
	}
	return due, created
}

func TestScheduledAtSetsWhenANotificationIsDue(t *testing.T) {
	mux, pool := newTestServer(t)
	future := time.Date(2099, 1, 2, 15, 4, 5, 123456000, time.UTC)
	tests := []struct {
		name        string
		scheduledAt string
		wantFuture  bool
	}{
		{name: "in the future", scheduledAt: `,"scheduled_at":"2099-01-02T15:04:05.123456Z"`, wantFuture: true},
		{name: "in the future, in another time zone", scheduledAt: `,"scheduled_at":"2099-01-02T18:04:05.123456+03:00"`, wantFuture: true},
		{name: "in the past", scheduledAt: `,"scheduled_at":"2001-01-02T15:04:05Z"`},
		{name: "before the year 0 in UTC", scheduledAt: `,"scheduled_at":"0000-01-01T00:00:00+01:00"`},
		{name: "not sent", scheduledAt: ""},
		{name: "null", scheduledAt: `,"scheduled_at":null`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := postNotification(t, mux, `{"recipient":"+905551234567","channel":"sms","content":"hello"`+tt.scheduledAt+`}`)

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
			}
			due, created := dueAt(t, pool, decodeBody[api.Notification](t, rec).Id)
			if tt.wantFuture && !due.Equal(future) {
				t.Errorf("due at %s, want the scheduled time %s", due, future)
			}
			if !tt.wantFuture && !due.Equal(created) {
				t.Errorf("due at %s, want the time it was created, %s", due, created)
			}
		})
	}
}

func TestScheduledAtInABatchIsSetPerItem(t *testing.T) {
	const (
		scheduled = `{"recipient":"+905551234567","channel":"sms","content":"scheduled","scheduled_at":"2099-01-02T18:04:05.123456+03:00"}`
		past      = `{"recipient":"+905551234567","channel":"sms","content":"past","scheduled_at":"2001-01-02T15:04:05Z"}`
		atOnce    = `{"recipient":"+905551234567","channel":"sms","content":"at once"}`
		invalid   = `{"recipient":"not a number","channel":"sms","content":"invalid","scheduled_at":"2099-01-02T15:04:05Z"}`
	)
	future := time.Date(2099, 1, 2, 15, 4, 5, 123456000, time.UTC)
	for name, keys := range map[string][]string{"without a key": nil, "with a key": {"batch-with-a-schedule"}} {
		t.Run(name, func(t *testing.T) {
			mux, pool := newTestServer(t)

			rec := postBatch(t, mux, batchBody(scheduled, past, atOnce, invalid), keys...)

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
			}
			results := decodeBody[api.BatchCreated](t, rec).Results
			wantBatchResults(t, results, []string{"", "", "", "recipient must be a phone number in E.164 format"})
			if due, _ := dueAt(t, pool, *results[0].Id); !due.Equal(future) {
				t.Errorf("scheduled item is due at %s, want %s", due, future)
			}
			for _, result := range results[1:3] {
				due, created := dueAt(t, pool, *result.Id)
				if due.After(created) || created.Sub(due) > 5*time.Second {
					t.Errorf("item %d is due at %s and was created at %s, want it due when it is created", result.Index, due, created)
				}
				if keys != nil && !due.Equal(created) {
					t.Errorf("item %d is due at %s and was created at %s, want the same time: with a key both come from one transaction", result.Index, due, created)
				}
			}
		})
	}
}

func TestScheduledAtThatIsNotADateIsRejected(t *testing.T) {
	mux, pool := newTestServer(t)
	for _, scheduledAt := range []string{`"tomorrow"`, `"2099-01-02"`, `"2099-01-02 15:04:05"`, `""`} {
		item := `{"recipient":"+905551234567","channel":"sms","content":"hello","scheduled_at":` + scheduledAt + `}`

		wantError(t, postNotification(t, mux, item), http.StatusBadRequest, badScheduledAtMessage)
		wantError(t, postBatch(t, mux, batchBody(`{"recipient":"+905551234567","channel":"sms","content":"valid"}`, item)), http.StatusBadRequest, badScheduledAtMessage)
	}
	wantError(t, postNotification(t, mux, `{"recipient":"+905551234567","channel":"sms","content":"hello","scheduled_at":5}`), http.StatusBadRequest, `field "scheduled_at" has the wrong type`)
	if rows := countNotifications(t, pool); rows != 0 {
		t.Errorf("%d notifications stored, want none", rows)
	}
}

func TestScheduledAtIsPartOfAnIdempotentRequest(t *testing.T) {
	mux, pool := newTestServer(t)
	body := func(scheduledAt string) string {
		return `{"recipient":"+905551234567","channel":"sms","content":"hello"` + scheduledAt + `}`
	}

	first := postNotification(t, mux, body(`,"scheduled_at":"2099-01-02T15:04:05Z"`), "scheduled")
	replay := postNotification(t, mux, body(`,"scheduled_at":"2099-01-02T15:04:05Z"`), "scheduled")
	otherTime := postNotification(t, mux, body(`,"scheduled_at":"2099-01-02T15:04:06Z"`), "scheduled")
	otherZone := postNotification(t, mux, body(`,"scheduled_at":"2099-01-02T18:04:05+03:00"`), "scheduled")
	noTime := postNotification(t, mux, body(""), "scheduled")
	firstBatch := postBatch(t, mux, batchBody(body(`,"scheduled_at":"2099-01-02T15:04:05Z"`)), "scheduled-batch")
	replayBatch := postBatch(t, mux, batchBody(body(`,"scheduled_at":"2099-01-02T15:04:05Z"`)), "scheduled-batch")
	otherTimeBatch := postBatch(t, mux, batchBody(body(`,"scheduled_at":"2099-01-02T15:04:06Z"`)), "scheduled-batch")

	if first.Code != http.StatusAccepted || replay.Code != http.StatusAccepted || first.Body.String() != replay.Body.String() {
		t.Errorf("replay = %d %s, want the first answer, %d %s", replay.Code, replay.Body, first.Code, first.Body)
	}
	wantError(t, otherTime, http.StatusConflict, "Idempotency-Key was already used with a different request body")
	wantError(t, otherZone, http.StatusConflict, "Idempotency-Key was already used with a different request body")
	wantError(t, noTime, http.StatusConflict, "Idempotency-Key was already used with a different request body")
	if firstBatch.Code != http.StatusAccepted || replayBatch.Code != http.StatusAccepted || firstBatch.Body.String() != replayBatch.Body.String() {
		t.Errorf("batch replay = %d %s, want the first answer, %d %s", replayBatch.Code, replayBatch.Body, firstBatch.Code, firstBatch.Body)
	}
	wantError(t, otherTimeBatch, http.StatusConflict, "Idempotency-Key was already used with a different request body")
	if rows := countNotifications(t, pool); rows != 2 {
		t.Errorf("%d notifications stored, want 2: one per key", rows)
	}
}

func TestScheduledAtAtTheEndsOfTheYearRangeIsAccepted(t *testing.T) {
	mux, pool := newTestServer(t)
	for i, scheduledAt := range []string{"0000-01-01T00:00:00+01:00", "9999-12-31T23:30:00-01:00"} {
		item := `{"recipient":"+905551234567","channel":"sms","content":"hello","scheduled_at":"` + scheduledAt + `"}`

		single := postNotification(t, mux, item, "single-"+scheduledAt)
		batch := postBatch(t, mux, batchBody(item), "batch-"+scheduledAt)

		if single.Code != http.StatusAccepted || batch.Code != http.StatusAccepted {
			t.Errorf("scheduled_at %s: statuses = %d and %d, want %d twice, bodies %s and %s", scheduledAt, single.Code, batch.Code, http.StatusAccepted, single.Body, batch.Body)
		}
		if rows := countNotifications(t, pool); rows != 2*(i+1) {
			t.Errorf("scheduled_at %s: %d notifications stored, want %d", scheduledAt, rows, 2*(i+1))
		}
	}
}
