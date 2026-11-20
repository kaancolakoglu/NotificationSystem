package store_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
)

func TestInsertThenGetNotification(t *testing.T) {
	queries := store.New(testdb.New(t))
	ctx := t.Context()

	params := store.InsertNotificationParams{
		ID:        uuid.Must(uuid.NewV7()),
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "hello",
		Priority:  1,
	}

	inserted, err := queries.InsertNotification(ctx, params)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if inserted.Status != "pending" {
		t.Errorf("status = %q, want %q", inserted.Status, "pending")
	}

	got, err := queries.GetNotification(ctx, params.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	want := store.Notification{
		ID:            params.ID,
		Recipient:     params.Recipient,
		Channel:       params.Channel,
		Content:       params.Content,
		Priority:      params.Priority,
		Status:        "pending",
		CreatedAt:     got.CreatedAt,
		NextAttemptAt: got.NextAttemptAt,
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if !got.CreatedAt.Equal(inserted.CreatedAt) {
		t.Errorf("created_at = %v, want %v", got.CreatedAt, inserted.CreatedAt)
	}
	if !got.NextAttemptAt.Equal(got.CreatedAt) {
		t.Errorf("next_attempt_at = %v, want the creation time %v", got.NextAttemptAt, got.CreatedAt)
	}
}

func TestClaimNotifications(t *testing.T) {
	pool := testdb.New(t)
	queries := store.New(pool)
	ctx := t.Context()

	insert := func(channel string, priority int16, due string) uuid.UUID {
		t.Helper()
		row, err := queries.InsertNotification(ctx, store.InsertNotificationParams{
			ID:        uuid.Must(uuid.NewV7()),
			Recipient: "+905551234567",
			Channel:   channel,
			Content:   "hello",
			Priority:  priority,
		})
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		if _, err := pool.Exec(ctx, "UPDATE notifications SET next_attempt_at = now() + $2::interval WHERE id = $1", row.ID, due); err != nil {
			t.Fatalf("set next_attempt_at: %v", err)
		}
		return row.ID
	}
	claim := func(limit int32) []uuid.UUID {
		t.Helper()
		rows, err := queries.ClaimNotifications(ctx, store.ClaimNotificationsParams{Channel: "sms", Priority: 0, RowLimit: limit})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		ids := make([]uuid.UUID, len(rows))
		for i, row := range rows {
			if row.Status != "processing" {
				t.Errorf("claimed row %s has status %q, want %q", row.ID, row.Status, "processing")
			}
			ids[i] = row.ID
		}
		return ids
	}

	newer := insert("sms", 0, "-1 minute")
	older := insert("sms", 0, "-2 minutes")
	oldest := insert("sms", 0, "-3 minutes")
	notDue := insert("sms", 0, "1 hour")
	otherChannel := insert("email", 0, "-1 hour")
	otherPriority := insert("sms", 1, "-1 hour")
	cancelled := insert("sms", 0, "-1 hour")
	if _, err := queries.CancelNotification(ctx, cancelled); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	if got, want := claim(2), []uuid.UUID{oldest, older}; !sameIDs(got, want) {
		t.Errorf("first claim of 2 = %v, want the two due longest %v", got, want)
	}
	if got, want := claim(10), []uuid.UUID{newer}; !sameIDs(got, want) {
		t.Errorf("second claim = %v, want the one left %v", got, want)
	}
	if got := claim(10); len(got) != 0 {
		t.Errorf("third claim = %v, want nothing", got)
	}

	for id, want := range map[uuid.UUID]string{
		oldest: "processing", older: "processing", newer: "processing",
		notDue: "pending", otherChannel: "pending", otherPriority: "pending", cancelled: "cancelled",
	} {
		row, err := queries.GetNotification(ctx, id)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if row.Status != want {
			t.Errorf("status of %s = %q, want %q", id, row.Status, want)
		}
	}
}

func sameIDs(got, want []uuid.UUID) bool {
	seen := map[uuid.UUID]bool{}
	for _, id := range got {
		seen[id] = true
	}
	if len(seen) != len(want) || len(got) != len(want) {
		return false
	}
	for _, id := range want {
		if !seen[id] {
			return false
		}
	}
	return true
}

func TestResultIsWrittenOnlyWhileProcessing(t *testing.T) {
	pool := testdb.New(t)
	queries := store.New(pool)
	ctx := t.Context()

	for _, status := range []string{"pending", "sent", "failed", "cancelled"} {
		row, err := queries.InsertNotification(ctx, store.InsertNotificationParams{
			ID:        uuid.Must(uuid.NewV7()),
			Recipient: "+905551234567",
			Channel:   "sms",
			Content:   "hello",
			Priority:  1,
		})
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		if _, err := pool.Exec(ctx, "UPDATE notifications SET status = $2 WHERE id = $1", row.ID, status); err != nil {
			t.Fatalf("set status: %v", err)
		}

		changed, err := queries.MarkSent(ctx, store.MarkSentParams{ID: row.ID, ProviderMessageID: pgtype.Text{String: "message-1", Valid: true}})
		if err != nil || changed != 0 {
			t.Errorf("mark %s row sent: changed %d rows, error %v, want 0 and none", status, changed, err)
		}
		changed, err = queries.MarkFailed(ctx, row.ID)
		if err != nil || changed != 0 {
			t.Errorf("mark %s row failed: changed %d rows, error %v, want 0 and none", status, changed, err)
		}

		got, err := queries.GetNotification(ctx, row.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.Status != status || got.SentAt.Valid || got.ProviderMessageID.Valid {
			t.Errorf("%s row after both writes = %+v, want it unchanged", status, got)
		}
	}
}
