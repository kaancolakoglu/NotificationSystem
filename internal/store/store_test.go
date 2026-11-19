package store_test

import (
	"testing"

	"github.com/google/uuid"

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
		ID:        params.ID,
		Recipient: params.Recipient,
		Channel:   params.Channel,
		Content:   params.Content,
		Priority:  params.Priority,
		Status:    "pending",
		CreatedAt: got.CreatedAt,
	}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if !got.CreatedAt.Equal(inserted.CreatedAt) {
		t.Errorf("created_at = %v, want %v", got.CreatedAt, inserted.CreatedAt)
	}
}
