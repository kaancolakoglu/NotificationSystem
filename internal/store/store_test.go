package store_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/migrations"
)

const postgresImage = "postgres:18.7-alpine3.24"

func TestInsertThenGetNotification(t *testing.T) {
	if testing.Short() {
		t.Skip("needs a Postgres container")
	}
	ctx := t.Context()

	container, err := postgres.Run(ctx, postgresImage, postgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	databaseURL, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	db := stdlib.OpenDBFromPool(pool)
	if err := migrations.Up(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close migration handle: %v", err)
	}

	queries := store.New(pool)
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
