package testdb

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/kaancolakoglu/NotificationSystem/migrations"
)

const postgresImage = "postgres:18.7-alpine3.24"

func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
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

	return pool
}
