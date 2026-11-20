package testdb

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/kaancolakoglu/NotificationSystem/migrations"
)

const (
	postgresImage = "postgres:18.7-alpine3.24"
	isolatedEnv   = "TESTDB_ISOLATED"
)

var (
	sharedURL string
	databases int
)

func New(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("needs a Postgres container")
	}

	if os.Getenv(isolatedEnv) != "" {
		container, serverURL := start(t.Context(), t)
		testcontainers.CleanupContainer(t, container)
		return connect(t, serverURL)
	}

	if sharedURL == "" {
		_, sharedURL = start(context.WithoutCancel(t.Context()), t)
	}
	databases++
	name := fmt.Sprintf("test_%d", databases)
	admin, err := pgx.Connect(t.Context(), sharedURL)
	if err != nil {
		t.Fatalf("connect to create database: %v", err)
	}
	_, err = admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize())
	if closeErr := admin.Close(t.Context()); closeErr != nil {
		t.Errorf("close connection after create database: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("create database: %v", err)
	}

	databaseURL, err := url.Parse(sharedURL)
	if err != nil {
		t.Fatalf("parse connection string: %v", err)
	}
	databaseURL.Path = "/" + name
	return connect(t, databaseURL.String())
}

func start(ctx context.Context, t *testing.T) (*postgres.PostgresContainer, string) {
	t.Helper()
	container, err := postgres.Run(ctx, postgresImage, postgres.BasicWaitStrategies())
	if err != nil {
		testcontainers.CleanupContainer(t, container)
		t.Fatalf("start postgres: %v", err)
	}
	serverURL, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		testcontainers.CleanupContainer(t, container)
		t.Fatalf("connection string: %v", err)
	}
	return container, serverURL
}

func connect(t *testing.T, databaseURL string) *pgxpool.Pool {
	t.Helper()
	ctx := t.Context()

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open database to migrate: %v", err)
	}
	err = migrations.Up(ctx, db)
	if closeErr := db.Close(); closeErr != nil {
		t.Errorf("close migration handle: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
