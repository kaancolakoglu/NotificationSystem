package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
)

func TestRunStopsWhenTheDatabaseNeverAnswers(t *testing.T) {
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	})

	err = run(t.Context(), "postgres://postgres@"+listener.Addr().String()+"/postgres?sslmode=disable", 200*time.Millisecond, time.Second)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestRunStopsWhenATableStaysLocked(t *testing.T) {
	pool := testdb.New(t)
	ctx := t.Context()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.Exec(ctx, "LOCK TABLE goose_db_version IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("lock the version table: %v", err)
	}

	err = run(ctx, pool.Config().ConnString(), 2*time.Second, 200*time.Millisecond)

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Errorf("error = %v, want a lock timeout", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Errorf("release the lock: %v", err)
	}
}
