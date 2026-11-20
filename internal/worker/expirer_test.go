package worker

import (
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
)

func storeKey(t *testing.T, db *pgxpool.Pool, key, age string) {
	t.Helper()
	const insertKey = `INSERT INTO idempotency_keys (key, request_hash, response, created_at) VALUES ($1, '\x', '{}', now() - $2::interval)`
	if _, err := db.Exec(t.Context(), insertKey, key, age); err != nil {
		t.Fatalf("store idempotency key %q: %v", key, err)
	}
}

func storedKeys(t *testing.T, db *pgxpool.Pool) []string {
	t.Helper()
	rows, err := db.Query(t.Context(), "SELECT key FROM idempotency_keys ORDER BY key")
	if err != nil {
		t.Fatalf("list idempotency keys: %v", err)
	}
	keys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("read idempotency keys: %v", err)
	}
	return keys
}

func runExpirer(t *testing.T, expirer KeyExpirer) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		expirer.Run(t.Context())
	}()
	t.Cleanup(func() { <-done })
}

func TestKeysOlderThan24HoursAreDeleted(t *testing.T) {
	db := testdb.New(t)
	storeKey(t, db, "expired", "24 hours 1 minute")
	storeKey(t, db, "kept", "23 hours 59 minutes")
	var logs syncBuffer

	runExpirer(t, KeyExpirer{Queries: store.New(db), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Interval: pollInterval, QueryTimeout: queryTimeout})
	waitFor(t, "the expired key is deleted", func() bool { return !slices.Contains(storedKeys(t, db), "expired") })

	if keys, want := storedKeys(t, db), []string{"kept"}; !slices.Equal(keys, want) {
		t.Errorf("stored keys = %v, want %v", keys, want)
	}
	waitFor(t, "the delete is logged", func() bool {
		return logs.lineWith(`"level":"INFO"`, `"msg":"expired idempotency keys deleted"`, `"count":1`)
	})
}

func TestKeyExpiryGivesUpWhenTheDatabaseDoesNotAnswer(t *testing.T) {
	db := testdb.New(t)
	storeKey(t, db, "expired", "25 hours")
	var logs syncBuffer
	lock, err := lockTable(t, db, "idempotency_keys")
	if err != nil {
		t.Fatal(err)
	}

	runExpirer(t, KeyExpirer{Queries: store.New(db), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Interval: pollInterval, QueryTimeout: 50 * time.Millisecond})
	waitFor(t, "the delete gives up", func() bool {
		return logs.lineWith(`"level":"ERROR"`, `"msg":"delete expired idempotency keys"`, "context deadline exceeded")
	})

	if err := lock.Rollback(t.Context()); err != nil {
		t.Fatalf("release table lock: %v", err)
	}
	waitFor(t, "the key is deleted once the database answers again", func() bool { return len(storedKeys(t, db)) == 0 })
}
