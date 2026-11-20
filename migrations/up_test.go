package migrations_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/migrations"
)

func newGooseProvider(t *testing.T, pool *pgxpool.Pool) *goose.Provider {
	t.Helper()
	db := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})
	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations.Files)
	if err != nil {
		t.Fatalf("create migration provider: %v", err)
	}
	return provider
}

func TestSimultaneousUpAppliesEachMigrationOnce(t *testing.T) {
	pool := testdb.New(t)
	ctx := t.Context()

	const countApplied = "SELECT count(*), count(DISTINCT version_id) FROM goose_db_version WHERE version_id > 0"
	var want, wantDistinct int
	if err := pool.QueryRow(ctx, countApplied).Scan(&want, &wantDistinct); err != nil {
		t.Fatalf("count migrations applied by one run: %v", err)
	}
	if want == 0 || want != wantDistinct {
		t.Fatalf("one run left %d rows for %d versions, want the same number above 0", want, wantDistinct)
	}

	provider := newGooseProvider(t, pool)
	if _, err := provider.DownTo(ctx, 0); err != nil {
		t.Fatalf("roll every migration back: %v", err)
	}

	errs := make([]error, 3)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range errs {
		db := stdlib.OpenDB(*pool.Config().ConnConfig)
		if err := db.PingContext(ctx); err != nil {
			t.Fatalf("connect run %d: %v", i, err)
		}
		wg.Go(func() {
			<-start
			errs[i] = errors.Join(migrations.Up(ctx, db), db.Close())
		})
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("run %d: %v", i, err)
		}
	}

	var applied, distinct int
	if err := pool.QueryRow(ctx, countApplied).Scan(&applied, &distinct); err != nil {
		t.Fatalf("count applied migrations: %v", err)
	}
	if applied != want || distinct != want {
		t.Errorf("version table holds %d rows for %d versions, want %d of each", applied, distinct, want)
	}
}

func TestLeaseMigrationGivesProcessingRowsALease(t *testing.T) {
	const leaseVersion = 6
	pool := testdb.New(t)
	ctx := t.Context()

	provider := newGooseProvider(t, pool)
	if _, err := provider.DownTo(ctx, leaseVersion-1); err != nil {
		t.Fatalf("roll back to before the lease: %v", err)
	}
	const insert = "INSERT INTO notifications (id, recipient, channel, content, priority, status) VALUES (gen_random_uuid(), '+905551234567', 'sms', 'hello', 1, $1)"
	for _, status := range []string{"processing", "pending", "sent"} {
		if _, err := pool.Exec(ctx, insert, status); err != nil {
			t.Fatalf("insert %s notification: %v", status, err)
		}
	}

	if _, err := provider.UpTo(ctx, leaseVersion); err != nil {
		t.Fatalf("apply the lease migration: %v", err)
	}

	rows, err := pool.Query(ctx, "SELECT status, extract(epoch FROM locked_until - now())::float8 FROM notifications")
	if err != nil {
		t.Fatalf("read leases: %v", err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var status string
		var seconds *float64
		if err := rows.Scan(&status, &seconds); err != nil {
			t.Fatalf("scan lease: %v", err)
		}
		seen++
		if status != "processing" {
			if seconds != nil {
				t.Errorf("%s notification has a lease of %f s, want none", status, *seconds)
			}
			continue
		}
		if seconds == nil {
			t.Error("processing notification has no lease, want one of just under 60 s")
			continue
		}
		if *seconds < 50 || *seconds > 60 {
			t.Errorf("processing notification has a lease of %f s, want just under 60", *seconds)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read leases: %v", err)
	}
	if seen != 3 {
		t.Errorf("read %d notifications, want 3", seen)
	}
}

func TestProcessingRowsMustHaveALease(t *testing.T) {
	const checkVersion = 7
	pool := testdb.New(t)
	ctx := t.Context()
	provider := newGooseProvider(t, pool)
	if _, err := provider.DownTo(ctx, checkVersion-1); err != nil {
		t.Fatalf("roll back to before the check: %v", err)
	}
	const insert = "INSERT INTO notifications (id, recipient, channel, content, priority, status, locked_until) VALUES (gen_random_uuid(), '+905551234567', 'sms', 'hello', 1, $1, $2::timestamptz)"
	if _, err := pool.Exec(ctx, insert, "processing", nil); err != nil {
		t.Fatalf("insert a processing notification without a lease: %v", err)
	}
	if _, err := pool.Exec(ctx, insert, "processing", "2000-01-01T00:00:00Z"); err != nil {
		t.Fatalf("insert a processing notification with a lease: %v", err)
	}
	if _, err := pool.Exec(ctx, insert, "pending", nil); err != nil {
		t.Fatalf("insert a pending notification without a lease: %v", err)
	}

	if _, err := provider.UpTo(ctx, checkVersion); err != nil {
		t.Fatalf("apply the check migration: %v", err)
	}

	var given, kept, without int
	err := pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE locked_until BETWEEN now() + interval '50 seconds' AND now() + interval '60 seconds'),
		count(*) FILTER (WHERE locked_until = '2000-01-01T00:00:00Z'),
		count(*) FILTER (WHERE locked_until IS NULL)
		FROM notifications`).Scan(&given, &kept, &without)
	if err != nil {
		t.Fatalf("read leases: %v", err)
	}
	if given != 1 || kept != 1 || without != 1 {
		t.Errorf("%d notifications got a lease of just under 60 s, %d kept theirs and %d have none, want 1, 1 and 1", given, kept, without)
	}

	writes := []struct {
		name string
		sql  string
		args []any
	}{
		{name: "insert processing without a lease", sql: insert, args: []any{"processing", nil}},
		{name: "remove the lease of a processing notification", sql: "UPDATE notifications SET locked_until = NULL"},
	}
	for _, write := range writes {
		_, err := pool.Exec(ctx, write.sql, write.args...)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "notifications_processing_has_lease" {
			t.Errorf("%s: error %v, want a violation of the check notifications_processing_has_lease", write.name, err)
		}
	}
	if _, err := pool.Exec(ctx, insert, "pending", nil); err != nil {
		t.Errorf("insert a pending notification without a lease: %v", err)
	}
}
