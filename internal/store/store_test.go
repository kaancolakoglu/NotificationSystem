package store_test

import (
	"maps"
	"sync"
	"testing"
	"time"

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
			if row.Status != "processing" || row.Attempts != 1 {
				t.Errorf("claimed row %s has status %q and %d attempts, want %q and 1", row.ID, row.Status, row.Attempts, "processing")
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
		wantAttempts := map[bool]int16{true: 1, false: 0}[want == "processing"]
		if row.Status != want || row.Attempts != wantAttempts {
			t.Errorf("%s has status %q and %d attempts, want %q and %d", id, row.Status, row.Attempts, want, wantAttempts)
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
		row.Status = status

		changed, err := queries.MarkSent(ctx, store.MarkSentParams{ID: row.ID, Attempts: row.Attempts, ProviderMessageID: pgtype.Text{String: "message-1", Valid: true}})
		if err != nil || changed != 0 {
			t.Errorf("mark %s row sent: changed %d rows, error %v, want 0 and none", status, changed, err)
		}
		changed, err = queries.MarkFailed(ctx, store.MarkFailedParams{ID: row.ID, Attempts: row.Attempts, Error: "failure"})
		if err != nil || changed != 0 {
			t.Errorf("mark %s row failed: changed %d rows, error %v, want 0 and none", status, changed, err)
		}
		changed, err = queries.ScheduleRetry(ctx, store.ScheduleRetryParams{ID: row.ID, Attempts: row.Attempts, Error: "failure", Delay: pgtype.Interval{Microseconds: time.Hour.Microseconds(), Valid: true}})
		if err != nil || changed != 0 {
			t.Errorf("schedule a retry of %s row: changed %d rows, error %v, want 0 and none", status, changed, err)
		}

		got, err := queries.GetNotification(ctx, row.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got != row {
			t.Errorf("%s row after the writes = %+v, want it unchanged %+v", status, got, row)
		}
		attempts, err := queries.ListDeliveryAttempts(ctx, row.ID)
		if err != nil || len(attempts) != 0 {
			t.Errorf("delivery attempts of %s row = %+v, error %v, want none", status, attempts, err)
		}
	}
}

func TestResultWritesRecordTheAttempt(t *testing.T) {
	pool := testdb.New(t)
	queries := store.New(pool)
	ctx := t.Context()

	claim := func(t *testing.T) store.Notification {
		t.Helper()
		created, err := queries.InsertNotification(ctx, store.InsertNotificationParams{
			ID:        uuid.Must(uuid.NewV7()),
			Recipient: "+905551234567",
			Channel:   "sms",
			Content:   "hello",
			Priority:  1,
		})
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		claimed, err := queries.ClaimNotifications(ctx, store.ClaimNotificationsParams{Channel: "sms", Priority: 1, RowLimit: 1})
		if err != nil || len(claimed) != 1 || claimed[0].ID != created.ID {
			t.Fatalf("claim = %+v, error %v, want the notification", claimed, err)
		}
		return claimed[0]
	}
	check := func(t *testing.T, changed int64, err error, want store.Notification, wantError pgtype.Text) store.ListDeliveryAttemptsRow {
		t.Helper()
		if err != nil || changed != 1 {
			t.Fatalf("write: changed %d rows, error %v, want 1 and none", changed, err)
		}
		got, err := queries.GetNotification(ctx, want.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got != want {
			t.Errorf("row = %+v, want %+v", got, want)
		}
		attempts, err := queries.ListDeliveryAttempts(ctx, want.ID)
		if err != nil || len(attempts) != 1 {
			t.Fatalf("delivery attempts = %+v, error %v, want one", attempts, err)
		}
		if attempts[0].Attempt != 1 || attempts[0].Error != wantError || attempts[0].CreatedAt.Before(want.CreatedAt) {
			t.Errorf("delivery attempt = %+v, want number 1 with error %+v, not before %s", attempts[0], wantError, want.CreatedAt)
		}
		return attempts[0]
	}

	t.Run("sent", func(t *testing.T) {
		row := claim(t)

		changed, err := queries.MarkSent(ctx, store.MarkSentParams{ID: row.ID, Attempts: row.Attempts, ProviderMessageID: pgtype.Text{String: "message-1", Valid: true}})

		row.Status = "sent"
		row.ProviderMessageID = pgtype.Text{String: "message-1", Valid: true}
		stored, getErr := queries.GetNotification(ctx, row.ID)
		if getErr != nil {
			t.Fatalf("get: %v", getErr)
		}
		row.SentAt = stored.SentAt
		attempt := check(t, changed, err, row, pgtype.Text{})
		if !stored.SentAt.Valid || !stored.SentAt.Time.Equal(attempt.CreatedAt) {
			t.Errorf("sent at = %+v, want the time of the attempt %s", stored.SentAt, attempt.CreatedAt)
		}
	})

	t.Run("failed", func(t *testing.T) {
		row := claim(t)

		changed, err := queries.MarkFailed(ctx, store.MarkFailedParams{ID: row.ID, Attempts: row.Attempts, Error: "provider answered 400, want 202"})

		row.Status = "failed"
		row.LastError = pgtype.Text{String: "provider answered 400, want 202", Valid: true}
		check(t, changed, err, row, row.LastError)
	})

	t.Run("retry", func(t *testing.T) {
		const delay = 90 * time.Second
		row := claim(t)

		changed, err := queries.ScheduleRetry(ctx, store.ScheduleRetryParams{
			ID:       row.ID,
			Attempts: row.Attempts,
			Error:    "provider answered 500, want 202",
			Delay:    pgtype.Interval{Microseconds: delay.Microseconds(), Valid: true},
		})

		row.Status = "pending"
		row.LastError = pgtype.Text{String: "provider answered 500, want 202", Valid: true}
		stored, getErr := queries.GetNotification(ctx, row.ID)
		if getErr != nil {
			t.Fatalf("get: %v", getErr)
		}
		row.NextAttemptAt = stored.NextAttemptAt
		attempt := check(t, changed, err, row, row.LastError)
		if got := stored.NextAttemptAt.Sub(attempt.CreatedAt); got != delay {
			t.Errorf("next attempt is %s after the failed one, want %s", got, delay)
		}

		claimed, err := queries.ClaimNotifications(ctx, store.ClaimNotificationsParams{Channel: "sms", Priority: 1, RowLimit: 1})
		if err != nil || len(claimed) != 0 {
			t.Errorf("claim before the retry is due = %+v, error %v, want nothing", claimed, err)
		}
		if _, err := pool.Exec(ctx, "UPDATE notifications SET next_attempt_at = now() WHERE id = $1", row.ID); err != nil {
			t.Fatalf("make the retry due: %v", err)
		}
		claimed, err = queries.ClaimNotifications(ctx, store.ClaimNotificationsParams{Channel: "sms", Priority: 1, RowLimit: 1})
		if err != nil || len(claimed) != 1 || claimed[0].ID != row.ID || claimed[0].Attempts != 2 {
			t.Errorf("claim once the retry is due = %+v, error %v, want the notification with 2 attempts", claimed, err)
		}
	})
}

func TestResultOfAnotherAttemptIsNotWritten(t *testing.T) {
	pool := testdb.New(t)
	queries := store.New(pool)
	ctx := t.Context()

	created, err := queries.InsertNotification(ctx, store.InsertNotificationParams{
		ID:        uuid.Must(uuid.NewV7()),
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "hello",
		Priority:  1,
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE notifications SET attempts = 1 WHERE id = $1", created.ID); err != nil {
		t.Fatalf("set attempts: %v", err)
	}
	claimed, err := queries.ClaimNotifications(ctx, store.ClaimNotificationsParams{Channel: "sms", Priority: 1, RowLimit: 1})
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 2 {
		t.Fatalf("claim = %+v, error %v, want the notification with 2 attempts", claimed, err)
	}

	for _, attempt := range []int16{1, 3} {
		changed, err := queries.MarkSent(ctx, store.MarkSentParams{ID: created.ID, Attempts: attempt, ProviderMessageID: pgtype.Text{String: "message-1", Valid: true}})
		if err != nil || changed != 0 {
			t.Errorf("mark sent for attempt %d: changed %d rows, error %v, want 0 and none", attempt, changed, err)
		}
		changed, err = queries.MarkFailed(ctx, store.MarkFailedParams{ID: created.ID, Attempts: attempt, Error: "failure"})
		if err != nil || changed != 0 {
			t.Errorf("mark failed for attempt %d: changed %d rows, error %v, want 0 and none", attempt, changed, err)
		}
		changed, err = queries.ScheduleRetry(ctx, store.ScheduleRetryParams{ID: created.ID, Attempts: attempt, Error: "failure", Delay: pgtype.Interval{Microseconds: time.Hour.Microseconds(), Valid: true}})
		if err != nil || changed != 0 {
			t.Errorf("schedule a retry for attempt %d: changed %d rows, error %v, want 0 and none", attempt, changed, err)
		}
	}

	got, err := queries.GetNotification(ctx, created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != claimed[0] {
		t.Errorf("row after the writes = %+v, want it unchanged %+v", got, claimed[0])
	}
	attempts, err := queries.ListDeliveryAttempts(ctx, created.ID)
	if err != nil || len(attempts) != 0 {
		t.Errorf("delivery attempts = %+v, error %v, want none", attempts, err)
	}
}

func TestClaimTakesALeaseOfOneMinute(t *testing.T) {
	pool := testdb.New(t)
	ctx := t.Context()

	created, err := store.New(pool).InsertNotification(ctx, store.InsertNotificationParams{
		ID:        uuid.Must(uuid.NewV7()),
		Recipient: "+905551234567",
		Channel:   "sms",
		Content:   "hello",
		Priority:  1,
	})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if created.LockedUntil.Valid {
		t.Errorf("locked until = %+v before a claim, want none", created.LockedUntil)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		t.Fatalf("read the transaction time: %v", err)
	}
	claimed, err := store.New(tx).ClaimNotifications(ctx, store.ClaimNotificationsParams{Channel: "sms", Priority: 1, RowLimit: 1})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %+v, error %v, want the notification", claimed, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if want := now.Add(time.Minute); !claimed[0].LockedUntil.Valid || !claimed[0].LockedUntil.Time.Equal(want) {
		t.Errorf("locked until = %+v, want one minute after the claim, %s", claimed[0].LockedUntil, want)
	}
}

func TestSweepExpiredLeases(t *testing.T) {
	const reason = "lease expired"
	pool := testdb.New(t)
	queries := store.New(pool)
	ctx := t.Context()

	insert := func(status string, attempts int16, lease string) store.Notification {
		t.Helper()
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
		if _, err := pool.Exec(ctx, "UPDATE notifications SET status = $2, attempts = $3, locked_until = now() + $4::interval WHERE id = $1", row.ID, status, attempts, lease); err != nil {
			t.Fatalf("set status, attempts and lease: %v", err)
		}
		row, err = queries.GetNotification(ctx, row.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		return row
	}
	expired := insert("processing", 1, "-1 second")
	lastTry := insert("processing", 4, "-1 second")
	usedUp := insert("processing", 5, "-1 hour")
	held := insert("processing", 1, "1 minute")
	pending := insert("pending", 1, "-1 hour")
	sent := insert("sent", 1, "-1 hour")

	swept, err := queries.SweepExpiredLeases(ctx, store.SweepExpiredLeasesParams{MaxAttempts: 5, Error: reason})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	got := map[uuid.UUID]store.SweepExpiredLeasesRow{}
	for _, row := range swept {
		got[row.ID] = row
	}
	want := map[uuid.UUID]store.SweepExpiredLeasesRow{
		expired.ID: {ID: expired.ID, Status: "pending", Attempts: 1},
		lastTry.ID: {ID: lastTry.ID, Status: "pending", Attempts: 4},
		usedUp.ID:  {ID: usedUp.ID, Status: "failed", Attempts: 5},
	}
	if len(swept) != len(want) || !maps.Equal(got, want) {
		t.Errorf("swept = %+v, want %+v", swept, want)
	}

	for _, before := range []store.Notification{expired, lastTry, usedUp} {
		after, err := queries.GetNotification(ctx, before.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		before.Status = want[before.ID].Status
		before.LastError = pgtype.Text{String: reason, Valid: true}
		if after != before {
			t.Errorf("swept row = %+v, want %+v", after, before)
		}
		attempts, err := queries.ListDeliveryAttempts(ctx, before.ID)
		if err != nil || len(attempts) != 1 || attempts[0].Attempt != before.Attempts || attempts[0].Error != before.LastError {
			t.Errorf("delivery attempts of swept row = %+v, error %v, want number %d with the error %q", attempts, err, before.Attempts, reason)
		}
	}
	for _, before := range []store.Notification{held, pending, sent} {
		after, err := queries.GetNotification(ctx, before.ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if after != before {
			t.Errorf("%s row with a lease until %s = %+v, want it unchanged %+v", before.Status, before.LockedUntil.Time, after, before)
		}
		attempts, err := queries.ListDeliveryAttempts(ctx, before.ID)
		if err != nil || len(attempts) != 0 {
			t.Errorf("delivery attempts of %s row = %+v, error %v, want none", before.Status, attempts, err)
		}
	}

	again, err := queries.SweepExpiredLeases(ctx, store.SweepExpiredLeasesParams{MaxAttempts: 5, Error: reason})
	if err != nil || len(again) != 0 {
		t.Errorf("second sweep = %+v, error %v, want nothing", again, err)
	}
}

func TestSweepsAtTheSameTimeTakeEachRowOnce(t *testing.T) {
	const notifications, sweeps = 200, 4
	pool := testdb.New(t)
	queries := store.New(pool)
	ctx := t.Context()
	for range notifications {
		if _, err := queries.InsertNotification(ctx, store.InsertNotificationParams{
			ID:        uuid.Must(uuid.NewV7()),
			Recipient: "+905551234567",
			Channel:   "sms",
			Content:   "hello",
			Priority:  1,
		}); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	if _, err := pool.Exec(ctx, "UPDATE notifications SET status = 'processing', attempts = 1, locked_until = now() - interval '1 second'"); err != nil {
		t.Fatalf("set status, attempts and lease: %v", err)
	}

	start := make(chan struct{})
	swept := make([]int, sweeps)
	errs := make([]error, sweeps)
	var running sync.WaitGroup
	for i := range sweeps {
		running.Go(func() {
			<-start
			rows, err := queries.SweepExpiredLeases(ctx, store.SweepExpiredLeasesParams{MaxAttempts: 5, Error: "lease expired"})
			swept[i], errs[i] = len(rows), err
		})
	}
	close(start)
	running.Wait()

	total := 0
	for i := range sweeps {
		if errs[i] != nil {
			t.Errorf("sweep %d: %v", i, errs[i])
		}
		total += swept[i]
	}
	if total != notifications {
		t.Errorf("the sweeps returned %v rows, %d in total, want %d", swept, total, notifications)
	}
	var recorded int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM delivery_attempts").Scan(&recorded); err != nil {
		t.Fatalf("count delivery attempts: %v", err)
	}
	if recorded != notifications {
		t.Errorf("delivery attempts = %d, want %d", recorded, notifications)
	}
}
