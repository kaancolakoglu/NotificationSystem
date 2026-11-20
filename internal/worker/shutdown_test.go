package worker

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/kaancolakoglu/NotificationSystem/internal/ratelimit"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

const (
	releasedAtStop = `"msg":"worker is stopping, releasing the notification"`
	sendsCutOff    = `"msg":"shutdown deadline passed, cancelling the sends in flight"`
)

func waitToReceive(t *testing.T, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting until %s", what)
	}
}

func TestShutdownLetsASendInFlightFinishAndClaimsNothingMore(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	arrived, finish := make(chan struct{}, 1), make(chan struct{})
	unblockProvider := sync.OnceFunc(func() { close(finish) })
	fake, client := newProvider(t, func(w http.ResponseWriter) error {
		arrived <- struct{}{}
		<-finish
		return accept(w)
	})
	inFlight := insert(t, queries, "sms", 1)
	var logs syncBuffer

	stop, stopped := runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: 2, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute, ShutdownTimeout: time.Minute})
	t.Cleanup(unblockProvider)
	waitToReceive(t, "the provider gets the notification", arrived)
	stop()
	time.Sleep(20 * pollInterval)
	late := insert(t, queries, "sms", 1)
	time.Sleep(20 * pollInterval)

	select {
	case <-stopped:
		t.Fatal("the pool stopped while a send was in flight")
	default:
	}
	if stored := get(t, queries, late.ID); stored.Status != "pending" || stored.Attempts != 0 {
		t.Errorf("notification created after the stop is %s with %d attempts, want pending with 0", stored.Status, stored.Attempts)
	}

	unblockProvider()
	waitToReceive(t, "the pool stops", stopped)

	if stored := get(t, queries, inFlight.ID); stored.Status != "sent" || stored.Attempts != 1 {
		t.Errorf("notification in flight at the stop is %s with %d attempts, want sent with 1", stored.Status, stored.Attempts)
	}
	if calls := len(fake.keys()); calls != 1 {
		t.Errorf("provider got %d requests, want 1", calls)
	}
	if logs.lineWith(releasedAtStop) {
		t.Error("a notification was claimed and released after the stop, want no claim")
	}
}

func TestShutdownReleasesANotificationThatWaitsForItsToken(t *testing.T) {
	const rows = 3
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, accept)
	for range rows {
		insert(t, queries, "sms", 1)
	}
	var logs syncBuffer
	oneCallEverySecond := ratelimit.New(testredis.New(t), 1, 1)

	stop, stopped := runPool(t, Pool{Queries: queries, Provider: client, Limiter: oneCallEverySecond, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: rows, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute, ShutdownTimeout: time.Minute})
	waitFor(t, "one notification is sent and another waits for its token", func() bool {
		return countByStatus(t, db, "sent") > 0 && countByStatus(t, db, "processing") > 0
	})
	stop()
	waitToReceive(t, "the pool stops", stopped)

	sent, pending := countByStatus(t, db, "sent"), countByStatus(t, db, "pending")
	if pending == 0 || sent+pending != rows {
		t.Errorf("%d notifications sent and %d pending, want all %d in one of the two and at least 1 pending", sent, pending, rows)
	}
	checkAttempts(t, db, sent)
	if calls := len(fake.keys()); calls != sent {
		t.Errorf("provider got %d requests, want %d", calls, sent)
	}
	if lines := logs.linesWith(`"level":"INFO"`, releasedAtStop, `"channel":"sms"`); lines < pending || lines > 2*pending {
		t.Errorf("logs have %d lines for a notification released at the stop, want %d, or up to twice that when a claim in progress at the stop took a released one again", lines, pending)
	}
	if logs.lineWith(`"level":"ERROR"`) {
		t.Error("logs have an error line, want none: a stop is not a failed rate limiter")
	}
}

func TestShutdownCancelsSendsStillInFlightAtTheDeadline(t *testing.T) {
	const (
		rows    = breakerFailures + 1
		timeout = 200 * time.Millisecond
	)
	db := testdb.New(t)
	queries := store.New(db)
	arrived, finish := make(chan struct{}, rows), make(chan struct{})
	_, client := newProvider(t, func(w http.ResponseWriter) error {
		arrived <- struct{}{}
		<-finish
		return accept(w)
	})
	for range rows {
		insert(t, queries, "sms", 1)
	}
	var logs syncBuffer

	stop, stopped := runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: rows, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute, ShutdownTimeout: timeout})
	t.Cleanup(func() { close(finish) })
	for range rows {
		waitToReceive(t, "the provider gets every notification", arrived)
	}
	started := time.Now()
	stop()
	waitToReceive(t, "the pool stops", stopped)

	if took := time.Since(started); took < timeout || took > 2*time.Second {
		t.Errorf("the pool stopped after %s, want between %s and 2 s, since the provider never answers", took, timeout)
	}
	if processing := countByStatus(t, db, "processing"); processing != rows {
		t.Errorf("%d notifications processing, want %d left for the sweeper", processing, rows)
	}
	if lines := logs.linesWith(`"level":"ERROR"`, sendsCutOff, `"channel":"sms"`); lines != 1 {
		t.Errorf("logs have %d lines for the deadline, want 1", lines)
	}
	if logs.lineWith(breakerChanged) {
		t.Error("the breaker changed state, want sends cut off at the deadline not to count as failures")
	}
}

func TestStopDoesNotCutOffASweep(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	lost := insert(t, queries, "sms", 1)
	hold(t, db, lost.ID, 1, "-1 second")
	var logs syncBuffer
	ctx, stop := context.WithCancel(t.Context())
	stop()

	Sweeper{Queries: queries, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Interval: pollInterval, QueryTimeout: queryTimeout}.sweep(ctx)

	if stored := get(t, queries, lost.ID); stored.Status != "pending" || stored.LastError.String != leaseExpired {
		t.Errorf("notification with an expired lease is %s with last error %+v, want pending with %q", stored.Status, stored.LastError, leaseExpired)
	}
	if !logs.lineWith(`"level":"WARN"`, `"msg":"lease expired"`, `"notification_id":"`+lost.ID.String()+`"`) {
		t.Error("logs have no lease expired line for the notification")
	}
	if logs.lineWith(`"level":"ERROR"`) {
		t.Error("logs have an error line, want none: a stop is not a failed sweep")
	}
}

func TestStopDoesNotCutOffAKeyExpiry(t *testing.T) {
	db := testdb.New(t)
	storeKey(t, db, "expired", "24 hours 1 minute")
	var logs syncBuffer
	ctx, stop := context.WithCancel(t.Context())
	stop()

	KeyExpirer{Queries: store.New(db), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Interval: pollInterval, QueryTimeout: queryTimeout, MaxKeys: 10}.expire(ctx)

	if keys := storedKeys(t, db); len(keys) != 0 {
		t.Errorf("stored keys = %v, want none", keys)
	}
	if !logs.lineWith(`"level":"INFO"`, `"msg":"expired idempotency keys deleted"`, `"count":1`) {
		t.Error("logs have no line for the deleted key")
	}
	if logs.lineWith(`"level":"ERROR"`) {
		t.Error("logs have an error line, want none: a stop is not a failed key expiry")
	}
}
