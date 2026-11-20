package worker

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kaancolakoglu/NotificationSystem/internal/provider"
	"github.com/kaancolakoglu/NotificationSystem/internal/ratelimit"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

const (
	breakerChanged = `"msg":"circuit breaker changed state"`
	breakerRefused = `"msg":"circuit breaker refused the call, releasing the notification"`
)

func checkAttempts(t *testing.T, db *pgxpool.Pool, want int) {
	t.Helper()
	var claimed, finished int
	if err := db.QueryRow(t.Context(), "SELECT (SELECT coalesce(sum(attempts), 0) FROM notifications), (SELECT count(*) FROM delivery_attempts)").Scan(&claimed, &finished); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	if claimed != want || finished != want {
		t.Errorf("attempts add up to %d with %d delivery attempts, want %d of each", claimed, finished, want)
	}
}

func requireStillOpen(t *testing.T, logs *syncBuffer) {
	t.Helper()
	if logs.lineWith(breakerChanged, `"from":"open"`) {
		t.Fatal("the open period ended before the checks, the test ran too slowly")
	}
}

func TestBreakerOpensAfterFiveFailuresAndClosesWhenTheProviderRecovers(t *testing.T) {
	const (
		rows    = 10
		openFor = 3 * time.Second
	)
	db := testdb.New(t)
	queries := store.New(db)
	var healthy atomic.Bool
	fake, client := newProvider(t, func(w http.ResponseWriter) error {
		if healthy.Load() {
			return accept(w)
		}
		return status(http.StatusInternalServerError)(w)
	})
	for range rows {
		insert(t, queries, "sms", 1)
	}
	var logs syncBuffer
	oneCallEvery100ms := ratelimit.New(testredis.New(t), 10, 1)

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: oneCallEvery100ms, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: rows, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: openFor})
	waitFor(t, "the breaker opens and every notification is pending again", func() bool {
		return logs.lineWith(`"level":"WARN"`, breakerChanged, `"channel":"sms"`, `"from":"closed"`, `"to":"open"`) && countByStatus(t, db, "pending") == rows
	})
	healthy.Store(true)
	failed := len(fake.keys())
	refused := logs.linesWith(`"level":"ERROR"`, breakerRefused, `"channel":"sms"`, `"error":"circuit breaker is open"`)
	if failed < breakerFailures {
		t.Errorf("provider got %d requests before the breaker opened, want at least %d", failed, breakerFailures)
	}
	if refused == 0 {
		t.Error("logs have no line for a notification the open breaker refused")
	}

	time.Sleep(500 * time.Millisecond)

	requireStillOpen(t, &logs)
	if calls := len(fake.keys()); calls != failed {
		t.Errorf("provider got %d requests while the breaker was open, want none", calls-failed)
	}
	checkAttempts(t, db, failed)
	if pending := countByStatus(t, db, "pending"); pending != rows {
		t.Errorf("%d notifications pending while the breaker was open, want %d", pending, rows)
	}
	if later := logs.linesWith(breakerRefused); later != refused {
		t.Errorf("%d notifications were claimed and refused while the breaker was open, want none", later-refused)
	}

	waitFor(t, "every notification is sent", func() bool { return countByStatus(t, db, "sent") == rows })
	time.Sleep(10 * pollInterval)

	if calls := len(fake.keys()); calls != failed+rows {
		t.Errorf("provider got %d requests, want %d: %d failed and %d accepted", calls, failed+rows, failed, rows)
	}
	checkAttempts(t, db, failed+rows)
	for _, change := range []struct{ from, to string }{{"closed", "open"}, {"open", "half-open"}, {"half-open", "closed"}} {
		if lines := logs.linesWith(`"level":"WARN"`, breakerChanged, `"channel":"sms"`, `"from":"`+change.from+`"`, `"to":"`+change.to+`"`); lines != 1 {
			t.Errorf("logs have %d lines for the breaker going from %s to %s, want 1", lines, change.from, change.to)
		}
	}
}

func TestHalfOpenBreakerTriesAtMostThreeCalls(t *testing.T) {
	const rows = 20
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, status(http.StatusInternalServerError))
	for range rows {
		insert(t, queries, "sms", 1)
	}
	var logs syncBuffer

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: rows, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Second})
	waitFor(t, "the breaker opens and no sender holds a notification", func() bool {
		return logs.lineWith(breakerChanged, `"from":"closed"`, `"to":"open"`) && countByStatus(t, db, "processing") == 0
	})
	calls, refused := len(fake.keys()), logs.linesWith(breakerRefused)
	requireStillOpen(t, &logs)
	if calls < breakerFailures {
		t.Fatalf("provider got %d requests before the breaker opened, want at least %d", calls, breakerFailures)
	}

	waitFor(t, "a trial call fails, the breaker opens again and no sender holds a notification", func() bool {
		return logs.lineWith(breakerChanged, `"from":"half-open"`, `"to":"open"`) && countByStatus(t, db, "processing") == 0
	})

	if trials := len(fake.keys()) - calls; trials < 1 || trials > breakerTrialCalls {
		t.Errorf("provider got %d requests while the breaker was half-open, want 1 to %d", trials, breakerTrialCalls)
	}
	if refusedHalfOpen := logs.linesWith(breakerRefused) - refused; refusedHalfOpen >= breakerTrialCalls {
		t.Errorf("%d notifications were claimed and refused while the breaker was half-open, want fewer than %d", refusedHalfOpen, breakerTrialCalls)
	}
	if lines := logs.linesWith(breakerChanged, `"from":"open"`, `"to":"half-open"`); lines != 1 {
		t.Errorf("logs have %d lines for the breaker going half-open, want 1", lines)
	}
}

func TestHalfOpenBreakerClaimsNoRowAfterItsThreeTrialCallsStarted(t *testing.T) {
	const (
		rows       = 6
		slowAnswer = 500 * time.Millisecond
	)
	db := testdb.New(t)
	queries := store.New(db)
	var healthy atomic.Bool
	var accepted atomic.Int32
	_, client := newProvider(t, func(w http.ResponseWriter) error {
		if !healthy.Load() {
			return status(http.StatusInternalServerError)(w)
		}
		if accepted.Add(1) > 1 {
			time.Sleep(slowAnswer)
		}
		return accept(w)
	})
	for range rows {
		insert(t, queries, "sms", 1)
	}
	var logs syncBuffer

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: breakerTrialCalls, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Second})
	waitFor(t, "the breaker opens", func() bool { return logs.lineWith(breakerChanged, `"from":"closed"`, `"to":"open"`) })
	healthy.Store(true)
	waitFor(t, "every notification is pending again", func() bool { return countByStatus(t, db, "pending") == rows })
	refused := logs.linesWith(breakerRefused)
	requireStillOpen(t, &logs)
	waitFor(t, "every notification is sent", func() bool { return countByStatus(t, db, "sent") == rows })

	if later := logs.linesWith(breakerRefused); later != refused {
		t.Errorf("%d notifications were claimed and refused while two trial calls waited for their answer, want none", later-refused)
	}
	if lines := logs.linesWith(breakerChanged, `"from":"half-open"`, `"to":"closed"`); lines != 1 {
		t.Errorf("logs have %d lines for the breaker closing, want 1", lines)
	}
	if logs.lineWith(breakerChanged, `"from":"half-open"`, `"to":"open"`) {
		t.Error("the breaker opened again, want every trial call to succeed")
	}
	if logs.lineWith(`"msg":"claim notifications"`) {
		t.Error("a claim failed while the pool held more rows than trial calls were left, want no claim then")
	}
}

func TestBreakerClosesOnlyAfterThreeTrialCallsSucceed(t *testing.T) {
	const rows = 6
	db := testdb.New(t)
	queries := store.New(db)
	failure, busy := status(http.StatusInternalServerError), status(http.StatusTooManyRequests)
	answers := []func(w http.ResponseWriter) error{failure, failure, failure, failure, failure, accept, busy, failure, busy, accept}
	fake, client := newProvider(t, inOrder(answers...))
	for range rows {
		insert(t, queries, "sms", 1)
	}
	var logs syncBuffer

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: 500 * time.Millisecond})
	waitFor(t, "every notification is sent", func() bool { return countByStatus(t, db, "sent") == rows })

	if calls := len(fake.keys()); calls != len(answers)+rows-2 {
		t.Errorf("provider got %d requests, want %d", calls, len(answers)+rows-2)
	}
	for _, change := range []struct{ from, to string }{{"closed", "open"}, {"half-open", "open"}, {"half-open", "closed"}} {
		if lines := logs.linesWith(breakerChanged, `"from":"`+change.from+`"`, `"to":"`+change.to+`"`); lines != 1 {
			t.Errorf("logs have %d lines for the breaker going from %s to %s, want 1: a 202 and a 429 are two trial calls that succeeded, the third failed", lines, change.from, change.to)
		}
	}
}

func TestAnswersThatAreNotServerErrorsKeepTheBreakerClosed(t *testing.T) {
	const rows = 2 * breakerFailures
	for _, code := range []int{http.StatusBadRequest, http.StatusRequestTimeout, http.StatusTooManyRequests} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			db := testdb.New(t)
			queries := store.New(db)
			fake, client := newProvider(t, status(code))
			for range rows {
				insert(t, queries, "sms", 1)
			}
			var logs syncBuffer

			runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute})
			waitFor(t, "every notification is failed", func() bool { return countByStatus(t, db, "failed") == rows })

			if calls := len(fake.keys()); calls < rows {
				t.Errorf("provider got %d requests, want at least %d", calls, rows)
			}
			if logs.lineWith(breakerChanged) || logs.lineWith(breakerRefused) {
				t.Errorf("the breaker changed state or refused a call after answers of %d, want neither", code)
			}
		})
	}
}

func TestAnAnswerBetweenFailuresKeepsTheBreakerClosed(t *testing.T) {
	const (
		rows     = 4
		minCalls = 13
	)
	db := testdb.New(t)
	queries := store.New(db)
	failure, busy := status(http.StatusInternalServerError), status(http.StatusTooManyRequests)
	fake, client := newProvider(t, inOrder(failure, failure, failure, failure, accept, failure, failure, failure, failure, busy, failure, failure, failure, failure, accept))
	for range rows {
		insert(t, queries, "sms", 1)
	}
	var logs syncBuffer

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute})
	waitFor(t, "every notification is sent or failed", func() bool {
		return countByStatus(t, db, "sent")+countByStatus(t, db, "failed") == rows
	})

	if calls := len(fake.keys()); calls < minCalls {
		t.Errorf("provider got %d requests, want at least %d", calls, minCalls)
	}
	if logs.lineWith(breakerChanged) || logs.lineWith(breakerRefused) {
		t.Error("the breaker changed state or refused a call, want neither: no five failures came in a row")
	}
}

func TestCallsThatGetNoAnswerOpenTheBreaker(t *testing.T) {
	const rows = 2
	db := testdb.New(t)
	queries := store.New(db)
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()
	client := provider.New(server.URL, 1, slog.New(slog.DiscardHandler))
	for range rows {
		insert(t, queries, "sms", 1)
	}
	var logs syncBuffer

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute})
	waitFor(t, "the breaker opens and every notification is pending again", func() bool {
		return logs.lineWith(breakerChanged, `"from":"closed"`, `"to":"open"`) && countByStatus(t, db, "pending") == rows
	})
	time.Sleep(10 * pollInterval)

	checkAttempts(t, db, breakerFailures)
}

func TestRowTheBreakerRefusesIsReleased(t *testing.T) {
	const openFor = 50 * time.Millisecond
	tests := []struct {
		name      string
		halfOpen  bool
		wantError string
	}{
		{name: "open", wantError: "circuit breaker is open"},
		{name: "half-open with every trial call started", halfOpen: true, wantError: "too many requests"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testdb.New(t)
			queries := store.New(db)
			fake, client := newProvider(t, accept)
			var logs syncBuffer
			pool := Pool{Queries: queries, Provider: client, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute}
			if tt.halfOpen {
				pool.BreakerOpenFor = openFor
			}
			breaker := pool.newBreaker(t.Context())
			for range breakerFailures {
				if _, err := breaker.Execute(func() (string, error) { return "", &provider.Error{Unavailable: true} }); err == nil {
					t.Fatal("the failing call returned no error")
				}
			}
			if tt.halfOpen {
				time.Sleep(2 * openFor)
				finish := make(chan struct{})
				var trials sync.WaitGroup
				for range breakerTrialCalls {
					trials.Go(func() {
						if _, err := breaker.Execute(func() (string, error) {
							<-finish
							return "", nil
						}); err != nil {
							t.Errorf("trial call: %v", err)
						}
					})
				}
				t.Cleanup(trials.Wait)
				t.Cleanup(func() { close(finish) })
				waitFor(t, "three trial calls start", func() bool { return breaker.Counts().Requests == breakerTrialCalls })
			}
			created := insert(t, queries, "sms", 1)
			claimed, err := queries.ClaimNotifications(t.Context(), store.ClaimNotificationsParams{Channel: "sms", Priority: 1, RowLimit: 1})
			if err != nil || len(claimed) != 1 || claimed[0].ID != created.ID {
				t.Fatalf("claim = %+v, error %v, want the notification", claimed, err)
			}

			pool.send(t.Context(), breaker, claimed[0])

			if stored := get(t, queries, created.ID); stored.Status != "pending" || stored.Attempts != 0 {
				t.Errorf("notification is %s with %d attempts, want pending with 0", stored.Status, stored.Attempts)
			}
			checkAttempts(t, db, 0)
			if calls := len(fake.keys()); calls != 0 {
				t.Errorf("provider got %d requests, want 0", calls)
			}
			if !logs.lineWith(`"level":"ERROR"`, breakerRefused, `"channel":"sms"`, `"error":"`+tt.wantError+`"`) {
				t.Errorf("logs have no line for the refused notification with the error %q", tt.wantError)
			}
		})
	}
}
