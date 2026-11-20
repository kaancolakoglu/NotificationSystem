package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kaancolakoglu/NotificationSystem/internal/provider"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
)

const (
	pollInterval = 5 * time.Millisecond
	queryTimeout = 5 * time.Second
	backoffBase  = time.Millisecond
)

type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) lineWith(parts ...string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for line := range strings.SplitSeq(b.buffer.String(), "\n") {
		if !slices.ContainsFunc(parts, func(part string) bool { return !strings.Contains(line, part) }) {
			return true
		}
	}
	return false
}

func lockTable(t *testing.T, db *pgxpool.Pool) (pgx.Tx, error) {
	tx, err := db.Begin(t.Context())
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	t.Cleanup(func() {
		if err := tx.Rollback(context.WithoutCancel(t.Context())); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Errorf("release table lock: %v", err)
		}
	})
	if _, err := tx.Exec(t.Context(), "LOCK TABLE notifications IN ACCESS EXCLUSIVE MODE"); err != nil {
		return nil, fmt.Errorf("lock table: %w", err)
	}
	return tx, nil
}

type request struct {
	key  string
	body string
	at   time.Time
}

type fakeProvider struct {
	mu       sync.Mutex
	requests []request
}

func (f *fakeProvider) record(r *http.Request) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, request{key: r.Header.Get("Idempotency-Key"), body: string(body), at: time.Now()})
	return nil
}

func (f *fakeProvider) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, len(f.requests))
	for i, request := range f.requests {
		keys[i] = request.key
	}
	return keys
}

func newProvider(t *testing.T, answer func(w http.ResponseWriter) error) (*fakeProvider, *provider.Client) {
	t.Helper()
	fake := &fakeProvider{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := fake.record(r); err != nil {
			t.Errorf("read provider request: %v", err)
		}
		if err := answer(w); err != nil {
			t.Errorf("write provider answer: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return fake, provider.New(server.URL, 10, slog.New(slog.DiscardHandler))
}

func accept(w http.ResponseWriter) error {
	w.WriteHeader(http.StatusAccepted)
	_, err := io.WriteString(w, `{"messageId":"message-1","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`)
	return err
}

func status(code int) func(w http.ResponseWriter) error {
	return func(w http.ResponseWriter) error {
		w.WriteHeader(code)
		return nil
	}
}

func inOrder(answers ...func(w http.ResponseWriter) error) func(w http.ResponseWriter) error {
	var mu sync.Mutex
	next := 0
	return func(w http.ResponseWriter) error {
		mu.Lock()
		answer := answers[min(next, len(answers)-1)]
		next++
		mu.Unlock()
		return answer(w)
	}
}

func attempts(t *testing.T, queries *store.Queries, id uuid.UUID) []store.ListDeliveryAttemptsRow {
	t.Helper()
	rows, err := queries.ListDeliveryAttempts(t.Context(), id)
	if err != nil {
		t.Fatalf("list delivery attempts: %v", err)
	}
	return rows
}

func runPool(t *testing.T, pool Pool) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Run(t.Context())
	}()
	t.Cleanup(func() { <-done })
}

func insert(t *testing.T, queries *store.Queries, channel string, priority int16) store.Notification {
	t.Helper()
	row, err := queries.InsertNotification(t.Context(), store.InsertNotificationParams{
		ID:        uuid.Must(uuid.NewV7()),
		Recipient: "+905551234567",
		Channel:   channel,
		Content:   "hello",
		Priority:  priority,
	})
	if err != nil {
		t.Fatalf("insert notification: %v", err)
	}
	return row
}

func get(t *testing.T, queries *store.Queries, id uuid.UUID) store.Notification {
	t.Helper()
	row, err := queries.GetNotification(t.Context(), id)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	return row
}

func countByStatus(t *testing.T, db *pgxpool.Pool, status string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(t.Context(), "SELECT count(*) FROM notifications WHERE status = $1", status).Scan(&count); err != nil {
		t.Fatalf("count %s notifications: %v", status, err)
	}
	return count
}

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(pollInterval)
	}
}

func TestCreatedRowIsSent(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, accept)
	created := insert(t, queries, "sms", 1)

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 2, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "the notification is sent", func() bool { return get(t, queries, created.ID).Status == "sent" })
	time.Sleep(10 * pollInterval)

	fake.mu.Lock()
	requests := slices.Clone(fake.requests)
	fake.mu.Unlock()
	if len(requests) != 1 {
		t.Fatalf("provider got %d requests, want 1", len(requests))
	}
	if requests[0].key != created.ID.String() {
		t.Errorf("idempotency key = %q, want %q", requests[0].key, created.ID)
	}
	var sent map[string]string
	if err := json.Unmarshal([]byte(requests[0].body), &sent); err != nil {
		t.Fatalf("provider request body %s is not a JSON object of strings: %v", requests[0].body, err)
	}
	if sent["to"] != created.Recipient || sent["channel"] != created.Channel || sent["content"] != created.Content {
		t.Errorf("provider request body = %s, want the recipient, channel and content of the notification", requests[0].body)
	}

	stored := get(t, queries, created.ID)
	if !stored.ProviderMessageID.Valid || stored.ProviderMessageID.String != "message-1" {
		t.Errorf("provider message id = %+v, want message-1", stored.ProviderMessageID)
	}
	if !stored.SentAt.Valid || stored.SentAt.Time.Before(created.CreatedAt) {
		t.Errorf("sent at = %+v, want a time at or after %s", stored.SentAt, created.CreatedAt)
	}
}

func TestPermanentFailureIsNotRetried(t *testing.T) {
	const failure = "provider answered 400, want 202"
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, status(http.StatusBadRequest))
	created := insert(t, queries, "sms", 1)

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 2, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "the notification is failed", func() bool { return get(t, queries, created.ID).Status == "failed" })
	time.Sleep(10 * pollInterval)

	if keys := fake.keys(); len(keys) != 1 {
		t.Errorf("provider got %d requests, want 1", len(keys))
	}
	stored := get(t, queries, created.ID)
	if stored.Attempts != 1 || stored.LastError.String != failure {
		t.Errorf("attempts = %d, last error = %+v, want 1 and %q", stored.Attempts, stored.LastError, failure)
	}
	if stored.SentAt.Valid || stored.ProviderMessageID.Valid {
		t.Errorf("sent at = %+v, provider message id = %+v, want neither", stored.SentAt, stored.ProviderMessageID)
	}
	history := attempts(t, queries, created.ID)
	if len(history) != 1 || history[0].Attempt != 1 || history[0].Error.String != failure {
		t.Errorf("delivery attempts = %+v, want one, number 1 with the error %q", history, failure)
	}
}

func TestRetryableFailureIsSentAgain(t *testing.T) {
	const failure = "provider answered 500, want 202"
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, inOrder(status(http.StatusInternalServerError), accept))
	created := insert(t, queries, "sms", 1)

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 2, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "the notification is sent", func() bool { return get(t, queries, created.ID).Status == "sent" })
	time.Sleep(10 * pollInterval)

	if keys, want := fake.keys(), []string{created.ID.String(), created.ID.String()}; !slices.Equal(keys, want) {
		t.Errorf("provider got requests for %v, want %v", keys, want)
	}
	stored := get(t, queries, created.ID)
	if stored.Attempts != 2 || stored.LastError.String != failure || stored.ProviderMessageID.String != "message-1" {
		t.Errorf("attempts = %d, last error = %+v, provider message id = %+v, want 2, %q and message-1", stored.Attempts, stored.LastError, stored.ProviderMessageID, failure)
	}
	history := attempts(t, queries, created.ID)
	if len(history) != 2 || history[0].Attempt != 1 || history[0].Error.String != failure || history[1].Attempt != 2 || history[1].Error.Valid {
		t.Errorf("delivery attempts = %+v, want number 1 with the error %q, then number 2 with none", history, failure)
	}
}

func TestNotificationFailsWhenAttemptsAreUsedUp(t *testing.T) {
	const failure = "provider answered 500, want 202"
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, status(http.StatusInternalServerError))
	created := insert(t, queries, "sms", 1)

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 2, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "the notification is failed", func() bool { return get(t, queries, created.ID).Status == "failed" })
	time.Sleep(50 * pollInterval)

	if keys := fake.keys(); len(keys) != 5 {
		t.Errorf("provider got %d requests, want 5", len(keys))
	}
	stored := get(t, queries, created.ID)
	if stored.Status != "failed" || stored.Attempts != 5 || stored.LastError.String != failure {
		t.Errorf("status = %q, attempts = %d, last error = %+v, want failed, 5 and %q", stored.Status, stored.Attempts, stored.LastError, failure)
	}
	history := attempts(t, queries, created.ID)
	if len(history) != 5 {
		t.Fatalf("delivery attempts = %+v, want 5 of them", history)
	}
	for i, attempt := range history {
		if attempt.Attempt != int16(i+1) || attempt.Error.String != failure {
			t.Errorf("delivery attempt %d = %+v, want number %d with the error %q", i, attempt, i+1, failure)
		}
	}
}

func TestBackoffGrowsWithTheAttempt(t *testing.T) {
	const rows = 30
	db := testdb.New(t)
	queries := store.New(db)
	_, client := newProvider(t, status(http.StatusInternalServerError))
	ids := make([]uuid.UUID, rows)
	for i := range ids {
		ids[i] = insert(t, queries, "sms", 1).ID
	}

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 10, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "every notification is failed", func() bool { return countByStatus(t, db, "failed") == rows })

	const ceiling = backoffBase << 4
	var longest time.Duration
	for _, id := range ids {
		history := attempts(t, queries, id)
		if len(history) != maxAttempts {
			t.Fatalf("delivery attempts = %+v, want %d of them", history, maxAttempts)
		}
		delay := get(t, queries, id).NextAttemptAt.Sub(history[3].CreatedAt)
		if delay < 0 || delay >= ceiling {
			t.Errorf("delay after attempt 4 = %s, want 0 up to %s", delay, ceiling)
		}
		longest = max(longest, delay)
	}
	if longest < ceiling/2 {
		t.Errorf("longest of %d delays after attempt 4 = %s, want some above %s", rows, longest, ceiling/2)
	}
}

func TestRetryWaitsForRetryAfter(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, inOrder(func(w http.ResponseWriter) error {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		return nil
	}, accept))
	created := insert(t, queries, "sms", 1)

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 2, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "the notification is sent", func() bool { return get(t, queries, created.ID).Status == "sent" })

	fake.mu.Lock()
	requests := slices.Clone(fake.requests)
	fake.mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("provider got %d requests, want 2", len(requests))
	}
	if waited := requests[1].at.Sub(requests[0].at); waited < 900*time.Millisecond {
		t.Errorf("retry came %s after the 429, want at least the Retry-After of 1 s", waited)
	}
}

func TestRetryDelay(t *testing.T) {
	const samples = 2000
	for attempt, ceiling := range map[int16]time.Duration{1: 2 * time.Second, 2: 4 * time.Second, 3: 8 * time.Second, 4: 16 * time.Second} {
		var longest time.Duration
		for range samples {
			delay := retryDelay(time.Second, attempt, 0)
			if delay < 0 || delay >= ceiling {
				t.Fatalf("delay after attempt %d = %s, want 0 up to %s", attempt, delay, ceiling)
			}
			longest = max(longest, delay)
		}
		if longest < ceiling/2 {
			t.Errorf("longest of %d delays after attempt %d = %s, want some above %s", samples, attempt, longest, ceiling/2)
		}
	}

	if delay := retryDelay(time.Second, 1, 10*time.Second); delay != 10*time.Second {
		t.Errorf("delay with a Retry-After above the backoff = %s, want the Retry-After of 10 s", delay)
	}
	var longest time.Duration
	for range samples {
		longest = max(longest, retryDelay(time.Second, 4, time.Millisecond))
	}
	if longest < 8*time.Second {
		t.Errorf("longest of %d delays with a Retry-After below the backoff = %s, want the backoff, some above 8 s", samples, longest)
	}
}

func TestTwoPoolsNeverSendTheSameRow(t *testing.T) {
	const rows = 200
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, accept)
	want := make([]string, rows)
	for i := range want {
		want[i] = insert(t, queries, "email", 1).ID.String()
	}

	for range 2 {
		runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "email", Size: 5, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	}
	waitFor(t, "every notification is sent", func() bool { return countByStatus(t, db, "sent") == rows })
	time.Sleep(10 * pollInterval)

	got := fake.keys()
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("provider got %d requests for %d notifications, want each notification once", len(got), rows)
	}
}

func TestHighPriorityGoesFirst(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, accept)
	priority := map[string]int16{}
	for _, p := range []int16{2, 2, 1, 1, 0, 0} {
		priority[insert(t, queries, "push", p).ID.String()] = p
	}

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "push", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "every notification is sent", func() bool { return countByStatus(t, db, "sent") == len(priority) })

	var got []int16
	for _, key := range fake.keys() {
		got = append(got, priority[key])
	}
	if want := []int16{0, 0, 1, 1, 2, 2}; !slices.Equal(got, want) {
		t.Errorf("priorities in the order sent = %v, want %v", got, want)
	}
}

func TestPoolClaimsOnlyItsFreeCapacity(t *testing.T) {
	const size = 2
	db := testdb.New(t)
	queries := store.New(db)
	release := make(chan struct{})
	fake, client := newProvider(t, func(w http.ResponseWriter) error {
		<-release
		return accept(w)
	})
	for range 5 {
		insert(t, queries, "sms", 1)
	}

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: size, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "the pool is full", func() bool { return len(fake.keys()) == size })
	time.Sleep(10 * pollInterval)

	if processing, pending := countByStatus(t, db, "processing"), countByStatus(t, db, "pending"); processing != size || pending != 5-size {
		t.Errorf("with %d sends in flight: %d processing and %d pending, want %d and %d", size, processing, pending, size, 5-size)
	}

	close(release)
	waitFor(t, "every notification is sent", func() bool { return countByStatus(t, db, "sent") == 5 })
}

func TestCancelledRowIsNeverSent(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, accept)
	first := insert(t, queries, "sms", 1)
	cancelled := insert(t, queries, "sms", 1)
	last := insert(t, queries, "sms", 1)
	if _, err := queries.CancelNotification(t.Context(), cancelled.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 5, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "the other notifications are sent", func() bool { return countByStatus(t, db, "sent") == 2 })
	time.Sleep(10 * pollInterval)

	got := fake.keys()
	slices.Sort(got)
	if want := []string{first.ID.String(), last.ID.String()}; !slices.Equal(got, want) {
		t.Errorf("provider got requests for %v, want %v", got, want)
	}
	if status := get(t, queries, cancelled.ID).Status; status != "cancelled" {
		t.Errorf("status of the cancelled notification = %q, want %q", status, "cancelled")
	}
}

func TestCancelRacingWithClaimHasOneWinner(t *testing.T) {
	const rows = 200
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, accept)
	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 5, Interval: time.Millisecond, QueryTimeout: queryTimeout, BackoffBase: backoffBase})

	ids := make([]uuid.UUID, rows)
	cancelled := make([]bool, rows)
	var cancels sync.WaitGroup
	for i := range ids {
		ids[i] = insert(t, queries, "sms", 1).ID
		cancels.Go(func() {
			_, err := queries.CancelNotification(t.Context(), ids[i])
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				t.Errorf("cancel notification %d: %v", i, err)
			}
			cancelled[i] = err == nil
		})
	}
	cancels.Wait()
	waitFor(t, "every notification is sent or cancelled", func() bool {
		return countByStatus(t, db, "pending") == 0 && countByStatus(t, db, "processing") == 0
	})

	requests := map[string]int{}
	for _, key := range fake.keys() {
		requests[key]++
	}
	sent := 0
	for i, id := range ids {
		want := map[bool]string{true: "cancelled", false: "sent"}[cancelled[i]]
		wantRequests := map[bool]int{true: 0, false: 1}[cancelled[i]]
		if status := get(t, queries, id).Status; status != want {
			t.Errorf("notification %d: status = %q, want %q", i, status, want)
		}
		if requests[id.String()] != wantRequests {
			t.Errorf("notification %d (%s): provider got %d requests, want %d", i, want, requests[id.String()], wantRequests)
		}
		sent += wantRequests
	}
	if sent == 0 || sent == rows {
		t.Errorf("%d sent and %d cancelled, want some of each for the race to be tested", sent, rows-sent)
	}
}

func TestResultForARowThatIsNotProcessingIsLogged(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	_, client := newProvider(t, accept)
	var logs bytes.Buffer
	pool := Pool{Queries: queries, Provider: client, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase}
	pending := insert(t, queries, "sms", 1)

	pool.send(t.Context(), pending)

	if status := get(t, queries, pending.ID).Status; status != "pending" {
		t.Errorf("status = %q, want %q", status, "pending")
	}
	for _, want := range []string{`"level":"ERROR"`, `"msg":"result not written, notification is no longer processing"`, `"notification_id":"` + pending.ID.String() + `"`, `"status":"sent"`, `"provider_message_id":"message-1"`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs = %s, want them to contain %s", logs.String(), want)
		}
	}
}

func TestClaimGivesUpWhenTheDatabaseDoesNotAnswer(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	_, client := newProvider(t, accept)
	insert(t, queries, "sms", 1)
	var logs syncBuffer
	lock, err := lockTable(t, db)
	if err != nil {
		t.Fatal(err)
	}

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: 2, Interval: pollInterval, QueryTimeout: 50 * time.Millisecond, BackoffBase: backoffBase})
	waitFor(t, "the claim gives up", func() bool {
		return logs.lineWith(`"msg":"claim notifications"`, `"channel":"sms"`, "context deadline exceeded")
	})

	if err := lock.Rollback(t.Context()); err != nil {
		t.Fatalf("release table lock: %v", err)
	}
	later := insert(t, queries, "sms", 1)
	waitFor(t, "a notification created after the database answers again is sent", func() bool {
		return get(t, queries, later.ID).Status == "sent"
	})
}

func TestResultWriteGivesUpWhenTheDatabaseDoesNotAnswer(t *testing.T) {
	tests := []struct {
		name          string
		answer        func(w http.ResponseWriter) error
		wantStatus    string
		wantMessageID string
	}{
		{name: "sent", answer: accept, wantStatus: "sent", wantMessageID: "message-1"},
		{name: "failed", answer: status(http.StatusBadRequest), wantStatus: "failed"},
		{name: "retry", answer: status(http.StatusInternalServerError), wantStatus: "pending"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testdb.New(t)
			queries := store.New(db)
			locked := make(chan error, 1)
			fake, client := newProvider(t, func(w http.ResponseWriter) error {
				_, err := lockTable(t, db)
				locked <- err
				return tt.answer(w)
			})
			created := insert(t, queries, "sms", 1)
			var logs syncBuffer

			runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: 2, Interval: pollInterval, QueryTimeout: 50 * time.Millisecond, BackoffBase: backoffBase})
			select {
			case err := <-locked:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(20 * time.Second):
				t.Fatal("timed out waiting for the provider request")
			}
			waitFor(t, "the result write gives up", func() bool {
				return logs.lineWith(`"msg":"write result"`, `"notification_id":"`+created.ID.String()+`"`, `"status":"`+tt.wantStatus+`"`, `"provider_message_id":"`+tt.wantMessageID+`"`, "context deadline exceeded")
			})

			if keys := fake.keys(); len(keys) != 1 {
				t.Errorf("provider got %d requests, want 1", len(keys))
			}
		})
	}
}
