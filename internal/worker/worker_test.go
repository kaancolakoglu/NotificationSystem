package worker

import (
	"bytes"
	"encoding/json"
	"errors"
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

const pollInterval = 5 * time.Millisecond

type request struct {
	key  string
	body string
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
	f.requests = append(f.requests, request{key: r.Header.Get("Idempotency-Key"), body: string(body)})
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

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 2, Interval: pollInterval})
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

func TestFailedSendIsMarkedFailed(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, func(w http.ResponseWriter) error {
		w.WriteHeader(http.StatusInternalServerError)
		return nil
	})
	created := insert(t, queries, "sms", 1)

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 2, Interval: pollInterval})
	waitFor(t, "the notification is failed", func() bool { return get(t, queries, created.ID).Status == "failed" })
	time.Sleep(10 * pollInterval)

	if keys := fake.keys(); len(keys) != 1 {
		t.Errorf("provider got %d requests, want 1", len(keys))
	}
	stored := get(t, queries, created.ID)
	if stored.SentAt.Valid || stored.ProviderMessageID.Valid {
		t.Errorf("sent at = %+v, provider message id = %+v, want neither", stored.SentAt, stored.ProviderMessageID)
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
		runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "email", Size: 5, Interval: pollInterval})
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

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "push", Size: 1, Interval: pollInterval})
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

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: size, Interval: pollInterval})
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

	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 5, Interval: pollInterval})
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
	runPool(t, Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 5, Interval: time.Millisecond})

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
	pool := Pool{Queries: queries, Provider: client, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: 1, Interval: pollInterval}
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
