package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/kaancolakoglu/NotificationSystem/internal/events"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

func unreadEvents(t *testing.T) events.Publisher {
	t.Helper()
	return events.Publisher{Client: testredis.New(t), Logger: slog.New(slog.DiscardHandler)}
}

type eventWatcher struct {
	publisher events.Publisher
	mu        sync.Mutex
	seen      []events.Event
}

func watchEvents(t *testing.T) *eventWatcher {
	t.Helper()
	client := testredis.New(t)
	pubsub := client.Subscribe(t.Context(), events.Channel)
	if _, err := pubsub.Receive(t.Context()); err != nil {
		t.Fatalf("subscribe to the status events: %v", err)
	}
	watcher := &eventWatcher{publisher: events.Publisher{Client: client, Logger: slog.New(slog.DiscardHandler)}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for message := range pubsub.Channel() {
			var event events.Event
			if err := json.Unmarshal([]byte(message.Payload), &event); err != nil {
				t.Errorf("decode status event %q: %v", message.Payload, err)
				continue
			}
			watcher.mu.Lock()
			watcher.seen = append(watcher.seen, event)
			watcher.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		if err := pubsub.Close(); err != nil {
			t.Errorf("close the subscription: %v", err)
		}
		<-done
	})
	return watcher
}

func (w *eventWatcher) of(t *testing.T, id uuid.UUID) []string {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	var encoded []string
	for _, event := range w.seen {
		if event.NotificationID == id {
			encoded = append(encoded, encode(t, event))
		}
	}
	return encoded
}

func (w *eventWatcher) wantEvents(t *testing.T, id uuid.UUID, want ...events.Event) {
	t.Helper()
	wanted := make([]string, len(want))
	for i, event := range want {
		wanted[i] = encode(t, event)
	}
	deadline := time.Now().Add(waitTimeout)
	for !slices.Equal(w.of(t, id), wanted) {
		if time.Now().After(deadline) {
			t.Errorf("status events of the notification = %v, want %v", w.of(t, id), wanted)
			return
		}
		time.Sleep(pollInterval)
	}
}

func (w *eventWatcher) waitForEverythingPublishedSoFar(t *testing.T) {
	t.Helper()
	marker := uuid.Must(uuid.NewV7())
	w.publisher.Publish(t.Context(), events.Event{NotificationID: marker, Status: "marker"})
	waitFor(t, "the marker event arrives", func() bool { return len(w.of(t, marker)) == 1 })
}

func encode(t *testing.T, event events.Event) string {
	t.Helper()
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("encode status event: %v", err)
	}
	return string(encoded)
}

func setBatchID(t *testing.T, db *pgxpool.Pool, id uuid.UUID) pgtype.UUID {
	t.Helper()
	batchID := pgtype.UUID{Bytes: uuid.Must(uuid.NewV7()), Valid: true}
	if _, err := db.Exec(t.Context(), "UPDATE notifications SET batch_id = $1 WHERE id = $2", batchID, id); err != nil {
		t.Fatalf("set the batch id: %v", err)
	}
	return batchID
}

func TestEveryStatusChangeOfASendIsPublished(t *testing.T) {
	tests := []struct {
		name   string
		answer func(w http.ResponseWriter) error
		want   func(id uuid.UUID, batchID pgtype.UUID) []events.Event
	}{
		{name: "sent", answer: accept, want: func(id uuid.UUID, batchID pgtype.UUID) []events.Event {
			return []events.Event{events.New(id, batchID, "processing", 1), events.New(id, batchID, "sent", 1)}
		}},
		{name: "retried, then sent", answer: inOrder(status(http.StatusInternalServerError), accept), want: func(id uuid.UUID, batchID pgtype.UUID) []events.Event {
			return []events.Event{events.New(id, batchID, "processing", 1), events.New(id, batchID, "pending", 1), events.New(id, batchID, "processing", 2), events.New(id, batchID, "sent", 2)}
		}},
		{name: "failed", answer: status(http.StatusBadRequest), want: func(id uuid.UUID, batchID pgtype.UUID) []events.Event {
			return []events.Event{events.New(id, batchID, "processing", 1), events.New(id, batchID, "failed", 1)}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testdb.New(t)
			queries := store.New(db)
			_, client := newProvider(t, tt.answer)
			watcher := watchEvents(t)
			created := insert(t, queries, "sms", 1)
			batchID := setBatchID(t, db, created.ID)

			runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.DiscardHandler), Events: watcher.publisher, Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute, ShutdownTimeout: time.Minute})

			watcher.wantEvents(t, created.ID, tt.want(created.ID, batchID)...)
		})
	}
}

type heldPipelines struct {
	started chan struct{}
	release chan struct{}
}

func (h heldPipelines) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h heldPipelines) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h heldPipelines) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		select {
		case h.started <- struct{}{}:
		default:
		}
		<-h.release
		return next(ctx, cmds)
	}
}

func TestClaimIsPublishedBeforeTheSendStarts(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, accept)
	watcher := watchEvents(t)
	held := heldPipelines{started: make(chan struct{}, 1), release: make(chan struct{})}
	releasePublish := sync.OnceFunc(func() { close(held.release) })
	publishing := testredis.New(t)
	publishing.AddHook(held)
	created := insert(t, queries, "sms", 1)

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.DiscardHandler), Events: events.Publisher{Client: publishing, Logger: slog.New(slog.DiscardHandler)}, Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute, ShutdownTimeout: time.Minute})
	t.Cleanup(releasePublish)
	waitToReceive(t, "the pool publishes the claim", held.started)
	time.Sleep(20 * pollInterval)

	if calls := len(fake.keys()); calls != 0 {
		t.Errorf("provider got %d requests while the claim was being published, want 0: processing comes before the result", calls)
	}
	releasePublish()
	watcher.wantEvents(t, created.ID, events.New(created.ID, pgtype.UUID{}, "processing", 1), events.New(created.ID, pgtype.UUID{}, "sent", 1))
}

func TestReleasedNotificationIsPublishedAsPendingWithItsAttemptRefunded(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	watcher := watchEvents(t)
	created := insert(t, queries, "sms", 1)
	batchID := setBatchID(t, db, created.ID)
	pool := Pool{Queries: queries, Logger: slog.New(slog.DiscardHandler), Events: watcher.publisher, Channel: "sms", QueryTimeout: queryTimeout}
	claimed, err := queries.ClaimNotifications(t.Context(), store.ClaimNotificationsParams{Channel: "sms", Priority: 1, RowLimit: 1})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %+v, error %v, want the notification", claimed, err)
	}

	pool.release(t.Context(), claimed[0], slog.LevelError, "releasing the notification", errors.New("redis is down"))

	watcher.wantEvents(t, created.ID, events.New(created.ID, batchID, "pending", 0))
}

func TestSweptNotificationsArePublished(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	watcher := watchEvents(t)
	lost := insert(t, queries, "sms", 1)
	batchID := setBatchID(t, db, lost.ID)
	hold(t, db, lost.ID, 1, "-1 second")
	usedUp := insert(t, queries, "email", 1)
	hold(t, db, usedUp.ID, maxAttempts, "-1 second")
	held := insert(t, queries, "push", 1)
	hold(t, db, held.ID, 1, "1 minute")

	Sweeper{Queries: queries, Logger: slog.New(slog.DiscardHandler), Metrics: unreadMetrics(), Events: watcher.publisher, QueryTimeout: queryTimeout}.sweep(t.Context())

	watcher.wantEvents(t, lost.ID, events.New(lost.ID, batchID, "pending", 1))
	watcher.wantEvents(t, usedUp.ID, events.New(usedUp.ID, pgtype.UUID{}, "failed", maxAttempts))
	watcher.waitForEverythingPublishedSoFar(t)
	if got := watcher.of(t, held.ID); len(got) != 0 {
		t.Errorf("status events of the notification whose lease still holds = %v, want none", got)
	}
}

func TestResultThatIsNotStoredIsNotPublished(t *testing.T) {
	tests := []struct {
		name    string
		answer  func(w http.ResponseWriter) error
		release bool
	}{
		{name: "sent", answer: accept},
		{name: "failed", answer: status(http.StatusBadRequest)},
		{name: "retry", answer: status(http.StatusInternalServerError)},
		{name: "release", answer: accept, release: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testdb.New(t)
			queries := store.New(db)
			_, client := newProvider(t, tt.answer)
			watcher := watchEvents(t)
			pool := Pool{Queries: queries, Provider: client, Logger: slog.New(slog.DiscardHandler), Metrics: unreadMetrics(), Events: watcher.publisher, Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase}
			created := insert(t, queries, "sms", 1)
			claim := func() store.Notification {
				t.Helper()
				claimed, err := queries.ClaimNotifications(t.Context(), store.ClaimNotificationsParams{Channel: "sms", Priority: 1, RowLimit: 1})
				if err != nil || len(claimed) != 1 || claimed[0].ID != created.ID {
					t.Fatalf("claim = %+v, error %v, want the notification", claimed, err)
				}
				return claimed[0]
			}
			stale := claim()
			hold(t, db, created.ID, stale.Attempts, "-1 second")
			Sweeper{Queries: queries, Logger: slog.New(slog.DiscardHandler), Metrics: unreadMetrics(), Events: watcher.publisher, QueryTimeout: queryTimeout}.sweep(t.Context())
			claim()

			if tt.release {
				pool.release(t.Context(), stale, slog.LevelError, "releasing the notification", errors.New("redis is down"))
			} else {
				pool.send(t.Context(), pool.newBreaker(t.Context()), stale)
			}

			watcher.waitForEverythingPublishedSoFar(t)
			if got, want := watcher.of(t, created.ID), []string{encode(t, events.New(created.ID, pgtype.UUID{}, "pending", 1))}; !slices.Equal(got, want) {
				t.Errorf("status events of the notification = %v, want only the sweep's %v: a result that is not stored is not an event", got, want)
			}
		})
	}
}

func TestFailedPublishDoesNotStopTheDelivery(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, accept)
	created := insert(t, queries, "sms", 1)
	down := redis.NewClient(&redis.Options{Dialer: func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("redis is down")
	}})
	t.Cleanup(func() {
		if err := down.Close(); err != nil {
			t.Errorf("close redis client: %v", err)
		}
	})
	var logs syncBuffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: logger, Events: events.Publisher{Client: down, Logger: logger}, Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute, ShutdownTimeout: time.Minute})
	waitFor(t, "the notification is sent", func() bool { return get(t, queries, created.ID).Status == "sent" })
	waitFor(t, "both failed publishes are logged", func() bool {
		return logs.linesWith(`"level":"ERROR"`, `"msg":"publish status events"`, `"count":1`, `"notification_ids":["`+created.ID.String()+`"]`, "redis is down") == 2
	})

	if calls := len(fake.keys()); calls != 1 {
		t.Errorf("provider got %d requests, want 1", calls)
	}
}
