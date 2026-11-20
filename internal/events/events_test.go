package events

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"

	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

const waitTimeout = 20 * time.Second

type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) linesWith(parts ...string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	count := 0
lines:
	for line := range strings.Lines(b.buffer.String()) {
		for _, part := range parts {
			if !strings.Contains(line, part) {
				continue lines
			}
		}
		count++
	}
	return count
}

func waitFor(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func receive(t *testing.T, what string, stream <-chan string) (payload string, open bool) {
	t.Helper()
	select {
	case payload, open := <-stream:
		return payload, open
	case <-time.After(waitTimeout):
		t.Fatalf("timed out waiting for %s", what)
		return "", false
	}
}

func runHub(t *testing.T, client *redis.Client) (hub *Hub, logs *syncBuffer, stop context.CancelFunc, stopped <-chan struct{}) {
	t.Helper()
	logs = &syncBuffer{}
	hub = NewHub(client, slog.New(slog.NewJSONHandler(logs, nil)))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		hub.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return hub, logs, cancel, done
}

func subscribe(t *testing.T, hub *Hub, id uuid.UUID) <-chan string {
	t.Helper()
	var stream <-chan string
	waitFor(t, "the hub accepts a stream", func() bool {
		added, unsubscribe, ok := hub.Subscribe(id)
		if ok {
			stream = added
			t.Cleanup(unsubscribe)
		}
		return ok
	})
	return stream
}

func silentServer(t *testing.T) string {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var conns []net.Conn
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conns = append(conns, conn)
		}
	}()
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
		<-accepted
		for _, conn := range conns {
			if err := conn.Close(); err != nil {
				t.Errorf("close connection: %v", err)
			}
		}
	})
	return listener.Addr().String()
}

func newID() uuid.UUID {
	return uuid.Must(uuid.NewV7())
}

func TestNew(t *testing.T) {
	id, batch := newID(), newID()

	single := New(id, pgtype.UUID{}, "sent", 2)
	inBatch := New(id, pgtype.UUID{Bytes: batch, Valid: true}, "pending", 0)

	if single.NotificationID != id || single.BatchID != nil || single.Status != "sent" || single.Attempts != 2 {
		t.Errorf("event without a batch = %+v, want the id, no batch id, sent and 2 attempts", single)
	}
	if inBatch.BatchID == nil || *inBatch.BatchID != batch {
		t.Errorf("batch id of the event = %v, want %s", inBatch.BatchID, batch)
	}
}

func TestPublishedEventReachesTheStreamsOfItsNotificationAndItsBatch(t *testing.T) {
	client := testredis.New(t)
	hub, _, _, _ := runHub(t, client)
	publisher := Publisher{Client: client, Logger: slog.New(slog.DiscardHandler)}
	id, batch, other := newID(), newID(), newID()
	byNotification, second := subscribe(t, hub, id), subscribe(t, hub, id)
	byBatch := subscribe(t, hub, batch)
	byOther := subscribe(t, hub, other)

	publisher.Publish(t.Context(),
		New(id, pgtype.UUID{Bytes: batch, Valid: true}, "sent", 1),
		New(other, pgtype.UUID{}, "failed", 5),
	)

	inBatch := `{"notification_id":"` + id.String() + `","batch_id":"` + batch.String() + `","status":"sent","attempts":1}`
	for name, stream := range map[string]<-chan string{"the notification": byNotification, "the notification, second stream": second, "the batch": byBatch} {
		if got, _ := receive(t, "the event on the stream of "+name, stream); got != inBatch {
			t.Errorf("event on the stream of %s = %s, want %s", name, got, inBatch)
		}
	}
	single := `{"notification_id":"` + other.String() + `","status":"failed","attempts":5}`
	if got, _ := receive(t, "the event of the other notification", byOther); got != single {
		t.Errorf("event on the stream of the other notification = %s, want %s", got, single)
	}
	for name, stream := range map[string]<-chan string{"the notification": byNotification, "the batch": byBatch, "the other notification": byOther} {
		select {
		case extra, open := <-stream:
			t.Errorf("stream of %s got a second event %q (open %t), want only its own", name, extra, open)
		default:
		}
	}
}

func TestPublishWithoutEventsDoesNothing(t *testing.T) {
	var logs syncBuffer

	Publisher{Logger: slog.New(slog.NewJSONHandler(&logs, nil))}.Publish(t.Context())

	if lines := logs.linesWith(""); lines != 0 {
		t.Errorf("logs have %d lines, want none", lines)
	}
}

func TestFailedPublishIsLoggedAndGivesUp(t *testing.T) {
	const giveUpWithin = 4 * time.Second
	tests := []struct {
		name    string
		options *redis.Options
	}{
		{name: "redis is down", options: &redis.Options{Addr: "127.0.0.1:1"}},
		{name: "redis never answers", options: &redis.Options{Addr: silentServer(t), ContextTimeoutEnabled: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := redis.NewClient(tt.options)
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Errorf("close redis client: %v", err)
				}
			})
			var logs syncBuffer
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			started := time.Now()

			first, second := newID(), newID()
			Publisher{Client: client, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}.Publish(ctx, New(first, pgtype.UUID{}, "sent", 1), New(second, pgtype.UUID{}, "sent", 1))

			if took := time.Since(started); took > giveUpWithin {
				t.Errorf("publish took %s, want it to give up within %s", took, giveUpWithin)
			}
			if lines := logs.linesWith(`"level":"ERROR"`, `"msg":"publish status events"`, `"count":2`, `"notification_ids":["`+first.String()+`","`+second.String()+`"]`, `"error":"`); lines != 1 {
				t.Errorf("logs have %d error lines that name the 2 notifications, want 1", lines)
			}
			if lines := logs.linesWith("context canceled"); lines != 0 {
				t.Error("the publish failed because the caller was cancelled, want it to go on")
			}
		})
	}
}

func TestHubRefusesAStreamUntilItIsSubscribed(t *testing.T) {
	down := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	t.Cleanup(func() {
		if err := down.Close(); err != nil {
			t.Errorf("close redis client: %v", err)
		}
	})
	notRunning := NewHub(testredis.New(t), slog.New(slog.DiscardHandler))
	withoutRedis, logs, _, _ := runHub(t, down)
	waitFor(t, "the hub logs that it cannot receive", func() bool {
		return logs.linesWith(`"level":"ERROR"`, `"msg":"receive status events, closing the streams"`) > 0
	})

	for name, hub := range map[string]*Hub{"a hub that is not running": notRunning, "a hub without Redis": withoutRedis} {
		if _, _, ok := hub.Subscribe(newID()); ok {
			t.Errorf("%s accepted a stream, want it refused: it would never get an event", name)
		}
	}
}

func TestHubClosesTheStreamsWhenItLosesRedisAndAcceptsNewOnes(t *testing.T) {
	client := testredis.New(t)
	hub, logs, _, _ := runHub(t, client)
	publisher := Publisher{Client: client, Logger: slog.New(slog.DiscardHandler)}
	id := newID()
	first, second := subscribe(t, hub, id), subscribe(t, hub, newID())

	if err := client.ClientKillByFilter(t.Context(), "TYPE", "pubsub").Err(); err != nil {
		t.Fatalf("kill the subscription: %v", err)
	}

	for name, stream := range map[string]<-chan string{"first": first, "second": second} {
		if payload, open := receive(t, "the "+name+" stream to close", stream); open {
			t.Errorf("the %s stream got %q, want it closed: events may have been missed", name, payload)
		}
	}
	if lines := logs.linesWith(`"level":"ERROR"`, `"msg":"receive status events, closing the streams"`); lines != 1 {
		t.Errorf("logs have %d error lines for the lost subscription, want 1", lines)
	}
	again := subscribe(t, hub, id)
	publisher.Publish(t.Context(), New(id, pgtype.UUID{}, "sent", 1))
	if payload, open := receive(t, "an event after the hub subscribed again", again); !open || !strings.Contains(payload, `"status":"sent"`) {
		t.Errorf("stream opened after the hub subscribed again got %q (open %t), want the sent event", payload, open)
	}
	if lines := logs.linesWith(`"level":"INFO"`, `"msg":"subscribed to status events"`); lines != 2 {
		t.Errorf("logs have %d lines for a subscription, want 2: at the start and after the loss", lines)
	}
}

func TestStreamThatIsNotReadIsClosedAndTheOthersGoOn(t *testing.T) {
	client := testredis.New(t)
	hub, logs, _, _ := runHub(t, client)
	publisher := Publisher{Client: client, Logger: slog.New(slog.DiscardHandler)}
	id := newID()
	notRead, read := subscribe(t, hub, id), subscribe(t, hub, id)
	changes := make([]Event, streamBuffer+2)
	for i := range changes {
		changes[i] = New(id, pgtype.UUID{}, "pending", int16(i))
	}
	var got atomic.Int32
	go func() {
		for range read {
			got.Add(1)
		}
	}()

	publisher.Publish(t.Context(), changes[:streamBuffer]...)
	waitFor(t, "the stream that is read gets the events that fill the other one", func() bool { return got.Load() == streamBuffer })
	publisher.Publish(t.Context(), changes[streamBuffer:]...)
	waitFor(t, "the stream that is read gets every event", func() bool { return int(got.Load()) == len(changes) })

	buffered := 0
	for range notRead {
		buffered++
	}
	if buffered != streamBuffer {
		t.Errorf("the stream that is not read held %d events before it was closed, want %d", buffered, streamBuffer)
	}
	if lines := logs.linesWith(`"level":"WARN"`, `"msg":"status stream is full, closing it"`, `"id":"`+id.String()+`"`); lines != 1 {
		t.Errorf("logs have %d lines for the full stream, want 1", lines)
	}
}

func TestUnsubscribeForgetsTheStream(t *testing.T) {
	client := testredis.New(t)
	hub, _, _, _ := runHub(t, client)
	id := newID()
	subscribe(t, hub, newID())
	_, unsubscribeFirst, ok := hub.Subscribe(id)
	if !ok {
		t.Fatal("the hub refused a stream")
	}
	_, unsubscribe, ok := hub.Subscribe(id)
	if !ok {
		t.Fatal("the hub refused a second stream")
	}
	streams := func() (ids, ofID int) {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		return len(hub.streams), len(hub.streams[id])
	}
	if ids, ofID := streams(); ids != 2 || ofID != 2 {
		t.Fatalf("the hub holds %d ids and %d streams of the id, want 2 and 2", ids, ofID)
	}

	unsubscribe()
	unsubscribe()

	if ids, ofID := streams(); ids != 2 || ofID != 1 {
		t.Errorf("the hub holds %d ids and %d streams of the id after one unsubscribed, want 2 and 1", ids, ofID)
	}
	unsubscribeFirst()
	if ids, ofID := streams(); ids != 1 || ofID != 0 {
		t.Errorf("the hub holds %d ids and %d streams of the id after both unsubscribed, want 1 and 0", ids, ofID)
	}
}

func TestEventThatCannotBeDecodedIsLoggedAndSkipped(t *testing.T) {
	client := testredis.New(t)
	hub, logs, _, _ := runHub(t, client)
	publisher := Publisher{Client: client, Logger: slog.New(slog.DiscardHandler)}
	id := newID()
	stream := subscribe(t, hub, id)

	for _, payload := range []string{"not json", `{"status":"sent"}`} {
		if err := client.Publish(t.Context(), Channel, payload).Err(); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	publisher.Publish(t.Context(), New(id, pgtype.UUID{}, "sent", 1))

	if payload, open := receive(t, "the event after the broken one", stream); !open || !strings.Contains(payload, `"status":"sent"`) {
		t.Errorf("stream got %q (open %t), want the sent event", payload, open)
	}
	if lines := logs.linesWith(`"level":"ERROR"`, `"msg":"decode status event"`, `"payload":"not json"`); lines != 1 {
		t.Errorf("logs have %d lines for the event that cannot be decoded, want 1", lines)
	}
	if lines := logs.linesWith(`"level":"ERROR"`, `"msg":"status event has no notification id"`, `"payload":"{\"status\":\"sent\"}"`); lines != 1 {
		t.Errorf("logs have %d lines for the event without a notification id, want 1", lines)
	}
}

func TestHubSubscribesAgainAfterRedisRefusedTheSubscription(t *testing.T) {
	admin := testredis.New(t)
	user := "hub-" + newID().String()
	if err := admin.Do(t.Context(), "ACL", "SETUSER", user, "on", ">secret", "+@all", "~*", "resetchannels").Err(); err != nil {
		t.Fatalf("create a Redis user without channels: %v", err)
	}
	t.Cleanup(func() {
		if err := admin.Do(context.WithoutCancel(t.Context()), "ACL", "DELUSER", user).Err(); err != nil {
			t.Errorf("delete the Redis user: %v", err)
		}
	})
	options := *admin.Options()
	options.Username, options.Password = user, "secret"
	refused := redis.NewClient(&options)
	t.Cleanup(func() {
		if err := refused.Close(); err != nil {
			t.Errorf("close redis client: %v", err)
		}
	})
	hub, logs, _, _ := runHub(t, refused)
	waitFor(t, "the hub logs that the subscription was refused", func() bool {
		return logs.linesWith(`"level":"ERROR"`, `"msg":"receive status events, closing the streams"`, "NOPERM") > 0
	})
	if _, _, ok := hub.Subscribe(newID()); ok {
		t.Fatal("the hub accepted a stream although Redis refused the subscription")
	}
	time.Sleep(retryInterval / 2)
	if lines := logs.linesWith(`"msg":"receive status events, closing the streams"`); lines > 2 {
		t.Fatalf("logs have %d lines for a refused subscription half a retry interval after the first, want at most 2: the hub waits before it tries again", lines)
	}

	if err := admin.Do(t.Context(), "ACL", "SETUSER", user, "allchannels").Err(); err != nil {
		t.Fatalf("allow the channels: %v", err)
	}

	id := newID()
	stream := subscribe(t, hub, id)
	Publisher{Client: admin, Logger: slog.New(slog.DiscardHandler)}.Publish(t.Context(), New(id, pgtype.UUID{}, "sent", 1))
	if payload, open := receive(t, "an event once Redis allows the subscription", stream); !open || !strings.Contains(payload, `"status":"sent"`) {
		t.Errorf("stream got %q (open %t), want the sent event", payload, open)
	}
}

func TestHubStopsWithItsContextAndClosesTheStreams(t *testing.T) {
	client := testredis.New(t)
	hub, logs, stop, stopped := runHub(t, client)
	stream := subscribe(t, hub, newID())

	stop()

	select {
	case <-stopped:
	case <-time.After(waitTimeout):
		t.Fatal("timed out waiting for the hub to stop")
	}
	if payload, open := receive(t, "the stream to close", stream); open {
		t.Errorf("stream got %q, want it closed", payload)
	}
	if _, _, ok := hub.Subscribe(newID()); ok {
		t.Error("the stopped hub accepted a stream, want it refused")
	}
	if lines := logs.linesWith(`"level":"ERROR"`); lines != 0 {
		t.Errorf("logs have %d error lines, want none: a stop is not a lost subscription", lines)
	}
}
