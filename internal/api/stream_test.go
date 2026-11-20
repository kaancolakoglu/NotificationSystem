package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/kaancolakoglu/NotificationSystem/internal/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/events"
	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

const (
	streamWait         = 20 * time.Second
	streamMissedReason = "status events may have been missed: reconnect and fetch the status"
)

type sharedLogs struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (l *sharedLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buffer.Write(p)
}

func (l *sharedLogs) linesWith(parts ...string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	count := 0
lines:
	for line := range strings.Lines(l.buffer.String()) {
		for _, part := range parts {
			if !strings.Contains(line, part) {
				continue lines
			}
		}
		count++
	}
	return count
}

type streamServer struct {
	url       string
	pool      *pgxpool.Pool
	redis     *redis.Client
	publisher events.Publisher
	logs      *sharedLogs
}

func newStreamServer(t *testing.T) streamServer {
	t.Helper()
	pool := testdb.New(t)
	client := testredis.New(t)
	logs := &sharedLogs{}
	logger := obs.NewLogger(logs)
	hub := events.NewHub(client, logger)
	ctx, stop := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		hub.Run(ctx)
	}()
	t.Cleanup(func() {
		stop()
		<-stopped
	})
	waitUntil(t, "the hub is subscribed", func() bool {
		_, unsubscribe, ok := hub.Subscribe(uuid.Nil)
		if ok {
			unsubscribe()
		}
		return ok
	})
	publisher := events.Publisher{Client: client, Logger: logger}
	mux := http.NewServeMux()
	api.Register(mux, pool, hub, publisher, logger)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return streamServer{url: server.URL, pool: pool, redis: client, publisher: publisher, logs: logs}
}

func waitUntil(t *testing.T, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(streamWait)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func openStream(t *testing.T, url string, header http.Header) (conn *websocket.Conn, correlationID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), streamWait)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatalf("open the stream %s: %v", url, err)
	}
	t.Cleanup(func() {
		if err := conn.CloseNow(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close the stream: %v", err)
		}
	})
	return conn, response.Header.Get(correlationIDHeader)
}

func readEvent(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	payload, err := readMessage(t, conn)
	if err != nil {
		t.Fatalf("read a status event: %v", err)
	}
	return payload
}

func readMessage(t *testing.T, conn *websocket.Conn) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), streamWait)
	defer cancel()
	kind, payload, err := conn.Read(ctx)
	if err == nil && kind != websocket.MessageText {
		t.Errorf("message %q is of type %s, want a text message", payload, kind)
	}
	return string(payload), err
}

func getStream(t *testing.T, url string, header http.Header) (status int, body string, correlationID string) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}
	request.Header = header
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close the response body: %v", err)
		}
	}()
	read, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read the response body: %v", err)
	}
	return response.StatusCode, string(read), response.Header.Get(correlationIDHeader)
}

func handshake() http.Header {
	return http.Header{
		"Connection":            {"Upgrade"},
		"Upgrade":               {"websocket"},
		"Sec-WebSocket-Version": {"13"},
		"Sec-WebSocket-Key":     {"dGhlIHNhbXBsZSBub25jZQ=="},
	}
}

func wantErrorBody(t *testing.T, body, wantMessage string) {
	t.Helper()
	var got api.Error
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("body = %q, want an Error object: %v", body, err)
	}
	if got.Message != wantMessage {
		t.Errorf("message = %q, want %q", got.Message, wantMessage)
	}
}

func newRow(t *testing.T, pool *pgxpool.Pool, status string, batchID pgtype.UUID) uuid.UUID {
	t.Helper()
	id := uuid.Must(uuid.NewV7())
	insertNotification(t, pool, id, "sms", status, batchID)
	return id
}

func TestNotificationEventsAreStreamed(t *testing.T) {
	server := newStreamServer(t)
	watched, other := newRow(t, server.pool, "pending", pgtype.UUID{}), newRow(t, server.pool, "pending", pgtype.UUID{})

	conn, correlationID := openStream(t, server.url+"/v1/notifications/"+watched.String()+"/events", http.Header{correlationIDHeader: {"order-42"}})
	server.publisher.Publish(t.Context(),
		events.New(other, pgtype.UUID{}, "processing", 1),
		events.New(watched, pgtype.UUID{}, "processing", 1),
		events.New(other, pgtype.UUID{}, "sent", 1),
		events.New(watched, pgtype.UUID{}, "sent", 1),
	)

	if correlationID != "order-42" {
		t.Errorf("%s of the upgrade response = %q, want the id that was sent", correlationIDHeader, correlationID)
	}
	for _, status := range []string{"processing", "sent"} {
		want := `{"notification_id":"` + watched.String() + `","status":"` + status + `","attempts":1}`
		if got := readEvent(t, conn); got != want {
			t.Errorf("event = %s, want %s", got, want)
		}
	}
}

func TestBatchEventsAreStreamed(t *testing.T) {
	server := newStreamServer(t)
	batch := pgtype.UUID{Bytes: uuid.Must(uuid.NewV7()), Valid: true}
	first, second := newRow(t, server.pool, "pending", batch), newRow(t, server.pool, "pending", batch)
	outside := newRow(t, server.pool, "pending", pgtype.UUID{})
	batchID := uuid.UUID(batch.Bytes).String()

	conn, _ := openStream(t, server.url+"/v1/batches/"+batchID+"/events", nil)
	server.publisher.Publish(t.Context(),
		events.New(first, batch, "sent", 1),
		events.New(outside, pgtype.UUID{}, "sent", 1),
		events.New(second, batch, "failed", 5),
	)

	for _, want := range []string{
		`{"notification_id":"` + first.String() + `","batch_id":"` + batchID + `","status":"sent","attempts":1}`,
		`{"notification_id":"` + second.String() + `","batch_id":"` + batchID + `","status":"failed","attempts":5}`,
	} {
		if got := readEvent(t, conn); got != want {
			t.Errorf("event = %s, want %s", got, want)
		}
	}
}

func TestCancelIsPublishedAsAnEvent(t *testing.T) {
	server := newStreamServer(t)
	batch := pgtype.UUID{Bytes: uuid.Must(uuid.NewV7()), Valid: true}
	pending, sent := newRow(t, server.pool, "pending", batch), newRow(t, server.pool, "sent", batch)
	batchID := uuid.UUID(batch.Bytes).String()
	byNotification, _ := openStream(t, server.url+"/v1/notifications/"+pending.String()+"/events", nil)
	byBatch, _ := openStream(t, server.url+"/v1/batches/"+batchID+"/events", nil)
	cancel := func(t *testing.T, id uuid.UUID) int {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.url+"/v1/notifications/"+id.String()+"/cancel", nil)
		if err != nil {
			t.Fatalf("build the request: %v", err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("cancel: %v", err)
		}
		if err := response.Body.Close(); err != nil {
			t.Errorf("close the response body: %v", err)
		}
		return response.StatusCode
	}

	if status := cancel(t, sent); status != http.StatusConflict {
		t.Fatalf("cancel of the sent notification answered %d, want %d", status, http.StatusConflict)
	}
	if status := cancel(t, pending); status != http.StatusOK {
		t.Fatalf("cancel of the pending notification answered %d, want %d", status, http.StatusOK)
	}

	want := `{"notification_id":"` + pending.String() + `","batch_id":"` + batchID + `","status":"cancelled","attempts":0}`
	for name, conn := range map[string]*websocket.Conn{"notification": byNotification, "batch": byBatch} {
		if got := readEvent(t, conn); got != want {
			t.Errorf("first event on the %s stream = %s, want %s: a cancel that is refused is not an event", name, got, want)
		}
	}
}

func TestStreamRequestIsRefused(t *testing.T) {
	server := newStreamServer(t)
	batch := pgtype.UUID{Bytes: uuid.Must(uuid.NewV7()), Valid: true}
	inBatch := newRow(t, server.pool, "pending", batch).String()
	batchID := uuid.UUID(batch.Bytes).String()
	unknown := uuid.Must(uuid.NewV7()).String()
	otherOrigin := handshake()
	otherOrigin.Set("Origin", "https://example.org")
	tests := []struct {
		name        string
		path        string
		header      http.Header
		wantStatus  int
		wantMessage string
		wantText    string
	}{
		{name: "unknown notification", path: "/v1/notifications/" + unknown + "/events", header: handshake(), wantStatus: http.StatusNotFound, wantMessage: "notification not found"},
		{name: "unknown batch", path: "/v1/batches/" + unknown + "/events", header: handshake(), wantStatus: http.StatusNotFound, wantMessage: "batch not found"},
		{name: "batch id on the notification route", path: "/v1/notifications/" + batchID + "/events", header: handshake(), wantStatus: http.StatusNotFound, wantMessage: "notification not found"},
		{name: "notification id on the batch route", path: "/v1/batches/" + inBatch + "/events", header: handshake(), wantStatus: http.StatusNotFound, wantMessage: "batch not found"},
		{name: "notification id is not a UUID", path: "/v1/notifications/42/events", header: handshake(), wantStatus: http.StatusBadRequest, wantMessage: `parameter "id" is not valid`},
		{name: "batch id is not a UUID", path: "/v1/batches/42/events", header: handshake(), wantStatus: http.StatusBadRequest, wantMessage: `parameter "id" is not valid`},
		{name: "query parameter", path: "/v1/notifications/" + inBatch + "/events?since=1", header: handshake(), wantStatus: http.StatusBadRequest, wantMessage: `parameter "since" is not known`},
		{name: "query parameter on the batch route", path: "/v1/batches/" + batchID + "/events?since=1", header: handshake(), wantStatus: http.StatusBadRequest, wantMessage: `parameter "since" is not known`},
		{name: "not a WebSocket handshake", path: "/v1/notifications/" + inBatch + "/events", header: http.Header{}, wantStatus: http.StatusUpgradeRequired, wantText: "WebSocket protocol violation"},
		{name: "page of another origin", path: "/v1/batches/" + batchID + "/events", header: otherOrigin, wantStatus: http.StatusForbidden, wantText: `request Origin "example.org" is not authorized`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, body, correlationID := getStream(t, server.url+tt.path, tt.header)

			if status != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body %s", status, tt.wantStatus, body)
			}
			if tt.wantText != "" && !strings.Contains(body, tt.wantText) {
				t.Errorf("body = %q, want it to contain %q", body, tt.wantText)
			}
			if tt.wantText == "" {
				wantErrorBody(t, body, tt.wantMessage)
			}
			if correlationID == "" {
				t.Errorf("the response has no %s", correlationIDHeader)
			}
			if lines := server.logs.linesWith(`"msg":"request handled"`, `"correlation_id":"`+correlationID+`"`, `"status":`+strconv.Itoa(tt.wantStatus)); lines != 1 {
				t.Errorf("logs have %d request lines with status %d and the correlation id, want 1", lines, tt.wantStatus)
			}
		})
	}
}

func TestStreamIsRefusedWhileTheHubHasNoSubscription(t *testing.T) {
	pool := testdb.New(t)
	client := testredis.New(t)
	logger := obs.NewLogger(io.Discard)
	mux := http.NewServeMux()
	api.Register(mux, pool, events.NewHub(client, logger), events.Publisher{Client: client, Logger: logger}, logger)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	id := newRow(t, pool, "pending", pgtype.UUID{})

	status, body, _ := getStream(t, server.URL+"/v1/notifications/"+id.String()+"/events", handshake())

	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body %s", status, http.StatusServiceUnavailable, body)
	}
	wantErrorBody(t, body, "status events are not available, try again later")
}

func TestStreamRequestFailsWhenTheDatabaseDoes(t *testing.T) {
	for _, path := range []string{"/v1/notifications/", "/v1/batches/"} {
		t.Run(path, func(t *testing.T) {
			server := newStreamServer(t)
			server.pool.Close()

			status, body, _ := getStream(t, server.url+path+uuid.Must(uuid.NewV7()).String()+"/events", handshake())

			if status != http.StatusInternalServerError {
				t.Fatalf("status = %d, want %d, body %s", status, http.StatusInternalServerError, body)
			}
			wantErrorBody(t, body, "internal error")
			if lines := server.logs.linesWith(`"level":"ERROR"`, `"msg":"request failed"`, `"path":"`+path); lines != 1 {
				t.Errorf("logs have %d error lines for the request, want 1", lines)
			}
		})
	}
}

func TestStreamIsClosedWhenEventsMayHaveBeenMissed(t *testing.T) {
	server := newStreamServer(t)
	id := newRow(t, server.pool, "pending", pgtype.UUID{})
	url := server.url + "/v1/notifications/" + id.String() + "/events"
	conn, _ := openStream(t, url, nil)

	if err := server.redis.ClientKillByFilter(t.Context(), "TYPE", "pubsub").Err(); err != nil {
		t.Fatalf("kill the subscription of the hub: %v", err)
	}

	payload, err := readMessage(t, conn)
	var closed websocket.CloseError
	if !errors.As(err, &closed) || closed.Code != websocket.StatusTryAgainLater || closed.Reason != streamMissedReason {
		t.Fatalf("read = %q, error %v, want the stream closed with code %d and the reason %q", payload, err, websocket.StatusTryAgainLater, streamMissedReason)
	}
	waitUntil(t, "the hub is subscribed again", func() bool {
		status, _, _ := getStream(t, url, http.Header{})
		return status == http.StatusUpgradeRequired
	})
	again, _ := openStream(t, url, nil)
	server.publisher.Publish(t.Context(), events.New(id, pgtype.UUID{}, "sent", 1))
	if got, want := readEvent(t, again), `{"notification_id":"`+id.String()+`","status":"sent","attempts":1}`; got != want {
		t.Errorf("event on the stream opened after the loss = %s, want %s", got, want)
	}
	if lines := server.logs.linesWith(`"msg":"close status stream"`); lines != 0 {
		t.Errorf("logs have %d lines for a failed close of the stream, want none", lines)
	}
}

func TestStreamEndsWhenTheClientLeaves(t *testing.T) {
	tests := []struct {
		name  string
		leave func(conn *websocket.Conn) error
	}{
		{name: "closes the stream", leave: func(conn *websocket.Conn) error { return conn.Close(websocket.StatusNormalClosure, "") }},
		{name: "drops the connection", leave: func(conn *websocket.Conn) error { return conn.CloseNow() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newStreamServer(t)
			id := newRow(t, server.pool, "pending", pgtype.UUID{})
			path := "/v1/notifications/" + id.String() + "/events"
			conn, correlationID := openStream(t, server.url+path, nil)

			if err := tt.leave(conn); err != nil {
				t.Fatalf("leave: %v", err)
			}

			waitUntil(t, "the request line of the stream is logged", func() bool {
				return server.logs.linesWith(`"msg":"request handled"`, `"method":"GET"`, `"path":"`+path+`"`, `"status":101`, `"correlation_id":"`+correlationID+`"`) == 1
			})
			later, _ := openStream(t, server.url+path, nil)
			const moreThanAStreamHolds = 300
			for i := range moreThanAStreamHolds {
				server.publisher.Publish(t.Context(), events.New(id, pgtype.UUID{}, "pending", int16(i)))
				readEvent(t, later)
			}
			if lines := server.logs.linesWith(`"level":"WARN"`) + server.logs.linesWith(`"level":"ERROR"`); lines != 0 {
				t.Errorf("logs have %d warning and error lines, want none: a client that leaves is not a failure, and its stream is forgotten", lines)
			}
		})
	}
}

func TestClientThatStopsReadingLosesItsStream(t *testing.T) {
	server := newStreamServer(t)
	id := newRow(t, server.pool, "pending", pgtype.UUID{})
	path := "/v1/notifications/" + id.String() + "/events"
	var dialer net.Dialer
	smallBuffer := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if err := conn.(*net.TCPConn).SetReadBuffer(4096); err != nil {
			return nil, err
		}
		return conn, nil
	}}}
	conn, _, err := websocket.Dial(t.Context(), server.url+path, &websocket.DialOptions{HTTPClient: smallBuffer})
	if err != nil {
		t.Fatalf("open the stream: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.CloseNow(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close the stream: %v", err)
		}
	})
	fewerThanAStreamHolds := make([]events.Event, 100)
	for i := range fewerThanAStreamHolds {
		fewerThanAStreamHolds[i] = events.New(id, pgtype.UUID{}, "pending", 1)
	}

	waitUntil(t, "the stream of the client that reads nothing ends", func() bool {
		server.publisher.Publish(t.Context(), fewerThanAStreamHolds...)
		time.Sleep(10 * time.Millisecond)
		return server.logs.linesWith(`"msg":"request handled"`, `"path":"`+path+`"`, `"status":101`) == 1
	})

	if lines := server.logs.linesWith(`"level":"WARN"`, `"msg":"write status event, closing the stream"`, "context deadline exceeded"); lines != 1 {
		t.Errorf("logs have %d warning lines for a write that timed out, want 1", lines)
	}
}

func TestDataMessageFromTheClientClosesTheStream(t *testing.T) {
	server := newStreamServer(t)
	id := newRow(t, server.pool, "pending", pgtype.UUID{})
	conn, _ := openStream(t, server.url+"/v1/notifications/"+id.String()+"/events", nil)

	if err := conn.Write(t.Context(), websocket.MessageText, []byte("hello")); err != nil {
		t.Fatalf("write to the stream: %v", err)
	}

	payload, err := readMessage(t, conn)
	if status := websocket.CloseStatus(err); status != websocket.StatusPolicyViolation {
		t.Errorf("read = %q, error %v, want the stream closed with code %d", payload, err, websocket.StatusPolicyViolation)
	}
}
