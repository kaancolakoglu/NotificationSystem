package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

const stopWait = 20 * time.Second

func freeAddr(t *testing.T) string {
	t.Helper()
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return listener.Addr().String()
}

func waitUntilListening(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(stopWait)
	for {
		var dialer net.Dialer
		conn, err := dialer.DialContext(t.Context(), "tcp", addr)
		if err == nil {
			if err := conn.Close(); err != nil {
				t.Fatalf("close connection: %v", err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing listens on %s: %v", addr, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

var errStillRunning = errors.New("timed out waiting until the API has stopped")

func waitForStop(stopped <-chan error) error {
	select {
	case err := <-stopped:
		return err
	case <-time.After(stopWait):
		return errStillRunning
	}
}

type answer struct {
	status int
	body   string
	err    error
}

func send(ctx context.Context, method, url, body string) answer {
	request, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return answer{err: err}
	}
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	response, err := client.Do(request)
	if err != nil {
		return answer{err: err}
	}
	read, err := io.ReadAll(response.Body)
	if closeErr := response.Body.Close(); err == nil {
		err = closeErr
	}
	return answer{status: response.StatusCode, body: string(read), err: err}
}

func TestServeLetsARequestInFlightFinishAfterItIsToldToStop(t *testing.T) {
	arrived, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	server := &http.Server{ReadHeaderTimeout: readHeaderTimeout, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		<-release
		if err := r.Context().Err(); err != nil {
			t.Errorf("the request was cancelled before the deadline: %v", err)
		}
		if _, err := io.WriteString(w, "finished"); err != nil {
			t.Errorf("write the answer: %v", err)
		}
	})}
	var logs bytes.Buffer
	addr := freeAddr(t)
	ctx, stop := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- serve(ctx, slog.New(slog.NewJSONHandler(&logs, nil)), server, addr, stopWait) }()
	waitUntilListening(t, addr)
	answered := make(chan answer, 1)
	go func() { answered <- send(t.Context(), http.MethodGet, "http://"+addr+"/", "") }()
	<-arrived

	stop()
	deadline := time.Now().Add(stopWait)
	for {
		var dialer net.Dialer
		conn, err := dialer.DialContext(t.Context(), "tcp", addr)
		if err != nil {
			break
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("close connection: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("the API still accepts connections after it was told to stop")
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case err := <-stopped:
		t.Fatalf("serve returned %v while a request was in flight, want it to wait", err)
	case <-time.After(200 * time.Millisecond):
	}
	unblock()

	if got := <-answered; got.err != nil || got.status != http.StatusOK || got.body != "finished" {
		t.Errorf("request in flight got %+v, want 200 with its answer", got)
	}
	if err := waitForStop(stopped); err != nil {
		t.Errorf("serve returned %v, want no error", err)
	}
	if !strings.Contains(logs.String(), `"msg":"api is stopping, waiting for the requests in flight"`) {
		t.Errorf("logs = %q, want a line that the API is stopping", logs.String())
	}
}

func TestServeCancelsARequestStillRunningAtTheDeadline(t *testing.T) {
	const timeout = 300 * time.Millisecond
	arrived, cancelled := make(chan struct{}), make(chan struct{})
	server := &http.Server{ReadHeaderTimeout: readHeaderTimeout, Handler: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(arrived)
		<-r.Context().Done()
		close(cancelled)
	})}
	addr := freeAddr(t)
	ctx, stop := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() { stopped <- serve(ctx, slog.New(slog.DiscardHandler), server, addr, timeout) }()
	waitUntilListening(t, addr)
	go send(t.Context(), http.MethodGet, "http://"+addr+"/", "")
	<-arrived

	told := time.Now()
	stop()
	err := waitForStop(stopped)

	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "shut down within 300ms of the stop signal") {
		t.Errorf("serve returned %v, want an error that it did not shut down within 300ms", err)
	}
	if took := time.Since(told); took < timeout || took > timeout+2*time.Second {
		t.Errorf("serve returned %s after it was told to stop, want about %s", took, timeout)
	}
	select {
	case <-cancelled:
	case <-time.After(stopWait):
		t.Error("the request was not cancelled after the deadline")
	}
}

func TestServeCountsTheDeadlineFromTheSignalWhileItWaitsForAStream(t *testing.T) {
	const (
		timeout     = 1500 * time.Millisecond
		requestEnds = time.Second
	)
	arrived, requestDone, streamDone := make(chan struct{}, 2), make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(streamDone) })
	mux := http.NewServeMux()
	mux.HandleFunc("GET /request", func(http.ResponseWriter, *http.Request) {
		arrived <- struct{}{}
		<-requestDone
	})
	mux.HandleFunc("GET /stream", func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		arrived <- struct{}{}
		<-streamDone
		if err := conn.Close(); err != nil {
			t.Errorf("close the hijacked connection: %v", err)
		}
	})
	addr := freeAddr(t)
	ctx, stop := context.WithCancel(t.Context())
	stopped := make(chan error, 1)
	go func() {
		stopped <- serve(ctx, slog.New(slog.DiscardHandler), &http.Server{ReadHeaderTimeout: readHeaderTimeout, Handler: mux}, addr, timeout)
	}()
	waitUntilListening(t, addr)
	go send(t.Context(), http.MethodGet, "http://"+addr+"/request", "")
	go send(t.Context(), http.MethodGet, "http://"+addr+"/stream", "")
	<-arrived
	<-arrived

	told := time.Now()
	stop()
	time.AfterFunc(requestEnds, func() { close(requestDone) })
	err := waitForStop(stopped)

	if err == nil || !strings.Contains(err.Error(), "status streams still open 1.5s after the stop signal") {
		t.Errorf("serve returned %v, want an error that streams were still open after 1.5s", err)
	}
	if took := time.Since(told); took < timeout || took > timeout+500*time.Millisecond {
		t.Errorf("serve returned %s after it was told to stop, want about %s: the request took %s of it", took, timeout, requestEnds)
	}
}

func TestServeFailsWhenTheAddressIsTaken(t *testing.T) {
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	})

	err = serve(t.Context(), slog.New(slog.DiscardHandler), &http.Server{ReadHeaderTimeout: readHeaderTimeout}, listener.Addr().String(), time.Second)

	if err == nil || !strings.Contains(err.Error(), "listen on "+listener.Addr().String()) {
		t.Errorf("error = %v, want one about listening on the address", err)
	}
}

type runningAPI struct {
	url     string
	db      *pgxpool.Pool
	stop    func()
	stopped func() error
}

func startAPI(t *testing.T, timeout time.Duration) runningAPI {
	t.Helper()
	db := testdb.New(t)
	redisOptions := testredis.New(t).Options()
	addr := freeAddr(t)
	ctx, stop := context.WithCancel(t.Context())
	returned := make(chan error, 1)
	go func() {
		returned <- run(ctx, slog.New(slog.DiscardHandler), db.Config().ConnString(), fmt.Sprintf("redis://%s/%d", redisOptions.Addr, redisOptions.DB), "", addr, timeout)
	}()
	stopped := sync.OnceValue(func() error { return waitForStop(returned) })
	t.Cleanup(func() {
		stop()
		if err := stopped(); errors.Is(err, errStillRunning) {
			t.Error(err)
		}
	})
	waitUntilListening(t, addr)
	return runningAPI{url: "http://" + addr, db: db, stop: stop, stopped: stopped}
}

func (a runningAPI) openStream(t *testing.T) *websocket.Conn {
	t.Helper()
	created := send(t.Context(), http.MethodPost, a.url+"/v1/notifications", `{"recipient":"+905551234567","channel":"sms","content":"hello","scheduled_at":"2099-01-01T00:00:00Z"}`)
	var notification struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.Unmarshal([]byte(created.body), &notification); err != nil || created.status != http.StatusAccepted {
		t.Fatalf("create answered %+v, decode error %v, want 202", created, err)
	}
	deadline := time.Now().Add(stopWait)
	for {
		conn, _, err := websocket.Dial(t.Context(), a.url+"/v1/notifications/"+notification.ID.String()+"/events", nil)
		if err == nil {
			t.Cleanup(func() {
				if err := conn.CloseNow(); err != nil && !errors.Is(err, net.ErrClosed) {
					t.Errorf("close the stream: %v", err)
				}
			})
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("open the stream: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRunClosesTheStatusStreamsWhenItIsToldToStop(t *testing.T) {
	api := startAPI(t, stopWait)
	conn := api.openStream(t)

	told := time.Now()
	api.stop()
	readCtx, cancel := context.WithTimeout(t.Context(), stopWait)
	defer cancel()
	_, _, err := conn.Read(readCtx)

	if got := websocket.CloseStatus(err); got != websocket.StatusTryAgainLater {
		t.Errorf("the stream ended with %v, want a close frame with code %d", err, websocket.StatusTryAgainLater)
	}
	if err := api.stopped(); err != nil {
		t.Errorf("run returned %v after it was told to stop, want no error", err)
	}
	if took := time.Since(told); took > 3*time.Second {
		t.Errorf("run returned %s after it was told to stop, want it not to wait once the stream is closed", took)
	}
}

func TestRunGivesUpOnAStreamWhoseClientDoesNotAnswerTheClose(t *testing.T) {
	const timeout = 300 * time.Millisecond
	api := startAPI(t, timeout)
	api.openStream(t)

	told := time.Now()
	api.stop()
	err := api.stopped()

	if err == nil || !strings.Contains(err.Error(), "status streams still open 300ms after the stop signal") {
		t.Errorf("run returned %v, want an error that streams were still open after 300ms", err)
	}
	if took := time.Since(told); took < timeout || took > timeout+2*time.Second {
		t.Errorf("run returned %s after it was told to stop, want about %s", took, timeout)
	}
}

func lockNotifications(t *testing.T, db *pgxpool.Pool) {
	t.Helper()
	lock, err := db.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() {
		if err := lock.Rollback(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("release the table lock: %v", err)
		}
	})
	if _, err := lock.Exec(t.Context(), "LOCK TABLE notifications IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatalf("lock the table: %v", err)
	}
}

func waitUntilARequestWaitsForTheTable(t *testing.T, db *pgxpool.Pool) {
	t.Helper()
	deadline := time.Now().Add(stopWait)
	for {
		var waiting int
		if err := db.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())").Scan(&waiting); err != nil {
			t.Fatalf("count waiting locks: %v", err)
		}
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the create request never waited for the table")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const createRequest = `{"recipient":"+905551234567","channel":"sms","content":"hello"}`

func TestRunStoresACreateWhoseBodyArrivesAfterItIsToldToStop(t *testing.T) {
	api := startAPI(t, stopWait)
	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", strings.TrimPrefix(api.url, "http://"))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close connection: %v", err)
		}
	})
	if err := conn.SetDeadline(time.Now().Add(stopWait)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := fmt.Fprintf(conn, "POST /v1/notifications HTTP/1.1\r\nHost: api\r\nContent-Type: application/json\r\nContent-Length: %d\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n", len(createRequest)); err != nil {
		t.Fatalf("write the request head: %v", err)
	}
	reader := bufio.NewReader(conn)
	goAhead, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("read the answer to the request head: %v", err)
	}
	if err := goAhead.Body.Close(); err != nil {
		t.Errorf("close the response body: %v", err)
	}
	if goAhead.StatusCode != http.StatusContinue {
		t.Fatalf("the API answered %d, want 100 Continue: its handler is reading the body", goAhead.StatusCode)
	}

	api.stop()
	time.Sleep(200 * time.Millisecond)
	if _, err := io.WriteString(conn, createRequest); err != nil {
		t.Fatalf("write the request body: %v", err)
	}
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("read the answer: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	if closeErr := response.Body.Close(); closeErr != nil {
		t.Errorf("close the response body: %v", closeErr)
	}

	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Errorf("create in flight at the stop answered %d %s, error %v, want 202", response.StatusCode, body, err)
	}
	if err := api.stopped(); err != nil {
		t.Errorf("run returned %v after it was told to stop, want no error", err)
	}
	var stored int
	if err := api.db.QueryRow(t.Context(), "SELECT count(*) FROM notifications").Scan(&stored); err != nil {
		t.Fatalf("count notifications: %v", err)
	}
	if stored != 1 {
		t.Errorf("%d notifications stored, want the 1 that was in flight at the stop", stored)
	}
}

func TestRunGivesUpOnARequestStillInTheDatabaseAtTheDeadline(t *testing.T) {
	const timeout = 300 * time.Millisecond
	api := startAPI(t, timeout)
	lockNotifications(t, api.db)
	go send(t.Context(), http.MethodPost, api.url+"/v1/notifications", createRequest)
	waitUntilARequestWaitsForTheTable(t, api.db)

	told := time.Now()
	api.stop()
	err := api.stopped()

	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "shut down within 300ms of the stop signal") {
		t.Errorf("run returned %v, want an error that it did not shut down within 300ms", err)
	}
	if took := time.Since(told); took < timeout || took > timeout+3*time.Second {
		t.Errorf("run returned %s after it was told to stop, want about %s: the table is still locked", took, timeout)
	}
}
