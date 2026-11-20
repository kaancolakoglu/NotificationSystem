package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	notificationID = uuid.MustParse("019a0000-0000-7000-8000-000000000000")
	message        = Message{To: "+905551234567", Channel: "sms", Content: "hello"}
)

func newClient(t *testing.T, handler http.HandlerFunc) (*Client, *bytes.Buffer) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	var logs bytes.Buffer
	return New(server.URL, 1, slog.New(slog.NewJSONHandler(&logs, nil))), &logs
}

func sendError(t *testing.T, client *Client) *Error {
	t.Helper()
	messageID, err := client.Send(t.Context(), notificationID, message)
	if messageID != "" {
		t.Errorf("message id = %q, want none", messageID)
	}
	var sendErr *Error
	if !errors.As(err, &sendErr) {
		t.Fatalf("error = %v, want a *provider.Error", err)
	}
	return sendErr
}

func TestSendDelivers(t *testing.T) {
	var got *http.Request
	var gotBody []byte
	client, logs := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		got = r
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		gotBody = body
		w.WriteHeader(http.StatusAccepted)
		if _, err := io.WriteString(w, `{"messageId":"message-1","status":"accepted","timestamp":"2026-01-01T00:00:00Z"}`); err != nil {
			t.Errorf("write answer: %v", err)
		}
	})

	messageID, err := client.Send(t.Context(), notificationID, message)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if messageID != "message-1" {
		t.Errorf("message id = %q, want %q", messageID, "message-1")
	}
	if got.Method != http.MethodPost {
		t.Errorf("method = %s, want POST", got.Method)
	}
	if contentType := got.Header.Get("Content-Type"); contentType != "application/json" {
		t.Errorf("content type = %q, want %q", contentType, "application/json")
	}
	if key := got.Header.Get("Idempotency-Key"); key != notificationID.String() {
		t.Errorf("idempotency key = %q, want %q", key, notificationID)
	}
	var sent map[string]string
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("request body %s is not a JSON object of strings: %v", gotBody, err)
	}
	want := map[string]string{"to": "+905551234567", "channel": "sms", "content": "hello"}
	if len(sent) != len(want) {
		t.Errorf("request body = %s, want %v", gotBody, want)
	}
	for name, value := range want {
		if sent[name] != value {
			t.Errorf("request field %s = %q, want %q", name, sent[name], value)
		}
	}
	if logs.Len() != 0 {
		t.Errorf("logged %s, want nothing", logs)
	}
}

func TestSendClassifiesAnswers(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		retryAfter     string
		wantRetryable  bool
		wantRetryAfter time.Duration
	}{
		{name: "500 is retried", status: http.StatusInternalServerError, wantRetryable: true},
		{name: "503 is retried", status: http.StatusServiceUnavailable, wantRetryable: true},
		{name: "503 with Retry-After is retried without it", status: http.StatusServiceUnavailable, retryAfter: "7", wantRetryable: true},
		{name: "408 is retried", status: http.StatusRequestTimeout, wantRetryable: true},
		{name: "429 is retried", status: http.StatusTooManyRequests, wantRetryable: true},
		{name: "429 with Retry-After in seconds", status: http.StatusTooManyRequests, retryAfter: "7", wantRetryable: true, wantRetryAfter: 7 * time.Second},
		{name: "429 with Retry-After of zero", status: http.StatusTooManyRequests, retryAfter: "0", wantRetryable: true},
		{name: "429 with negative Retry-After", status: http.StatusTooManyRequests, retryAfter: "-7", wantRetryable: true},
		{name: "429 with Retry-After that is not a number or a date", status: http.StatusTooManyRequests, retryAfter: "soon", wantRetryable: true},
		{name: "429 with Retry-After just under the ceiling", status: http.StatusTooManyRequests, retryAfter: "59", wantRetryable: true, wantRetryAfter: 59 * time.Second},
		{name: "429 with Retry-After at the ceiling", status: http.StatusTooManyRequests, retryAfter: "60", wantRetryable: true, wantRetryAfter: time.Minute},
		{name: "429 with Retry-After over the ceiling", status: http.StatusTooManyRequests, retryAfter: "61", wantRetryable: true, wantRetryAfter: time.Minute},
		{name: "429 with Retry-After of 68 years", status: http.StatusTooManyRequests, retryAfter: "2147483648", wantRetryable: true, wantRetryAfter: time.Minute},
		{name: "429 with Retry-After too large for 64 bits", status: http.StatusTooManyRequests, retryAfter: "99999999999999999999999", wantRetryable: true, wantRetryAfter: time.Minute},
		{name: "429 with Retry-After date in the past", status: http.StatusTooManyRequests, retryAfter: "Thu, 01 Jan 2026 00:00:00 GMT", wantRetryable: true},
		{name: "400 is permanent", status: http.StatusBadRequest},
		{name: "404 is permanent", status: http.StatusNotFound},
		{name: "400 with Retry-After is permanent", status: http.StatusBadRequest, retryAfter: "7"},
		{name: "200 is permanent", status: http.StatusOK},
		{name: "204 is permanent", status: http.StatusNoContent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
				if tt.retryAfter != "" {
					w.Header().Set("Retry-After", tt.retryAfter)
				}
				w.WriteHeader(tt.status)
			})

			err := sendError(t, client)

			if err.Retryable != tt.wantRetryable {
				t.Errorf("retryable = %t, want %t", err.Retryable, tt.wantRetryable)
			}
			if err.RetryAfter != tt.wantRetryAfter {
				t.Errorf("retry after = %s, want %s", err.RetryAfter, tt.wantRetryAfter)
			}
			if want := fmt.Sprintf("provider answered %d, want 202", tt.status); err.Error() != want {
				t.Errorf("error = %q, want %q", err, want)
			}
		})
	}
}

func TestSendReadsRetryAfterDate(t *testing.T) {
	tests := []struct {
		name    string
		ahead   time.Duration
		wantMin time.Duration
		wantMax time.Duration
	}{
		{name: "under the ceiling", ahead: 30 * time.Second, wantMin: 28 * time.Second, wantMax: 30 * time.Second},
		{name: "over the ceiling", ahead: time.Hour, wantMin: time.Minute, wantMax: time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", time.Now().Add(tt.ahead).UTC().Format(http.TimeFormat))
				w.WriteHeader(http.StatusTooManyRequests)
			})

			err := sendError(t, client)

			if !err.Retryable {
				t.Error("retryable = false, want true")
			}
			if err.RetryAfter < tt.wantMin || err.RetryAfter > tt.wantMax {
				t.Errorf("retry after = %s, want %s to %s", err.RetryAfter, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestSendDoesNotFollowRedirect(t *testing.T) {
	var followed int
	client, _ := newClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/moved" {
			followed++
			w.WriteHeader(http.StatusAccepted)
			return
		}
		http.Redirect(w, r, "/moved", http.StatusMovedPermanently)
	})

	err := sendError(t, client)

	if err.Retryable {
		t.Error("retryable = true, want false")
	}
	if followed != 0 {
		t.Errorf("redirect target got %d requests, want 0", followed)
	}
}

func TestSendTimesOut(t *testing.T) {
	client, _ := newClient(t, func(_ http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Errorf("read request body: %v", err)
		}
		select {
		case <-r.Context().Done():
		case <-time.After(3 * time.Second):
		}
	})
	if client.http.Timeout != 5*time.Second {
		t.Errorf("timeout = %s, want 5s", client.http.Timeout)
	}
	client.http.Timeout = 50 * time.Millisecond

	err := sendError(t, client)

	if !err.Retryable {
		t.Error("retryable = false, want true")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want a deadline exceeded error", err)
	}
}

func TestSendStopsWhenTheContextIsCancelled(t *testing.T) {
	var requests int
	client, _ := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusAccepted)
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	messageID, err := client.Send(ctx, notificationID, message)

	if messageID != "" {
		t.Errorf("message id = %q, want none", messageID)
	}
	var sendErr *Error
	if !errors.As(err, &sendErr) {
		t.Fatalf("error = %v, want a *provider.Error", err)
	}
	if !sendErr.Retryable {
		t.Error("retryable = false, want true")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want a cancelled error", err)
	}
	if requests != 0 {
		t.Errorf("provider got %d requests, want 0", requests)
	}
}

func TestSendReusesConnections(t *testing.T) {
	const senders = 10
	var opened atomic.Int32
	arrived := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		arrived <- struct{}{}
		<-release
		w.WriteHeader(http.StatusAccepted)
		if _, err := io.WriteString(w, `{"messageId":"message-1"}`); err != nil {
			t.Errorf("write answer: %v", err)
		}
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	client := New(server.URL, senders, slog.New(slog.DiscardHandler))

	for range 2 {
		var sends sync.WaitGroup
		for range senders {
			sends.Go(func() {
				if _, err := client.Send(t.Context(), notificationID, message); err != nil {
					t.Errorf("send: %v", err)
				}
			})
		}
		for range senders {
			<-arrived
		}
		for range senders {
			release <- struct{}{}
		}
		sends.Wait()
		time.Sleep(100 * time.Millisecond)
	}

	if got := opened.Load(); got != senders {
		t.Errorf("two rounds of %d simultaneous sends opened %d connections, want %d", senders, got, senders)
	}
}

func TestSendTransportError(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()
	client := New(server.URL, 1, slog.New(slog.DiscardHandler))

	err := sendError(t, client)

	if !err.Retryable {
		t.Error("retryable = false, want true")
	}
}

func TestSendAcceptedWithoutMessageID(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "no body", body: ""},
		{name: "not JSON", body: "accepted"},
		{name: "no messageId", body: `{"status":"accepted"}`},
		{name: "messageId is not a string", body: `{"messageId":5}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, logs := newClient(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusAccepted)
				if _, err := io.WriteString(w, tt.body); err != nil {
					t.Errorf("write answer: %v", err)
				}
			})

			messageID, err := client.Send(t.Context(), notificationID, message)
			if err != nil {
				t.Fatalf("send: %v", err)
			}
			if messageID != "" {
				t.Errorf("message id = %q, want none", messageID)
			}
			lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
			if len(lines) != 1 {
				t.Fatalf("logged %d lines, want 1: %s", len(lines), logs)
			}
			for _, want := range []string{`"level":"WARN"`, `"msg":"provider accepted the notification without a readable message id"`, `"notification_id":"` + notificationID.String() + `"`} {
				if !strings.Contains(lines[0], want) {
					t.Errorf("log line = %s, want it to contain %s", lines[0], want)
				}
			}
		})
	}
}
