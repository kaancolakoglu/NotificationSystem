package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/kaancolakoglu/NotificationSystem/internal/events"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

func TestStatusEventsAreStreamedThroughTheWholeHandler(t *testing.T) {
	const wait = 20 * time.Second
	db := testdb.New(t)
	client := testredis.New(t)
	logger := slog.New(slog.DiscardHandler)
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
	server := httptest.NewServer(newHandler(db, client, hub, untraced, logger))
	t.Cleanup(server.Close)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/v1/notifications", strings.NewReader(`{"recipient":"+905551234567","channel":"sms","content":"hello"}`))
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("create a notification: %v", err)
	}
	var created struct {
		ID uuid.UUID `json:"id"`
	}
	err = json.NewDecoder(response.Body).Decode(&created)
	if closeErr := response.Body.Close(); closeErr != nil {
		t.Errorf("close the response body: %v", closeErr)
	}
	if err != nil || response.StatusCode != http.StatusAccepted {
		t.Fatalf("create answered %d, decode error %v, want %d", response.StatusCode, err, http.StatusAccepted)
	}
	dialCtx, cancel := context.WithTimeout(t.Context(), wait)
	defer cancel()
	var conn *websocket.Conn
	deadline := time.Now().Add(wait)
	for {
		conn, _, err = websocket.Dial(dialCtx, server.URL+"/v1/notifications/"+created.ID.String()+"/events", nil)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("open the stream: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Cleanup(func() {
		if err := conn.CloseNow(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close the stream: %v", err)
		}
	})

	cancelRequest, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/v1/notifications/"+created.ID.String()+"/cancel", nil)
	if err != nil {
		t.Fatalf("build the request: %v", err)
	}
	cancelled, err := http.DefaultClient.Do(cancelRequest)
	if err != nil {
		t.Fatalf("cancel the notification: %v", err)
	}
	if err := cancelled.Body.Close(); err != nil {
		t.Errorf("close the response body: %v", err)
	}
	if cancelled.StatusCode != http.StatusOK {
		t.Fatalf("cancel answered %d, want %d", cancelled.StatusCode, http.StatusOK)
	}

	_, payload, err := conn.Read(dialCtx)
	if want := `{"notification_id":"` + created.ID.String() + `","status":"cancelled","attempts":0}`; err != nil || string(payload) != want {
		t.Errorf("read = %s, error %v, want %s", payload, err, want)
	}
}
