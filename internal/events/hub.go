package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	streamBuffer  = 256
	retryInterval = time.Second
)

type Hub struct {
	client *redis.Client
	logger *slog.Logger

	mu         sync.Mutex
	subscribed bool
	streams    map[uuid.UUID]map[chan string]struct{}
}

func NewHub(client *redis.Client, logger *slog.Logger) *Hub {
	return &Hub{client: client, logger: logger, streams: map[uuid.UUID]map[chan string]struct{}{}}
}

func (h *Hub) Run(ctx context.Context) {
	for ctx.Err() == nil {
		h.receive(ctx)
		select {
		case <-ctx.Done():
		case <-time.After(retryInterval):
		}
	}
}

func (h *Hub) receive(ctx context.Context) {
	pubsub := h.client.Subscribe(ctx, Channel)
	subscription, end := context.WithCancel(ctx)
	closed := make(chan struct{})
	context.AfterFunc(subscription, func() {
		defer close(closed)
		if err := pubsub.Close(); err != nil {
			h.logger.ErrorContext(ctx, "close the status events subscription", "error", err)
		}
	})
	defer func() {
		end()
		<-closed
		h.reset(false)
	}()
	for {
		received, err := pubsub.Receive(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			h.logger.ErrorContext(ctx, "receive status events, closing the streams", "error", err)
			return
		}
		switch message := received.(type) {
		case *redis.Subscription:
			if message.Kind == "subscribe" {
				h.reset(true)
				h.logger.InfoContext(ctx, "subscribed to status events")
			}
		case *redis.Message:
			h.dispatch(ctx, message.Payload)
		}
	}
}

func (h *Hub) Subscribe(id uuid.UUID) (stream <-chan string, unsubscribe func(), ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.subscribed {
		return nil, nil, false
	}
	added := make(chan string, streamBuffer)
	if h.streams[id] == nil {
		h.streams[id] = map[chan string]struct{}{}
	}
	h.streams[id][added] = struct{}{}
	return added, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.remove(id, added)
	}, true
}

func (h *Hub) remove(id uuid.UUID, stream chan string) {
	delete(h.streams[id], stream)
	if len(h.streams[id]) == 0 {
		delete(h.streams, id)
	}
}

func (h *Hub) reset(subscribed bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subscribed = subscribed
	for _, streams := range h.streams {
		for stream := range streams {
			close(stream)
		}
	}
	clear(h.streams)
}

func (h *Hub) dispatch(ctx context.Context, payload string) {
	var event Event
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		h.logger.ErrorContext(ctx, "decode status event", "payload", payload, "error", err)
		return
	}
	if event.NotificationID == uuid.Nil {
		h.logger.ErrorContext(ctx, "status event has no notification id", "payload", payload)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.send(ctx, event.NotificationID, payload)
	if event.BatchID != nil {
		h.send(ctx, *event.BatchID, payload)
	}
}

func (h *Hub) send(ctx context.Context, id uuid.UUID, payload string) {
	for stream := range h.streams[id] {
		select {
		case stream <- payload:
		default:
			h.logger.WarnContext(ctx, "status stream is full, closing it", "id", id)
			h.remove(id, stream)
			close(stream)
		}
	}
}
