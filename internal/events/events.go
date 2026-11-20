package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redis/go-redis/v9"
)

const (
	Channel        = "notifications:status"
	publishTimeout = 2 * time.Second
)

type Event struct {
	NotificationID uuid.UUID  `json:"notification_id"`
	BatchID        *uuid.UUID `json:"batch_id,omitempty"`
	Status         string     `json:"status"`
	Attempts       int16      `json:"attempts"`
}

func New(id uuid.UUID, batchID pgtype.UUID, status string, attempts int16) Event {
	event := Event{NotificationID: id, Status: status, Attempts: attempts}
	if batchID.Valid {
		batch := uuid.UUID(batchID.Bytes)
		event.BatchID = &batch
	}
	return event
}

type Publisher struct {
	Client *redis.Client
	Logger *slog.Logger
}

func (p Publisher) Publish(ctx context.Context, changes ...Event) {
	if len(changes) == 0 {
		return
	}
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), publishTimeout)
	defer cancel()
	_, err := p.Client.Pipelined(publishCtx, func(pipe redis.Pipeliner) error {
		for _, change := range changes {
			payload, err := json.Marshal(change)
			if err != nil {
				return fmt.Errorf("encode status event of notification %s: %w", change.NotificationID, err)
			}
			pipe.Publish(publishCtx, Channel, payload)
		}
		return nil
	})
	if err != nil {
		ids := make([]uuid.UUID, len(changes))
		for i, change := range changes {
			ids[i] = change.NotificationID
		}
		p.Logger.ErrorContext(ctx, "publish status events", "count", len(changes), "notification_ids", ids, "error", err)
	}
}
