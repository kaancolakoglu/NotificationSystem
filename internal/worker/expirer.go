package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/kaancolakoglu/NotificationSystem/internal/store"
)

type KeyExpirer struct {
	Queries      *store.Queries
	Logger       *slog.Logger
	Interval     time.Duration
	QueryTimeout time.Duration
}

func (e KeyExpirer) Run(ctx context.Context) {
	ticker := time.NewTicker(e.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.expire(ctx)
		}
	}
}

func (e KeyExpirer) expire(ctx context.Context) {
	queryCtx, cancel := context.WithTimeout(ctx, e.QueryTimeout)
	defer cancel()
	deleted, err := e.Queries.DeleteExpiredIdempotencyKeys(queryCtx)
	if err != nil {
		e.Logger.ErrorContext(ctx, "delete expired idempotency keys", "error", err)
		return
	}
	if deleted > 0 {
		e.Logger.InfoContext(ctx, "expired idempotency keys deleted", "count", deleted)
	}
}
