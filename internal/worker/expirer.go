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
	MaxKeys      int32
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
	deleted, err := e.Queries.DeleteExpiredIdempotencyKeys(queryCtx, e.MaxKeys)
	if err != nil {
		e.Logger.ErrorContext(ctx, "delete expired idempotency keys", "error", err)
		return
	}
	if deleted == int64(e.MaxKeys) {
		e.Logger.WarnContext(ctx, "key expiry reached its limit, expired idempotency keys may remain", "count", deleted)
		return
	}
	if deleted > 0 {
		e.Logger.InfoContext(ctx, "expired idempotency keys deleted", "count", deleted)
	}
}
