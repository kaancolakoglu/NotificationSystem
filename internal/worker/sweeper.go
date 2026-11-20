package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/kaancolakoglu/NotificationSystem/internal/store"
)

const leaseExpired = "lease expired before a result was stored"

type Sweeper struct {
	Queries      *store.Queries
	Logger       *slog.Logger
	Interval     time.Duration
	QueryTimeout time.Duration
}

func (s Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s Sweeper) sweep(ctx context.Context) {
	queryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.QueryTimeout)
	defer cancel()
	swept, err := s.Queries.SweepExpiredLeases(queryCtx, store.SweepExpiredLeasesParams{
		MaxAttempts: maxAttempts,
		Error:       leaseExpired,
	})
	if err != nil {
		s.Logger.ErrorContext(ctx, "sweep expired leases", "error", err)
		return
	}
	for _, notification := range swept {
		level := slog.LevelWarn
		if notification.Status == "failed" {
			level = slog.LevelError
		}
		s.Logger.Log(ctx, level, "lease expired", "notification_id", notification.ID, "attempt", notification.Attempts, "status", notification.Status)
	}
}
