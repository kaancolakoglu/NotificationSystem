package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/kaancolakoglu/NotificationSystem/internal/events"
	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
)

const leaseExpired = "lease expired before a result was stored"

type Sweeper struct {
	Queries      *store.Queries
	Logger       *slog.Logger
	Metrics      *obs.WorkerMetrics
	Events       events.Publisher
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
	changes := make([]events.Event, len(swept))
	for i, notification := range swept {
		changes[i] = events.New(notification.ID, notification.BatchID, notification.Status, notification.Attempts)
		level := slog.LevelWarn
		if notification.Status == "failed" {
			level = slog.LevelError
			s.Metrics.Failed(notification.Channel)
		} else {
			s.Metrics.Retried(notification.Channel)
		}
		s.Logger.Log(obs.WithCorrelationID(ctx, notification.CorrelationID.String), level, "lease expired", "channel", notification.Channel, "notification_id", notification.ID, "attempt", notification.Attempts, "status", notification.Status)
	}
	s.Events.Publish(ctx, changes...)
}
