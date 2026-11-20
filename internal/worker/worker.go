package worker

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/kaancolakoglu/NotificationSystem/internal/provider"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
)

const maxAttempts = 5

var priorities = []int16{0, 1, 2}

type Pool struct {
	Queries      *store.Queries
	Provider     *provider.Client
	Logger       *slog.Logger
	Channel      string
	Size         int32
	Interval     time.Duration
	QueryTimeout time.Duration
	BackoffBase  time.Duration
}

func (p Pool) Run(ctx context.Context) {
	jobs := make(chan store.Notification, p.Size)
	var busy atomic.Int32
	var senders sync.WaitGroup
	for range p.Size {
		senders.Go(func() {
			for notification := range jobs {
				p.send(ctx, notification)
				busy.Add(-1)
			}
		})
	}

	ticker := time.NewTicker(p.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			close(jobs)
			senders.Wait()
			return
		case <-ticker.C:
			p.claim(ctx, jobs, &busy)
		}
	}
}

func (p Pool) claim(ctx context.Context, jobs chan<- store.Notification, busy *atomic.Int32) {
	free := p.Size - busy.Load()
	for _, priority := range priorities {
		if free == 0 {
			return
		}
		queryCtx, cancel := context.WithTimeout(ctx, p.QueryTimeout)
		claimed, err := p.Queries.ClaimNotifications(queryCtx, store.ClaimNotificationsParams{
			Channel:  p.Channel,
			Priority: priority,
			RowLimit: free,
		})
		cancel()
		if err != nil {
			p.Logger.ErrorContext(ctx, "claim notifications", "channel", p.Channel, "priority", priority, "error", err)
			return
		}
		for _, notification := range claimed {
			busy.Add(1)
			free--
			jobs <- notification
		}
	}
}

func (p Pool) send(ctx context.Context, notification store.Notification) {
	messageID, sendErr := p.Provider.Send(ctx, notification.ID, provider.Message{
		To:      notification.Recipient,
		Channel: notification.Channel,
		Content: notification.Content,
	})
	queryCtx, cancel := context.WithTimeout(ctx, p.QueryTimeout)
	defer cancel()
	if sendErr == nil {
		changed, err := p.Queries.MarkSent(queryCtx, store.MarkSentParams{
			ID:                notification.ID,
			Attempts:          notification.Attempts,
			ProviderMessageID: pgtype.Text{String: messageID, Valid: messageID != ""},
		})
		p.checkWrite(ctx, notification, "sent", messageID, changed, err)
		return
	}

	p.Logger.ErrorContext(ctx, "send notification", "notification_id", notification.ID, "attempt", notification.Attempts, "error", sendErr)
	var failure *provider.Error
	if !errors.As(sendErr, &failure) || !failure.Retryable || notification.Attempts >= maxAttempts {
		changed, err := p.Queries.MarkFailed(queryCtx, store.MarkFailedParams{ID: notification.ID, Attempts: notification.Attempts, Error: sendErr.Error()})
		p.checkWrite(ctx, notification, "failed", "", changed, err)
		return
	}
	delay := retryDelay(p.BackoffBase, notification.Attempts, failure.RetryAfter)
	changed, err := p.Queries.ScheduleRetry(queryCtx, store.ScheduleRetryParams{
		ID:       notification.ID,
		Attempts: notification.Attempts,
		Error:    sendErr.Error(),
		Delay:    pgtype.Interval{Microseconds: delay.Microseconds(), Valid: true},
	})
	p.checkWrite(ctx, notification, "pending", "", changed, err)
}

func retryDelay(base time.Duration, attempts int16, retryAfter time.Duration) time.Duration {
	return max(rand.N(base<<attempts), retryAfter)
}

func (p Pool) checkWrite(ctx context.Context, notification store.Notification, status, messageID string, changed int64, err error) {
	if err != nil {
		p.Logger.ErrorContext(ctx, "write result", "notification_id", notification.ID, "attempt", notification.Attempts, "status", status, "provider_message_id", messageID, "error", err)
		return
	}
	if changed == 0 {
		p.Logger.ErrorContext(ctx, "result not written, this attempt no longer holds the notification", "notification_id", notification.ID, "attempt", notification.Attempts, "status", status, "provider_message_id", messageID)
	}
}
