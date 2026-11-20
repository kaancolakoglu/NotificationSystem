package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/kaancolakoglu/NotificationSystem/internal/provider"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
)

var priorities = []int16{0, 1, 2}

type Pool struct {
	Queries      *store.Queries
	Provider     *provider.Client
	Logger       *slog.Logger
	Channel      string
	Size         int32
	Interval     time.Duration
	QueryTimeout time.Duration
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
	messageID, err := p.Provider.Send(ctx, notification.ID, provider.Message{
		To:      notification.Recipient,
		Channel: notification.Channel,
		Content: notification.Content,
	})
	queryCtx, cancel := context.WithTimeout(ctx, p.QueryTimeout)
	defer cancel()
	if err != nil {
		p.Logger.ErrorContext(ctx, "send notification", "notification_id", notification.ID, "error", err)
		changed, err := p.Queries.MarkFailed(queryCtx, notification.ID)
		p.checkWrite(ctx, notification.ID, "failed", "", changed, err)
		return
	}
	changed, err := p.Queries.MarkSent(queryCtx, store.MarkSentParams{
		ID:                notification.ID,
		ProviderMessageID: pgtype.Text{String: messageID, Valid: messageID != ""},
	})
	p.checkWrite(ctx, notification.ID, "sent", messageID, changed, err)
}

func (p Pool) checkWrite(ctx context.Context, id uuid.UUID, status, messageID string, changed int64, err error) {
	if err != nil {
		p.Logger.ErrorContext(ctx, "write result", "notification_id", id, "status", status, "provider_message_id", messageID, "error", err)
		return
	}
	if changed == 0 {
		p.Logger.ErrorContext(ctx, "result not written, notification is no longer processing", "notification_id", id, "status", status, "provider_message_id", messageID)
	}
}
