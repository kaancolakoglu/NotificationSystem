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
	"github.com/sony/gobreaker/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/kaancolakoglu/NotificationSystem/internal/events"
	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
	"github.com/kaancolakoglu/NotificationSystem/internal/provider"
	"github.com/kaancolakoglu/NotificationSystem/internal/ratelimit"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
)

const (
	maxAttempts       = 5
	breakerFailures   = 5
	breakerTrialCalls = 3
)

var priorities = []int16{0, 1, 2}

type Pool struct {
	Queries         *store.Queries
	Provider        *provider.Client
	Limiter         *ratelimit.Limiter
	Logger          *slog.Logger
	Metrics         *obs.WorkerMetrics
	Events          events.Publisher
	Tracer          trace.Tracer
	Channel         string
	Size            int32
	Interval        time.Duration
	QueryTimeout    time.Duration
	BackoffBase     time.Duration
	BreakerOpenFor  time.Duration
	ShutdownTimeout time.Duration
}

func (p Pool) Run(ctx context.Context) {
	work, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	go func() {
		<-ctx.Done()
		select {
		case <-work.Done():
		case <-time.After(p.ShutdownTimeout):
			p.Logger.ErrorContext(work, "shutdown deadline passed, cancelling the sends in flight", "channel", p.Channel)
			cancel()
		}
	}()
	jobs := make(chan store.Notification, p.Size)
	breaker := p.newBreaker(work)
	var busy atomic.Int32
	var limiterDown atomic.Bool
	var senders sync.WaitGroup
	for range p.Size {
		senders.Go(func() {
			for notification := range jobs {
				notificationCtx := obs.WithCorrelationID(work, notification.CorrelationID.String)
				switch err := p.Limiter.Wait(ctx, p.Channel); {
				case err == nil:
					p.send(notificationCtx, breaker, notification)
				case errors.Is(err, context.Canceled):
					p.release(notificationCtx, notification, slog.LevelInfo, "worker is stopping, releasing the notification", err)
				default:
					limiterDown.Store(true)
					p.Metrics.LimiterFailed(p.Channel)
					p.release(notificationCtx, notification, slog.LevelError, "rate limiter failed, releasing the notification", err)
				}
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
			if ctx.Err() != nil {
				continue
			}
			capacity := p.Size
			switch breaker.State() {
			case gobreaker.StateOpen:
				continue
			case gobreaker.StateHalfOpen:
				started := breaker.Counts().Requests
				if started >= breakerTrialCalls {
					continue
				}
				capacity = min(capacity, breakerTrialCalls-int32(started))
			}
			if limiterDown.Load() {
				err := p.Limiter.Wait(ctx, p.Channel)
				if errors.Is(err, context.Canceled) {
					continue
				}
				if err != nil {
					p.Metrics.LimiterFailed(p.Channel)
					p.Logger.ErrorContext(ctx, "rate limiter still fails, not claiming", "channel", p.Channel, "error", err)
					continue
				}
				limiterDown.Store(false)
				p.Logger.InfoContext(ctx, "rate limiter works again, claiming", "channel", p.Channel)
			}
			p.claim(work, jobs, &busy, capacity)
		}
	}
}

func (p Pool) newBreaker(ctx context.Context) *gobreaker.CircuitBreaker[string] {
	return gobreaker.NewCircuitBreaker[string](gobreaker.Settings{
		Name:        p.Channel,
		MaxRequests: breakerTrialCalls,
		Timeout:     p.BreakerOpenFor,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= breakerFailures
		},
		IsSuccessful: func(err error) bool {
			var failure *provider.Error
			return !errors.As(err, &failure) || !failure.Unavailable
		},
		IsExcluded: func(err error) bool {
			return errors.Is(err, context.Canceled)
		},
		OnStateChange: func(_ string, from, to gobreaker.State) {
			p.Metrics.BreakerChanged(p.Channel, int(to))
			p.Logger.WarnContext(ctx, "circuit breaker changed state", "channel", p.Channel, "from", from.String(), "to", to.String())
		},
	})
}

func (p Pool) claim(ctx context.Context, jobs chan<- store.Notification, busy *atomic.Int32, capacity int32) {
	free := capacity - busy.Load()
	for _, priority := range priorities {
		if free <= 0 {
			return
		}
		queryCtx, cancel := context.WithTimeout(ctx, p.QueryTimeout)
		started := time.Now()
		claimed, err := p.Queries.ClaimNotifications(queryCtx, store.ClaimNotificationsParams{
			Channel:  p.Channel,
			Priority: priority,
			RowLimit: free,
		})
		ended := time.Now()
		cancel()
		if err != nil {
			p.Logger.ErrorContext(ctx, "claim notifications", "channel", p.Channel, "priority", priority, "error", err)
			return
		}
		changes := make([]events.Event, len(claimed))
		for i, notification := range claimed {
			changes[i] = events.New(notification.ID, notification.BatchID, notification.Status, notification.Attempts)
			_, span := p.startSpan(ctx, notification, "claim", trace.WithTimestamp(started))
			span.End(trace.WithTimestamp(ended))
		}
		p.Events.Publish(ctx, changes...)
		for _, notification := range claimed {
			busy.Add(1)
			free--
			jobs <- notification
		}
	}
}

func (p Pool) startSpan(ctx context.Context, notification store.Notification, name string, options ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !notification.TraceContext.Valid {
		return ctx, noop.Span{}
	}
	options = append(options, trace.WithAttributes(
		attribute.String("notification.id", notification.ID.String()),
		attribute.String("notification.channel", notification.Channel),
		attribute.Int("notification.attempt", int(notification.Attempts)),
	))
	return p.Tracer.Start(obs.WithTraceParent(ctx, notification.TraceContext.String), name, options...)
}

func (p Pool) release(ctx context.Context, notification store.Notification, level slog.Level, message string, cause error) {
	p.Logger.Log(ctx, level, message, "channel", p.Channel, "notification_id", notification.ID, "attempt", notification.Attempts, "error", cause)
	queryCtx, cancel := context.WithTimeout(ctx, p.QueryTimeout)
	defer cancel()
	changed, err := p.Queries.ReleaseNotification(queryCtx, store.ReleaseNotificationParams{ID: notification.ID, Attempts: notification.Attempts})
	if p.checkWrite(ctx, notification, "pending", "", changed, err) {
		p.Events.Publish(ctx, events.New(notification.ID, notification.BatchID, "pending", notification.Attempts-1))
	}
}

func (p Pool) send(ctx context.Context, breaker *gobreaker.CircuitBreaker[string], notification store.Notification) {
	messageID, sendErr := breaker.Execute(func() (string, error) {
		started := time.Now()
		callCtx, span := p.startSpan(ctx, notification, "provider call", trace.WithSpanKind(trace.SpanKindClient))
		messageID, err := p.Provider.Send(callCtx, notification.ID, provider.Message{
			To:      notification.Recipient,
			Channel: notification.Channel,
			Content: notification.Content,
		})
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		p.Metrics.ProviderCalled(p.Channel, time.Since(started))
		return messageID, err
	})
	if errors.Is(sendErr, gobreaker.ErrOpenState) || errors.Is(sendErr, gobreaker.ErrTooManyRequests) {
		p.release(ctx, notification, slog.LevelError, "circuit breaker refused the call, releasing the notification", sendErr)
		return
	}
	queryCtx, cancel := context.WithTimeout(ctx, p.QueryTimeout)
	defer cancel()
	if sendErr == nil {
		changed, err := p.Queries.MarkSent(queryCtx, store.MarkSentParams{
			ID:                notification.ID,
			Attempts:          notification.Attempts,
			ProviderMessageID: pgtype.Text{String: messageID, Valid: messageID != ""},
		})
		if p.checkWrite(ctx, notification, "sent", messageID, changed, err) {
			p.Metrics.Sent(p.Channel, time.Since(notification.CreatedAt))
			p.Events.Publish(ctx, events.New(notification.ID, notification.BatchID, "sent", notification.Attempts))
		}
		return
	}

	var failure *provider.Error
	if !errors.As(sendErr, &failure) || !failure.Retryable || notification.Attempts >= maxAttempts {
		p.Logger.ErrorContext(ctx, "send notification", "channel", p.Channel, "notification_id", notification.ID, "attempt", notification.Attempts, "status", "failed", "error", sendErr)
		changed, err := p.Queries.MarkFailed(queryCtx, store.MarkFailedParams{ID: notification.ID, Attempts: notification.Attempts, Error: sendErr.Error()})
		if p.checkWrite(ctx, notification, "failed", "", changed, err) {
			p.Metrics.Failed(p.Channel)
			p.Events.Publish(ctx, events.New(notification.ID, notification.BatchID, "failed", notification.Attempts))
		}
		return
	}
	delay := retryDelay(p.BackoffBase, notification.Attempts, failure.RetryAfter)
	p.Logger.WarnContext(ctx, "send notification", "channel", p.Channel, "notification_id", notification.ID, "attempt", notification.Attempts, "status", "pending", "delay_seconds", delay.Seconds(), "error", sendErr)
	changed, err := p.Queries.ScheduleRetry(queryCtx, store.ScheduleRetryParams{
		ID:       notification.ID,
		Attempts: notification.Attempts,
		Error:    sendErr.Error(),
		Delay:    pgtype.Interval{Microseconds: delay.Microseconds(), Valid: true},
	})
	if p.checkWrite(ctx, notification, "pending", "", changed, err) {
		p.Metrics.Retried(p.Channel)
		p.Events.Publish(ctx, events.New(notification.ID, notification.BatchID, "pending", notification.Attempts))
	}
}

func retryDelay(base time.Duration, attempts int16, retryAfter time.Duration) time.Duration {
	return max(rand.N(base<<attempts), retryAfter)
}

func (p Pool) checkWrite(ctx context.Context, notification store.Notification, status, messageID string, changed int64, err error) (stored bool) {
	if err != nil {
		p.Logger.ErrorContext(ctx, "write result", "channel", p.Channel, "notification_id", notification.ID, "attempt", notification.Attempts, "status", status, "provider_message_id", messageID, "error", err)
		return false
	}
	if changed == 0 {
		p.Logger.ErrorContext(ctx, "result not written, this attempt no longer holds the notification", "channel", p.Channel, "notification_id", notification.ID, "attempt", notification.Attempts, "status", status, "provider_message_id", messageID)
		return false
	}
	return true
}
