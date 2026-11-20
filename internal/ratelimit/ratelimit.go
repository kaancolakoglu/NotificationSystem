package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	limiter *redis_rate.Limiter
	limit   redis_rate.Limit
}

func New(client *redis.Client, perSecond, burst int) *Limiter {
	return &Limiter{
		limiter: redis_rate.NewLimiter(client),
		limit:   redis_rate.Limit{Rate: perSecond, Burst: burst, Period: time.Second},
	}
}

func (l *Limiter) Wait(ctx context.Context, channel string) error {
	for {
		result, err := l.limiter.Allow(ctx, channel, l.limit)
		if err != nil {
			return fmt.Errorf("take a rate limit token: %w", err)
		}
		if result.Allowed > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for a rate limit token: %w", ctx.Err())
		case <-time.After(result.RetryAfter):
		}
	}
}
