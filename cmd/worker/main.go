package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/kaancolakoglu/NotificationSystem/internal/provider"
	"github.com/kaancolakoglu/NotificationSystem/internal/ratelimit"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/worker"
)

const (
	poolSize          = 20
	pollInterval      = 200 * time.Millisecond
	databaseTimeout   = 5 * time.Second
	backoffBase       = time.Second
	sendsPerSecond    = 90
	sendBurst         = 5
	breakerOpenFor    = 30 * time.Second
	sweepInterval     = 15 * time.Second
	keyExpiryInterval = time.Minute
	keyExpiryMaxKeys  = 100_000
)

var channels = []string{"sms", "email", "push"}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	err := run(context.Background(), logger, os.Getenv("DATABASE_URL"), os.Getenv("REDIS_URL"), os.Getenv("PROVIDER_URL"), databaseTimeout)
	logger.Error("worker exited", "error", err)
	os.Exit(1)
}

func run(ctx context.Context, logger *slog.Logger, databaseURL, redisURL, providerURL string, timeout time.Duration) error {
	if databaseURL == "" {
		return errors.New("DATABASE_URL is not set")
	}
	if err := checkProviderURL(providerURL); err != nil {
		return err
	}
	redisOptions, err := redis.ParseURL(redisURL)
	if err != nil {
		return fmt.Errorf("REDIS_URL is not a Redis URL: %w", err)
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("create database pool: %w", err)
	}
	defer pool.Close()

	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	err = pool.Ping(pingCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	redisClient := redis.NewClient(redisOptions)
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.ErrorContext(ctx, "close redis client", "error", err)
		}
	}()

	queries := store.New(pool)
	limiter := ratelimit.New(redisClient, sendsPerSecond, sendBurst)
	client := provider.New(providerURL, len(channels)*poolSize, logger)
	var loops sync.WaitGroup
	for _, channel := range channels {
		loops.Go(func() {
			worker.Pool{
				Queries:        queries,
				Provider:       client,
				Limiter:        limiter,
				Logger:         logger,
				Channel:        channel,
				Size:           poolSize,
				Interval:       pollInterval,
				QueryTimeout:   timeout,
				BackoffBase:    backoffBase,
				BreakerOpenFor: breakerOpenFor,
			}.Run(ctx)
		})
	}
	loops.Go(func() {
		worker.Sweeper{
			Queries:      queries,
			Logger:       logger,
			Interval:     sweepInterval,
			QueryTimeout: timeout,
		}.Run(ctx)
	})
	loops.Go(func() {
		worker.KeyExpirer{
			Queries:      queries,
			Logger:       logger,
			Interval:     keyExpiryInterval,
			QueryTimeout: timeout,
			MaxKeys:      keyExpiryMaxKeys,
		}.Run(ctx)
	})
	loops.Wait()
	return ctx.Err()
}

func checkProviderURL(providerURL string) error {
	parsed, err := url.Parse(providerURL)
	if err != nil {
		return fmt.Errorf("PROVIDER_URL is not a URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("PROVIDER_URL must be an http or https URL with a host, got %q", providerURL)
	}
	return nil
}
