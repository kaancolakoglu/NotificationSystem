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

	"github.com/kaancolakoglu/NotificationSystem/internal/provider"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/worker"
)

const (
	poolSize          = 20
	pollInterval      = 200 * time.Millisecond
	databaseTimeout   = 5 * time.Second
	backoffBase       = time.Second
	sweepInterval     = 15 * time.Second
	keyExpiryInterval = time.Minute
)

var channels = []string{"sms", "email", "push"}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	err := run(context.Background(), logger, os.Getenv("DATABASE_URL"), os.Getenv("PROVIDER_URL"), databaseTimeout)
	logger.Error("worker exited", "error", err)
	os.Exit(1)
}

func run(ctx context.Context, logger *slog.Logger, databaseURL, providerURL string, timeout time.Duration) error {
	if databaseURL == "" {
		return errors.New("DATABASE_URL is not set")
	}
	if err := checkProviderURL(providerURL); err != nil {
		return err
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

	queries := store.New(pool)
	client := provider.New(providerURL, len(channels)*poolSize, logger)
	var loops sync.WaitGroup
	for _, channel := range channels {
		loops.Go(func() {
			worker.Pool{
				Queries:      queries,
				Provider:     client,
				Logger:       logger,
				Channel:      channel,
				Size:         poolSize,
				Interval:     pollInterval,
				QueryTimeout: timeout,
				BackoffBase:  backoffBase,
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
