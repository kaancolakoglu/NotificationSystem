package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/kaancolakoglu/NotificationSystem/internal/events"
	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
	"github.com/kaancolakoglu/NotificationSystem/internal/provider"
	"github.com/kaancolakoglu/NotificationSystem/internal/ratelimit"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/worker"
)

const (
	metricsAddr       = ":8082"
	serviceName       = "notification-worker"
	traceFlushTimeout = 2 * time.Second
	readHeaderTimeout = 5 * time.Second
	poolSize          = 20
	pollInterval      = 200 * time.Millisecond
	databaseTimeout   = 5 * time.Second
	backoffBase       = time.Second
	sendsPerSecond    = 90
	sendBurst         = 5
	breakerOpenFor    = 30 * time.Second
	shutdownTimeout   = 30 * time.Second
	sweepInterval     = 15 * time.Second
	keyExpiryInterval = time.Minute
	keyExpiryMaxKeys  = 100_000
)

var channels = []string{"sms", "email", "push"}

func main() {
	logger := obs.NewLogger(os.Stdout)
	hostname, err := os.Hostname()
	if err != nil {
		logger.Error("worker exited", "error", fmt.Errorf("read host name: %w", err))
		os.Exit(1)
	}
	logger = logger.With("worker", hostname)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if err := run(ctx, logger, os.Getenv("DATABASE_URL"), os.Getenv("REDIS_URL"), os.Getenv("PROVIDER_URL"), os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), metricsAddr, databaseTimeout); err != nil {
		logger.Error("worker exited", "error", err)
		os.Exit(1)
	}
	logger.Info("worker stopped")
}

func run(ctx context.Context, logger *slog.Logger, databaseURL, redisURL, providerURL, otlpEndpoint, metricsAddr string, timeout time.Duration) error {
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
	redis.SetLogger(obs.RedisLogger{Logger: logger})
	tracer, shutdownTracer, err := obs.NewTracer(ctx, logger, serviceName, otlpEndpoint)
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), traceFlushTimeout)
		defer cancel()
		if err := shutdownTracer(flushCtx); err != nil {
			logger.ErrorContext(ctx, "flush traces", "error", err)
		}
	}()

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

	registry := obs.NewRegistry()
	metrics := obs.NewWorkerMetrics(registry, channels)
	stopMetrics, err := serveMetrics(ctx, logger, metricsAddr, registry)
	if err != nil {
		return err
	}
	defer stopMetrics()

	queries := store.New(pool)
	limiter := ratelimit.New(redisClient, sendsPerSecond, sendBurst)
	client := provider.New(providerURL, len(channels)*poolSize, logger)
	publisher := events.Publisher{Client: redisClient, Logger: logger}
	var loops sync.WaitGroup
	for _, channel := range channels {
		loops.Go(func() {
			worker.Pool{
				Queries:         queries,
				Provider:        client,
				Limiter:         limiter,
				Logger:          logger,
				Metrics:         metrics,
				Events:          publisher,
				Tracer:          tracer,
				Channel:         channel,
				Size:            poolSize,
				Interval:        pollInterval,
				QueryTimeout:    timeout,
				BackoffBase:     backoffBase,
				BreakerOpenFor:  breakerOpenFor,
				ShutdownTimeout: shutdownTimeout,
			}.Run(ctx)
		})
	}
	loops.Go(func() {
		worker.Sweeper{
			Queries:      queries,
			Logger:       logger,
			Metrics:      metrics,
			Events:       publisher,
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
	<-ctx.Done()
	logger.InfoContext(ctx, "worker is stopping, waiting for the sends in flight")
	loops.Wait()
	return nil
}

func serveMetrics(ctx context.Context, logger *slog.Logger, addr string, registry *prometheus.Registry) (stop func(), err error) {
	var config net.ListenConfig
	listener, err := config.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for metrics on %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", obs.MetricsHandler(registry, logger))
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			logger.ErrorContext(ctx, "metrics server stopped", "error", err)
		}
	}()
	return func() {
		if err := server.Close(); err != nil {
			logger.ErrorContext(ctx, "close metrics server", "error", err)
		}
		<-stopped
	}, nil
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
