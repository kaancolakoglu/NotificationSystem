package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"

	spec "github.com/kaancolakoglu/NotificationSystem/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/events"
	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
)

const (
	addr              = ":8080"
	serviceName       = "notification-api"
	shutdownTimeout   = 10 * time.Second
	traceFlushTimeout = 2 * time.Second
	readHeaderTimeout = 5 * time.Second
	readyTimeout      = time.Second
	queueCountTimeout = 5 * time.Second
	maxBodyBytes      = 1 << 20
	maxBatchBodyBytes = 10 << 20
)

var channels = []string{"sms", "email", "push"}

func main() {
	logger := obs.NewLogger(os.Stdout)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if err := run(ctx, logger, os.Getenv("DATABASE_URL"), os.Getenv("REDIS_URL"), os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), addr, shutdownTimeout); err != nil {
		logger.Error("api server exited", "error", err)
		os.Exit(1)
	}
	logger.Info("api stopped")
}

func run(ctx context.Context, logger *slog.Logger, databaseURL, redisURL, otlpEndpoint, addr string, timeout time.Duration) error {
	if databaseURL == "" {
		return errors.New("DATABASE_URL is not set")
	}
	redisOptions, err := parseRedisURL(redisURL)
	if err != nil {
		return err
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

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	redisClient := redis.NewClient(redisOptions)
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.ErrorContext(ctx, "close redis client", "error", err)
		}
	}()

	hub := events.NewHub(redisClient, logger)
	go hub.Run(ctx)
	return serve(ctx, logger, newServer(pool, redisClient, hub, tracer, logger), addr, timeout)
}

func serve(ctx context.Context, logger *slog.Logger, server *http.Server, addr string, timeout time.Duration) error {
	var config net.ListenConfig
	listener, err := config.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	requests, cancelRequests := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRequests()
	server.BaseContext = func(net.Listener) context.Context { return requests }
	var handlers sync.WaitGroup
	handler := server.Handler
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		handler.ServeHTTP(w, r)
	})
	failed := make(chan error, 1)
	go func() { failed <- server.Serve(listener) }()
	select {
	case err := <-failed:
		return fmt.Errorf("serve on %s: %w", addr, err)
	case <-ctx.Done():
	}
	logger.InfoContext(ctx, "api is stopping, waiting for the requests in flight")
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	if err := server.Shutdown(stopCtx); err != nil {
		return fmt.Errorf("shut down within %s of the stop signal: %w", timeout, err)
	}
	streamsClosed := make(chan struct{})
	go func() {
		defer close(streamsClosed)
		handlers.Wait()
	}()
	select {
	case <-streamsClosed:
		return nil
	case <-stopCtx.Done():
		return fmt.Errorf("status streams still open %s after the stop signal", timeout)
	}
}

func parseRedisURL(redisURL string) (*redis.Options, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("REDIS_URL is not a Redis URL: %w", err)
	}
	options.ContextTimeoutEnabled = true
	return options, nil
}

func newServer(pool *pgxpool.Pool, redisClient *redis.Client, hub *events.Hub, tracer trace.Tracer, logger *slog.Logger) *http.Server {
	return &http.Server{
		Handler:           newHandler(pool, redisClient, hub, tracer, logger),
		ReadHeaderTimeout: readHeaderTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
}

func newHandler(pool *pgxpool.Pool, redisClient *redis.Client, hub *events.Hub, tracer trace.Tracer, logger *slog.Logger) http.Handler {
	registry := obs.NewRegistry()
	registry.MustRegister(obs.QueueCollector{Queries: store.New(pool), Channels: channels, Timeout: queueCountTimeout})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		dependencies := []struct {
			name string
			ping func(ctx context.Context) error
		}{
			{name: "postgres", ping: pool.Ping},
			{name: "redis", ping: func(ctx context.Context) error { return redisClient.Ping(ctx).Err() }},
		}
		for _, dependency := range dependencies {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), readyTimeout)
			err := dependency.ping(ctx)
			cancel()
			if err != nil {
				logger.ErrorContext(ctx, "not ready", "dependency", dependency.name, "error", err)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /metrics", obs.MetricsHandler(registry, logger))
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		if _, err := w.Write(spec.Spec); err != nil {
			logger.ErrorContext(r.Context(), "write openapi spec", "error", err)
		}
	})
	api.Register(mux, pool, hub, events.Publisher{Client: redisClient, Logger: logger}, tracer, logger)

	limited := http.NewServeMux()
	limited.Handle("/", http.MaxBytesHandler(mux, maxBodyBytes))
	limited.Handle("POST /v1/notifications/batch", http.MaxBytesHandler(mux, maxBatchBodyBytes))
	return limited
}
