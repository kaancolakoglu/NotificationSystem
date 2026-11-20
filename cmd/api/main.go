package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	spec "github.com/kaancolakoglu/NotificationSystem/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/events"
	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
)

const (
	addr              = ":8080"
	readHeaderTimeout = 5 * time.Second
	readyTimeout      = time.Second
	queueCountTimeout = 5 * time.Second
	maxBodyBytes      = 1 << 20
	maxBatchBodyBytes = 10 << 20
)

var channels = []string{"sms", "email", "push"}

func main() {
	logger := obs.NewLogger(os.Stdout)

	err := run(context.Background(), logger, os.Getenv("DATABASE_URL"), os.Getenv("REDIS_URL"))
	logger.Error("api server exited", "error", err)
	os.Exit(1)
}

func run(ctx context.Context, logger *slog.Logger, databaseURL, redisURL string) error {
	if databaseURL == "" {
		return errors.New("DATABASE_URL is not set")
	}
	redisOptions, err := parseRedisURL(redisURL)
	if err != nil {
		return err
	}
	redis.SetLogger(obs.RedisLogger{Logger: logger})

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
	return newServer(pool, redisClient, hub, logger).ListenAndServe()
}

func parseRedisURL(redisURL string) (*redis.Options, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("REDIS_URL is not a Redis URL: %w", err)
	}
	options.ContextTimeoutEnabled = true
	return options, nil
}

func newServer(pool *pgxpool.Pool, redisClient *redis.Client, hub *events.Hub, logger *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           newHandler(pool, redisClient, hub, logger),
		ReadHeaderTimeout: readHeaderTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
}

func newHandler(pool *pgxpool.Pool, redisClient *redis.Client, hub *events.Hub, logger *slog.Logger) http.Handler {
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
	api.Register(mux, pool, hub, events.Publisher{Client: redisClient, Logger: logger}, logger)

	limited := http.NewServeMux()
	limited.Handle("/", http.MaxBytesHandler(mux, maxBodyBytes))
	limited.Handle("POST /v1/notifications/batch", http.MaxBytesHandler(mux, maxBatchBodyBytes))
	return limited
}
