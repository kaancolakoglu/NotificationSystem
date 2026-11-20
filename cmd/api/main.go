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

	spec "github.com/kaancolakoglu/NotificationSystem/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
)

const (
	addr              = ":8080"
	readHeaderTimeout = 5 * time.Second
	maxBodyBytes      = 1 << 20
	maxBatchBodyBytes = 10 << 20
)

func main() {
	logger := obs.NewLogger(os.Stdout)

	err := run(context.Background(), logger, os.Getenv("DATABASE_URL"))
	logger.Error("api server exited", "error", err)
	os.Exit(1)
}

func run(ctx context.Context, logger *slog.Logger, databaseURL string) error {
	if databaseURL == "" {
		return errors.New("DATABASE_URL is not set")
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("create database pool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	return newServer(pool, logger).ListenAndServe()
}

func newServer(pool *pgxpool.Pool, logger *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           newHandler(pool, logger),
		ReadHeaderTimeout: readHeaderTimeout,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
}

func newHandler(pool *pgxpool.Pool, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		if _, err := w.Write(spec.Spec); err != nil {
			logger.ErrorContext(r.Context(), "write openapi spec", "error", err)
		}
	})
	api.Register(mux, pool, logger)

	limited := http.NewServeMux()
	limited.Handle("/", http.MaxBytesHandler(mux, maxBodyBytes))
	limited.Handle("POST /v1/notifications/batch", http.MaxBytesHandler(mux, maxBatchBodyBytes))
	return limited
}
