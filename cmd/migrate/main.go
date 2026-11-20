package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/kaancolakoglu/NotificationSystem/migrations"
)

const (
	migrateTimeout     = 5 * time.Minute
	migrateLockTimeout = 3 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(context.Background(), os.Getenv("DATABASE_URL"), migrateTimeout, migrateLockTimeout); err != nil {
		logger.Error("migrate failed", "error", err)
		os.Exit(1)
	}
	logger.Info("migrations applied")
}

func run(ctx context.Context, databaseURL string, timeout, lockTimeout time.Duration) (err error) {
	if databaseURL == "" {
		return errors.New("DATABASE_URL is not set")
	}
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	config.RuntimeParams["lock_timeout"] = strconv.FormatInt(lockTimeout.Milliseconds(), 10)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	db := stdlib.OpenDB(*config)
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	return migrations.Up(ctx, db)
}
