package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/kaancolakoglu/NotificationSystem/migrations"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(context.Background(), os.Getenv("DATABASE_URL")); err != nil {
		logger.Error("migrate failed", "error", err)
		os.Exit(1)
	}
	logger.Info("migrations applied")
}

func run(ctx context.Context, databaseURL string) (err error) {
	if databaseURL == "" {
		return errors.New("DATABASE_URL is not set")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	return migrations.Up(ctx, db)
}
