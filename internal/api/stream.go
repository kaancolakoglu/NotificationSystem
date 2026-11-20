package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	streamWriteTimeout = 5 * time.Second
	streamMissedReason = "status events may have been missed: reconnect and fetch the status"
)

func (s *server) streamEvents(logger *slog.Logger, notFound string, exists func(ctx context.Context, id uuid.UUID) (bool, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			writeError(ctx, w, logger, http.StatusBadRequest, `parameter "id" is not valid`)
			return
		}
		found, err := exists(ctx, id)
		if err != nil {
			logger.ErrorContext(ctx, "request failed", "method", r.Method, "path", r.URL.Path, "error", err)
			writeError(ctx, w, logger, http.StatusInternalServerError, "internal error")
			return
		}
		if !found {
			writeError(ctx, w, logger, http.StatusNotFound, notFound)
			return
		}
		stream, unsubscribe, ok := s.hub.Subscribe(id)
		if !ok {
			writeError(ctx, w, logger, http.StatusServiceUnavailable, "status events are not available, try again later")
			return
		}
		defer unsubscribe()
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			logger.WarnContext(ctx, "accept websocket", "error", err)
			return
		}
		forward(ctx, logger, conn, stream)
	})
}

func forward(ctx context.Context, logger *slog.Logger, conn *websocket.Conn, stream <-chan string) {
	open := conn.CloseRead(ctx)
	for {
		select {
		case <-open.Done():
			return
		case payload, ok := <-stream:
			if !ok {
				if err := conn.Close(websocket.StatusTryAgainLater, streamMissedReason); err != nil && !errors.Is(err, net.ErrClosed) {
					logger.WarnContext(ctx, "close status stream", "error", err)
				}
				return
			}
			writeCtx, cancel := context.WithTimeout(open, streamWriteTimeout)
			err := conn.Write(writeCtx, websocket.MessageText, []byte(payload))
			cancel()
			if err == nil {
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) || open.Err() == nil {
				logger.WarnContext(ctx, "write status event, closing the stream", "error", err)
			}
			if err := conn.CloseNow(); err != nil && !errors.Is(err, net.ErrClosed) {
				logger.WarnContext(ctx, "close status stream", "error", err)
			}
			return
		}
	}
}

func (s *server) notificationExists(ctx context.Context, id uuid.UUID) (bool, error) {
	_, err := s.queries.GetNotification(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get notification: %w", err)
	}
	return true, nil
}

func (s *server) batchExists(ctx context.Context, id uuid.UUID) (bool, error) {
	counts, err := s.queries.CountBatchByStatus(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return false, fmt.Errorf("count batch by status: %w", err)
	}
	return counts.Total > 0, nil
}
