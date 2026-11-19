package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/validate"
)

var priorities = map[Priority]int16{
	High:   0,
	Normal: 1,
	Low:    2,
}

type server struct {
	queries *store.Queries
}

func Register(mux *http.ServeMux, queries *store.Queries, logger *slog.Logger) {
	handler := NewStrictHandlerWithOptions(&server{queries: queries}, nil, StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			status, message := requestError(err)
			writeError(r.Context(), w, logger, status, message)
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			logger.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "error", err)
			writeError(r.Context(), w, logger, http.StatusInternalServerError, "internal error")
		},
	})
	HandlerFromMux(handler, mux)
}

func (s *server) CreateNotification(ctx context.Context, request CreateNotificationRequestObject) (CreateNotificationResponseObject, error) {
	body := request.Body

	priority := Normal
	if body.Priority != nil {
		priority = *body.Priority
	}
	priorityValue, ok := priorities[priority]
	if !ok {
		return CreateNotification422JSONResponse{Message: "priority must be one of high, normal, low"}, nil
	}
	if err := validate.Notification(string(body.Channel), body.Recipient, body.Content); err != nil {
		return CreateNotification422JSONResponse{Message: err.Error()}, nil
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate id: %w", err)
	}
	row, err := s.queries.InsertNotification(ctx, store.InsertNotificationParams{
		ID:        id,
		Recipient: body.Recipient,
		Channel:   string(body.Channel),
		Content:   body.Content,
		Priority:  priorityValue,
	})
	if err != nil {
		return nil, fmt.Errorf("insert notification: %w", err)
	}

	return CreateNotification202JSONResponse{
		Id:        row.ID,
		Recipient: row.Recipient,
		Channel:   Channel(row.Channel),
		Content:   row.Content,
		Priority:  priority,
		Status:    Status(row.Status),
		CreatedAt: row.CreatedAt,
	}, nil
}

func requestError(err error) (int, string) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return http.StatusRequestEntityTooLarge, fmt.Sprintf("request body must be at most %d bytes", tooLarge.Limit)
	}
	var wrongType *json.UnmarshalTypeError
	if errors.As(err, &wrongType) && wrongType.Field != "" {
		return http.StatusBadRequest, fmt.Sprintf("field %q has the wrong type", wrongType.Field)
	}
	return http.StatusBadRequest, "request body must be a valid JSON object"
}

func writeError(ctx context.Context, w http.ResponseWriter, logger *slog.Logger, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(Error{Message: message}); err != nil {
		logger.ErrorContext(ctx, "write error response", "error", err)
	}
}
