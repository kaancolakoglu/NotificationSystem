package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/validate"
)

var priorities = map[Priority]int16{
	High:   0,
	Normal: 1,
	Low:    2,
}

var priorityWords = map[int16]Priority{
	0: High,
	1: Normal,
	2: Low,
}

const maxIdempotencyKeyBytes = 255

var errIdempotencyKeyUsed = errors.New("idempotency key already used")

type server struct {
	pool    *pgxpool.Pool
	queries *store.Queries
}

func Register(mux *http.ServeMux, pool *pgxpool.Pool, logger *slog.Logger) {
	requestErrorHandler := func(w http.ResponseWriter, r *http.Request, err error) {
		status, message := requestError(err)
		writeError(r.Context(), w, logger, status, message)
	}
	handler := NewStrictHandlerWithOptions(&server{pool: pool, queries: store.New(pool)}, nil, StrictHTTPServerOptions{
		RequestErrorHandlerFunc: requestErrorHandler,
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			logger.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "error", err)
			writeError(r.Context(), w, logger, http.StatusInternalServerError, "internal error")
		},
	})
	HandlerWithOptions(handler, StdHTTPServerOptions{
		BaseRouter:       mux,
		ErrorHandlerFunc: requestErrorHandler,
	})
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

	key := request.Params.IdempotencyKey
	if key != nil && (*key == "" || len(*key) > maxIdempotencyKeyBytes) {
		return CreateNotification400JSONResponse{Message: fmt.Sprintf("Idempotency-Key must be 1 to %d bytes long", maxIdempotencyKeyBytes)}, nil
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate id: %w", err)
	}
	insert := store.InsertNotificationParams{
		ID:        id,
		Recipient: body.Recipient,
		Channel:   string(body.Channel),
		Content:   body.Content,
		Priority:  priorityValue,
	}

	if key == nil {
		row, err := s.queries.InsertNotification(ctx, insert)
		if err != nil {
			return nil, fmt.Errorf("insert notification: %w", err)
		}
		notification, err := toNotification(row)
		if err != nil {
			return nil, err
		}
		return CreateNotification202JSONResponse(notification), nil
	}

	encoded, err := json.Marshal(CreateNotificationRequest{
		Recipient: body.Recipient,
		Channel:   body.Channel,
		Content:   body.Content,
		Priority:  &priority,
	})
	if err != nil {
		return nil, fmt.Errorf("encode request for hashing: %w", err)
	}
	requestHash := sha256.Sum256(encoded)
	return s.createWithKey(ctx, *key, requestHash[:], insert)
}

func (s *server) createWithKey(ctx context.Context, key string, requestHash []byte, insert store.InsertNotificationParams) (CreateNotificationResponseObject, error) {
	var notification Notification
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		queries := s.queries.WithTx(tx)
		row, err := queries.InsertNotification(ctx, insert)
		if err != nil {
			return fmt.Errorf("insert notification: %w", err)
		}
		notification, err = toNotification(row)
		if err != nil {
			return err
		}
		response, err := json.Marshal(notification)
		if err != nil {
			return fmt.Errorf("encode response: %w", err)
		}
		inserted, err := queries.InsertIdempotencyKey(ctx, store.InsertIdempotencyKeyParams{
			Key:         key,
			RequestHash: requestHash,
			Response:    response,
		})
		if err != nil {
			return fmt.Errorf("insert idempotency key: %w", err)
		}
		if inserted == 0 {
			return errIdempotencyKeyUsed
		}
		return nil
	})
	if err == nil {
		return CreateNotification202JSONResponse(notification), nil
	}
	if !errors.Is(err, errIdempotencyKeyUsed) {
		return nil, fmt.Errorf("create with idempotency key: %w", err)
	}

	stored, err := s.queries.GetIdempotencyKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get idempotency key: %w", err)
	}
	if !bytes.Equal(stored.RequestHash, requestHash) {
		return CreateNotification409JSONResponse{Message: "Idempotency-Key was already used with a different request body"}, nil
	}
	var first Notification
	if err := json.Unmarshal(stored.Response, &first); err != nil {
		return nil, fmt.Errorf("decode stored response: %w", err)
	}
	return CreateNotification202JSONResponse(first), nil
}

func (s *server) GetNotification(ctx context.Context, request GetNotificationRequestObject) (GetNotificationResponseObject, error) {
	row, err := s.queries.GetNotification(ctx, request.Id)
	if errors.Is(err, pgx.ErrNoRows) {
		return GetNotification404JSONResponse{Message: "notification not found"}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get notification: %w", err)
	}

	notification, err := toNotification(row)
	if err != nil {
		return nil, err
	}
	return GetNotification200JSONResponse(notification), nil
}

func toNotification(row store.Notification) (Notification, error) {
	priority, ok := priorityWords[row.Priority]
	if !ok {
		return Notification{}, fmt.Errorf("notification %s has unknown priority %d", row.ID, row.Priority)
	}
	return Notification{
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
	var tooMany *TooManyValuesForParamError
	if errors.As(err, &tooMany) {
		return http.StatusBadRequest, fmt.Sprintf("parameter %q must be sent once", tooMany.ParamName)
	}
	var invalidParam *InvalidParamFormatError
	if errors.As(err, &invalidParam) {
		return http.StatusBadRequest, fmt.Sprintf("parameter %q is not valid", invalidParam.ParamName)
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
