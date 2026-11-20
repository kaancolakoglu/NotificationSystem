package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oapi-codegen/runtime"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/kaancolakoglu/NotificationSystem/internal/events"
	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
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

const (
	maxIdempotencyKeyBytes = 255
	maxBatchSize           = 1000

	correlationIDHeader   = "X-Correlation-ID"
	traceParentHeader     = "traceparent"
	maxCorrelationIDBytes = 128

	defaultPageSize int32 = 50
	maxPageSize     int32 = 200

	idempotencyKeyConflictMessage = "Idempotency-Key was already used with a different request body"
)

var (
	idempotencyKeyLengthMessage = fmt.Sprintf("Idempotency-Key must be 1 to %d bytes long", maxIdempotencyKeyBytes)
	correlationIDMessage        = fmt.Sprintf("%s must be sent once and be 1 to %d printable ASCII characters without spaces", correlationIDHeader, maxCorrelationIDBytes)
)

var queryParameters = map[string][]string{
	"GET /v1/notifications": {"status", "channel", "created_from", "created_to", "batch_id", "limit", "cursor"},
}

var (
	errIdempotencyKeyUsed     = errors.New("idempotency key already used")
	errIdempotencyKeyConflict = errors.New("idempotency key already used with a different request")
)

type server struct {
	pool      *pgxpool.Pool
	queries   *store.Queries
	hub       *events.Hub
	publisher events.Publisher
}

func Register(mux *http.ServeMux, pool *pgxpool.Pool, hub *events.Hub, publisher events.Publisher, tracer trace.Tracer, logger *slog.Logger) {
	runtime.SetJSONCodec(strictJSON{})
	requestErrorHandler := func(w http.ResponseWriter, r *http.Request, err error) {
		status, message := requestError(err)
		writeError(r.Context(), w, logger, status, message)
	}
	s := &server{pool: pool, queries: store.New(pool), hub: hub, publisher: publisher}
	handler := NewStrictHandlerWithOptions(s, nil, StrictHTTPServerOptions{
		RequestErrorHandlerFunc: requestErrorHandler,
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			if response, ok := w.(*startedResponse); ok && response.status != 0 {
				logger.ErrorContext(r.Context(), "write response", "method", r.Method, "path", r.URL.Path, "status", response.status, "error", err)
				return
			}
			logger.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "error", err)
			writeError(r.Context(), w, logger, http.StatusInternalServerError, "internal error")
		},
	})
	v1 := http.NewServeMux()
	checkQuery := checkQueryParameters(logger)
	HandlerWithOptions(handler, StdHTTPServerOptions{
		BaseRouter:       v1,
		ErrorHandlerFunc: requestErrorHandler,
		Middlewares:      []MiddlewareFunc{checkQuery},
	})
	v1.Handle("GET /v1/notifications/{id}/events", checkQuery(s.streamEvents(logger, "notification not found", s.notificationExists)))
	v1.Handle("GET /v1/batches/{id}/events", checkQuery(s.streamEvents(logger, "batch not found", s.batchExists)))
	mux.Handle("/v1/", correlate(logger, tracer, v1))
}

type startedResponse struct {
	http.ResponseWriter
	status int
}

func (w *startedResponse) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *startedResponse) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func correlate(logger *slog.Logger, tracer trace.Tracer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		response := &startedResponse{ResponseWriter: w}
		sent := r.Header.Values(correlationIDHeader)
		valid := len(sent) == 1 && validCorrelationID(sent[0])
		var id string
		if valid {
			id = sent[0]
		} else {
			generated, err := uuid.NewV7()
			if err != nil {
				logger.ErrorContext(r.Context(), "generate correlation id", "method", r.Method, "path", r.URL.Path, "error", err)
				writeError(r.Context(), response, logger, http.StatusInternalServerError, "internal error")
				return
			}
			id = generated.String()
		}
		ctx := obs.WithTraceParent(obs.WithCorrelationID(r.Context(), id), r.Header.Get(traceParentHeader))
		ctx, span := tracer.Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("correlation_id", id),
		))
		request := r.WithContext(ctx)
		request.Pattern = ""
		response.Header().Set(correlationIDHeader, id)
		defer func() {
			if request.Pattern != "" {
				span.SetName(request.Pattern)
			}
			span.SetAttributes(attribute.Int("http.response.status_code", response.status))
			if response.status >= http.StatusInternalServerError {
				span.SetStatus(codes.Error, http.StatusText(response.status))
			}
			span.End()
			logger.InfoContext(ctx, "request handled", "method", r.Method, "path", r.URL.Path, "status", response.status, "duration_seconds", time.Since(started).Seconds())
		}()
		if len(sent) > 0 && !valid {
			writeError(ctx, response, logger, http.StatusBadRequest, correlationIDMessage)
			return
		}
		next.ServeHTTP(response, request)
	})
}

func validCorrelationID(id string) bool {
	if id == "" || len(id) > maxCorrelationIDBytes {
		return false
	}
	for _, char := range []byte(id) {
		if char < '!' || char > '~' {
			return false
		}
	}
	return true
}

func checkQueryParameters(logger *slog.Logger) MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			query, err := url.ParseQuery(r.URL.RawQuery)
			if err != nil {
				writeError(r.Context(), w, logger, http.StatusBadRequest, "query string is not valid")
				return
			}
			for _, name := range slices.Sorted(maps.Keys(query)) {
				if !slices.Contains(queryParameters[r.Pattern], name) {
					writeError(r.Context(), w, logger, http.StatusBadRequest, fmt.Sprintf("parameter %q is not known", name))
					return
				}
				if len(query[name]) > 1 {
					writeError(r.Context(), w, logger, http.StatusBadRequest, fmt.Sprintf("parameter %q must be sent once", name))
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

func (s *server) CreateNotification(ctx context.Context, request CreateNotificationRequestObject) (CreateNotificationResponseObject, error) {
	body := *request.Body
	priority := priorityOf(body)
	body.Priority = &priority

	templates, err := s.templatesOf(ctx, []CreateNotificationRequest{body})
	if err != nil {
		return nil, err
	}
	priorityValue, content, err := validateRequest(body, priority, templates)
	if err != nil {
		return CreateNotification422JSONResponse{Message: err.Error()}, nil
	}
	key := request.Params.IdempotencyKey
	if !validIdempotencyKey(key) {
		return CreateNotification400JSONResponse{Message: idempotencyKeyLengthMessage}, nil
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate id: %w", err)
	}
	notification, err := createOnce(ctx, s, key, body, func(queries *store.Queries) (Notification, error) {
		row, err := queries.InsertNotification(ctx, store.InsertNotificationParams{
			ID:            id,
			Recipient:     body.Recipient,
			Channel:       string(body.Channel),
			Content:       content,
			Priority:      priorityValue,
			CorrelationID: pgtype.Text{String: obs.CorrelationID(ctx), Valid: true},
			TraceContext:  traceContext(ctx),
			ScheduledAt:   timestamptz(body.ScheduledAt),
		})
		if err != nil {
			return Notification{}, fmt.Errorf("insert notification: %w", err)
		}
		return toNotification(row)
	})
	if errors.Is(err, errIdempotencyKeyConflict) {
		return CreateNotification409JSONResponse{Message: idempotencyKeyConflictMessage}, nil
	}
	if err != nil {
		return nil, err
	}
	return CreateNotification202JSONResponse(notification), nil
}

func (s *server) CreateBatch(ctx context.Context, request CreateBatchRequestObject) (CreateBatchResponseObject, error) {
	body := *request.Body
	if len(body.Notifications) == 0 || len(body.Notifications) > maxBatchSize {
		return CreateBatch422JSONResponse{Message: fmt.Sprintf("a batch must hold 1 to %d notifications", maxBatchSize)}, nil
	}

	batchID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate batch id: %w", err)
	}
	templates, err := s.templatesOf(ctx, body.Notifications)
	if err != nil {
		return nil, err
	}
	results := make([]BatchItemResult, len(body.Notifications))
	inserts := make([]store.InsertBatchNotificationsParams, 0, len(body.Notifications))
	for i, item := range body.Notifications {
		priority := priorityOf(item)
		body.Notifications[i].Priority = &priority
		results[i].Index = i

		priorityValue, content, err := validateRequest(item, priority, templates)
		if err != nil {
			message := err.Error()
			results[i].Error = &message
			continue
		}
		id, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("generate id: %w", err)
		}
		results[i].Id = &id
		var scheduledAt time.Time
		if item.ScheduledAt != nil {
			scheduledAt = *item.ScheduledAt
		}
		inserts = append(inserts, store.InsertBatchNotificationsParams{
			ID:            id,
			Recipient:     item.Recipient,
			Channel:       string(item.Channel),
			Content:       content,
			Priority:      priorityValue,
			BatchID:       pgtype.UUID{Bytes: batchID, Valid: true},
			CorrelationID: pgtype.Text{String: obs.CorrelationID(ctx), Valid: true},
			TraceContext:  traceContext(ctx),
			NextAttemptAt: scheduledAt,
		})
	}
	if len(inserts) == 0 {
		return CreateBatch422JSONResponse{Message: "no notification in the batch is valid", Results: &results}, nil
	}
	key := request.Params.IdempotencyKey
	if !validIdempotencyKey(key) {
		return CreateBatch400JSONResponse{Message: idempotencyKeyLengthMessage}, nil
	}

	created, err := createOnce(ctx, s, key, body, func(queries *store.Queries) (BatchCreated, error) {
		now, err := queries.GetDatabaseTime(ctx)
		if err != nil {
			return BatchCreated{}, fmt.Errorf("get database time: %w", err)
		}
		for i := range inserts {
			if inserts[i].NextAttemptAt.Before(now) {
				inserts[i].NextAttemptAt = now
			}
		}
		if _, err := queries.InsertBatchNotifications(ctx, inserts); err != nil {
			return BatchCreated{}, fmt.Errorf("insert batch notifications: %w", err)
		}
		return BatchCreated{BatchId: batchID, Results: results}, nil
	})
	if errors.Is(err, errIdempotencyKeyConflict) {
		return CreateBatch409JSONResponse{Message: idempotencyKeyConflictMessage}, nil
	}
	if err != nil {
		return nil, err
	}
	return CreateBatch202JSONResponse(created), nil
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
	attempts, err := s.queries.ListDeliveryAttempts(ctx, request.Id)
	if err != nil {
		return nil, fmt.Errorf("list delivery attempts: %w", err)
	}
	deliveryAttempts := make([]DeliveryAttempt, len(attempts))
	for i, attempt := range attempts {
		deliveryAttempts[i] = DeliveryAttempt{
			Attempt:   int(attempt.Attempt),
			Error:     textPointer(attempt.Error),
			CreatedAt: attempt.CreatedAt,
		}
	}
	return GetNotification200JSONResponse{
		Id:               notification.Id,
		Recipient:        notification.Recipient,
		Channel:          notification.Channel,
		Content:          notification.Content,
		Priority:         notification.Priority,
		Status:           notification.Status,
		CreatedAt:        notification.CreatedAt,
		BatchId:          notification.BatchId,
		Attempts:         notification.Attempts,
		LastError:        notification.LastError,
		DeliveryAttempts: deliveryAttempts,
	}, nil
}

func (s *server) CancelNotification(ctx context.Context, request CancelNotificationRequestObject) (CancelNotificationResponseObject, error) {
	row, err := s.queries.CancelNotification(ctx, request.Id)
	if errors.Is(err, pgx.ErrNoRows) {
		_, err := s.queries.GetNotification(ctx, request.Id)
		if errors.Is(err, pgx.ErrNoRows) {
			return CancelNotification404JSONResponse{Message: "notification not found"}, nil
		}
		if err != nil {
			return nil, fmt.Errorf("get notification after cancel changed no row: %w", err)
		}
		return CancelNotification409JSONResponse{Message: "only a pending notification can be cancelled"}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cancel notification: %w", err)
	}

	s.publisher.Publish(ctx, events.New(row.ID, row.BatchID, row.Status, row.Attempts))
	notification, err := toNotification(row)
	if err != nil {
		return nil, err
	}
	return CancelNotification200JSONResponse(notification), nil
}

func (s *server) ListNotifications(ctx context.Context, request ListNotificationsRequestObject) (ListNotificationsResponseObject, error) {
	params := request.Params
	limit := defaultPageSize
	if params.Limit != nil {
		limit = *params.Limit
	}
	if limit < 1 || limit > maxPageSize {
		return ListNotifications400JSONResponse{Message: fmt.Sprintf("limit must be 1 to %d", maxPageSize)}, nil
	}
	var status, channel pgtype.Text
	if params.Status != nil {
		if !params.Status.Valid() {
			return ListNotifications400JSONResponse{Message: "status must be one of pending, processing, sent, failed, cancelled"}, nil
		}
		status = pgtype.Text{String: string(*params.Status), Valid: true}
	}
	if params.Channel != nil {
		if !params.Channel.Valid() {
			return ListNotifications400JSONResponse{Message: "channel must be one of sms, email, push"}, nil
		}
		channel = pgtype.Text{String: string(*params.Channel), Valid: true}
	}

	var minID, maxID pgtype.UUID
	if params.CreatedFrom != nil {
		if params.CreatedFrom.IsZero() {
			return ListNotifications400JSONResponse{Message: `parameter "created_from" is not valid`}, nil
		}
		minID = pgtype.UUID{Bytes: firstIDAt(*params.CreatedFrom), Valid: true}
	}
	if params.CreatedTo != nil {
		if params.CreatedTo.IsZero() {
			return ListNotifications400JSONResponse{Message: `parameter "created_to" is not valid`}, nil
		}
		maxID = pgtype.UUID{Bytes: firstIDAt(*params.CreatedTo), Valid: true}
	}
	if params.CreatedFrom != nil && params.CreatedTo != nil && !params.CreatedFrom.Before(*params.CreatedTo) {
		return ListNotifications400JSONResponse{Message: "created_from must be before created_to"}, nil
	}
	if params.Cursor != nil {
		cursor, err := decodeCursor(*params.Cursor)
		if err != nil {
			return ListNotifications400JSONResponse{Message: "cursor is not valid"}, nil
		}
		if !maxID.Valid || bytes.Compare(cursor[:], maxID.Bytes[:]) < 0 {
			maxID = pgtype.UUID{Bytes: cursor, Valid: true}
		}
	}

	var rows []store.Notification
	var err error
	if params.BatchId != nil {
		rows, err = s.queries.ListBatchNotifications(ctx, store.ListBatchNotificationsParams{
			BatchID:  pgtype.UUID{Bytes: *params.BatchId, Valid: true},
			MinID:    minID,
			MaxID:    maxID,
			Status:   status,
			Channel:  channel,
			RowLimit: limit + 1,
		})
	} else {
		rows, err = s.queries.ListNotifications(ctx, store.ListNotificationsParams{
			MinID:    minID,
			MaxID:    maxID,
			Status:   status,
			Channel:  channel,
			RowLimit: limit + 1,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}

	var nextCursor *string
	if len(rows) > int(limit) {
		rows = rows[:limit]
		cursor := encodeCursor(rows[limit-1].ID)
		nextCursor = &cursor
	}
	notifications := make([]Notification, 0, len(rows))
	for _, row := range rows {
		notification, err := toNotification(row)
		if err != nil {
			return nil, err
		}
		notifications = append(notifications, notification)
	}
	return ListNotifications200JSONResponse{Notifications: notifications, NextCursor: nextCursor}, nil
}

func (s *server) GetBatch(ctx context.Context, request GetBatchRequestObject) (GetBatchResponseObject, error) {
	counts, err := s.queries.CountBatchByStatus(ctx, pgtype.UUID{Bytes: request.Id, Valid: true})
	if err != nil {
		return nil, fmt.Errorf("count batch by status: %w", err)
	}
	if counts.Total == 0 {
		return GetBatch404JSONResponse{Message: "batch not found"}, nil
	}
	return GetBatch200JSONResponse{
		Id:         request.Id,
		Total:      counts.Total,
		Pending:    counts.Pending,
		Processing: counts.Processing,
		Sent:       counts.Sent,
		Failed:     counts.Failed,
		Cancelled:  counts.Cancelled,
	}, nil
}

func createOnce[T any](ctx context.Context, s *server, key *string, request any, create func(*store.Queries) (T, error)) (T, error) {
	var zero T
	if key == nil {
		return create(s.queries)
	}

	encoded, err := json.Marshal(request)
	if err != nil {
		return zero, fmt.Errorf("encode request for hashing: %w", err)
	}
	requestHash := sha256.Sum256(encoded)

	var created T
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		queries := s.queries.WithTx(tx)
		var err error
		created, err = create(queries)
		if err != nil {
			return err
		}
		response, err := json.Marshal(created)
		if err != nil {
			return fmt.Errorf("encode response: %w", err)
		}
		inserted, err := queries.InsertIdempotencyKey(ctx, store.InsertIdempotencyKeyParams{
			Key:         *key,
			RequestHash: requestHash[:],
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
		return created, nil
	}
	if !errors.Is(err, errIdempotencyKeyUsed) {
		return zero, fmt.Errorf("create with idempotency key: %w", err)
	}

	stored, err := s.queries.GetIdempotencyKey(ctx, *key)
	if err != nil {
		return zero, fmt.Errorf("get idempotency key: %w", err)
	}
	if !bytes.Equal(stored.RequestHash, requestHash[:]) {
		return zero, errIdempotencyKeyConflict
	}
	var first T
	if err := json.Unmarshal(stored.Response, &first); err != nil {
		return zero, fmt.Errorf("decode stored response: %w", err)
	}
	return first, nil
}

func priorityOf(body CreateNotificationRequest) Priority {
	if body.Priority == nil {
		return Normal
	}
	return *body.Priority
}

func traceContext(ctx context.Context) pgtype.Text {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return pgtype.Text{}
	}
	return pgtype.Text{String: obs.TraceParent(ctx), Valid: true}
}

func timestamptz(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *t, Valid: true}
}

func validateRequest(body CreateNotificationRequest, priority Priority, templates map[uuid.UUID]string) (priorityValue int16, content string, err error) {
	priorityValue, ok := priorities[priority]
	if !ok {
		return 0, "", errors.New("priority must be one of high, normal, low")
	}
	content, err = contentOf(body, templates)
	if err != nil {
		return 0, "", err
	}
	if err := validate.Notification(string(body.Channel), body.Recipient, content); err != nil {
		return 0, "", err
	}
	return priorityValue, content, nil
}

func firstIDAt(t time.Time) uuid.UUID {
	millis := t.UnixMilli()
	if t.After(time.UnixMilli(millis)) {
		millis++
	}
	if millis < 0 {
		millis = 0
	}
	var id uuid.UUID
	binary.BigEndian.PutUint64(id[:8], uint64(millis)<<16)
	return id
}

func encodeCursor(id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString(id[:])
}

func decodeCursor(cursor string) (uuid.UUID, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return uuid.Nil, fmt.Errorf("decode cursor: %w", err)
	}
	id, err := uuid.FromBytes(decoded)
	if err != nil {
		return uuid.Nil, fmt.Errorf("decode cursor: %w", err)
	}
	return id, nil
}

func validIdempotencyKey(key *string) bool {
	return key == nil || (*key != "" && len(*key) <= maxIdempotencyKeyBytes)
}

func toNotification(row store.Notification) (Notification, error) {
	priority, ok := priorityWords[row.Priority]
	if !ok {
		return Notification{}, fmt.Errorf("notification %s has unknown priority %d", row.ID, row.Priority)
	}
	var batchID *uuid.UUID
	if row.BatchID.Valid {
		id := uuid.UUID(row.BatchID.Bytes)
		batchID = &id
	}
	return Notification{
		Id:        row.ID,
		Recipient: row.Recipient,
		Channel:   Channel(row.Channel),
		Content:   row.Content,
		Priority:  priority,
		Status:    Status(row.Status),
		CreatedAt: row.CreatedAt,
		BatchId:   batchID,
		Attempts:  int(row.Attempts),
		LastError: textPointer(row.LastError),
	}, nil
}

func textPointer(text pgtype.Text) *string {
	if !text.Valid {
		return nil
	}
	return &text.String
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
	var unknownField *unknownFieldError
	if errors.As(err, &unknownField) {
		return http.StatusBadRequest, fmt.Sprintf("field %s is not known", unknownField.quotedName)
	}
	var tooMany *TooManyValuesForParamError
	if errors.As(err, &tooMany) {
		return http.StatusBadRequest, fmt.Sprintf("parameter %q must be sent once", tooMany.ParamName)
	}
	var invalidParam *InvalidParamFormatError
	if errors.As(err, &invalidParam) {
		return http.StatusBadRequest, fmt.Sprintf("parameter %q is not valid", invalidParam.ParamName)
	}
	var badTime *time.ParseError
	if errors.As(err, &badTime) {
		return http.StatusBadRequest, "scheduled_at must be a date and time in RFC 3339, for example 2026-01-02T15:04:05Z"
	}
	if strings.Contains(err.Error(), "invalid UUID") || strings.Contains(err.Error(), "invalid urn prefix") {
		return http.StatusBadRequest, "template_id must be a UUID"
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
