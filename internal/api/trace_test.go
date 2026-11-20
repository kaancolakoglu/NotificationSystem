package api_test

import (
	"log/slog"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/kaancolakoglu/NotificationSystem/internal/api"
	"github.com/kaancolakoglu/NotificationSystem/internal/events"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

const (
	callerTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	callerSpanID      = "00f067aa0ba902b7"
	callerTraceParent = "00-" + callerTraceID + "-" + callerSpanID + "-01"
)

func newTracedServer(t *testing.T) (*http.ServeMux, *pgxpool.Pool, *tracetest.SpanRecorder) {
	t.Helper()
	pool := testdb.New(t)
	logger := slog.New(slog.DiscardHandler)
	recorder := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)).Tracer("test")
	mux := http.NewServeMux()
	api.Register(mux, pool, nil, events.Publisher{Client: testredis.New(t), Logger: logger}, tracer, logger)
	return mux, pool, recorder
}

func onlySpan(t *testing.T, recorder *tracetest.SpanRecorder) sdktrace.ReadOnlySpan {
	t.Helper()
	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("%d spans ended, want 1", len(spans))
	}
	return spans[0]
}

func traceParentOf(span sdktrace.ReadOnlySpan) string {
	return "00-" + span.SpanContext().TraceID().String() + "-" + span.SpanContext().SpanID().String() + "-01"
}

func storedTraceContext(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) pgtype.Text {
	t.Helper()
	row, err := store.New(pool).GetNotification(t.Context(), id)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	return row.TraceContext
}

func wantAttributes(t *testing.T, span sdktrace.ReadOnlySpan, want ...attribute.KeyValue) {
	t.Helper()
	got := map[attribute.Key]attribute.Value{}
	for _, attr := range span.Attributes() {
		got[attr.Key] = attr.Value
	}
	for _, attr := range want {
		if got[attr.Key] != attr.Value {
			t.Errorf("attribute %s of the span = %v, want %v", attr.Key, got[attr.Key].String(), attr.Value.String())
		}
	}
}

func TestCreateIsTracedAndTheNotificationKeepsTheTraceContext(t *testing.T) {
	mux, pool, recorder := newTracedServer(t)

	rec := send(t, mux, http.MethodPost, "/v1/notifications", smsBody, map[string][]string{correlationIDHeader: {"order-42"}})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
	}
	span := onlySpan(t, recorder)
	if span.Name() != "POST /v1/notifications" || span.SpanKind() != trace.SpanKindServer || span.Parent().IsValid() || span.Status().Code != codes.Unset {
		t.Errorf("span = %q of kind %s with parent %+v and status %+v, want POST /v1/notifications, a server span without a parent or an error", span.Name(), span.SpanKind(), span.Parent(), span.Status())
	}
	wantAttributes(t, span,
		attribute.String("http.request.method", http.MethodPost),
		attribute.Int("http.response.status_code", http.StatusAccepted),
		attribute.String("correlation_id", "order-42"),
	)
	created := decodeBody[api.Notification](t, rec)
	if got, want := storedTraceContext(t, pool, created.Id), (pgtype.Text{String: traceParentOf(span), Valid: true}); got != want {
		t.Errorf("stored trace context = %+v, want the request span %+v", got, want)
	}
}

func TestRequestContinuesTheTraceOfItsCaller(t *testing.T) {
	mux, pool, recorder := newTracedServer(t)

	rec := send(t, mux, http.MethodPost, "/v1/notifications", smsBody, map[string][]string{"Traceparent": {callerTraceParent}})

	span := onlySpan(t, recorder)
	if span.SpanContext().TraceID().String() != callerTraceID || span.Parent().SpanID().String() != callerSpanID || !span.Parent().IsRemote() {
		t.Errorf("span has the trace %s and the parent %+v, want the trace %s and the remote parent %s", span.SpanContext().TraceID(), span.Parent(), callerTraceID, callerSpanID)
	}
	created := decodeBody[api.Notification](t, rec)
	if got, want := storedTraceContext(t, pool, created.Id), (pgtype.Text{String: traceParentOf(span), Valid: true}); got != want {
		t.Errorf("stored trace context = %+v, want the request span %+v, not the caller's", got, want)
	}
}

func TestTraceParentThatIsNotValidStartsANewTrace(t *testing.T) {
	mux, _, recorder := newTracedServer(t)

	rec := send(t, mux, http.MethodPost, "/v1/notifications", smsBody, map[string][]string{"Traceparent": {"order-42"}})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
	}
	if span := onlySpan(t, recorder); span.Parent().IsValid() || !span.SpanContext().IsValid() {
		t.Errorf("span = %+v with parent %+v, want a new trace without a parent", span.SpanContext(), span.Parent())
	}
}

func TestBatchNotificationsShareTheTraceContextOfTheRequest(t *testing.T) {
	mux, pool, recorder := newTracedServer(t)

	rec := postBatch(t, mux, batchBody(smsBody, smsBody))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
	}
	span := onlySpan(t, recorder)
	if span.Name() != "POST /v1/notifications/batch" {
		t.Errorf("span = %q, want POST /v1/notifications/batch", span.Name())
	}
	for _, result := range decodeBody[api.BatchCreated](t, rec).Results {
		if got, want := storedTraceContext(t, pool, *result.Id), (pgtype.Text{String: traceParentOf(span), Valid: true}); got != want {
			t.Errorf("stored trace context of item %d = %+v, want the request span %+v", result.Index, got, want)
		}
	}
}

func TestReplayIsTracedAndTheNotificationKeepsTheFirstTraceContext(t *testing.T) {
	mux, pool, recorder := newTracedServer(t)

	first := postNotification(t, mux, smsBody, "key-1")
	replay := postNotification(t, mux, smsBody, "key-1")

	spans := recorder.Ended()
	if len(spans) != 2 || spans[0].SpanContext().TraceID() == spans[1].SpanContext().TraceID() {
		t.Fatalf("%d spans ended, want 2 in different traces", len(spans))
	}
	created := decodeBody[api.Notification](t, first)
	if replayed := decodeBody[api.Notification](t, replay); replayed.Id != created.Id {
		t.Fatalf("replay answered the notification %s, want %s", replayed.Id, created.Id)
	}
	if got, want := storedTraceContext(t, pool, created.Id), (pgtype.Text{String: traceParentOf(spans[0]), Valid: true}); got != want {
		t.Errorf("stored trace context = %+v, want the first request's %+v", got, want)
	}
}

func TestSpanNamesTheRouteAndReportsTheStatus(t *testing.T) {
	unknown := uuid.Must(uuid.NewV7()).String()
	tests := []struct {
		name       string
		method     string
		path       string
		header     map[string][]string
		closePool  bool
		wantName   string
		wantStatus int
		wantCode   codes.Code
	}{
		{name: "route with a parameter", method: http.MethodGet, path: "/v1/notifications/" + unknown, wantName: "GET /v1/notifications/{id}", wantStatus: http.StatusNotFound, wantCode: codes.Unset},
		{name: "stream route", method: http.MethodGet, path: "/v1/batches/" + unknown + "/events", wantName: "GET /v1/batches/{id}/events", wantStatus: http.StatusNotFound, wantCode: codes.Unset},
		{name: "no route", method: http.MethodGet, path: "/v1/nothing", wantName: "GET", wantStatus: http.StatusNotFound, wantCode: codes.Unset},
		{name: "correlation id that is not valid", method: http.MethodGet, path: "/v1/notifications", header: map[string][]string{correlationIDHeader: {"not valid"}}, wantName: "GET", wantStatus: http.StatusBadRequest, wantCode: codes.Unset},
		{name: "server error", method: http.MethodGet, path: "/v1/notifications/" + unknown, closePool: true, wantName: "GET /v1/notifications/{id}", wantStatus: http.StatusInternalServerError, wantCode: codes.Error},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux, pool, recorder := newTracedServer(t)
			if tt.closePool {
				pool.Close()
			}

			rec := send(t, mux, tt.method, tt.path, "", tt.header)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body %s", rec.Code, tt.wantStatus, rec.Body)
			}
			span := onlySpan(t, recorder)
			if span.Name() != tt.wantName || span.Status().Code != tt.wantCode {
				t.Errorf("span = %q with status %+v, want %q with the status code %s", span.Name(), span.Status(), tt.wantName, tt.wantCode)
			}
			wantAttributes(t, span, attribute.Int("http.response.status_code", tt.wantStatus), attribute.String("correlation_id", rec.Header().Get(correlationIDHeader)))
		})
	}
}

func TestNotificationHasNoTraceContextWhenTracingIsOff(t *testing.T) {
	for name, header := range map[string]map[string][]string{
		"caller sends no trace":  nil,
		"caller sends its trace": {"Traceparent": {callerTraceParent}},
	} {
		t.Run(name, func(t *testing.T) {
			mux, pool := newTestServer(t)

			single := send(t, mux, http.MethodPost, "/v1/notifications", smsBody, header)
			batch := send(t, mux, http.MethodPost, "/v1/notifications/batch", batchBody(smsBody), header)

			ids := []uuid.UUID{decodeBody[api.Notification](t, single).Id, *decodeBody[api.BatchCreated](t, batch).Results[0].Id}
			for _, id := range ids {
				if got := storedTraceContext(t, pool, id); got.Valid {
					t.Errorf("stored trace context of %s = %+v, want null", id, got)
				}
			}
		})
	}
}

func TestNotificationHasNoTraceContextWhenTheCallerDidNotSampleItsTrace(t *testing.T) {
	mux, pool, recorder := newTracedServer(t)

	rec := send(t, mux, http.MethodPost, "/v1/notifications", smsBody, map[string][]string{"Traceparent": {"00-" + callerTraceID + "-" + callerSpanID + "-00"}})

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body %s", rec.Code, http.StatusAccepted, rec.Body)
	}
	if spans := recorder.Ended(); len(spans) != 0 {
		t.Errorf("%d spans were recorded, want none: the caller did not sample the trace", len(spans))
	}
	if got := storedTraceContext(t, pool, decodeBody[api.Notification](t, rec).Id); got.Valid {
		t.Errorf("stored trace context = %+v, want null: the worker would start spans nobody records", got)
	}
}
