package worker

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/kaancolakoglu/NotificationSystem/internal/ratelimit"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
)

const (
	requestTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	requestSpanID      = "00f067aa0ba902b7"
	requestTraceParent = "00-" + requestTraceID + "-" + requestSpanID + "-01"
)

func newTracer() (trace.Tracer, *tracetest.SpanRecorder) {
	recorder := tracetest.NewSpanRecorder()
	return sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)).Tracer("test"), recorder
}

func setTraceContext(t *testing.T, db *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	if _, err := db.Exec(t.Context(), "UPDATE notifications SET trace_context = $1 WHERE id = $2", requestTraceParent, id); err != nil {
		t.Fatalf("set the trace context: %v", err)
	}
}

type wantSpan struct {
	name      string
	kind      trace.SpanKind
	attempt   int
	wantError string
}

func checkSpans(t *testing.T, recorder *tracetest.SpanRecorder, notification store.Notification, want []wantSpan) {
	t.Helper()
	spans := recorder.Ended()
	if len(spans) != len(want) {
		names := make([]string, len(spans))
		for i, span := range spans {
			names[i] = span.Name()
		}
		t.Fatalf("spans = %v, want %d", names, len(want))
	}
	for i, wanted := range want {
		span := spans[i]
		if span.Name() != wanted.name || span.SpanKind() != wanted.kind {
			t.Errorf("span %d = %q of kind %s, want %q of kind %s", i, span.Name(), span.SpanKind(), wanted.name, wanted.kind)
		}
		if span.SpanContext().TraceID().String() != requestTraceID || span.Parent().SpanID().String() != requestSpanID || !span.Parent().IsRemote() {
			t.Errorf("span %d %q has the trace %s and the parent %+v, want the trace %s and the remote parent %s of the request", i, span.Name(), span.SpanContext().TraceID(), span.Parent(), requestTraceID, requestSpanID)
		}
		wantCode := codes.Unset
		if wanted.wantError != "" {
			wantCode = codes.Error
		}
		if span.Status().Code != wantCode || span.Status().Description != wanted.wantError {
			t.Errorf("span %d %q has the status %+v, want the code %s and the description %q", i, span.Name(), span.Status(), wantCode, wanted.wantError)
		}
		if span.EndTime().Before(span.StartTime()) {
			t.Errorf("span %d %q ends at %s, before its start at %s", i, span.Name(), span.EndTime(), span.StartTime())
		}
		got := map[attribute.Key]attribute.Value{}
		for _, attr := range span.Attributes() {
			got[attr.Key] = attr.Value
		}
		for _, attr := range []attribute.KeyValue{
			attribute.String("notification.id", notification.ID.String()),
			attribute.String("notification.channel", notification.Channel),
			attribute.Int("notification.attempt", wanted.attempt),
		} {
			if got[attr.Key] != attr.Value {
				t.Errorf("attribute %s of span %d %q = %v, want %v", attr.Key, i, span.Name(), got[attr.Key].String(), attr.Value.String())
			}
		}
	}
}

func TestClaimAndProviderCallContinueTheTraceOfTheRequest(t *testing.T) {
	tests := []struct {
		name       string
		answer     func(w http.ResponseWriter) error
		wantStatus string
		want       []wantSpan
	}{
		{name: "sent", answer: accept, wantStatus: "sent", want: []wantSpan{
			{name: "claim", kind: trace.SpanKindInternal, attempt: 1},
			{name: "provider call", kind: trace.SpanKindClient, attempt: 1},
		}},
		{name: "retried, then sent", answer: inOrder(status(http.StatusInternalServerError), accept), wantStatus: "sent", want: []wantSpan{
			{name: "claim", kind: trace.SpanKindInternal, attempt: 1},
			{name: "provider call", kind: trace.SpanKindClient, attempt: 1, wantError: "provider answered 500, want 202"},
			{name: "claim", kind: trace.SpanKindInternal, attempt: 2},
			{name: "provider call", kind: trace.SpanKindClient, attempt: 2},
		}},
		{name: "failed", answer: status(http.StatusBadRequest), wantStatus: "failed", want: []wantSpan{
			{name: "claim", kind: trace.SpanKindInternal, attempt: 1},
			{name: "provider call", kind: trace.SpanKindClient, attempt: 1, wantError: "provider answered 400, want 202"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testdb.New(t)
			queries := store.New(db)
			fake, client := newProvider(t, tt.answer)
			tracer, recorder := newTracer()
			created := insert(t, queries, "sms", 1)
			setTraceContext(t, db, created.ID)

			runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.DiscardHandler), Tracer: tracer, Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute, ShutdownTimeout: time.Minute})
			waitFor(t, "the notification is "+tt.wantStatus, func() bool { return get(t, queries, created.ID).Status == tt.wantStatus })
			waitFor(t, "every span has ended", func() bool { return len(recorder.Ended()) == len(tt.want) })

			checkSpans(t, recorder, created, tt.want)
			if failed := tt.want[1].wantError; failed != "" && get(t, queries, created.ID).LastError.String != failed {
				t.Errorf("last error = %q, want the text of the failed span %q", get(t, queries, created.ID).LastError.String, failed)
			}
			if sent := fake.traceParents(); len(sent) != 0 {
				t.Errorf("the provider got the trace parents %v, want none: the trace stops at the provider", sent)
			}
		})
	}
}

func TestClaimSpanCoversTheClaimQuery(t *testing.T) {
	const held = 300 * time.Millisecond
	db := testdb.New(t)
	queries := store.New(db)
	_, client := newProvider(t, accept)
	tracer, recorder := newTracer()
	claimedByTheFirstQuery := insert(t, queries, "sms", 0)
	setTraceContext(t, db, claimedByTheFirstQuery.ID)
	lock, err := lockTable(t, db, "notifications")
	if err != nil {
		t.Fatal(err)
	}

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.DiscardHandler), Tracer: tracer, Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute, ShutdownTimeout: time.Minute})
	time.Sleep(held)
	if err := lock.Rollback(t.Context()); err != nil {
		t.Fatalf("release table lock: %v", err)
	}
	waitFor(t, "both spans have ended", func() bool { return len(recorder.Ended()) == 2 })

	claim, call := recorder.Ended()[0], recorder.Ended()[1]
	if took := claim.EndTime().Sub(claim.StartTime()); took < held-50*time.Millisecond || took > queryTimeout {
		t.Errorf("claim span took %s, want about the %s the claim query waited for the table", took, held)
	}
	if call.StartTime().Before(claim.EndTime()) {
		t.Errorf("provider call span starts at %s, before the claim span ends at %s", call.StartTime(), claim.EndTime())
	}
}

func TestReleasedNotificationHasAClaimSpanAndNoProviderCall(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, accept)
	tracer, recorder := newTracer()
	created := insert(t, queries, "sms", 1)
	setTraceContext(t, db, created.ID)
	down := redis.NewClient(&redis.Options{Dialer: func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("redis is down")
	}})
	t.Cleanup(func() {
		if err := down.Close(); err != nil {
			t.Errorf("close redis client: %v", err)
		}
	})
	var logs syncBuffer

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: ratelimit.New(down, sendsPerSecond, sendBurst), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Tracer: tracer, Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute, ShutdownTimeout: time.Minute})
	waitFor(t, "the notification is released", func() bool {
		return logs.lineWith(`"msg":"rate limiter failed, releasing the notification"`) && get(t, queries, created.ID).Status == "pending"
	})
	waitFor(t, "the pool checks the rate limiter instead of claiming", func() bool {
		return logs.lineWith(`"msg":"rate limiter still fails, not claiming"`)
	})

	checkSpans(t, recorder, created, []wantSpan{{name: "claim", kind: trace.SpanKindInternal, attempt: 1}})
	if calls := len(fake.keys()); calls != 0 {
		t.Errorf("provider got %d requests, want 0", calls)
	}
}

func TestNotificationWithoutATraceContextIsNotTraced(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	_, client := newProvider(t, accept)
	tracer, recorder := newTracer()
	created := insert(t, queries, "sms", 1)

	runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: slog.New(slog.DiscardHandler), Tracer: tracer, Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase, BreakerOpenFor: time.Minute, ShutdownTimeout: time.Minute})
	waitFor(t, "the notification is sent", func() bool { return get(t, queries, created.ID).Status == "sent" })
	time.Sleep(10 * pollInterval)

	if spans := recorder.Started(); len(spans) != 0 {
		t.Errorf("%d spans started, want none: the request of the notification was not traced", len(spans))
	}
}
