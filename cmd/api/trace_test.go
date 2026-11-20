package main

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRequestsThroughTheHandlerAreTraced(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)).Tracer("test")
	handler := newHandler(nil, nil, nil, tracer, slog.New(slog.DiscardHandler))

	for _, target := range []string{"/v1/notifications?limit=0", "/healthz", "/metrics"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, target, nil))
	}

	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Name() != "GET /v1/notifications" {
		t.Errorf("%d spans ended, the first %+v, want one for GET /v1/notifications: routes outside /v1 are not traced", len(spans), spans)
	}
}

func TestRunRejectsATraceEndpointThatIsNotAURL(t *testing.T) {
	err := run(t.Context(), slog.New(slog.DiscardHandler), "postgres://postgres@127.0.0.1:1/postgres", "redis://127.0.0.1:1", "jaeger:4318")

	if err == nil || !strings.Contains(err.Error(), "OTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Errorf("run = %v, want an error that names OTEL_EXPORTER_OTLP_ENDPOINT", err)
	}
}
