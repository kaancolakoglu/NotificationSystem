package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type exportRequest struct {
	path        string
	contentType string
	body        []byte
}

type collector struct {
	mu       sync.Mutex
	requests []exportRequest
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	c.requests = append(c.requests, exportRequest{path: r.URL.Path, contentType: r.Header.Get("Content-Type"), body: body})
	c.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (c *collector) received() []exportRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]exportRequest(nil), c.requests...)
}

func TestTraceParentNamesTheSpanAndIsReadBack(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	ctx, span := provider.Tracer("test").Start(t.Context(), "request")
	defer span.End()

	traceParent := TraceParent(ctx)
	continued := trace.SpanContextFromContext(WithTraceParent(t.Context(), traceParent))

	want := "00-" + span.SpanContext().TraceID().String() + "-" + span.SpanContext().SpanID().String() + "-01"
	if traceParent != want {
		t.Errorf("trace parent = %q, want %q", traceParent, want)
	}
	if continued.TraceID() != span.SpanContext().TraceID() || continued.SpanID() != span.SpanContext().SpanID() || !continued.IsRemote() || !continued.IsSampled() {
		t.Errorf("span context read back = %+v, want the trace and span of %q, remote and sampled", continued, traceParent)
	}
}

func TestTraceParentIsEmptyWithoutASpan(t *testing.T) {
	if got := TraceParent(t.Context()); got != "" {
		t.Errorf("trace parent of a context without a span = %q, want none", got)
	}
}

func TestWithTraceParentIgnoresWhatIsNotATraceParent(t *testing.T) {
	for _, traceParent := range []string{"", "order-42", "00-00000000000000000000000000000000-0000000000000000-01"} {
		if got := trace.SpanContextFromContext(WithTraceParent(t.Context(), traceParent)); got.IsValid() {
			t.Errorf("span context from %q = %+v, want none", traceParent, got)
		}
	}
}

func TestNewTracerWithoutAnEndpointTracesNothing(t *testing.T) {
	tracer, shutdown, err := NewTracer(t.Context(), slog.New(slog.DiscardHandler), "notification-test", "")
	if err != nil {
		t.Fatalf("new tracer: %v", err)
	}

	ctx, span := tracer.Start(t.Context(), "request")
	span.End()

	if got := TraceParent(ctx); got != "" {
		t.Errorf("trace parent = %q, want none: tracing is off", got)
	}
	if err := shutdown(t.Context()); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func TestNewTracerExportsSpansToTheEndpoint(t *testing.T) {
	collected := &collector{}
	server := httptest.NewServer(collected)
	t.Cleanup(server.Close)
	tracer, shutdown, err := NewTracer(t.Context(), slog.New(slog.DiscardHandler), "notification-test", server.URL)
	if err != nil {
		t.Fatalf("new tracer: %v", err)
	}

	ctx, span := tracer.Start(t.Context(), "provider call")
	span.End()
	if err := shutdown(t.Context()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if TraceParent(ctx) == "" {
		t.Error("the span has no trace parent, want one: tracing is on")
	}
	requests := collected.received()
	if len(requests) != 1 {
		t.Fatalf("the endpoint got %d requests, want 1", len(requests))
	}
	if requests[0].path != "/v1/traces" || requests[0].contentType != "application/x-protobuf" {
		t.Errorf("request = %s with content type %q, want /v1/traces with application/x-protobuf", requests[0].path, requests[0].contentType)
	}
	for _, want := range []string{"service.name", "notification-test", "provider call"} {
		if !bytes.Contains(requests[0].body, []byte(want)) {
			t.Errorf("the exported spans do not contain %q", want)
		}
	}
}

func TestNewTracerRejectsAnEndpointThatIsNotAnHTTPURL(t *testing.T) {
	for _, endpoint := range []string{"jaeger:4318", "grpc://jaeger:4317", "http://", "http://:4318", "://"} {
		_, _, err := NewTracer(t.Context(), slog.New(slog.DiscardHandler), "notification-test", endpoint)
		if err == nil || !strings.Contains(err.Error(), "OTEL_EXPORTER_OTLP_ENDPOINT") {
			t.Errorf("new tracer with the endpoint %q: error = %v, want one that names OTEL_EXPORTER_OTLP_ENDPOINT", endpoint, err)
		}
	}
}

func TestOpenTelemetryErrorsAreLoggedAsJSON(t *testing.T) {
	var logs bytes.Buffer
	_, shutdown, err := NewTracer(t.Context(), NewLogger(&logs), "notification-test", "http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("new tracer: %v", err)
	}
	t.Cleanup(func() {
		if err := shutdown(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})

	otel.Handle(errors.New("traces export: connection refused"))

	var line struct {
		Level string `json:"level"`
		Msg   string `json:"msg"`
		Error string `json:"error"`
	}
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &line); err != nil {
		t.Fatalf("logs = %q, want JSON lines: %v", logs.String(), err)
	}
	if line.Level != "WARN" || line.Msg != "opentelemetry" || line.Error != "traces export: connection refused" {
		t.Errorf("last line = %+v, want a warning with the error", line)
	}
}

func TestNewTracerSaysWhetherTracingIsOn(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{name: "off", endpoint: "", want: `"level":"INFO","msg":"tracing is off, OTEL_EXPORTER_OTLP_ENDPOINT is not set"`},
		{name: "on", endpoint: "http://127.0.0.1:1", want: `"level":"INFO","msg":"tracing is on","endpoint":"http://127.0.0.1:1"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer

			if _, _, err := NewTracer(t.Context(), NewLogger(&logs), "notification-test", tt.endpoint); err != nil {
				t.Fatalf("new tracer: %v", err)
			}

			if !strings.Contains(logs.String(), tt.want) || strings.Count(logs.String(), "\n") != 1 {
				t.Errorf("logs = %q, want one line with %s", logs.String(), tt.want)
			}
		})
	}
}

func TestTraceFollowsTheSampledFlagOfTheCaller(t *testing.T) {
	const caller = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-"
	t.Setenv("OTEL_TRACES_SAMPLER", "always_off")
	tracer, _, err := NewTracer(t.Context(), slog.New(slog.DiscardHandler), "notification-test", "http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("new tracer: %v", err)
	}
	tests := []struct {
		name          string
		parent        context.Context
		wantRecording bool
	}{
		{name: "no caller", parent: t.Context(), wantRecording: true},
		{name: "caller sampled its trace", parent: WithTraceParent(t.Context(), caller+"01"), wantRecording: true},
		{name: "caller did not sample its trace", parent: WithTraceParent(t.Context(), caller+"00"), wantRecording: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, span := tracer.Start(tt.parent, "request")
			defer span.End()

			if span.IsRecording() != tt.wantRecording {
				t.Errorf("span is recorded = %t, want %t: OTEL_TRACES_SAMPLER does not change the sampling", span.IsRecording(), tt.wantRecording)
			}
		})
	}
}

func TestMessagesOfTheOpenTelemetryLibrariesAreLoggedAsJSON(t *testing.T) {
	var logs bytes.Buffer
	t.Setenv("OTEL_EXPORTER_OTLP_TIMEOUT", "soon")

	if _, _, err := NewTracer(t.Context(), NewLogger(&logs), "notification-test", "http://127.0.0.1:1"); err != nil {
		t.Fatalf("new tracer: %v", err)
	}

	if !strings.Contains(logs.String(), `"level":"ERROR","msg":"parse duration"`) || !strings.Contains(logs.String(), `"input":"soon"`) {
		t.Errorf("logs = %q, want an error line about the timeout that is not a number", logs.String())
	}
	for line := range strings.Lines(logs.String()) {
		if !json.Valid([]byte(line)) {
			t.Errorf("log line %q is not JSON", line)
		}
	}
}
