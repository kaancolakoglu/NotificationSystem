package obs

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const (
	tracerName        = "github.com/kaancolakoglu/NotificationSystem"
	traceParentHeader = "traceparent"
)

func NewTracer(ctx context.Context, logger *slog.Logger, service, endpoint string) (tracer trace.Tracer, shutdown func(context.Context) error, err error) {
	if endpoint == "" {
		logger.InfoContext(ctx, "tracing is off, OTEL_EXPORTER_OTLP_ENDPOINT is not set")
		return noop.Tracer{}, func(context.Context) error { return nil }, nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, nil, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT is not a URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
		return nil, nil, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must be an http or https URL with a host, got %q", endpoint)
	}
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Warn("opentelemetry", "error", err)
	}))
	otel.SetLogger(logr.FromSlogHandler(logger.Handler()))
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(parsed.JoinPath("v1", "traces").String()))
	if err != nil {
		return nil, nil, fmt.Errorf("create trace exporter: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	logger.InfoContext(ctx, "tracing is on", "endpoint", endpoint)
	return provider.Tracer(tracerName), provider.Shutdown, nil
}

func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier.Get(traceParentHeader)
}

func WithTraceParent(ctx context.Context, traceParent string) context.Context {
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{traceParentHeader: traceParent})
}
