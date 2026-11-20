package obs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
)

type correlationIDKey struct{}

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, id)
}

func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationIDKey{}).(string)
	return id
}

func NewLogger(w io.Writer) *slog.Logger {
	return slog.New(correlationHandler{slog.NewJSONHandler(w, nil)})
}

type correlationHandler struct {
	slog.Handler
}

func (h correlationHandler) Handle(ctx context.Context, record slog.Record) error {
	if id := CorrelationID(ctx); id != "" {
		record.AddAttrs(slog.String("correlation_id", id))
	}
	return h.Handler.Handle(ctx, record)
}

func (h correlationHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return correlationHandler{h.Handler.WithAttrs(attrs)}
}

func (h correlationHandler) WithGroup(name string) slog.Handler {
	return correlationHandler{h.Handler.WithGroup(name)}
}

type RedisLogger struct {
	Logger *slog.Logger
}

func (l RedisLogger) Printf(ctx context.Context, format string, v ...any) {
	l.Logger.WarnContext(ctx, "redis client", "detail", fmt.Sprintf(format, v...))
}
