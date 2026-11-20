package obs_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
)

func TestLoggerAddsTheCorrelationIDOfTheContext(t *testing.T) {
	withID := obs.WithCorrelationID(t.Context(), "request-1")
	tests := []struct {
		name   string
		ctx    context.Context
		derive func(*slog.Logger) *slog.Logger
		want   string
	}{
		{name: "context with an id", ctx: withID, derive: func(l *slog.Logger) *slog.Logger { return l }, want: `"status":200,"correlation_id":"request-1"}`},
		{name: "logger with an attribute", ctx: withID, derive: func(l *slog.Logger) *slog.Logger { return l.With("worker", "host-1") }, want: `"worker":"host-1","status":200,"correlation_id":"request-1"}`},
		{name: "logger with a group has the id inside the group", ctx: withID, derive: func(l *slog.Logger) *slog.Logger { return l.WithGroup("request") }, want: `"request":{"status":200,"correlation_id":"request-1"}}`},
		{name: "context without an id", ctx: t.Context(), derive: func(l *slog.Logger) *slog.Logger { return l }},
		{name: "context with an empty id", ctx: obs.WithCorrelationID(t.Context(), ""), derive: func(l *slog.Logger) *slog.Logger { return l }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer

			tt.derive(obs.NewLogger(&logs)).InfoContext(tt.ctx, "hello", "status", 200)

			line := logs.String()
			for _, want := range []string{`"level":"INFO"`, `"msg":"hello"`, `"status":200`} {
				if !strings.Contains(line, want) {
					t.Errorf("log line = %s, want it to contain %s", line, want)
				}
			}
			if tt.want != "" && !strings.Contains(line, tt.want) {
				t.Errorf("log line = %s, want it to contain %s", line, tt.want)
			}
			if tt.want == "" && strings.Contains(line, "correlation_id") {
				t.Errorf("log line = %s, want no correlation_id attribute", line)
			}
		})
	}
}

func TestCorrelationIDIsReadBackFromTheContext(t *testing.T) {
	if got := obs.CorrelationID(obs.WithCorrelationID(t.Context(), "request-1")); got != "request-1" {
		t.Errorf("correlation id = %q, want %q", got, "request-1")
	}
	if got := obs.CorrelationID(t.Context()); got != "" {
		t.Errorf("correlation id of a context without one = %q, want none", got)
	}
}

func TestRedisLoggerWritesTheMessageAsAJSONLine(t *testing.T) {
	var logs bytes.Buffer

	obs.RedisLogger{Logger: obs.NewLogger(&logs)}.Printf(t.Context(), "dial %s: %v", "redis:6379", errors.New("connection refused"))

	for _, want := range []string{`"level":"WARN"`, `"msg":"redis client"`, `"detail":"dial redis:6379: connection refused"`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log line = %s, want it to contain %s", logs.String(), want)
		}
	}
}
