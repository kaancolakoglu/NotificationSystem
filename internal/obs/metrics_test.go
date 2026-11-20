package obs

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestWorkerMetricsStartAtZeroForEveryChannel(t *testing.T) {
	registry := prometheus.NewRegistry()
	NewWorkerMetrics(registry, []string{"sms", "email"})
	rec := httptest.NewRecorder()

	MetricsHandler(registry, slog.New(slog.DiscardHandler)).ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil))

	for _, channel := range []string{"sms", "email"} {
		for _, name := range []string{
			"notifications_sent_total",
			"notifications_failed_total",
			"notifications_retried_total",
			"rate_limiter_failures_total",
			"provider_call_duration_seconds_count",
			"notification_delivery_duration_seconds_count",
			"circuit_breaker_state",
		} {
			if series := name + `{channel="` + channel + `"} 0` + "\n"; !strings.Contains(rec.Body.String(), "\n"+series) {
				t.Errorf("metrics have no line %q, want every series at 0 before its first event", series)
			}
		}
	}
	if strings.Contains(rec.Body.String(), `channel="push"`) {
		t.Error("metrics have a series for push, want only the channels given")
	}
}
