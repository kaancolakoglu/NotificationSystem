package obs

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/kaancolakoglu/NotificationSystem/internal/store"
)

var (
	deliveryBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}
	priorityNames   = map[int16]string{0: "high", 1: "normal", 2: "low"}

	pendingDesc = prometheus.NewDesc(
		"notifications_pending",
		"Notifications in status pending, due or not.",
		[]string{"channel", "priority"}, nil,
	)
	oldestPendingDesc = prometheus.NewDesc(
		"notifications_oldest_pending_age_seconds",
		"Seconds since the pending notification that has been due the longest became due. 0 when none is due.",
		[]string{"channel", "priority"}, nil,
	)
)

func NewRegistry() *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return registry
}

func MetricsHandler(registry *prometheus.Registry, logger *slog.Logger) http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		ErrorLog:      slog.NewLogLogger(logger.Handler(), slog.LevelError),
		ErrorHandling: promhttp.ContinueOnError,
		Registry:      registry,
	})
}

type WorkerMetrics struct {
	sent            *prometheus.CounterVec
	failed          *prometheus.CounterVec
	retried         *prometheus.CounterVec
	limiterFailures *prometheus.CounterVec
	providerCall    *prometheus.HistogramVec
	delivery        *prometheus.HistogramVec
	breakerState    *prometheus.GaugeVec
}

func NewWorkerMetrics(registerer prometheus.Registerer, channels []string) *WorkerMetrics {
	labels := []string{"channel"}
	m := &WorkerMetrics{
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_sent_total",
			Help: "Notifications stored as sent.",
		}, labels),
		failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_failed_total",
			Help: "Notifications stored as failed.",
		}, labels),
		retried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_retried_total",
			Help: "Failed or lost attempts whose notification was stored as pending again.",
		}, labels),
		limiterFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rate_limiter_failures_total",
			Help: "Rate limiter calls that failed.",
		}, labels),
		providerCall: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "provider_call_duration_seconds",
			Help:    "Duration of a provider call, whatever its result.",
			Buckets: prometheus.DefBuckets,
		}, labels),
		delivery: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notification_delivery_duration_seconds",
			Help:    "Time from the creation of a notification until it was stored as sent.",
			Buckets: deliveryBuckets,
		}, labels),
		breakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "State of the circuit breaker: 0 closed, 1 half-open, 2 open.",
		}, labels),
	}
	registerer.MustRegister(m.sent, m.failed, m.retried, m.limiterFailures, m.providerCall, m.delivery, m.breakerState)
	for _, channel := range channels {
		m.sent.WithLabelValues(channel)
		m.failed.WithLabelValues(channel)
		m.retried.WithLabelValues(channel)
		m.limiterFailures.WithLabelValues(channel)
		m.providerCall.WithLabelValues(channel)
		m.delivery.WithLabelValues(channel)
		m.breakerState.WithLabelValues(channel)
	}
	return m
}

func (m *WorkerMetrics) Sent(channel string, sinceCreated time.Duration) {
	m.sent.WithLabelValues(channel).Inc()
	m.delivery.WithLabelValues(channel).Observe(sinceCreated.Seconds())
}

func (m *WorkerMetrics) Failed(channel string) {
	m.failed.WithLabelValues(channel).Inc()
}

func (m *WorkerMetrics) Retried(channel string) {
	m.retried.WithLabelValues(channel).Inc()
}

func (m *WorkerMetrics) LimiterFailed(channel string) {
	m.limiterFailures.WithLabelValues(channel).Inc()
}

func (m *WorkerMetrics) ProviderCalled(channel string, took time.Duration) {
	m.providerCall.WithLabelValues(channel).Observe(took.Seconds())
}

func (m *WorkerMetrics) BreakerChanged(channel string, state int) {
	m.breakerState.WithLabelValues(channel).Set(float64(state))
}

type QueueCollector struct {
	Queries  *store.Queries
	Channels []string
	Timeout  time.Duration
}

func (c QueueCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- pendingDesc
	descs <- oldestPendingDesc
}

func (c QueueCollector) Collect(metrics chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	rows, err := c.Queries.CountPendingNotifications(ctx)
	if err != nil {
		metrics <- prometheus.NewInvalidMetric(pendingDesc, fmt.Errorf("count pending notifications: %w", err))
		return
	}

	type queue struct {
		channel  string
		priority int16
	}
	queues := map[queue]store.CountPendingNotificationsRow{}
	for _, channel := range c.Channels {
		for priority := range priorityNames {
			queues[queue{channel, priority}] = store.CountPendingNotificationsRow{}
		}
	}
	for _, row := range rows {
		queues[queue{row.Channel, row.Priority}] = row
	}
	for q, row := range queues {
		metrics <- prometheus.MustNewConstMetric(pendingDesc, prometheus.GaugeValue, float64(row.Pending), q.channel, priorityNames[q.priority])
		metrics <- prometheus.MustNewConstMetric(oldestPendingDesc, prometheus.GaugeValue, row.OldestAgeSeconds, q.channel, priorityNames[q.priority])
	}
}
