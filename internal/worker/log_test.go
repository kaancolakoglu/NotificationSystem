package worker

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kaancolakoglu/NotificationSystem/internal/obs"
	"github.com/kaancolakoglu/NotificationSystem/internal/provider"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
)

const hasCorrelationID = `"correlation_id":"order-42"`

func setCorrelationID(t *testing.T, db *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	if _, err := db.Exec(t.Context(), "UPDATE notifications SET correlation_id = 'order-42' WHERE id = $1", id); err != nil {
		t.Fatalf("set the correlation id: %v", err)
	}
}

type sendLine struct {
	Level          string   `json:"level"`
	Channel        string   `json:"channel"`
	NotificationID string   `json:"notification_id"`
	Attempt        int16    `json:"attempt"`
	Status         string   `json:"status"`
	DelaySeconds   *float64 `json:"delay_seconds"`
	Error          string   `json:"error"`
	CorrelationID  *string  `json:"correlation_id"`
}

func sendLines(t *testing.T, logs *syncBuffer) []sendLine {
	t.Helper()
	logs.mu.Lock()
	defer logs.mu.Unlock()
	var lines []sendLine
	for line := range strings.Lines(logs.buffer.String()) {
		if !strings.Contains(line, `"msg":"send notification"`) {
			continue
		}
		var decoded sendLine
		if err := json.Unmarshal([]byte(line), &decoded); err != nil {
			t.Fatalf("decode log line %s: %v", line, err)
		}
		lines = append(lines, decoded)
	}
	return lines
}

func TestFailedSendIsLoggedWithWhatHappensNext(t *testing.T) {
	const year = 365 * 24 * time.Hour
	tests := []struct {
		name             string
		answer           int
		attemptsMade     int16
		withCorrelation  bool
		wantLevel        string
		wantStatus       string
		wantDelayCeiling time.Duration
	}{
		{name: "retry after the first attempt", answer: http.StatusTooManyRequests, withCorrelation: true, wantLevel: "WARN", wantStatus: "pending", wantDelayCeiling: 2 * year},
		{name: "retry after the fourth attempt", answer: http.StatusTooManyRequests, attemptsMade: maxAttempts - 2, withCorrelation: true, wantLevel: "WARN", wantStatus: "pending", wantDelayCeiling: 16 * year},
		{name: "permanent failure", answer: http.StatusBadRequest, withCorrelation: true, wantLevel: "ERROR", wantStatus: "failed"},
		{name: "attempts used up", answer: http.StatusTooManyRequests, attemptsMade: maxAttempts - 1, withCorrelation: true, wantLevel: "ERROR", wantStatus: "failed"},
		{name: "notification without a correlation id", answer: http.StatusBadRequest, wantLevel: "ERROR", wantStatus: "failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := testdb.New(t)
			queries := store.New(db)
			_, client := newProvider(t, status(tt.answer))
			created := insert(t, queries, "sms", 1)
			if _, err := db.Exec(t.Context(), "UPDATE notifications SET attempts = $2 WHERE id = $1", created.ID, tt.attemptsMade); err != nil {
				t.Fatalf("set the attempts already made: %v", err)
			}
			if tt.withCorrelation {
				setCorrelationID(t, db, created.ID)
			}
			var logs syncBuffer

			runPool(t, Pool{Queries: queries, Provider: client, Limiter: unlimited(t), Logger: obs.NewLogger(&logs), Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: year})
			waitFor(t, "the failed attempt is stored", func() bool { return len(attempts(t, queries, created.ID)) == 1 })

			lines := sendLines(t, &logs)
			if len(lines) != 1 {
				t.Fatalf("logs have %d lines for the failed send, want 1", len(lines))
			}
			line := lines[0]
			if line.Level != tt.wantLevel || line.Channel != "sms" || line.NotificationID != created.ID.String() || line.Attempt != tt.attemptsMade+1 || line.Status != tt.wantStatus {
				t.Errorf("log line = %+v, want level %s, channel sms, the notification id, attempt %d and status %s", line, tt.wantLevel, tt.attemptsMade+1, tt.wantStatus)
			}
			if !strings.Contains(line.Error, "provider answered") {
				t.Errorf("error in the log line = %q, want the provider's answer", line.Error)
			}
			if tt.withCorrelation && (line.CorrelationID == nil || *line.CorrelationID != "order-42") {
				t.Errorf("correlation id in the log line = %v, want order-42", line.CorrelationID)
			}
			if !tt.withCorrelation && line.CorrelationID != nil {
				t.Errorf("correlation id in the log line = %q, want none", *line.CorrelationID)
			}
			if tt.wantDelayCeiling == 0 {
				if line.DelaySeconds != nil {
					t.Errorf("delay in the log line = %v s, want none for a notification that is not retried", *line.DelaySeconds)
				}
				return
			}
			if line.DelaySeconds == nil {
				t.Fatal("log line has no delay, want the delay before the retry")
			}
			stored := get(t, queries, created.ID).NextAttemptAt.Sub(attempts(t, queries, created.ID)[0].CreatedAt)
			if math.Abs(*line.DelaySeconds-stored.Seconds()) > 0.001 || stored < 0 || stored >= tt.wantDelayCeiling {
				t.Errorf("delay in the log line = %v s, stored delay = %s, want them equal and under %s", *line.DelaySeconds, stored, tt.wantDelayCeiling)
			}
		})
	}
}

func TestSentNotificationIsNotLogged(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	var logs syncBuffer
	logger := obs.NewLogger(&logs)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := accept(w); err != nil {
			t.Errorf("write provider answer: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	created := insert(t, queries, "sms", 1)
	setCorrelationID(t, db, created.ID)

	stop, stopped := runPool(t, Pool{Queries: queries, Provider: provider.New(server.URL, 1, logger), Limiter: unlimited(t), Logger: logger, Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "the notification is sent", func() bool { return get(t, queries, created.ID).Status == "sent" })
	stop()
	waitToReceive(t, "the pool stops", stopped)

	if logs.lineWith(created.ID.String()) || logs.lineWith(hasCorrelationID) {
		t.Error("logs have a line for a notification that was sent without a problem, want none")
	}
}

func TestProviderLineOfASendHasTheCorrelationID(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	var logs syncBuffer
	logger := obs.NewLogger(&logs)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)
	created := insert(t, queries, "sms", 1)
	setCorrelationID(t, db, created.ID)

	runPool(t, Pool{Queries: queries, Provider: provider.New(server.URL, 1, logger), Limiter: unlimited(t), Logger: logger, Channel: "sms", Size: 1, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	waitFor(t, "the notification is sent", func() bool { return get(t, queries, created.ID).Status == "sent" })

	if !logs.lineWith(`"msg":"provider accepted the notification without a readable message id"`, `"notification_id":"`+created.ID.String()+`"`, hasCorrelationID) {
		t.Error("logs have no provider line with the correlation id of the notification")
	}
}
