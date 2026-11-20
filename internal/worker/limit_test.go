package worker

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/kaancolakoglu/NotificationSystem/internal/ratelimit"
	"github.com/kaancolakoglu/NotificationSystem/internal/store"
	"github.com/kaancolakoglu/NotificationSystem/internal/testdb"
	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

const (
	sendsPerSecond = 90
	sendBurst      = 5
	providerLimit  = 100
)

func busiestSecond(arrivals []time.Time) int {
	most, end := 0, 0
	for start := range arrivals {
		for end < len(arrivals) && arrivals[end].Sub(arrivals[start]) < time.Second {
			end++
		}
		most = max(most, end-start)
	}
	return most
}

func TestThreePoolsSendAtMostTheLimitPerSecond(t *testing.T) {
	const rows = 300
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, accept)
	limiter := ratelimit.New(testredis.New(t), sendsPerSecond, sendBurst)
	for range rows {
		insert(t, queries, "sms", 1)
	}

	for range 3 {
		runPool(t, Pool{Queries: queries, Provider: client, Limiter: limiter, Logger: slog.New(slog.DiscardHandler), Channel: "sms", Size: 20, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	}
	waitFor(t, "every notification is sent", func() bool { return countByStatus(t, db, "sent") == rows })

	arrivals := fake.arrivals("sms")
	if len(arrivals) != rows {
		t.Fatalf("provider got %d requests, want %d", len(arrivals), rows)
	}
	if most := busiestSecond(arrivals); most > providerLimit || most < sendsPerSecond*2/3 {
		t.Errorf("busiest second at the provider had %d requests, want at most %d and at least %d", most, providerLimit, sendsPerSecond*2/3)
	}
}

func TestChannelsHaveTheirOwnLimit(t *testing.T) {
	const rows = 150
	db := testdb.New(t)
	queries := store.New(db)
	fake, client := newProvider(t, accept)
	limiter := ratelimit.New(testredis.New(t), sendsPerSecond, sendBurst)
	channels := []string{"sms", "email"}
	for range rows {
		for _, channel := range channels {
			insert(t, queries, channel, 1)
		}
	}

	for _, channel := range channels {
		runPool(t, Pool{Queries: queries, Provider: client, Limiter: limiter, Logger: slog.New(slog.DiscardHandler), Channel: channel, Size: 20, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
	}
	waitFor(t, "every notification is sent", func() bool { return countByStatus(t, db, "sent") == len(channels)*rows })

	var all []time.Time
	for _, channel := range channels {
		arrivals := fake.arrivals(channel)
		if most := busiestSecond(arrivals); len(arrivals) != rows || most > providerLimit {
			t.Errorf("%s: provider got %d requests, %d of them in the busiest second, want %d and at most %d", channel, len(arrivals), most, rows, providerLimit)
		}
		all = append(all, arrivals...)
	}
	slices.SortFunc(all, time.Time.Compare)
	if most := busiestSecond(all); most <= providerLimit {
		t.Errorf("busiest second over both channels had %d requests, want more than the %d one channel may send", most, providerLimit)
	}
}

func TestNothingIsSentWhileTheLimiterFails(t *testing.T) {
	tests := []struct {
		name    string
		breakIt func(ctx context.Context, container *tcredis.RedisContainer, client *redis.Client) error
		repair  func(ctx context.Context, container *tcredis.RedisContainer, client *redis.Client) error
	}{
		{
			name: "redis is stopped",
			breakIt: func(ctx context.Context, container *tcredis.RedisContainer, _ *redis.Client) error {
				return container.Stop(ctx, nil)
			},
			repair: func(ctx context.Context, container *tcredis.RedisContainer, _ *redis.Client) error {
				return container.Start(ctx)
			},
		},
		{
			name: "redis refuses writes",
			breakIt: func(ctx context.Context, _ *tcredis.RedisContainer, client *redis.Client) error {
				return client.Do(ctx, "REPLICAOF", "127.0.0.1", "1").Err()
			},
			repair: func(ctx context.Context, _ *tcredis.RedisContainer, client *redis.Client) error {
				return client.Do(ctx, "REPLICAOF", "NO", "ONE").Err()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const rows = 5
			db := testdb.New(t)
			queries := store.New(db)
			fake, client := newProvider(t, accept)
			container := testredis.Start(t)
			redisClient := redis.NewClient(&redis.Options{Dialer: func(ctx context.Context, network, _ string) (net.Conn, error) {
				address, err := container.PortEndpoint(ctx, "6379/tcp", "")
				if err != nil {
					return nil, err
				}
				var dialer net.Dialer
				return dialer.DialContext(ctx, network, address)
			}})
			t.Cleanup(func() {
				if err := redisClient.Close(); err != nil {
					t.Errorf("close redis client: %v", err)
				}
			})
			if err := tt.breakIt(t.Context(), container, redisClient); err != nil {
				t.Fatalf("break redis: %v", err)
			}
			want := make([]string, rows)
			for i := range want {
				want[i] = insert(t, queries, "sms", 1).ID.String()
			}
			var logs syncBuffer

			runPool(t, Pool{Queries: queries, Provider: client, Limiter: ratelimit.New(redisClient, sendsPerSecond, sendBurst), Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", Size: 2, Interval: pollInterval, QueryTimeout: queryTimeout, BackoffBase: backoffBase})
			runSweeper(t, Sweeper{Queries: queries, Logger: slog.New(slog.DiscardHandler), Interval: pollInterval, QueryTimeout: queryTimeout})
			waitFor(t, "the pool stops claiming and every notification is pending again", func() bool {
				return logs.lineWith(`"level":"ERROR"`, `"msg":"rate limiter still fails, not claiming"`, `"channel":"sms"`) && countByStatus(t, db, "pending") == rows
			})
			if !logs.lineWith(`"level":"ERROR"`, `"msg":"rate limiter failed, releasing the notification"`, `"channel":"sms"`, `"attempt":1`) {
				t.Error("logs have no line for a released notification")
			}

			for range 100 {
				if _, err := db.Exec(t.Context(), "UPDATE notifications SET locked_until = now() - interval '1 second' WHERE status = 'processing'"); err != nil {
					t.Fatalf("expire the leases: %v", err)
				}
				time.Sleep(pollInterval)
			}
			if keys := fake.keys(); len(keys) != 0 {
				t.Errorf("provider got requests for %v while the limiter failed, want none", keys)
			}
			var untouched, history int
			if err := db.QueryRow(t.Context(), "SELECT (SELECT count(*) FROM notifications WHERE status = 'pending' AND attempts = 0 AND last_error IS NULL), (SELECT count(*) FROM delivery_attempts)").Scan(&untouched, &history); err != nil {
				t.Fatalf("count notifications and delivery attempts: %v", err)
			}
			if untouched != rows || history != 0 {
				t.Errorf("while the limiter failed: %d notifications pending with no attempt and %d delivery attempts, want %d and 0", untouched, history, rows)
			}

			if err := tt.repair(t.Context(), container, redisClient); err != nil {
				t.Fatalf("repair redis: %v", err)
			}
			waitFor(t, "every notification is sent", func() bool { return countByStatus(t, db, "sent") == rows })
			time.Sleep(10 * pollInterval)

			got := fake.keys()
			slices.Sort(got)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("provider got requests for %v, want each of %v once", got, want)
			}
			var firstAttempt int
			if err := db.QueryRow(t.Context(), "SELECT count(*) FROM notifications WHERE attempts = 1").Scan(&firstAttempt); err != nil {
				t.Fatalf("count notifications sent on the first attempt: %v", err)
			}
			if firstAttempt != rows {
				t.Errorf("%d notifications were sent on attempt 1, want %d", firstAttempt, rows)
			}
			if lines := logs.linesWith(`"level":"INFO"`, `"msg":"rate limiter works again, claiming"`, `"channel":"sms"`); lines != 1 {
				t.Errorf("logs have %d lines for the rate limiter working again, want 1", lines)
			}
		})
	}
}

func TestStaleReleaseChangesNothing(t *testing.T) {
	db := testdb.New(t)
	queries := store.New(db)
	var logs syncBuffer
	pool := Pool{Queries: queries, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Channel: "sms", QueryTimeout: queryTimeout}
	created := insert(t, queries, "sms", 1)
	claim := func() store.Notification {
		t.Helper()
		claimed, err := queries.ClaimNotifications(t.Context(), store.ClaimNotificationsParams{Channel: "sms", Priority: 1, RowLimit: 1})
		if err != nil || len(claimed) != 1 || claimed[0].ID != created.ID {
			t.Fatalf("claim = %+v, error %v, want the notification", claimed, err)
		}
		return claimed[0]
	}
	stale := claim()
	hold(t, db, created.ID, stale.Attempts, "-1 second")
	Sweeper{Queries: queries, Logger: slog.New(slog.DiscardHandler), QueryTimeout: queryTimeout}.sweep(t.Context())
	swept := get(t, queries, created.ID)

	pool.release(t.Context(), stale, errors.New("redis is down"))

	if stored := get(t, queries, created.ID); stored != swept || stored.Status != "pending" || stored.Attempts != 1 {
		t.Errorf("row after the stale release = %+v, want it unchanged, pending with 1 attempt: %+v", stored, swept)
	}
	current := claim()

	pool.release(t.Context(), stale, errors.New("redis is down"))

	if stored := get(t, queries, created.ID); stored != current {
		t.Errorf("row after the stale release = %+v, want it unchanged %+v", stored, current)
	}
	if !logs.lineWith(`"msg":"result not written, this attempt no longer holds the notification"`, `"notification_id":"`+created.ID.String()+`"`, `"attempt":1`, `"status":"pending"`) {
		t.Error("logs have no line for the release that was not written")
	}

	pool.release(t.Context(), current, errors.New("redis is down"))

	if stored := get(t, queries, created.ID); stored.Status != "pending" || stored.Attempts != 1 {
		t.Errorf("after the release by the attempt that holds it: status = %q, attempts = %d, want pending and 1", stored.Status, stored.Attempts)
	}
}
