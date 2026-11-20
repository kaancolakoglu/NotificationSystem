package ratelimit

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kaancolakoglu/NotificationSystem/internal/testredis"
)

func TestWaitLetsTheBurstThroughThenKeepsTheRate(t *testing.T) {
	const (
		perSecond = 20
		burst     = 5
		tokens    = 15
	)
	limiter := New(testredis.New(t), perSecond, burst)

	start := time.Now()
	for i := range tokens {
		if err := limiter.Wait(t.Context(), "sms"); err != nil {
			t.Fatalf("wait for token %d: %v", i, err)
		}
		if i == burst-1 {
			if took := time.Since(start); took > 100*time.Millisecond {
				t.Errorf("the burst of %d tokens took %s, want them at once", burst, took)
			}
		}
	}

	want := (tokens - burst) * time.Second / perSecond
	if took := time.Since(start); took < want*95/100 || took > 2*want {
		t.Errorf("%d tokens took %s, want about %s", tokens, took, want)
	}
}

func TestChannelsDoNotShareTokens(t *testing.T) {
	limiter := New(testredis.New(t), 1, 1)
	if err := limiter.Wait(t.Context(), "sms"); err != nil {
		t.Fatalf("wait for the sms token: %v", err)
	}

	start := time.Now()
	if err := limiter.Wait(t.Context(), "email"); err != nil {
		t.Fatalf("wait for the email token: %v", err)
	}

	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("the email token took %s after the sms token was taken, want it at once", took)
	}
}

func TestWaitStopsWhenTheContextEnds(t *testing.T) {
	limiter := New(testredis.New(t), 1, 1)
	if err := limiter.Wait(t.Context(), "sms"); err != nil {
		t.Fatalf("wait for the only token: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := limiter.Wait(ctx, "sms")

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want %v", err, context.DeadlineExceeded)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Errorf("wait returned after %s, want it to stop when the context ended after 50 ms", took)
	}
}
