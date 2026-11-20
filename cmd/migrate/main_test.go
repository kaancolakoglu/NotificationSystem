package main

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestRunStopsWhenTheDatabaseNeverAnswers(t *testing.T) {
	var config net.ListenConfig
	listener, err := config.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	})

	err = run(t.Context(), "postgres://postgres@"+listener.Addr().String()+"/postgres?sslmode=disable", 200*time.Millisecond)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want %v", err, context.DeadlineExceeded)
	}
}
