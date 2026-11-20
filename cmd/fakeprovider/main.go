package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	addr              = ":8081"
	readHeaderTimeout = 5 * time.Second
	scriptPrefix      = "script:"
	retryAfterSeconds = "1"
)

type message struct {
	Content string `json:"content"`
}

type accepted struct {
	MessageID string    `json:"messageId"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type scripts struct {
	mu       sync.Mutex
	requests map[string]int
}

func (s *scripts) next(key string, statuses []int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	request := s.requests[key]
	s.requests[key]++
	if request >= len(statuses) {
		return http.StatusAccepted
	}
	return statuses[request]
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	server := &http.Server{
		Addr:              addr,
		Handler:           newHandler(logger),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	err := server.ListenAndServe()
	logger.Error("fake provider exited", "error", err)
	os.Exit(1)
}

func newHandler(logger *slog.Logger) http.Handler {
	scripted := &scripts{requests: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		var sent message
		if err := json.NewDecoder(r.Body).Decode(&sent); err != nil {
			logger.WarnContext(r.Context(), "read request", "error", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		status := http.StatusAccepted
		if script, ok := strings.CutPrefix(sent.Content, scriptPrefix); ok {
			statuses, err := parseScript(script)
			if err != nil {
				logger.WarnContext(r.Context(), "read script", "error", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			key := r.Header.Get("Idempotency-Key")
			status = scripted.next(key, statuses)
			logger.InfoContext(r.Context(), "scripted answer", "idempotency_key", key, "status", status)
		}
		if status != http.StatusAccepted {
			if status == http.StatusTooManyRequests {
				w.Header().Set("Retry-After", retryAfterSeconds)
			}
			w.WriteHeader(status)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		answer := accepted{MessageID: rand.Text(), Status: "accepted", Timestamp: time.Now().UTC()}
		if err := json.NewEncoder(w).Encode(answer); err != nil {
			logger.ErrorContext(r.Context(), "write answer", "error", err)
		}
	})
	return mux
}

func parseScript(script string) ([]int, error) {
	var statuses []int
	for field := range strings.SplitSeq(script, ",") {
		status, err := strconv.Atoi(field)
		if err != nil {
			return nil, fmt.Errorf("script status %q: %w", field, err)
		}
		if status < http.StatusOK || status > 599 {
			return nil, fmt.Errorf("script status %d is not 200 to 599", status)
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}
