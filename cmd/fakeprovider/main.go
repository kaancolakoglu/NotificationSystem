package main

import (
	"crypto/rand"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"
)

const (
	addr              = ":8081"
	readHeaderTimeout = 5 * time.Second
)

type accepted struct {
	MessageID string    `json:"messageId"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
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
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		answer := accepted{MessageID: rand.Text(), Status: "accepted", Timestamp: time.Now().UTC()}
		if err := json.NewEncoder(w).Encode(answer); err != nil {
			logger.ErrorContext(r.Context(), "write answer", "error", err)
		}
	})
	return mux
}
