package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"
)

const (
	addr              = ":8080"
	readHeaderTimeout = 5 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	server := &http.Server{
		Addr:              addr,
		Handler:           newMux(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	err := server.ListenAndServe()
	logger.Error("api server exited", "error", err)
	os.Exit(1)
}

func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
