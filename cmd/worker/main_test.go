package main

import (
	"log/slog"
	"strings"
	"testing"
)

func TestRunRejectsABadProviderURL(t *testing.T) {
	tests := []struct {
		name        string
		providerURL string
	}{
		{name: "not set", providerURL: ""},
		{name: "no scheme", providerURL: "fakeprovider:8081"},
		{name: "host only", providerURL: "webhook.site/token"},
		{name: "not http", providerURL: "ftp://fakeprovider:8081/"},
		{name: "no host", providerURL: "http:///token"},
		{name: "not a URL", providerURL: "http://[::1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := run(t.Context(), slog.New(slog.DiscardHandler), "postgres://postgres@127.0.0.1:1/postgres", tt.providerURL)

			if err == nil || !strings.Contains(err.Error(), "PROVIDER_URL") {
				t.Errorf("error = %v, want one about PROVIDER_URL", err)
			}
		})
	}
}

func TestCheckProviderURLAcceptsHTTPAndHTTPS(t *testing.T) {
	for _, providerURL := range []string{"http://fakeprovider:8081/", "https://webhook.site/token"} {
		if err := checkProviderURL(providerURL); err != nil {
			t.Errorf("check %s: %v", providerURL, err)
		}
	}
}
