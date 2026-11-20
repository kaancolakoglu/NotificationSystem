package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	timeout       = 5 * time.Second
	maxRetryAfter = 60 * time.Second
	maxErrorBody  = 200
)

type Message struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type Error struct {
	Retryable   bool
	Unavailable bool
	RetryAfter  time.Duration
	err         error
}

func (e *Error) Error() string {
	return e.err.Error()
}

func (e *Error) Unwrap() error {
	return e.err
}

type Client struct {
	url    string
	http   *http.Client
	logger *slog.Logger
}

func New(providerURL string, connections int, logger *slog.Logger) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = connections
	return &Client{
		url: providerURL,
		http: &http.Client{
			Transport: transport,
			Timeout:   timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		logger: logger,
	}
}

func (c *Client) Send(ctx context.Context, id uuid.UUID, message Message) (string, error) {
	body, err := json.Marshal(message)
	if err != nil {
		return "", &Error{err: fmt.Errorf("encode provider request: %w", err)}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return "", &Error{err: fmt.Errorf("build provider request: %w", err)}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", id.String())

	response, err := c.http.Do(request)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return "", &Error{Retryable: true, Unavailable: true, err: fmt.Errorf("call provider: %w", err)}
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			c.logger.WarnContext(ctx, "close provider response", "notification_id", id, "error", err)
		}
	}()

	if response.StatusCode != http.StatusAccepted {
		body, err := io.ReadAll(io.LimitReader(response.Body, maxErrorBody))
		if err != nil {
			c.logger.WarnContext(ctx, "read provider response", "notification_id", id, "error", err)
		}
		return "", statusError(response, body)
	}

	var accepted struct {
		MessageID string `json:"messageId"`
	}
	err = json.NewDecoder(response.Body).Decode(&accepted)
	if err == nil && accepted.MessageID == "" {
		err = errors.New("messageId is missing")
	}
	if err != nil {
		c.logger.WarnContext(ctx, "provider accepted the notification without a readable message id", "notification_id", id, "error", err)
	}
	return accepted.MessageID, nil
}

func statusError(response *http.Response, body []byte) *Error {
	text := fmt.Sprintf("provider answered %d, want %d", response.StatusCode, http.StatusAccepted)
	answer := strings.TrimSpace(strings.ReplaceAll(strings.ToValidUTF8(string(body), "\uFFFD"), "\x00", ""))
	if answer != "" {
		text += ": " + answer
	}
	e := &Error{err: errors.New(text)}
	switch {
	case response.StatusCode == http.StatusTooManyRequests:
		e.Retryable = true
		e.RetryAfter = retryAfter(response.Header)
	case response.StatusCode == http.StatusRequestTimeout:
		e.Retryable = true
	case response.StatusCode >= http.StatusInternalServerError:
		e.Retryable = true
		e.Unavailable = true
	}
	return e
}

func retryAfter(header http.Header) time.Duration {
	value := header.Get("Retry-After")
	seconds, err := strconv.ParseUint(value, 10, 64)
	if err == nil || errors.Is(err, strconv.ErrRange) {
		return time.Duration(min(seconds, uint64(maxRetryAfter/time.Second))) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return min(max(time.Until(at), 0), maxRetryAfter)
	}
	return 0
}
