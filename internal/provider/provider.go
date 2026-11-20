package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

const timeout = 5 * time.Second

type Message struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type Error struct {
	Retryable  bool
	RetryAfter time.Duration
	err        error
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

func New(url string, connections int, logger *slog.Logger) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = connections
	return &Client{
		url: url,
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
		return "", &Error{Retryable: true, err: fmt.Errorf("call provider: %w", err)}
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			c.logger.WarnContext(ctx, "close provider response", "notification_id", id, "error", err)
		}
	}()

	if response.StatusCode != http.StatusAccepted {
		return "", statusError(response)
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

func statusError(response *http.Response) *Error {
	e := &Error{err: fmt.Errorf("provider answered %d, want %d", response.StatusCode, http.StatusAccepted)}
	switch {
	case response.StatusCode == http.StatusTooManyRequests:
		e.Retryable = true
		e.RetryAfter = retryAfter(response.Header)
	case response.StatusCode == http.StatusRequestTimeout, response.StatusCode >= http.StatusInternalServerError:
		e.Retryable = true
	}
	return e
}

func retryAfter(header http.Header) time.Duration {
	value := header.Get("Retry-After")
	if seconds, err := strconv.ParseUint(value, 10, 31); err == nil {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(time.Until(at), 0)
	}
	return 0
}
