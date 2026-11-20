package testredis

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

const (
	redisImage  = "redis:8.10.3-alpine3.23"
	isolatedEnv = "TESTDB_ISOLATED"
	databases   = 1024
)

var (
	sharedURL string
	used      int
)

func New(t *testing.T) *redis.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("needs a Redis container")
	}

	if os.Getenv(isolatedEnv) != "" {
		return connect(t, url(t, Start(t)), 0)
	}

	if sharedURL == "" {
		sharedURL = url(t, start(context.WithoutCancel(t.Context()), t))
	}
	used++
	return connect(t, sharedURL, used)
}

func Start(t *testing.T) *tcredis.RedisContainer {
	t.Helper()
	if testing.Short() {
		t.Skip("needs a Redis container")
	}
	container := start(t.Context(), t)
	testcontainers.CleanupContainer(t, container)
	return container
}

func start(ctx context.Context, t *testing.T) *tcredis.RedisContainer {
	t.Helper()
	container, err := tcredis.Run(ctx, redisImage, testcontainers.WithCmdArgs("--databases", strconv.Itoa(databases)))
	if err != nil {
		testcontainers.CleanupContainer(t, container)
		t.Fatalf("start redis: %v", err)
	}
	return container
}

func url(t *testing.T, container *tcredis.RedisContainer) string {
	t.Helper()
	serverURL, err := container.ConnectionString(t.Context())
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	return serverURL
}

func connect(t *testing.T, serverURL string, database int) *redis.Client {
	t.Helper()
	options, err := redis.ParseURL(serverURL)
	if err != nil {
		t.Fatalf("parse connection string: %v", err)
	}
	options.DB = database
	client := redis.NewClient(options)
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close redis client: %v", err)
		}
	})
	return client
}
