-- +goose Up
CREATE TABLE idempotency_keys (
    key          text PRIMARY KEY,
    request_hash bytea NOT NULL,
    response     jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE idempotency_keys;
