-- +goose Up
CREATE INDEX idempotency_keys_created_at_idx ON idempotency_keys (created_at);

-- +goose Down
DROP INDEX idempotency_keys_created_at_idx;
