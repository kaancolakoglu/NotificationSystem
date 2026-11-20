-- +goose Up
ALTER TABLE notifications ADD COLUMN correlation_id text;

-- +goose Down
ALTER TABLE notifications DROP COLUMN correlation_id;
