-- +goose Up
ALTER TABLE notifications ADD COLUMN trace_context text;

-- +goose Down
ALTER TABLE notifications DROP COLUMN trace_context;
