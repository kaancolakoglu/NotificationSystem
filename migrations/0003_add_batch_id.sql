-- +goose Up
ALTER TABLE notifications ADD COLUMN batch_id uuid;

CREATE INDEX notifications_batch_id_idx ON notifications (batch_id)
    WHERE batch_id IS NOT NULL;

-- +goose Down
DROP INDEX notifications_batch_id_idx;

ALTER TABLE notifications DROP COLUMN batch_id;
