-- +goose Up
ALTER TABLE notifications ADD COLUMN locked_until timestamptz;

UPDATE notifications
SET locked_until = now() + interval '60 seconds'
WHERE status = 'processing';

CREATE INDEX notifications_lease_idx ON notifications (locked_until)
    WHERE status = 'processing';

-- +goose Down
DROP INDEX notifications_lease_idx;

ALTER TABLE notifications DROP COLUMN locked_until;
