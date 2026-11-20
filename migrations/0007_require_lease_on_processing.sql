-- +goose Up
LOCK TABLE notifications IN ACCESS EXCLUSIVE MODE;

UPDATE notifications
SET locked_until = now() + interval '60 seconds'
WHERE status = 'processing' AND locked_until IS NULL;

ALTER TABLE notifications ADD CONSTRAINT notifications_processing_has_lease
    CHECK (status <> 'processing' OR locked_until IS NOT NULL);

-- +goose Down
ALTER TABLE notifications DROP CONSTRAINT notifications_processing_has_lease;
