-- +goose Up
ALTER TABLE notifications
    ADD COLUMN next_attempt_at     timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN sent_at             timestamptz,
    ADD COLUMN provider_message_id text;

CREATE INDEX notifications_claim_idx ON notifications (channel, priority, next_attempt_at)
    WHERE status = 'pending';

-- +goose Down
DROP INDEX notifications_claim_idx;

ALTER TABLE notifications
    DROP COLUMN provider_message_id,
    DROP COLUMN sent_at,
    DROP COLUMN next_attempt_at;
