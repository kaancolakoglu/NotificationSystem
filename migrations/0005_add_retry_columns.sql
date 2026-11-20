-- +goose Up
ALTER TABLE notifications
    ADD COLUMN attempts   smallint NOT NULL DEFAULT 0,
    ADD COLUMN last_error text;

CREATE TABLE delivery_attempts (
    notification_id uuid NOT NULL REFERENCES notifications (id),
    attempt         smallint NOT NULL,
    error           text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (notification_id, attempt)
);

-- +goose Down
DROP TABLE delivery_attempts;

ALTER TABLE notifications
    DROP COLUMN last_error,
    DROP COLUMN attempts;
