-- +goose Up
CREATE TABLE notifications (
    id         uuid PRIMARY KEY,
    recipient  text NOT NULL,
    channel    text NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    content    text NOT NULL,
    priority   smallint NOT NULL CHECK (priority IN (0, 1, 2)),
    status     text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'sent', 'failed', 'cancelled')),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE notifications;
