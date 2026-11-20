-- +goose Up
CREATE TABLE templates (
    id         uuid PRIMARY KEY,
    content    text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE templates;
