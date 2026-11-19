-- name: InsertNotification :one
INSERT INTO notifications (id, recipient, channel, content, priority)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetNotification :one
SELECT * FROM notifications
WHERE id = $1;

-- name: InsertIdempotencyKey :execrows
INSERT INTO idempotency_keys (key, request_hash, response)
VALUES ($1, $2, $3)
ON CONFLICT (key) DO NOTHING;

-- name: GetIdempotencyKey :one
SELECT request_hash, response FROM idempotency_keys
WHERE key = $1;
