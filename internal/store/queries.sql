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

-- name: InsertBatchNotifications :copyfrom
INSERT INTO notifications (id, recipient, channel, content, priority, batch_id)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: CountBatchByStatus :one
SELECT
    count(*) AS total,
    count(*) FILTER (WHERE status = 'pending') AS pending,
    count(*) FILTER (WHERE status = 'processing') AS processing,
    count(*) FILTER (WHERE status = 'sent') AS sent,
    count(*) FILTER (WHERE status = 'failed') AS failed,
    count(*) FILTER (WHERE status = 'cancelled') AS cancelled
FROM notifications
WHERE batch_id = $1;
