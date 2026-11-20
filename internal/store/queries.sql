-- name: InsertNotification :one
INSERT INTO notifications (id, recipient, channel, content, priority)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetNotification :one
SELECT * FROM notifications
WHERE id = $1;

-- name: CancelNotification :one
UPDATE notifications
SET status = 'cancelled'
WHERE id = $1 AND status = 'pending'
RETURNING *;

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

-- name: ListNotifications :many
SELECT * FROM notifications
WHERE id >= coalesce(sqlc.narg('min_id'), '00000000-0000-0000-0000-000000000000'::uuid)
  AND id < coalesce(sqlc.narg('max_id'), 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid)
  AND (sqlc.narg('status')::text IS NULL OR status = sqlc.narg('status'))
  AND (sqlc.narg('channel')::text IS NULL OR channel = sqlc.narg('channel'))
ORDER BY id DESC
LIMIT sqlc.arg('row_limit');

-- name: ListBatchNotifications :many
SELECT * FROM notifications
WHERE batch_id = sqlc.arg('batch_id')
  AND (sqlc.narg('min_id')::uuid IS NULL OR id >= sqlc.narg('min_id'))
  AND (sqlc.narg('max_id')::uuid IS NULL OR id < sqlc.narg('max_id'))
  AND (sqlc.narg('status')::text IS NULL OR status = sqlc.narg('status'))
  AND (sqlc.narg('channel')::text IS NULL OR channel = sqlc.narg('channel'))
ORDER BY id DESC
LIMIT sqlc.arg('row_limit');
