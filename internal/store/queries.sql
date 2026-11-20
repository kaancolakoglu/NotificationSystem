-- name: InsertNotification :one
INSERT INTO notifications (id, recipient, channel, content, priority, correlation_id, next_attempt_at)
VALUES (
    sqlc.arg('id'),
    sqlc.arg('recipient'),
    sqlc.arg('channel'),
    sqlc.arg('content'),
    sqlc.arg('priority'),
    sqlc.narg('correlation_id'),
    greatest(sqlc.narg('scheduled_at')::timestamptz, now())
)
RETURNING *;

-- name: GetDatabaseTime :one
SELECT now()::timestamptz;

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

-- name: DeleteExpiredIdempotencyKeys :execrows
DELETE FROM idempotency_keys
WHERE key = ANY (ARRAY(
    SELECT expired.key FROM idempotency_keys AS expired
    WHERE expired.created_at < now() - interval '24 hours'
    LIMIT sqlc.arg('row_limit')
));

-- name: InsertBatchNotifications :copyfrom
INSERT INTO notifications (id, recipient, channel, content, priority, batch_id, correlation_id, next_attempt_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

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

-- name: ClaimNotifications :many
WITH due AS (
    SELECT candidate.id FROM notifications AS candidate
    WHERE candidate.status = 'pending'
      AND candidate.channel = sqlc.arg('channel')
      AND candidate.priority = sqlc.arg('priority')
      AND candidate.next_attempt_at <= now()
    ORDER BY candidate.next_attempt_at
    LIMIT sqlc.arg('row_limit')
    FOR UPDATE SKIP LOCKED
)
UPDATE notifications
SET status = 'processing',
    attempts = notifications.attempts + 1,
    locked_until = now() + interval '60 seconds'
FROM due
WHERE notifications.id = due.id
RETURNING notifications.*;

-- name: MarkSent :execrows
WITH written AS (
    UPDATE notifications
    SET status = 'sent',
        sent_at = now(),
        provider_message_id = sqlc.narg('provider_message_id')
    WHERE id = sqlc.arg('id') AND status = 'processing' AND attempts = sqlc.arg('attempts')
    RETURNING id, attempts
)
INSERT INTO delivery_attempts (notification_id, attempt)
SELECT id, attempts FROM written;

-- name: MarkFailed :execrows
WITH written AS (
    UPDATE notifications
    SET status = 'failed',
        last_error = sqlc.arg('error')::text
    WHERE id = sqlc.arg('id') AND status = 'processing' AND attempts = sqlc.arg('attempts')
    RETURNING id, attempts, last_error
)
INSERT INTO delivery_attempts (notification_id, attempt, error)
SELECT id, attempts, last_error FROM written;

-- name: ScheduleRetry :execrows
WITH written AS (
    UPDATE notifications
    SET status = 'pending',
        next_attempt_at = now() + sqlc.arg('delay')::interval,
        last_error = sqlc.arg('error')::text
    WHERE id = sqlc.arg('id') AND status = 'processing' AND attempts = sqlc.arg('attempts')
    RETURNING id, attempts, last_error
)
INSERT INTO delivery_attempts (notification_id, attempt, error)
SELECT id, attempts, last_error FROM written;

-- name: ReleaseNotification :execrows
UPDATE notifications
SET status = 'pending',
    attempts = attempts - 1
WHERE id = sqlc.arg('id') AND status = 'processing' AND attempts = sqlc.arg('attempts');

-- name: SweepExpiredLeases :many
WITH swept AS (
    UPDATE notifications
    SET status = CASE WHEN attempts >= sqlc.arg('max_attempts')::smallint THEN 'failed' ELSE 'pending' END,
        last_error = sqlc.arg('error')::text
    WHERE status = 'processing' AND locked_until < now()
    RETURNING id, channel, status, attempts, last_error, correlation_id
), recorded AS (
    INSERT INTO delivery_attempts (notification_id, attempt, error)
    SELECT id, attempts, last_error FROM swept
)
SELECT id, channel, status, attempts, correlation_id FROM swept;

-- name: ListDeliveryAttempts :many
SELECT attempt, error, created_at FROM delivery_attempts
WHERE notification_id = $1
ORDER BY attempt;

-- name: CountPendingNotifications :many
SELECT
    channel,
    priority,
    count(*) AS pending,
    extract(epoch FROM greatest(now() - min(next_attempt_at), interval '0'))::float8 AS oldest_age_seconds
FROM notifications
WHERE status = 'pending'
GROUP BY channel, priority;
