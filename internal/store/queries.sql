-- name: InsertNotification :one
INSERT INTO notifications (id, recipient, channel, content, priority)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetNotification :one
SELECT * FROM notifications
WHERE id = $1;
