SELECT
    CASE priority WHEN 0 THEN 'high' WHEN 1 THEN 'normal' ELSE 'low' END AS priority,
    count(*) AS sent,
    round(1000 * percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - created_at))) AS created_to_sent_p50_ms,
    round(1000 * percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - created_at))) AS created_to_sent_p95_ms,
    round(1000 * percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - created_at))) AS created_to_sent_p99_ms,
    round(1000 * percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - next_attempt_at))) AS due_to_sent_p50_ms,
    round(1000 * percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - next_attempt_at))) AS due_to_sent_p95_ms,
    round(1000 * percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - next_attempt_at))) AS due_to_sent_p99_ms
FROM notifications
WHERE status = 'sent'
GROUP BY priority
ORDER BY priority;
