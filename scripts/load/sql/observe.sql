-- Each invocation is one read-only committed snapshot. Counts never compare separate transactions.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='5s';
WITH scope AS (SELECT id FROM resource WHERE name LIKE 'load:' || :'run_id' || ':%'),
events AS (SELECT e.* FROM outbox_event e JOIN reservation r ON r.id=e.aggregate_id WHERE r.resource_id IN (SELECT id FROM scope)),
connections AS (SELECT count(*) AS total, count(*) FILTER (WHERE application_name='atlas-load-api') AS application,
  count(*) FILTER (WHERE application_name='atlas-load-api' AND state='active') AS active,
  count(*) FILTER (WHERE application_name='atlas-load-observer') AS observers
  FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend')
SELECT json_build_object('observedAt',clock_timestamp(),'totalConnections',c.total,'applicationConnections',c.application,
  'activeApplicationConnections',c.active,'observerConnections',c.observers,
  'pending',count(*) FILTER (WHERE e.status='PENDING'),'failed',count(*) FILTER (WHERE e.status='FAILED'),
  'published',count(*) FILTER (WHERE e.status='PUBLISHED'),
  'oldestPendingAgeMs',max(extract(epoch FROM clock_timestamp()-e.created_at)*1000) FILTER (WHERE e.status='PENDING'),
  'publishedDelayP50Ms',percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM e.published_at-e.created_at)*1000) FILTER (WHERE e.status='PUBLISHED'),
  'publishedDelayP95Ms',percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM e.published_at-e.created_at)*1000) FILTER (WHERE e.status='PUBLISHED'),
  'publishedDelayP99Ms',percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM e.published_at-e.created_at)*1000) FILTER (WHERE e.status='PUBLISHED'))
FROM connections c LEFT JOIN events e ON true GROUP BY c.total,c.application,c.active,c.observers;
COMMIT;
