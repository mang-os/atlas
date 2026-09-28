-- Window is taken from the measured k6 scenario, not from a guessed wall-clock start.
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='5s';
WITH events AS (SELECT e.* FROM outbox_event e JOIN reservation r ON r.id=e.aggregate_id JOIN resource s ON s.id=r.resource_id
 WHERE s.name LIKE 'load:' || :'run_id' || ':%'
 AND e.created_at>=to_timestamp(:'start_epoch_ms'::double precision/1000)
 AND e.created_at<=to_timestamp(:'end_epoch_ms'::double precision/1000))
SELECT json_build_object('windowStartEpochMs',:'start_epoch_ms'::double precision,'windowEndEpochMs',:'end_epoch_ms'::double precision,
 'events',count(*),'published',count(*) FILTER (WHERE status='PUBLISHED'),
 'delayP50Ms',percentile_cont(0.50) WITHIN GROUP(ORDER BY extract(epoch FROM published_at-created_at)*1000),
 'delayP95Ms',percentile_cont(0.95) WITHIN GROUP(ORDER BY extract(epoch FROM published_at-created_at)*1000),
 'delayP99Ms',percentile_cont(0.99) WITHIN GROUP(ORDER BY extract(epoch FROM published_at-created_at)*1000),
 'delayMaxMs',max(extract(epoch FROM published_at-created_at)*1000)) FROM events;
COMMIT;
