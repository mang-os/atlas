-- Caller must first stop all application workers and verify committed Kafka group lag is zero.
-- The manifest IDs AND run-prefixed names must match exactly. No table-wide DELETE or TRUNCATE.
BEGIN;
SET LOCAL statement_timeout='5s';
SET LOCAL lock_timeout='2s';
SELECT set_config('atlas.load.run_id',:'run_id',true);
SELECT set_config('atlas.load.resource_ids',:'resource_ids',true);
CREATE TEMP TABLE load_scope ON COMMIT DROP AS SELECT id FROM resource WHERE name LIKE 'load:' || :'run_id' || ':%';
DO $$ BEGIN
 IF current_setting('atlas.load.run_id') !~ '^[a-z0-9-]{8,64}$' THEN RAISE EXCEPTION 'Invalid run identity'; END IF;
 IF EXISTS ((SELECT id::text FROM load_scope EXCEPT SELECT jsonb_array_elements_text(current_setting('atlas.load.resource_ids')::jsonb))
   UNION ALL (SELECT jsonb_array_elements_text(current_setting('atlas.load.resource_ids')::jsonb) EXCEPT SELECT id::text FROM load_scope))
   OR NOT EXISTS (SELECT 1 FROM load_scope) THEN RAISE EXCEPTION 'Run resource identity mismatch'; END IF;
 IF EXISTS (SELECT 1 FROM outbox_event e JOIN reservation r ON r.id=e.aggregate_id WHERE r.resource_id IN (SELECT id FROM load_scope) AND e.status<>'PUBLISHED')
 THEN RAISE EXCEPTION 'Undelivered events prohibit cleanup'; END IF;
 IF EXISTS (SELECT 1 FROM outbox_event e JOIN reservation r ON r.id=e.aggregate_id WHERE r.resource_id IN (SELECT id FROM load_scope)
   AND NOT EXISTS (SELECT 1 FROM reservation_audit a WHERE a.event_id=e.event_id AND a.consumer_name='reservation-audit-v1'))
 THEN RAISE EXCEPTION 'Unconsumed events prohibit cleanup'; END IF;
END $$;
CREATE TEMP TABLE load_events ON COMMIT DROP AS SELECT e.event_id FROM outbox_event e JOIN reservation r ON r.id=e.aggregate_id WHERE r.resource_id IN (SELECT id FROM load_scope);
DELETE FROM consumer_failure WHERE event_id IN (SELECT event_id FROM load_events);
DELETE FROM reservation_audit WHERE event_id IN (SELECT event_id FROM load_events);
DELETE FROM processed_event WHERE event_id IN (SELECT event_id FROM load_events);
DELETE FROM outbox_event WHERE event_id IN (SELECT event_id FROM load_events);
DELETE FROM idempotency_record WHERE idempotency_key LIKE :'run_id' || '.%'
 AND caller_id IN ('11111111-1111-4111-8111-111111111111','33333333-3333-4333-8333-333333333333');
DELETE FROM reservation WHERE resource_id IN (SELECT id FROM load_scope);
DELETE FROM resource WHERE id IN (SELECT id FROM load_scope);
SELECT json_build_object('runId',:'run_id','remainingResources',(SELECT count(*) FROM resource WHERE name LIKE 'load:' || :'run_id' || ':%'),
 'remainingKeys',(SELECT count(*) FROM idempotency_record WHERE idempotency_key LIKE :'run_id' || '.%'));
COMMIT;
