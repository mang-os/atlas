BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='5s';
WITH scope AS (SELECT * FROM resource WHERE name LIKE 'load:' || :'run_id' || ':%'),
holds AS (SELECT * FROM reservation WHERE resource_id IN (SELECT id FROM scope)),
events AS (SELECT * FROM outbox_event WHERE aggregate_id IN (SELECT id FROM holds)),
markers AS (SELECT * FROM processed_event WHERE event_id IN (SELECT event_id FROM events)),
effects AS (SELECT * FROM reservation_audit WHERE event_id IN (SELECT event_id FROM events)),
drift AS (SELECT s.id FROM scope s WHERE s.available_capacity<0 OR s.available_capacity>s.total_capacity OR
  s.available_capacity+COALESCE((SELECT sum(quantity) FROM holds r WHERE r.resource_id=s.id AND r.status IN ('HELD','CONFIRMED')),0)<>s.total_capacity),
bad_history AS (SELECT r.id FROM holds r WHERE
  (SELECT count(*) FROM events e WHERE e.aggregate_id=r.id)<>r.transition_sequence OR
  NOT EXISTS (SELECT 1 FROM events e WHERE e.aggregate_id=r.id AND e.aggregate_sequence=1 AND e.event_type='ReservationHeld') OR
  (r.status<>'HELD' AND NOT EXISTS (SELECT 1 FROM events e WHERE e.aggregate_id=r.id AND e.aggregate_sequence=2 AND e.event_type=CASE r.status
     WHEN 'CONFIRMED' THEN 'ReservationConfirmed' WHEN 'CANCELLED' THEN 'ReservationCancelled' WHEN 'EXPIRED' THEN 'ReservationExpired' END)))
SELECT json_build_object('observedAt',clock_timestamp(),'resources',(SELECT count(*) FROM scope),
 'capacityDrift',(SELECT count(*) FROM drift),'illegalStates',(SELECT count(*) FROM holds WHERE quantity<=0 OR status NOT IN ('HELD','CONFIRMED','CANCELLED','EXPIRED')),
 'missingOrIllegalHistory',(SELECT count(*) FROM bad_history),
 'badEnvelopeIdentity',(SELECT count(*) FROM events WHERE payload->>'eventId' IS DISTINCT FROM event_id::text OR payload->>'aggregateId' IS DISTINCT FROM aggregate_id::text OR payload->>'aggregateSequence' IS DISTINCT FROM aggregate_sequence::text OR payload->>'eventType' IS DISTINCT FROM event_type),
 'incompleteClaims',(SELECT count(*) FROM idempotency_record WHERE idempotency_key LIKE :'run_id' || '.%' AND completed_at IS NULL),
 'reservations',(SELECT count(*) FROM holds),'held',(SELECT count(*) FROM holds WHERE status='HELD'),
 'confirmed',(SELECT count(*) FROM holds WHERE status='CONFIRMED'),'cancelled',(SELECT count(*) FROM holds WHERE status='CANCELLED'),
 'expired',(SELECT count(*) FROM holds WHERE status='EXPIRED'),
 'allocatedQuantity',(SELECT coalesce(sum(quantity),0) FROM holds WHERE status IN ('HELD','CONFIRMED')),
 'holdClaims',(SELECT count(*) FROM idempotency_record WHERE idempotency_key LIKE :'run_id' || '.%' AND operation='v1.reservation.create' AND response_status=201),
 'events',(SELECT count(*) FROM events),'pending',(SELECT count(*) FROM events WHERE status='PENDING'),
 'failed',(SELECT count(*) FROM events WHERE status='FAILED'),'markers',(SELECT count(*) FROM markers),'effects',(SELECT count(*) FROM effects),
 'consumerFailures',(SELECT count(*) FROM consumer_failure WHERE event_id IN (SELECT event_id FROM events) AND status<>'RESOLVED'),
 'missingEffects',(SELECT count(*) FROM markers p WHERE NOT EXISTS(SELECT 1 FROM effects a WHERE a.event_id=p.event_id AND a.consumer_name=p.consumer_name)),
 'missingMarkers',(SELECT count(*) FROM effects a WHERE NOT EXISTS(SELECT 1 FROM markers p WHERE p.event_id=a.event_id AND p.consumer_name=a.consumer_name)));
COMMIT;
