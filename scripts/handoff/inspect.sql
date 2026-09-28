BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='10s';
SELECT json_build_object('database',current_database(),'version',version(),'observedAt',clock_timestamp(),
 'migrations',(SELECT json_agg(json_build_object('version',version,'success',success) ORDER BY installed_rank) FROM flyway_schema_history),
 'resources',(SELECT coalesce(json_agg(json_build_object('id',s.id,'name',s.name,'total',s.total_capacity,'available',s.available_capacity,'allocated',
  COALESCE((SELECT sum(r.quantity) FROM reservation r WHERE r.resource_id=s.id AND r.status IN ('HELD','CONFIRMED')),0)) ORDER BY s.id),'[]'::json) FROM resource s),
 'reservations',(SELECT coalesce(json_agg(json_build_object('id',id,'userId',user_id,'resourceId',resource_id,'quantity',quantity,'status',status,'expiresAt',expires_at,'due',expires_at<=clock_timestamp(),'sequence',transition_sequence) ORDER BY id),'[]'::json) FROM reservation),
 'outbox',(SELECT coalesce(json_agg(json_build_object('id',event_id,'reservationId',aggregate_id,'sequence',aggregate_sequence,'status',status,'attempts',attempt_count,'error',last_error,'replays',replay_count) ORDER BY event_id),'[]'::json) FROM outbox_event),
 'capacityDrift',(SELECT count(*) FROM resource s WHERE available_capacity+COALESCE((SELECT sum(r.quantity) FROM reservation r WHERE r.resource_id=s.id AND r.status IN ('HELD','CONFIRMED')),0)<>total_capacity),
 'events',(SELECT count(*) FROM outbox_event),'pending',(SELECT count(*) FROM outbox_event WHERE status='PENDING'),'failed',(SELECT count(*) FROM outbox_event WHERE status='FAILED'),
 'markers',(SELECT count(*) FROM processed_event),'effects',(SELECT count(*) FROM reservation_audit),'incompleteClaims',(SELECT count(*) FROM idempotency_record WHERE completed_at IS NULL),
 'activeConsumerFailures',(SELECT count(*) FROM consumer_failure WHERE status<>'RESOLVED'));
COMMIT;
