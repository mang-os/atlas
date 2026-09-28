ALTER TABLE outbox_event DROP CONSTRAINT outbox_publication_check;
ALTER TABLE outbox_event ADD COLUMN failed_at TIMESTAMPTZ;
ALTER TABLE outbox_event ADD COLUMN replay_count INTEGER NOT NULL DEFAULT 0 CHECK (replay_count>=0);
ALTER TABLE outbox_event ADD COLUMN last_replayed_at TIMESTAMPTZ;
ALTER TABLE outbox_event ADD COLUMN last_replay_reason VARCHAR(32);
ALTER TABLE outbox_event ADD CONSTRAINT outbox_publication_check CHECK (
 (status='PENDING' AND published_at IS NULL AND failed_at IS NULL)
 OR (status='PUBLISHED' AND published_at IS NOT NULL AND failed_at IS NULL)
 OR (status='FAILED' AND published_at IS NULL AND failed_at IS NOT NULL AND last_error IS NOT NULL AND attempt_count>0));
ALTER TABLE outbox_event ADD CONSTRAINT outbox_replay_check CHECK (
 (replay_count=0 AND last_replayed_at IS NULL AND last_replay_reason IS NULL)
 OR (replay_count>0 AND last_replayed_at IS NOT NULL AND last_replay_reason IN ('DEPENDENCY_RECOVERED','FIX_DEPLOYED','OPERATOR_VERIFIED')));
ALTER TABLE outbox_event ADD CONSTRAINT outbox_error_category_check CHECK (last_error IS NULL OR last_error IN ('INVALID_ENVELOPE','BROKER_SEND_FAILED','DATABASE_MARK_FAILED','ATTEMPT_BUDGET_EXHAUSTED'));
CREATE INDEX outbox_pending_created_idx ON outbox_event(created_at,event_id) WHERE status='PENDING';
CREATE INDEX outbox_failed_idx ON outbox_event(failed_at,event_id) WHERE status='FAILED';

CREATE TABLE consumer_failure (
 consumer_name VARCHAR(100) NOT NULL CHECK (consumer_name='reservation-audit-v1'),
 topic VARCHAR(255) NOT NULL CHECK (topic='atlas.reservation-events.v1'),
 partition INTEGER NOT NULL CHECK (partition>=0),
 record_offset BIGINT NOT NULL CHECK (record_offset>=0),
 event_id UUID,
 payload_hash VARCHAR(64) NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
 attempt_count INTEGER NOT NULL CHECK (attempt_count>=0),
 status VARCHAR(16) NOT NULL CHECK (status IN ('RETRY_WAIT','FAILED','RESOLVED')),
 next_attempt_at TIMESTAMPTZ,
 last_error VARCHAR(64) NOT NULL CHECK (last_error IN ('INVALID_ENVELOPE','EFFECT_FAILED','ACK_FAILED')),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 replay_count INTEGER NOT NULL DEFAULT 0 CHECK (replay_count>=0),
 last_replayed_at TIMESTAMPTZ,
 last_replay_reason VARCHAR(32),
 PRIMARY KEY (consumer_name,topic,partition,record_offset),
 CONSTRAINT consumer_schedule_check CHECK ((status='RETRY_WAIT' AND next_attempt_at IS NOT NULL) OR (status IN ('FAILED','RESOLVED') AND next_attempt_at IS NULL)),
 CONSTRAINT consumer_replay_check CHECK (
 (replay_count=0 AND last_replayed_at IS NULL AND last_replay_reason IS NULL)
 OR (replay_count>0 AND last_replayed_at IS NOT NULL AND last_replay_reason IN ('DEPENDENCY_RECOVERED','FIX_DEPLOYED','OPERATOR_VERIFIED')))
);
CREATE INDEX consumer_retry_idx ON consumer_failure(next_attempt_at) WHERE status='RETRY_WAIT';
CREATE INDEX consumer_failed_idx ON consumer_failure(updated_at) WHERE status='FAILED';

-- SECURITY INVOKER: requires the caller's ordinary table privileges. Not a public API or permission bypass.
-- The expected replay count makes a repeated/concurrent operator command a no-op.
CREATE FUNCTION atlas_replay_outbox(p_event UUID,p_expected_replays INTEGER,p_reason TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE changed INTEGER;
BEGIN
 IF p_reason IS NULL OR p_reason NOT IN ('DEPENDENCY_RECOVERED','FIX_DEPLOYED','OPERATOR_VERIFIED') THEN
  RAISE EXCEPTION 'Use a documented replay reason code';
 END IF;
 UPDATE outbox_event SET status='PENDING',attempt_count=0,next_attempt_at=clock_timestamp(),failed_at=NULL,last_error=NULL,
  replay_count=replay_count+1,last_replayed_at=clock_timestamp(),last_replay_reason=p_reason
 WHERE event_id=p_event AND status='FAILED' AND replay_count=p_expected_replays
  AND payload->>'eventId'=event_id::text AND payload->>'aggregateId'=aggregate_id::text
  AND payload->>'eventType'=event_type AND payload->>'aggregateSequence'=aggregate_sequence::text;
 GET DIAGNOSTICS changed=ROW_COUNT;
 RETURN changed=1;
END $$;
CREATE FUNCTION atlas_replay_consumer(p_topic TEXT,p_partition INTEGER,p_offset BIGINT,p_expected_replays INTEGER,p_reason TEXT) RETURNS BOOLEAN
LANGUAGE plpgsql AS $$
DECLARE changed INTEGER;
BEGIN
 IF p_reason IS NULL OR p_reason NOT IN ('DEPENDENCY_RECOVERED','FIX_DEPLOYED','OPERATOR_VERIFIED') THEN
  RAISE EXCEPTION 'Use a documented replay reason code';
 END IF;
 UPDATE consumer_failure SET status='RETRY_WAIT',attempt_count=0,next_attempt_at=clock_timestamp(),
  replay_count=replay_count+1,last_replayed_at=clock_timestamp(),last_replay_reason=p_reason,updated_at=clock_timestamp()
 WHERE consumer_name='reservation-audit-v1' AND topic=p_topic AND partition=p_partition AND record_offset=p_offset
  AND status='FAILED' AND replay_count=p_expected_replays;
 GET DIAGNOSTICS changed=ROW_COUNT;
 RETURN changed=1;
END $$;
COMMENT ON TABLE consumer_failure IS 'Durable failed-record schedules; no payload logging or offset skipping. Identity hash includes key and value. Marker/effect remain independent authority.';
