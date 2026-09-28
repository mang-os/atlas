-- Stop all old application writers before activation. Never invent historical events.
LOCK TABLE reservation IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM reservation) THEN
        RAISE EXCEPTION 'Phase 9 requires an empty reservation dataset. Preserve existing data and plan an explicit historical import; no automatic reset or backfill is allowed.';
    END IF;
END $$;

ALTER TABLE reservation ADD COLUMN transition_sequence BIGINT NOT NULL DEFAULT 1;
ALTER TABLE reservation ADD CONSTRAINT reservation_transition_sequence_check CHECK (
    (status = 'HELD' AND transition_sequence = 1)
    OR (status IN ('CONFIRMED','CANCELLED','EXPIRED') AND transition_sequence = 2)
);

CREATE TABLE outbox_event (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL REFERENCES reservation(id) ON DELETE RESTRICT,
    aggregate_type VARCHAR(32) NOT NULL CHECK (aggregate_type = 'RESERVATION'),
    aggregate_sequence BIGINT NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    payload JSONB NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    last_error TEXT CHECK (char_length(last_error) <= 1024),
    CONSTRAINT outbox_aggregate_sequence_unique UNIQUE (aggregate_id, aggregate_sequence),
    CONSTRAINT outbox_type_sequence_check CHECK (
        (event_type = 'ReservationHeld' AND aggregate_sequence = 1)
        OR (event_type IN ('ReservationConfirmed','ReservationCancelled','ReservationExpired') AND aggregate_sequence = 2)
    ),
    CONSTRAINT outbox_publication_check CHECK (
        (status = 'PENDING' AND published_at IS NULL)
        OR (status = 'PUBLISHED' AND published_at IS NOT NULL)
    ),
    CONSTRAINT outbox_envelope_shape_check CHECK (
        jsonb_typeof(payload) = 'object'
        AND payload ?& ARRAY['eventId','schemaVersion','eventType','aggregateType','aggregateId','aggregateSequence','occurredAt','correlationId','data']
        AND jsonb_typeof(payload->'data') = 'object'
    )
);
CREATE INDEX outbox_pending_idx ON outbox_event(next_attempt_at,created_at,event_id) WHERE status='PENDING';
COMMENT ON TABLE outbox_event IS 'One immutable envelope per committed reservation transition. Indefinite retention. No publisher in Phase 9.';
