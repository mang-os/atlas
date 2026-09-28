CREATE TABLE processed_event (
    event_id UUID NOT NULL,
    consumer_name VARCHAR(100) NOT NULL CHECK (char_length(consumer_name) > 0),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (event_id, consumer_name)
);
CREATE TABLE reservation_audit (
    event_id UUID NOT NULL,
    consumer_name VARCHAR(100) NOT NULL,
    reservation_id UUID NOT NULL,
    aggregate_sequence BIGINT NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (event_id, consumer_name),
    FOREIGN KEY (event_id, consumer_name) REFERENCES processed_event(event_id, consumer_name) ON DELETE RESTRICT,
    CONSTRAINT audit_type_sequence_check CHECK (
        (event_type = 'ReservationHeld' AND aggregate_sequence = 1)
        OR (event_type IN ('ReservationConfirmed','ReservationCancelled','ReservationExpired') AND aggregate_sequence = 2)
    )
);
CREATE INDEX reservation_audit_reservation_sequence_idx ON reservation_audit(reservation_id, aggregate_sequence);
COMMENT ON TABLE reservation_audit IS 'Append-only consumer effect, order-independent. No FK to producer reservation/outbox. Marker and effect commit together before Kafka acknowledgment.';
