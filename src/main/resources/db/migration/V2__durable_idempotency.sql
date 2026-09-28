CREATE TABLE idempotency_record (
    caller_id UUID NOT NULL,
    operation VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL,
    request_hash CHAR(64) NOT NULL,
    response_status SMALLINT,
    response_body TEXT,
    response_content_type VARCHAR(100),
    response_location TEXT,
    reservation_id UUID REFERENCES reservation(id) ON DELETE RESTRICT,
    resource_id UUID REFERENCES resource(id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (caller_id, operation, idempotency_key),
    CONSTRAINT idempotency_operation_check CHECK (operation IN (
        'v1.resource.create', 'v1.reservation.create', 'v1.reservation.confirm', 'v1.reservation.cancel')),
    CONSTRAINT idempotency_key_check CHECK (idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$'),
    CONSTRAINT idempotency_hash_check CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT idempotency_response_shape_check CHECK (
        (completed_at IS NULL AND response_status IS NULL AND response_body IS NULL
            AND response_content_type IS NULL AND response_location IS NULL
            AND reservation_id IS NULL AND resource_id IS NULL)
        OR (completed_at IS NOT NULL AND response_status IS NOT NULL AND response_body IS NOT NULL
            AND response_content_type IS NOT NULL
            AND response_status IN (200, 201, 404, 409) AND response_content_type = 'application/json')
    )
);

-- Small corruption probe: application transactions must never commit unfinished claims.
CREATE INDEX idempotency_incomplete_idx ON idempotency_record(created_at) WHERE completed_at IS NULL;
CREATE INDEX idempotency_reservation_idx ON idempotency_record(reservation_id) WHERE reservation_id IS NOT NULL;
CREATE INDEX idempotency_resource_idx ON idempotency_record(resource_id) WHERE resource_id IS NOT NULL;
COMMENT ON TABLE idempotency_record IS
    'Indefinite retention. Claim, business mutation and exact UTF-8 JSON response commit together. No separate claim commit or cleanup.';
