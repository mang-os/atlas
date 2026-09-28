CREATE TABLE resource (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(120) NOT NULL,
    total_capacity INTEGER NOT NULL,
    available_capacity INTEGER NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT resource_name_check CHECK (
        char_length(name) BETWEEN 1 AND 120 AND name !~ '^[[:space:]]|[[:space:]]$'
    ),
    CONSTRAINT resource_total_capacity_check CHECK (total_capacity > 0),
    CONSTRAINT resource_available_capacity_check CHECK (
        available_capacity >= 0 AND available_capacity <= total_capacity
    ),
    CONSTRAINT resource_version_check CHECK (version >= 0)
);

CREATE TABLE reservation (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    resource_id UUID NOT NULL,
    quantity INTEGER NOT NULL,
    status VARCHAR(16) NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    confirmed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    expired_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    CONSTRAINT reservation_resource_fk FOREIGN KEY (resource_id)
        REFERENCES resource (id) ON DELETE RESTRICT,
    CONSTRAINT reservation_quantity_check CHECK (quantity > 0),
    CONSTRAINT reservation_status_check CHECK (
        status IN ('HELD', 'CONFIRMED', 'CANCELLED', 'EXPIRED')
    ),
    CONSTRAINT reservation_deadline_check CHECK (expires_at > created_at),
    CONSTRAINT reservation_terminal_timestamp_check CHECK (
        (status = 'HELD' AND confirmed_at IS NULL AND cancelled_at IS NULL AND expired_at IS NULL)
        OR (status = 'CONFIRMED' AND confirmed_at IS NOT NULL AND cancelled_at IS NULL AND expired_at IS NULL)
        OR (status = 'CANCELLED' AND confirmed_at IS NULL AND cancelled_at IS NOT NULL AND expired_at IS NULL)
        OR (status = 'EXPIRED' AND confirmed_at IS NULL AND cancelled_at IS NULL AND expired_at IS NOT NULL)
    ),
    CONSTRAINT reservation_confirmed_time_check CHECK (
        confirmed_at IS NULL OR (confirmed_at >= created_at AND confirmed_at < expires_at)
    ),
    CONSTRAINT reservation_cancelled_time_check CHECK (
        cancelled_at IS NULL OR (cancelled_at >= created_at AND cancelled_at < expires_at)
    ),
    CONSTRAINT reservation_expired_time_check CHECK (
        expired_at IS NULL OR expired_at >= expires_at
    )
);

CREATE INDEX reservation_user_created_id_idx ON reservation (user_id, created_at, id);
CREATE INDEX reservation_resource_status_idx ON reservation (resource_id, status);
CREATE INDEX reservation_held_expiry_idx ON reservation (expires_at, id) WHERE status = 'HELD';

COMMENT ON TABLE resource IS
    'Row capacity bounds only; conservation with HELD and CONFIRMED reservations requires application transactions.';
COMMENT ON TABLE reservation IS
    'Row shape only; legal transitions are HELD to CONFIRMED/CANCELLED/EXPIRED. CONFIRMED retains capacity. No transition trigger.';
