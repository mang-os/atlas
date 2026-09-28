package com.atlas.reservation;

import java.time.Instant;
import java.util.UUID;

/** Public DTO separate from the internal snapshot and JDBC persistence. */
public record ReservationResponse(UUID id, UUID userId, UUID resourceId, int quantity, ReservationStatus status,
                                  Instant expiresAt, Instant confirmedAt, Instant cancelledAt, Instant expiredAt,
                                  Instant createdAt, Instant updatedAt) {
    static ReservationResponse from(Reservation row) {
        return new ReservationResponse(row.id(), row.userId(), row.resourceId(), row.quantity(), row.status(),
                row.expiresAt(), row.confirmedAt(), row.cancelledAt(), row.expiredAt(), row.createdAt(), row.updatedAt());
    }
}
