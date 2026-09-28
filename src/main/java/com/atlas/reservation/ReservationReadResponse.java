package com.atlas.reservation;

import java.time.Instant;
import java.util.UUID;

/** Read metadata is deliberately absent from mutation DTOs and stored idempotency responses. */
public record ReservationReadResponse(UUID id, UUID userId, UUID resourceId, int quantity, ReservationStatus status,
        Instant expiresAt, Instant confirmedAt, Instant cancelledAt, Instant expiredAt, Instant createdAt, Instant updatedAt,
        Instant observedAt, boolean cached) {
    public static ReservationReadResponse from(ReservationResponse row, Instant observedAt, boolean cached) {
        return new ReservationReadResponse(row.id(),row.userId(),row.resourceId(),row.quantity(),row.status(),row.expiresAt(),
                row.confirmedAt(),row.cancelledAt(),row.expiredAt(),row.createdAt(),row.updatedAt(),observedAt,cached);
    }
}
