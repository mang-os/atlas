package com.atlas.reservation;

import java.time.Instant;
import java.util.UUID;

/**
 * Internal immutable reservation snapshot. Persistence and future API DTOs are separate boundaries.
 * Terminal timestamps are nullable according to status; PostgreSQL enforces their row shape.
 * This snapshot does not authorize an operation or enforce cross-row conservation.
 */
public record Reservation(
        UUID id,
        UUID userId,
        UUID resourceId,
        int quantity,
        ReservationStatus status,
        Instant expiresAt,
        Instant confirmedAt,
        Instant cancelledAt,
        Instant expiredAt,
        Instant createdAt,
        Instant updatedAt,
        long transitionSequence) {
}
