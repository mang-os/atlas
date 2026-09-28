package com.atlas.outbox;

import java.time.Instant;
import java.util.UUID;
import com.atlas.reservation.Reservation;
import com.atlas.reservation.ReservationStatus;

/** Version 1 wire envelope. Ordering is logical per reservation, never global or transport arrival order. */
public record ReservationEvent(UUID eventId, int schemaVersion, String eventType, String aggregateType,
                               UUID aggregateId, long aggregateSequence, Instant occurredAt, UUID correlationId, Data data) {
    public record Data(UUID resourceId, UUID userId, int quantity, ReservationStatus status,
                       Instant expiresAt, int releasedQuantity) {}

    static ReservationEvent from(Reservation row, UUID correlationId) {
        String type = switch (row.status()) {
            case HELD -> "ReservationHeld";
            case CONFIRMED -> "ReservationConfirmed";
            case CANCELLED -> "ReservationCancelled";
            case EXPIRED -> "ReservationExpired";
        };
        boolean released = row.status() == ReservationStatus.CANCELLED || row.status() == ReservationStatus.EXPIRED;
        return new ReservationEvent(UUID.randomUUID(), 1, type, "RESERVATION", row.id(), row.transitionSequence(),
                row.updatedAt(), correlationId, new Data(row.resourceId(), row.userId(), row.quantity(), row.status(),
                row.expiresAt(), released ? row.quantity() : 0));
    }
}
