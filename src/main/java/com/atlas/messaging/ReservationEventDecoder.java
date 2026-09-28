package com.atlas.messaging;

import java.util.Objects;
import com.atlas.outbox.ReservationEvent;
import com.atlas.reservation.ReservationStatus;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** The broker payload is untrusted. Reject malformed/unsupported envelopes before any marker or acknowledgment. */
@Component
public class ReservationEventDecoder {
    private final JsonMapper json;
    public ReservationEventDecoder(JsonMapper json) { this.json = json; }

    public ReservationEvent decode(String key, String payload) {
        var tree = json.readTree(payload);
        if (tree == null || !tree.isObject()) throw invalid();
        for (String field : java.util.List.of("eventId", "schemaVersion", "eventType", "aggregateType", "aggregateId",
                "aggregateSequence", "occurredAt", "correlationId", "data")) if (!tree.hasNonNull(field)) throw invalid();
        var dataTree = tree.get("data");
        for (String field : java.util.List.of("resourceId", "userId", "quantity", "status", "expiresAt", "releasedQuantity"))
            if (!dataTree.hasNonNull(field)) throw invalid();
        var event = json.treeToValue(tree, ReservationEvent.class);
        if (event == null || event.eventId() == null || event.aggregateId() == null || event.correlationId() == null
                || event.occurredAt() == null || event.schemaVersion() != 1 || !"RESERVATION".equals(event.aggregateType())
                || !event.aggregateId().toString().equals(key) || event.data() == null) throw invalid();
        var data = event.data();
        if (data.resourceId() == null || data.userId() == null || data.quantity() < 1 || data.status() == null || data.expiresAt() == null) throw invalid();
        String type = switch (data.status()) {
            case HELD -> "ReservationHeld";
            case CONFIRMED -> "ReservationConfirmed";
            case CANCELLED -> "ReservationCancelled";
            case EXPIRED -> "ReservationExpired";
        };
        boolean released = data.status() == ReservationStatus.CANCELLED || data.status() == ReservationStatus.EXPIRED;
        long sequence = data.status() == ReservationStatus.HELD ? 1 : 2;
        if (!Objects.equals(event.eventType(), type) || event.aggregateSequence() != sequence
                || data.releasedQuantity() != (released ? data.quantity() : 0)) throw invalid();
        if (data.status() == ReservationStatus.EXPIRED ? event.occurredAt().isBefore(data.expiresAt())
                : !event.occurredAt().isBefore(data.expiresAt())) throw invalid();
        return event;
    }

    private IllegalArgumentException invalid() { return new IllegalArgumentException("Invalid or unsupported reservation event envelope"); }
}
