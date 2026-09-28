package com.atlas.outbox;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import com.atlas.common.RequestIdFilter;
import com.atlas.reservation.Reservation;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import tools.jackson.databind.json.JsonMapper;

/** Only appends to the caller's business transaction; never opens a separate commit or contacts a broker. */
@Service
public class OutboxWriter {
    private final JdbcTemplate jdbc;
    private final JsonMapper json;

    public OutboxWriter(JdbcTemplate jdbc, JsonMapper json) { this.jdbc = jdbc; this.json = json; }

    @Transactional(propagation = Propagation.MANDATORY, rollbackFor = Exception.class)
    public void append(Reservation reservation) {
        var event = ReservationEvent.from(reservation, correlationId());
        var time = OffsetDateTime.ofInstant(event.occurredAt(), ZoneOffset.UTC);
        jdbc.update("""
                INSERT INTO outbox_event(event_id,aggregate_id,aggregate_type,aggregate_sequence,event_type,
                    schema_version,payload,next_attempt_at,created_at)
                VALUES (?,?,'RESERVATION',?,?,?,CAST(? AS jsonb),?,?)
                """, event.eventId(), event.aggregateId(), event.aggregateSequence(), event.eventType(),
                event.schemaVersion(), json.writeValueAsString(event), time, time);
    }

    private UUID correlationId() {
        if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes context) {
            String requestId = RequestIdFilter.id(context.getRequest());
            if (requestId != null) return UUID.fromString(requestId);
        }
        // A worker/internal transition has no HTTP attempt; assign its own server-generated correlation.
        return UUID.randomUUID();
    }
}
