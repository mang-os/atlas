package com.atlas.messaging;

import com.atlas.outbox.ReservationEvent;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ReservationAuditService {
    public static final String CONSUMER = "reservation-audit-v1";
    private final JdbcTemplate jdbc;
    public ReservationAuditService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED, rollbackFor = Exception.class)
    public boolean apply(ReservationEvent event, String payload) {
        boolean claimed = !jdbc.queryForList("""
                INSERT INTO processed_event(event_id,consumer_name) VALUES (?,?) ON CONFLICT DO NOTHING RETURNING event_id
                """, java.util.UUID.class, event.eventId(), CONSUMER).isEmpty();
        if (!claimed) {
            // Separate snapshot after the unique-index wait. Do not silently accept an ID with changed content.
            boolean same = Boolean.TRUE.equals(jdbc.queryForObject("""
                    SELECT EXISTS (SELECT 1 FROM reservation_audit WHERE event_id=? AND consumer_name=? AND payload=CAST(? AS jsonb))
                    """, Boolean.class, event.eventId(), CONSUMER, payload));
            if (!same) throw new DataIntegrityViolationException("Missing or conflicting audit effect for processed event");
            return false;
        }
        jdbc.update("""
                INSERT INTO reservation_audit(event_id,consumer_name,reservation_id,aggregate_sequence,event_type,payload)
                VALUES (?,?,?,?,?,CAST(? AS jsonb))
                """, event.eventId(), CONSUMER, event.aggregateId(), event.aggregateSequence(), event.eventType(), payload);
        return true;
    }
}
