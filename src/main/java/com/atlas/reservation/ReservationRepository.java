package com.atlas.reservation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ReservationRepository {
    private final JdbcTemplate jdbc;

    public ReservationRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public boolean allocate(UUID resourceId, int quantity) {
        // No pre-read: PostgreSQL evaluates/rechecks the predicate while serializing this row's update.
        return jdbc.update("""
                UPDATE resource
                SET available_capacity = available_capacity - ?, version = version + 1, updated_at = clock_timestamp()
                WHERE id = ? AND available_capacity >= ?
                """, quantity, resourceId, quantity) == 1;
    }

    public boolean resourceExists(UUID resourceId) {
        return Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM resource WHERE id=?)", Boolean.class, resourceId));
    }

    public Reservation insert(UUID ownerId, UUID resourceId, int quantity, int ttlSeconds) {
        // This statement runs AFTER allocation/lock acquisition. Materialization samples one DB decision time.
        return jdbc.queryForObject("""
                WITH decision AS MATERIALIZED (SELECT clock_timestamp() AS decided_at)
                INSERT INTO reservation(user_id,resource_id,quantity,status,created_at,updated_at,expires_at)
                SELECT ?, ?, ?, 'HELD', decided_at, decided_at, decided_at + (? * interval '1 second') FROM decision
                RETURNING *
                """, this::map, ownerId, resourceId, quantity, ttlSeconds);
    }

    public Optional<Reservation> findOwned(UUID id, UUID ownerId) {
        return jdbc.query("SELECT * FROM reservation WHERE id=? AND user_id=?", this::map, id, ownerId).stream().findFirst();
    }

    public Optional<java.time.Instant> ownedReadTime(UUID id, UUID ownerId) {
        return jdbc.query("SELECT clock_timestamp() AS observed_at FROM reservation WHERE id=? AND user_id=?",
                (row,index) -> row.getObject("observed_at",OffsetDateTime.class).toInstant(),id,ownerId).stream().findFirst();
    }
    public Optional<com.atlas.common.ReadCache.ReservationSnapshot> observedOwned(UUID id, UUID ownerId) {
        return jdbc.query("SELECT *,clock_timestamp() AS observed_at FROM reservation WHERE id=? AND user_id=?",
                (row,index) -> new com.atlas.common.ReadCache.ReservationSnapshot(ReservationResponse.from(map(row,index)),
                        row.getObject("observed_at",OffsetDateTime.class).toInstant()),id,ownerId).stream().findFirst();
    }

    public Optional<Reservation> lockOwned(UUID id, UUID ownerId) {
        return jdbc.query("SELECT * FROM reservation WHERE id=? AND user_id=? FOR UPDATE", this::map, id, ownerId)
                .stream().findFirst();
    }

    public List<UUID> dueCandidates(int limit) {
        return jdbc.queryForList("""
                SELECT id FROM reservation WHERE status='HELD' AND expires_at<=statement_timestamp()
                ORDER BY expires_at,id LIMIT ?
                """, UUID.class, limit);
    }

    public Optional<Reservation> lockForExpiration(UUID id) {
        return jdbc.query("SELECT * FROM reservation WHERE id=? FOR UPDATE SKIP LOCKED", this::map, id)
                .stream().findFirst();
    }

    /** Reservation is already locked; resource is second. Next statement sees all resource writers committed before this lock. */
    public boolean integrityForExpiration(UUID resourceId) {
        jdbc.queryForList("SELECT id FROM resource WHERE id=? FOR UPDATE",resourceId);
        return Boolean.TRUE.equals(jdbc.queryForObject("""
            SELECT s.available_capacity+COALESCE((SELECT sum(r.quantity) FROM reservation r
             WHERE r.resource_id=s.id AND r.status IN ('HELD','CONFIRMED')),0)=s.total_capacity
             AND NOT EXISTS(SELECT 1 FROM reservation r CROSS JOIN LATERAL generate_series(1,r.transition_sequence) seq(n)
              LEFT JOIN outbox_event e ON e.aggregate_id=r.id AND e.aggregate_sequence=seq.n
              WHERE r.resource_id=s.id AND (e.event_id IS NULL OR e.event_type<>
               CASE WHEN seq.n=1 THEN 'ReservationHeld' ELSE 'Reservation'||initcap(lower(r.status)) END
               OR e.payload->>'eventId' IS DISTINCT FROM e.event_id::text
               OR e.payload->>'aggregateId' IS DISTINCT FROM e.aggregate_id::text
               OR e.payload->>'aggregateSequence' IS DISTINCT FROM e.aggregate_sequence::text
               OR e.payload->>'eventType' IS DISTINCT FROM e.event_type))
            FROM resource s WHERE s.id=?
            """,Boolean.class,resourceId));
    }

    public java.time.Instant decisionTime() {
        // A separate statement AFTER the reservation lock returns: never transaction-start time or a pre-lock projection.
        return jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class).toInstant();
    }

    public Reservation transition(UUID id, ReservationStatus target, java.time.Instant decidedAt) {
        var time = OffsetDateTime.ofInstant(decidedAt, ZoneOffset.UTC);
        return jdbc.queryForObject("""
                UPDATE reservation SET status=?, updated_at=?, transition_sequence=transition_sequence+1,
                    confirmed_at=CASE WHEN ?='CONFIRMED' THEN ? ELSE NULL END,
                    cancelled_at=CASE WHEN ?='CANCELLED' THEN ? ELSE NULL END,
                    expired_at=CASE WHEN ?='EXPIRED' THEN ? ELSE NULL END
                WHERE id=? AND status='HELD' RETURNING *
                """, this::map, target.name(), time, target.name(), time, target.name(), time, target.name(), time, id);
    }

    public void release(UUID resourceId, int quantity, java.time.Instant decidedAt) {
        // Caller already holds the reservation lock. The UPDATE obtains the resource lock second.
        // Row bounds fail loudly on corruption; never clamp or silently skip a release.
        int changed = jdbc.update("""
                UPDATE resource SET available_capacity=available_capacity+?, version=version+1, updated_at=?
                WHERE id=?
                """, quantity, OffsetDateTime.ofInstant(decidedAt, ZoneOffset.UTC), resourceId);
        if (changed != 1) throw new org.springframework.dao.DataIntegrityViolationException("Reservation resource missing");
    }

    public List<Reservation> listOwned(UUID ownerId, int count, ReservationCursor cursor) {
        if (cursor == null) {
            return jdbc.query("SELECT * FROM reservation WHERE user_id=? ORDER BY created_at,id LIMIT ?", this::map, ownerId, count);
        }
        return jdbc.query("""
                SELECT * FROM reservation WHERE user_id=? AND (created_at,id) > (?,?)
                ORDER BY created_at,id LIMIT ?
                """, this::map, ownerId, OffsetDateTime.ofInstant(cursor.createdAt(), ZoneOffset.UTC), cursor.id(), count);
    }

    private Reservation map(ResultSet row, int index) throws SQLException {
        return new Reservation(row.getObject("id", UUID.class), row.getObject("user_id", UUID.class),
                row.getObject("resource_id", UUID.class), row.getInt("quantity"), ReservationStatus.valueOf(row.getString("status")),
                instant(row, "expires_at"), instant(row, "confirmed_at"), instant(row, "cancelled_at"), instant(row, "expired_at"),
                instant(row, "created_at"), instant(row, "updated_at"), row.getLong("transition_sequence"));
    }

    private java.time.Instant instant(ResultSet row, String column) throws SQLException {
        var value = row.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }
}
