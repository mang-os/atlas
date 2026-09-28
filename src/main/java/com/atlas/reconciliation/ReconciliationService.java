package com.atlas.reconciliation;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.atlas.configuration.RecoveryProperties;
import com.atlas.reservation.ExpirationProcessor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Reports use one repeatable-read snapshot. They never become an instruction to overwrite capacity or invent history. */
@Service
public class ReconciliationService {
 private final JdbcTemplate jdbc;
 private final RecoveryProperties properties;
 public ReconciliationService(JdbcTemplate jdbc,RecoveryProperties properties) { this.jdbc=jdbc;this.properties=properties; }
 private static final String FINDINGS="""
  WITH findings AS (
   SELECT 'CAPACITY_DRIFT'::text AS kind,s.id::text AS object_id,jsonb_build_object('total',s.total_capacity,'available',s.available_capacity,
    'allocated',COALESCE((SELECT sum(r.quantity) FROM reservation r WHERE r.resource_id=s.id AND r.status IN ('HELD','CONFIRMED')),0)) AS details
    FROM resource s WHERE s.available_capacity+COALESCE((SELECT sum(r.quantity) FROM reservation r WHERE r.resource_id=s.id AND r.status IN ('HELD','CONFIRMED')),0)<>s.total_capacity
   UNION ALL SELECT 'MISSING_EVENT',r.id::text,jsonb_build_object('sequence',seq.n,'resourceId',r.resource_id)
    FROM reservation r CROSS JOIN LATERAL generate_series(1,r.transition_sequence) seq(n)
    LEFT JOIN outbox_event e ON e.aggregate_id=r.id AND e.aggregate_sequence=seq.n WHERE e.event_id IS NULL
   UNION ALL SELECT 'EVENT_STATE_MISMATCH',e.event_id::text,jsonb_build_object('reservationId',r.id)
    FROM outbox_event e JOIN reservation r ON r.id=e.aggregate_id WHERE
     e.event_type<>CASE WHEN e.aggregate_sequence=1 THEN 'ReservationHeld' ELSE 'Reservation'||initcap(lower(r.status)) END
     OR e.aggregate_sequence>r.transition_sequence OR e.payload->>'eventId' IS DISTINCT FROM e.event_id::text
     OR e.payload->>'aggregateId' IS DISTINCT FROM e.aggregate_id::text
     OR e.payload->>'aggregateSequence' IS DISTINCT FROM e.aggregate_sequence::text
     OR e.payload->>'eventType' IS DISTINCT FROM e.event_type
   UNION ALL SELECT 'OVERDUE_HOLD',r.id::text,jsonb_build_object('expiresAt',r.expires_at,'resourceId',r.resource_id)
    FROM reservation r WHERE r.status='HELD' AND r.expires_at<=transaction_timestamp()
   UNION ALL SELECT 'STUCK_OUTBOX',e.event_id::text,jsonb_build_object('dueAt',e.next_attempt_at,'attempts',e.attempt_count)
    FROM outbox_event e WHERE e.status='PENDING' AND e.next_attempt_at<transaction_timestamp()-?*interval '1 millisecond'
   UNION ALL SELECT 'OLD_OUTBOX',e.event_id::text,jsonb_build_object('createdAt',e.created_at,'dueAt',e.next_attempt_at,'attempts',e.attempt_count)
    FROM outbox_event e WHERE e.status='PENDING' AND e.created_at<transaction_timestamp()-?*interval '1 millisecond'
   UNION ALL SELECT 'FAILED_OUTBOX',e.event_id::text,jsonb_build_object('attempts',e.attempt_count,'category',e.last_error,'replays',e.replay_count)
    FROM outbox_event e WHERE e.status='FAILED'
   UNION ALL SELECT 'FAILED_CONSUMER',concat(f.topic,':',f.partition,':',f.record_offset),jsonb_build_object('eventId',f.event_id,'attempts',f.attempt_count,'category',f.last_error,'replays',f.replay_count)
    FROM consumer_failure f WHERE f.status='FAILED'
   UNION ALL SELECT 'STUCK_CONSUMER',concat(f.topic,':',f.partition,':',f.record_offset),jsonb_build_object('dueAt',f.next_attempt_at,'attempts',f.attempt_count)
    FROM consumer_failure f WHERE f.status='RETRY_WAIT' AND f.next_attempt_at<transaction_timestamp()-?*interval '1 millisecond'
   UNION ALL SELECT 'CONSUMER_EFFECT_MISSING',p.event_id::text,jsonb_build_object('consumer',p.consumer_name)
    FROM processed_event p LEFT JOIN reservation_audit a ON a.event_id=p.event_id AND a.consumer_name=p.consumer_name WHERE a.event_id IS NULL
  )
  """;
 @Transactional(propagation=Propagation.REQUIRES_NEW,isolation=Isolation.REPEATABLE_READ,readOnly=true,timeout=10)
 public Report inspect() {
  Instant observed=jdbc.queryForObject("SELECT transaction_timestamp()",OffsetDateTime.class).toInstant();
  var counts=jdbc.query(FINDINGS+"SELECT kind,count(*) AS amount FROM findings GROUP BY kind ORDER BY kind",
   (row,index)->Map.entry(row.getString("kind"),row.getLong("amount")),properties.stuckMs(),properties.stuckMs(),properties.stuckMs());
  Map<String,Long> totals=new java.util.LinkedHashMap<>();counts.forEach(entry->totals.put(entry.getKey(),entry.getValue()));
  var findings=jdbc.query(FINDINGS+"SELECT kind,object_id,details::text FROM findings ORDER BY kind,object_id LIMIT ?",
   (row,index)->new Finding(row.getString("kind"),row.getString("object_id"),row.getString("details")),properties.stuckMs(),properties.stuckMs(),properties.stuckMs(),properties.batchSize());
  return new Report(observed,Map.copyOf(totals),List.copyOf(findings),totals.values().stream().mapToLong(Long::longValue).sum()>findings.size());
 }
 public List<UUID> overdueCandidates() {
  return jdbc.queryForList("SELECT id FROM reservation WHERE status='HELD' AND expires_at<=statement_timestamp() ORDER BY expires_at,id LIMIT ?",UUID.class,properties.batchSize());
 }
 public record Finding(String kind,String objectId,String details) { }
 public record Report(Instant observedAt,Map<String,Long> counts,List<Finding> findings,boolean truncated) {
  public long count(String kind) { return counts.getOrDefault(kind,0L); }
 }
}
