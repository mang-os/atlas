package com.atlas.outbox;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import com.atlas.configuration.KafkaMessagingConfiguration;
import com.atlas.messaging.ReservationEventDecoder;
import com.atlas.reconciliation.RetryPolicy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(prefix="atlas.kafka",name="enabled",havingValue="true")
public class OutboxPublisher {
 private final PublisherTransactions transactions;
 private final KafkaTemplate<String,String> kafka;
 private final RetryPolicy retry;
 private final ReservationEventDecoder decoder;
 public OutboxPublisher(PublisherTransactions transactions,KafkaTemplate<String,String> kafka,RetryPolicy retry,ReservationEventDecoder decoder) {
  this.transactions=transactions;this.kafka=kafka;this.retry=retry;this.decoder=decoder;
 }
 public boolean publishOne() {
  var result=transactions.execute(jdbc -> jdbc.execute((ConnectionCallback<Outcome>)connection -> {
   var rows=jdbc.queryForList("""
    SELECT event_id,aggregate_id,payload::text AS body,attempt_count FROM outbox_event
    WHERE status='PENDING' AND next_attempt_at<=statement_timestamp()
    ORDER BY next_attempt_at,created_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED
    """);
   if (rows.isEmpty()) return new Outcome(false,null);
   var row=rows.getFirst();
   UUID id=(UUID)row.get("event_id");
   int attempt=((Number)row.get("attempt_count")).intValue()+1;
   // The savepoint is AFTER the row lock. Rollback of a failed mark retains ownership of this event.
   var savepoint=connection.setSavepoint();
   String category="INVALID_ENVELOPE";
   try {
    if (attempt>retry.maxAttempts()) { category="ATTEMPT_BUDGET_EXHAUSTED"; throw new IllegalStateException("Attempt budget exhausted"); }
    var event=decoder.decode(row.get("aggregate_id").toString(),(String)row.get("body"));
    if (!id.equals(event.eventId())) throw new IllegalArgumentException("Outbox envelope identity mismatch");
    category="BROKER_SEND_FAILED";
    kafka.send(KafkaMessagingConfiguration.TOPIC,row.get("aggregate_id").toString(),(String)row.get("body")).get(6,TimeUnit.SECONDS);
    category="DATABASE_MARK_FAILED";
    int changed=jdbc.update("""
     UPDATE outbox_event SET status='PUBLISHED',published_at=clock_timestamp(),attempt_count=?,last_error=NULL
     WHERE event_id=? AND status='PENDING'
     """,attempt,id);
    if (changed!=1) throw new IllegalStateException("Locked event not marked");
    connection.releaseSavepoint(savepoint);
    return new Outcome(true,null);
   } catch (Exception failure) {
    connection.rollback(savepoint);
    connection.releaseSavepoint(savepoint);
    boolean exhausted=attempt>=retry.maxAttempts();
    jdbc.update("""
     UPDATE outbox_event SET status=?,attempt_count=?,next_attempt_at=clock_timestamp()+?*interval '1 millisecond',
      failed_at=CASE WHEN ? THEN clock_timestamp() ELSE NULL END,last_error=? WHERE event_id=? AND status='PENDING'
     """,exhausted?"FAILED":"PENDING",attempt>retry.maxAttempts()?attempt-1:attempt,retry.delayMs(attempt),exhausted,category,id);
    if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
    return new Outcome(false,exhausted?"FAILED":"RETRY_SCHEDULED");
   }
  }));
  // Throw only AFTER the failure ledger commits. Lost acknowledgment can still duplicate delivery.
  if (result.failure()!=null) throw new IllegalStateException("Outbox publication "+result.failure());
  return result.published();
 }
 private record Outcome(boolean published,String failure) { }
}
