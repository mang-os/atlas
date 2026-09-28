package com.atlas.messaging;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;
import com.atlas.reconciliation.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ConsumerFailures {
 private final JdbcTemplate jdbc;
 private final RetryPolicy retry;
 public ConsumerFailures(JdbcTemplate jdbc,RetryPolicy retry) { this.jdbc=jdbc; this.retry=retry; }
 public void check(ConsumerRecord<String,String> record) {
  var rows=jdbc.queryForList("SELECT payload_hash,status,next_attempt_at<=clock_timestamp() AS due FROM consumer_failure WHERE consumer_name=? AND topic=? AND partition=? AND record_offset=?",
   ReservationAuditService.CONSUMER,record.topic(),record.partition(),record.offset());
  if (rows.isEmpty()) return;
  var row=rows.getFirst();
  if (!hash(record).equals(row.get("payload_hash"))) throw new IllegalStateException("Record identity changed; investigate without skipping");
  if ("FAILED".equals(row.get("status")) || "RETRY_WAIT".equals(row.get("status")) && !Boolean.TRUE.equals(row.get("due")))
   throw new IllegalStateException("Consumer record blocked by durable retry schedule");
 }
 @Transactional(propagation=Propagation.REQUIRES_NEW,isolation=Isolation.READ_COMMITTED,rollbackFor=Exception.class)
 public void failed(ConsumerRecord<String,String> record,UUID event,String category) {
  String hash=hash(record);
  jdbc.update("""
   INSERT INTO consumer_failure(consumer_name,topic,partition,record_offset,event_id,payload_hash,attempt_count,status,next_attempt_at,last_error)
   VALUES (?,?,?,?,?,?,1,'RETRY_WAIT',clock_timestamp(),?) ON CONFLICT (consumer_name,topic,partition,record_offset) DO UPDATE SET attempt_count=consumer_failure.attempt_count+1 WHERE consumer_failure.payload_hash=EXCLUDED.payload_hash
   """,ReservationAuditService.CONSUMER,record.topic(),record.partition(),record.offset(),event,hash,category);
  var row=jdbc.queryForMap("SELECT *,clock_timestamp() AS decided_at FROM consumer_failure WHERE consumer_name=? AND topic=? AND partition=? AND record_offset=? FOR UPDATE",
   ReservationAuditService.CONSUMER,record.topic(),record.partition(),record.offset());
  if (!hash.equals(row.get("payload_hash"))) throw new IllegalStateException("Record identity changed; failure ledger retained");
  int attempts=((Number)row.get("attempt_count")).intValue();
  long delay=retry.delayMs(attempts);
  boolean exhausted=attempts>=retry.maxAttempts();
  jdbc.update("""
   UPDATE consumer_failure SET event_id=COALESCE(event_id,?),status=?,next_attempt_at=CASE WHEN ? THEN NULL ELSE clock_timestamp()+?*interval '1 millisecond' END,
    last_error=?,updated_at=clock_timestamp() WHERE consumer_name=? AND topic=? AND partition=? AND record_offset=?
   """,event,exhausted?"FAILED":"RETRY_WAIT",exhausted,delay,category,ReservationAuditService.CONSUMER,record.topic(),record.partition(),record.offset());
 }
 @Transactional(propagation=Propagation.REQUIRES_NEW,rollbackFor=Exception.class)
 public void resolved(ConsumerRecord<String,String> record) {
  jdbc.update("UPDATE consumer_failure SET status='RESOLVED',next_attempt_at=NULL,updated_at=clock_timestamp() WHERE consumer_name=? AND topic=? AND partition=? AND record_offset=? AND payload_hash=?",
   ReservationAuditService.CONSUMER,record.topic(),record.partition(),record.offset(),hash(record));
 }
 public boolean canResume() {
  return Boolean.FALSE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM consumer_failure WHERE consumer_name=? AND (status='FAILED' OR status='RETRY_WAIT' AND next_attempt_at>clock_timestamp()))",Boolean.class,ReservationAuditService.CONSUMER));
 }
 public static String hash(ConsumerRecord<String,String> record) {
  try {
   var digest=MessageDigest.getInstance("SHA-256");
   for (String value:new String[]{record.key(),record.value()}) {
    byte[] bytes=value==null?null:value.getBytes(StandardCharsets.UTF_8);
    digest.update(ByteBuffer.allocate(4).putInt(bytes==null?-1:bytes.length).array());
    if (bytes!=null) digest.update(bytes);
   }
   return HexFormat.of().formatHex(digest.digest());
  } catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
 }
}
