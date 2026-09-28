package com.atlas;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.atlas.configuration.KafkaMessagingConfiguration;
import com.atlas.configuration.LocalIdentityConfiguration;
import com.atlas.messaging.ConsumerFailures;
import com.atlas.messaging.ReservationAuditListener;
import com.atlas.messaging.ReservationAuditService;
import com.atlas.messaging.ReservationEventDecoder;
import com.atlas.outbox.OutboxPublisher;
import com.atlas.outbox.PublisherTransactions;
import com.atlas.reconciliation.ReconciliationService;
import com.atlas.reconciliation.ReconciliationWorker;
import com.atlas.reconciliation.RetryPolicy;
import com.atlas.reservation.CreateReservationRequest;
import com.atlas.reservation.ReservationService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@Testcontainers(disabledWithoutDocker=false)
@Timeout(100)
class RecoveryIT {
 @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652").withDatabaseName("atlas_recovery_test").withUsername("atlas_test").withPassword("testcontainers-only");
 @Container static final KafkaContainer KAFKA=new KafkaContainer("apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837").withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE","false").withStartupTimeout(Duration.ofSeconds(120));
 static final String TOPIC=KafkaMessagingConfiguration.TOPIC;
 static final TopicPartition PARTITION=new TopicPartition(TOPIC,0);
 static final UUID ALICE=LocalIdentityConfiguration.ALICE_ID;
 @Autowired JdbcTemplate jdbc;
 @Autowired ReservationService reservations;
 @Autowired OutboxPublisher publisher;
 @Autowired PublisherTransactions transactions;
 @Autowired RetryPolicy retry;
 @Autowired ReservationEventDecoder decoder;
 @Autowired ReservationAuditListener listener;
 @Autowired ConsumerFailures failures;
 @Autowired ReconciliationService checks;
 @Autowired ReconciliationWorker worker;
 @Autowired org.springframework.kafka.core.KafkaTemplate<String,String> kafka;
 long start;
 Admin admin;
 @DynamicPropertySource static void properties(DynamicPropertyRegistry registry) {
  registry.add("spring.datasource.url",POSTGRES::getJdbcUrl);registry.add("spring.datasource.username",POSTGRES::getUsername);registry.add("spring.datasource.password",POSTGRES::getPassword);
  registry.add("spring.datasource.hikari.maximum-pool-size",()->16);
  registry.add("atlas.hold.ttl-seconds",()->3600);
  registry.add("atlas.kafka.enabled",()->true);registry.add("atlas.kafka.bootstrap-servers",KAFKA::getBootstrapServers);
  registry.add("atlas.kafka.publisher-enabled",()->false);registry.add("atlas.kafka.consumer-enabled",()->false);
  registry.add("atlas.recovery.max-attempts",()->3);registry.add("atlas.recovery.base-delay-ms",()->1000);registry.add("atlas.recovery.max-delay-ms",()->4000);
  registry.add("atlas.recovery.stuck-ms",()->1000);registry.add("atlas.recovery.batch-size",()->2);
 }
 @BeforeEach void reset() throws Exception {
  dropFaults();
  for(String table:List.of("consumer_failure","reservation_audit","processed_event","outbox_event","idempotency_record","reservation","resource")) jdbc.update("DELETE FROM "+table);
  admin=Admin.create(Map.of("bootstrap.servers",KAFKA.getBootstrapServers(),"default.api.timeout.ms",5000,"request.timeout.ms",3000));
  start=end();admin.deleteRecords(Map.of(PARTITION,RecordsToDelete.beforeOffset(start))).all().get(10,TimeUnit.SECONDS);
 }
 @AfterEach void cleanup() { dropFaults();if(admin!=null)admin.close(Duration.ofSeconds(5)); }

 @Test void publisherSchedulesEqualJitterPersistsAcrossInstancesAndExhausts() throws Exception {
  UUID id=hold(resource(10),1);failPublishedMark();String body=payload();UUID event=eventId();
  for(int attempt=1;attempt<=3;attempt++) {
   OffsetDateTime before=now();
   assertThatThrownBy(()->new OutboxPublisher(transactions,kafka,retry,decoder).publishOne()).isInstanceOf(IllegalStateException.class);
   OffsetDateTime after=now();var row=jdbc.queryForMap("SELECT * FROM outbox_event");
   assertThat(row.get("attempt_count")).isEqualTo(attempt);assertThat(row.get("last_error")).isEqualTo("DATABASE_MARK_FAILED");
   var next=jdbc.queryForObject("SELECT next_attempt_at FROM outbox_event",OffsetDateTime.class);long cap=retry.cap(attempt);
   assertThat(Duration.between(before,next).toMillis()).isGreaterThanOrEqualTo(cap/2);
   assertThat(Duration.between(after,next).toMillis()).isLessThanOrEqualTo(cap);
   assertThat(publisher.publishOne()).isFalse();
   if(attempt<3) { assertThat(row.get("status")).isEqualTo("PENDING");dueOutbox(); }
   else { assertThat(row.get("status")).isEqualTo("FAILED");assertThat(row.get("failed_at")).isNotNull(); }
  }
  assertThat(records(3)).allSatisfy(record->assertThat(record.value()).isEqualTo(body));
  assertThat(eventId()).isEqualTo(event);assertThat(reservations.find(ALICE,id).status().name()).isEqualTo("HELD");conserved();
  System.out.println("PHASE12 publisher failures=3 exhausted=true stableEvent=true boundedJitter=true earlySend=false");
 }
 @Test void concurrentControlledReplayHasOneWinnerAndOneConsumerEffect() throws Exception {
  hold(resource(10),1);failPublishedMark();exhaustPublisher();var event=eventId();String body=payload();dropFaults();
  var barrier=new CyclicBarrier(2);
  try(var pool=Executors.newFixedThreadPool(2)) {
   var first=pool.submit(()->{barrier.await(5,TimeUnit.SECONDS);return replayOutbox(event,0,"DEPENDENCY_RECOVERED");});
   var second=pool.submit(()->{barrier.await(5,TimeUnit.SECONDS);return replayOutbox(event,0,"DEPENDENCY_RECOVERED");});
   assertThat(List.of(first.get(10,TimeUnit.SECONDS),second.get(10,TimeUnit.SECONDS))).containsExactlyInAnyOrder(true,false);
  }
  assertThat(replayOutbox(event,0,"DEPENDENCY_RECOVERED")).isFalse();
  assertThatThrownBy(()->replayOutbox(event,1,"secret text")).isInstanceOf(RuntimeException.class);
  assertThat(jdbc.queryForMap("SELECT replay_count,attempt_count,last_replay_reason FROM outbox_event"))
   .containsEntry("replay_count",1).containsEntry("attempt_count",0).containsEntry("last_replay_reason","DEPENDENCY_RECOVERED");
  assertThat(publisher.publishOne()).isTrue();
  AtomicInteger acknowledgments=new AtomicInteger();
  for(var record:records(4)) listener.onRecord(record,()->acknowledgments.incrementAndGet());
  assertThat(acknowledgments).hasValue(4);assertThat(count("processed_event")).isEqualTo(1);assertThat(count("reservation_audit")).isEqualTo(1);
  assertThat(eventId()).isEqualTo(event);assertThat(payload()).isEqualTo(body);conserved();
  System.out.println("PHASE12 concurrentReplay winners=1 brokerDuplicates=4 auditEffects=1 eventIdPreserved=true");
 }
 @Test void publisherApplicationRestartRecoversOnlyWhenDurableScheduleIsDue() throws Exception {
  hold(resource(10),1);failPublishedMark();assertThatThrownBy(publisher::publishOne).isInstanceOf(RuntimeException.class);dropFaults();
  assertThat(publisher.publishOne()).isFalse();
  try(var app=application(true,false)) {
   await().atMost(Duration.ofSeconds(15)).until(()->"PUBLISHED".equals(jdbc.queryForObject("SELECT status FROM outbox_event",String.class)));
   assertThat(jdbc.queryForObject("SELECT attempt_count FROM outbox_event",Integer.class)).isEqualTo(2);
  }
  assertThat(records(2).get(0).value()).isEqualTo(records(2).get(1).value());conserved();
 }
 @Test void failedFailureBookkeepingRollsBackAndFreshPublisherKeepsOriginalId() throws Exception {
  hold(resource(10),1);UUID id=eventId();failAllOutboxUpdates();assertThatThrownBy(publisher::publishOne).isInstanceOf(RuntimeException.class);
  assertThat(jdbc.queryForMap("SELECT status,attempt_count FROM outbox_event")).containsEntry("status","PENDING").containsEntry("attempt_count",0);
  dropFaults();assertThat(publisher.publishOne()).isTrue();assertThat(eventId()).isEqualTo(id);assertThat(records(2)).hasSize(2);conserved();
 }
 @Test void invalidOutboxIdentityFailsVisiblyAndCannotBeReplayedOrInvented() {
  hold(resource(10),1);UUID id=eventId();jdbc.update("UPDATE outbox_event SET payload=jsonb_set(payload,'{eventId}',to_jsonb(?::text))",UUID.randomUUID().toString());
  exhaustPublisher();assertThat(jdbc.queryForObject("SELECT last_error FROM outbox_event",String.class)).isEqualTo("INVALID_ENVELOPE");
  assertThat(replayOutbox(id,0,"OPERATOR_VERIFIED")).isFalse();assertThat(checks.inspect().count("EVENT_STATE_MISMATCH")).isEqualTo(1);conserved();
 }
 @Test void acknowledgmentFailureRetainsEffectAndRespectsDurableWaitBeforeRetry() throws Exception {
  hold(resource(10),1);assertThat(publisher.publishOne()).isTrue();var record=records(1).getFirst();
  assertThatThrownBy(()->listener.onRecord(record,()->{throw new IllegalStateException("injected ack failure");})).isInstanceOf(RuntimeException.class);
  assertThat(count("processed_event")).isEqualTo(1);assertThat(count("reservation_audit")).isEqualTo(1);
  assertThat(jdbc.queryForMap("SELECT status,attempt_count,last_error FROM consumer_failure")).containsEntry("status","RETRY_WAIT").containsEntry("attempt_count",1).containsEntry("last_error","ACK_FAILED");
  assertThatThrownBy(()->listener.onRecord(record,()->{throw new AssertionError("must not acknowledge before due");})).isInstanceOf(RuntimeException.class);
  assertThat(jdbc.queryForObject("SELECT attempt_count FROM consumer_failure",Integer.class)).isEqualTo(1);
  dueConsumer();AtomicInteger ack=new AtomicInteger();listener.onRecord(record,()->ack.incrementAndGet());
  assertThat(ack).hasValue(1);assertThat(jdbc.queryForObject("SELECT status FROM consumer_failure",String.class)).isEqualTo("RESOLVED");assertThat(count("reservation_audit")).isEqualTo(1);conserved();
 }
 @Test void consumerExhaustionAndReplayPreserveOffsetHashAndEventId() throws Exception {
  hold(resource(10),1);assertThat(publisher.publishOne()).isTrue();var record=records(1).getFirst();failAuditInsert();
  for(int attempt=1;attempt<=3;attempt++) {
   assertThatThrownBy(()->listener.onRecord(record,()->{throw new AssertionError("failed effect cannot ack");})).isInstanceOf(RuntimeException.class);
   assertThat(jdbc.queryForObject("SELECT attempt_count FROM consumer_failure",Integer.class)).isEqualTo(attempt);
   assertThat(count("processed_event")).isZero();assertThat(count("reservation_audit")).isZero();if(attempt<3)dueConsumer();
  }
  assertThat(failures.canResume()).isFalse();assertThat(checks.inspect().count("FAILED_CONSUMER")).isEqualTo(1);
  String hash=jdbc.queryForObject("SELECT payload_hash FROM consumer_failure",String.class);UUID id=eventId();
  assertThatThrownBy(()->listener.onRecord(record,()->{throw new AssertionError();})).isInstanceOf(RuntimeException.class);
  assertThat(jdbc.queryForObject("SELECT attempt_count FROM consumer_failure",Integer.class)).isEqualTo(3);
  dropFaults();assertThat(replayConsumer(record,0)).isTrue();assertThat(replayConsumer(record,0)).isFalse();AtomicInteger ack=new AtomicInteger();listener.onRecord(record,()->ack.incrementAndGet());
  assertThat(ack).hasValue(1);assertThat(count("reservation_audit")).isEqualTo(1);
  assertThat(jdbc.queryForMap("SELECT status,event_id,payload_hash,replay_count FROM consumer_failure")).containsEntry("status","RESOLVED").containsEntry("event_id",id).containsEntry("payload_hash",hash).containsEntry("replay_count",1);conserved();
 }
 @Test void actualPoisonConsumerExhaustsStopsAcrossRestartAndNeverSkipsLaterRecord() throws Exception {
  UUID id=hold(resource(10),1);kafka.send(TOPIC,id.toString(),"{}").get(10,TimeUnit.SECONDS);assertThat(publisher.publishOne()).isTrue();
  try(var app=application(false,true)) { awaitFailedConsumer(); }
  assertThat(jdbc.queryForMap("SELECT status,attempt_count,event_id,last_error FROM consumer_failure"))
   .containsEntry("status","FAILED").containsEntry("attempt_count",3).containsEntry("event_id",null).containsEntry("last_error","INVALID_ENVELOPE");
  try(var app=application(false,true)) {
   var registry=app.getBean(org.springframework.kafka.config.KafkaListenerEndpointRegistry.class);
   await().atMost(Duration.ofSeconds(15)).until(()->!registry.getListenerContainer("reservationAudit").isRunning());
   assertThat(jdbc.queryForObject("SELECT attempt_count FROM consumer_failure",Integer.class)).isEqualTo(3);
  }
  assertThat(committedOffset()).isLessThan(start+1);assertThat(count("processed_event")).isZero();assertThat(count("reservation_audit")).isZero();conserved();
  System.out.println("PHASE12 poison attempts=3 stopped=true survivesRestart=true skippedOffsets=0");
 }
 @Test void exhaustedRealConsumerRecoversAfterControlledReplayAndApplicationRestart() throws Exception {
  hold(resource(10),1);assertThat(publisher.publishOne()).isTrue();var record=records(1).getFirst();UUID id=eventId();failAuditInsert();
  try(var app=application(false,true)) { awaitFailedConsumer(); }
  assertThat(committedOffset()).isLessThan(start+1);assertThat(count("processed_event")).isZero();dropFaults();
  assertThat(replayConsumer(record,0)).isTrue();
  try(var app=application(false,true)) {
   await().atMost(Duration.ofSeconds(20)).untilAsserted(()->{
    assertThat(count("reservation_audit")).isEqualTo(1);assertThat(committedOffset()).isEqualTo(start+1);
    assertThat(jdbc.queryForObject("SELECT status FROM consumer_failure",String.class)).isEqualTo("RESOLVED");
   });
  }
  assertThat(jdbc.queryForObject("SELECT event_id FROM reservation_audit",UUID.class)).isEqualTo(id);assertThat(count("processed_event")).isEqualTo(1);conserved();
 }
 @Test void consumerPayloadIdentityMismatchIsBlockedWithoutMutatingLedger() throws Exception {
  hold(resource(10),1);assertThat(publisher.publishOne()).isTrue();var original=records(1).getFirst();
  assertThatThrownBy(()->listener.onRecord(original,()->{throw new IllegalStateException();})).isInstanceOf(RuntimeException.class);dueConsumer();
  var changed=new ConsumerRecord<String,String>(original.topic(),original.partition(),original.offset(),original.key(),"{}");
  var before=jdbc.queryForMap("SELECT * FROM consumer_failure");assertThatThrownBy(()->listener.onRecord(changed,()->{throw new AssertionError();})).isInstanceOf(RuntimeException.class);
  assertThat(jdbc.queryForMap("SELECT * FROM consumer_failure")).isEqualTo(before);assertThat(count("reservation_audit")).isEqualTo(1);conserved();
 }
 @Test void reconciliationReportsOverdueStuckAndFailedWorkWithoutResettingBudgets() {
  UUID id=hold(resource(10),1);overdue(id);jdbc.update("UPDATE outbox_event SET next_attempt_at=clock_timestamp()-interval '5 seconds'");
  var report=checks.inspect();assertThat(report.count("OVERDUE_HOLD")).isEqualTo(1);assertThat(report.count("STUCK_OUTBOX")).isEqualTo(1);assertThat(report.count("CAPACITY_DRIFT")).isZero();
  failPublishedMark();exhaustPublisher();assertThat(checks.inspect().count("FAILED_OUTBOX")).isEqualTo(1);
  worker.runBatch();assertThat(jdbc.queryForObject("SELECT status FROM outbox_event WHERE aggregate_sequence=1",String.class)).isEqualTo("FAILED");conserved();
 }
 @Test void repeatedBoundedReconciliationExpiresOnceAndReportsTruncation() {
  UUID resource=resource(10);for(int i=0;i<5;i++)overdue(hold(resource,1));
  var report=checks.inspect();assertThat(report.count("OVERDUE_HOLD")).isEqualTo(5);assertThat(report.findings()).hasSize(2);assertThat(report.truncated()).isTrue();
  assertThat(worker.runBatch().expired()).isEqualTo(2);assertThat(worker.runBatch().expired()).isEqualTo(2);assertThat(worker.runBatch().expired()).isEqualTo(1);assertThat(worker.runBatch().expired()).isZero();
  assertThat(count("outbox_event")).isEqualTo(10);assertThat(jdbc.queryForObject("SELECT available_capacity FROM resource",Integer.class)).isEqualTo(10);conserved();
 }
 @Test void concurrentReconcilersReleaseOneDueHoldExactlyOnce() throws Exception {
  UUID id=hold(resource(10),1);overdue(id);var barrier=new CyclicBarrier(2);
  try(var pool=Executors.newFixedThreadPool(2)) {
   var first=pool.submit(()->{barrier.await(5,TimeUnit.SECONDS);return worker.runBatch().expired();});
   var second=pool.submit(()->{barrier.await(5,TimeUnit.SECONDS);return worker.runBatch().expired();});
   assertThat(first.get(10,TimeUnit.SECONDS)+second.get(10,TimeUnit.SECONDS)).isEqualTo(1);
  }
  assertThat(count("outbox_event")).isEqualTo(2);conserved();
 }
 @Test void capacityCorruptionIsReportedAndGuardedRepairRefusesToOverwriteIt() {
  UUID id=hold(resource(10),2);overdue(id);jdbc.update("UPDATE resource SET available_capacity=9");
  var before=jdbc.queryForMap("SELECT * FROM resource");assertThat(checks.inspect().count("CAPACITY_DRIFT")).isEqualTo(1);
  assertThat(worker.runBatch().refused()).isEqualTo(1);assertThat(worker.runBatch().refused()).isEqualTo(1);
  assertThat(jdbc.queryForMap("SELECT * FROM resource")).isEqualTo(before);assertThat(jdbc.queryForObject("SELECT status FROM reservation",String.class)).isEqualTo("HELD");assertThat(count("outbox_event")).isEqualTo(1);
 }
 @Test void missingHistoryIsReportedWithoutFabricationOrExpirationRepair() {
  UUID id=hold(resource(10),1);overdue(id);jdbc.update("DELETE FROM outbox_event");
  assertThat(checks.inspect().count("MISSING_EVENT")).isEqualTo(1);assertThat(worker.runBatch().refused()).isEqualTo(1);
  assertThat(count("outbox_event")).isZero();assertThat(jdbc.queryForObject("SELECT status FROM reservation",String.class)).isEqualTo("HELD");conserved();
 }
 @Test void missingConsumerEffectIsReportedWithoutRemovingMarkerOrInventingEffect() {
  UUID id=UUID.randomUUID();jdbc.update("INSERT INTO processed_event(event_id,consumer_name) VALUES (?,?)",id,ReservationAuditService.CONSUMER);
  assertThat(checks.inspect().count("CONSUMER_EFFECT_MISSING")).isEqualTo(1);worker.runBatch();assertThat(count("processed_event")).isEqualTo(1);assertThat(count("reservation_audit")).isZero();
 }
 @Test void snapshotInspectionDoesNotSeeUncommittedAllocationHalfwayThroughInsert() throws Exception {
  UUID resource=resource(10);gateReservationInsert();
  try(var gate=connection();var pool=Executors.newSingleThreadExecutor()) {
   gate.createStatement().execute("SELECT pg_advisory_lock(121212)");var future=pool.submit(()->hold(resource,1));awaitInsertionGate();
   for(int i=0;i<5;i++){var report=checks.inspect();assertThat(report.count("CAPACITY_DRIFT")).isZero();assertThat(report.count("MISSING_EVENT")).isZero();}
   gate.createStatement().execute("SELECT pg_advisory_unlock(121212)");future.get(10,TimeUnit.SECONDS);
  }
  assertThat(checks.inspect().count("CAPACITY_DRIFT")).isZero();conserved();
 }
 @Test void guardedExpirationWaitsForResourceWriterThenChecksFreshCommittedCapacity() throws Exception {
  UUID resource=resource(10);UUID due=hold(resource,1);overdue(due);gateReservationInsert();
  try(var gate=connection();var pool=Executors.newFixedThreadPool(2)) {
   gate.createStatement().execute("SELECT pg_advisory_lock(121212)");var allocation=pool.submit(()->hold(resource,1));awaitInsertionGate();
   var repair=pool.submit(worker::runBatch);
   await().atMost(Duration.ofSeconds(3)).until(()->jdbc.queryForObject("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='transactionid' AND query LIKE 'SELECT id FROM resource%FOR UPDATE%'",Long.class)>0);
   gate.createStatement().execute("SELECT pg_advisory_unlock(121212)");allocation.get(10,TimeUnit.SECONDS);assertThat(repair.get(10,TimeUnit.SECONDS).expired()).isEqualTo(1);
  }
  assertThat(jdbc.queryForObject("SELECT available_capacity FROM resource",Integer.class)).isEqualTo(9);assertThat(checks.inspect().count("CAPACITY_DRIFT")).isZero();conserved();
 }
 @Test void reconciliationDuringIndependentAllocationAndLifecycleTrafficHasNoFalseDrift() throws Exception {
  UUID resource=resource(100);var barrier=new CyclicBarrier(9);var finished=new CountDownLatch(8);AtomicInteger observations=new AtomicInteger();
  try(var pool=Executors.newFixedThreadPool(9)) {
   var tasks=new java.util.ArrayList<java.util.concurrent.Future<?>>();
   for(int thread=0;thread<8;thread++) tasks.add(pool.submit(()->{try {barrier.await(5,TimeUnit.SECONDS);for(int i=0;i<10;i++){UUID id=hold(resource,1);if(i%2==0)reservations.confirm(ALICE,id);else reservations.cancel(ALICE,id);}}finally{finished.countDown();}return null;}));
   var observer=pool.submit(()->{barrier.await(5,TimeUnit.SECONDS);do{var report=checks.inspect();assertThat(report.count("CAPACITY_DRIFT")).isZero();assertThat(report.count("MISSING_EVENT")).isZero();assertThat(report.count("EVENT_STATE_MISMATCH")).isZero();observations.incrementAndGet();}while(finished.getCount()>0);return null;});
   for(var task:tasks)task.get(30,TimeUnit.SECONDS);observer.get(30,TimeUnit.SECONDS);
  }
  assertThat(observations.get()).isPositive();assertThat(count("reservation")).isEqualTo(80);assertThat(count("outbox_event")).isEqualTo(160);assertThat(jdbc.queryForObject("SELECT available_capacity FROM resource",Integer.class)).isEqualTo(60);conserved();
  System.out.println("PHASE12 activeTraffic holds=80 transitions=80 snapshotChecks="+observations.get()+" falseDrift=0 available=60");
 }
 @Test void automaticReconciliationOnFreshApplicationRecoversDueHolds() {
  UUID id=hold(resource(10),1);overdue(id);
  try(var app=application(false,false,"--atlas.recovery.enabled=true","--atlas.recovery.poll-ms=100")) {
   await().atMost(Duration.ofSeconds(10)).until(()->"EXPIRED".equals(jdbc.queryForObject("SELECT status FROM reservation",String.class)));
   assertThat(app.containsBean("recoveryScheduling")).isTrue();
  }
  assertThat(count("outbox_event")).isEqualTo(2);conserved();
 }

 @Test void operationalInspectionScriptReportsCorruptionAndLeavesDataUnchanged() throws Exception {
  UUID id=hold(resource(10),1);jdbc.update("UPDATE resource SET available_capacity=10");var before=jdbc.queryForMap("SELECT * FROM resource");
  POSTGRES.copyFileToContainer(org.testcontainers.utility.MountableFile.forHostPath("scripts/reconcile.sql"),"/tmp/phase12-reconcile.sql");
  var result=POSTGRES.execInContainer("psql","-U",POSTGRES.getUsername(),"-d",POSTGRES.getDatabaseName(),"-v","stuck_ms=1000","-v","limit=2","-f","/tmp/phase12-reconcile.sql");
  assertThat(result.getExitCode()).isZero();assertThat(result.getStdout()).contains("CAPACITY_DRIFT","observed_at","COMMIT");
  assertThat(jdbc.queryForMap("SELECT * FROM resource")).isEqualTo(before);
  System.out.println("PHASE12 operatorScript exit=0 driftReported=true readOnly=true");
 }

 private UUID resource(int capacity){return jdbc.queryForObject("INSERT INTO resource(name,total_capacity,available_capacity) VALUES ('Recovery fixture',?,?) RETURNING id",UUID.class,capacity,capacity);}
 private UUID hold(UUID resource,int quantity){return reservations.create(ALICE,new CreateReservationRequest(resource,quantity)).id();}
 private UUID eventId(){return jdbc.queryForObject("SELECT event_id FROM outbox_event WHERE aggregate_sequence=1 ORDER BY created_at LIMIT 1",UUID.class);}
 private String payload(){return jdbc.queryForObject("SELECT payload::text FROM outbox_event WHERE aggregate_sequence=1 ORDER BY created_at LIMIT 1",String.class);}
 private long count(String table){return jdbc.queryForObject("SELECT count(*) FROM "+table,Long.class);}
 private OffsetDateTime now(){return jdbc.queryForObject("SELECT clock_timestamp()",OffsetDateTime.class);}
 private void conserved(){assertThat(checks.inspect().count("CAPACITY_DRIFT")).isZero();}
 private void dueOutbox(){jdbc.update("UPDATE outbox_event SET next_attempt_at=clock_timestamp()-interval '1 millisecond' WHERE status='PENDING'");}
 private void dueConsumer(){jdbc.update("UPDATE consumer_failure SET next_attempt_at=clock_timestamp()-interval '1 millisecond' WHERE status='RETRY_WAIT'");}
 private void exhaustPublisher(){for(int i=0;i<3;i++){dueOutbox();assertThatThrownBy(publisher::publishOne).isInstanceOf(RuntimeException.class);}}
 private boolean replayOutbox(UUID id,int expected,String reason){return Boolean.TRUE.equals(jdbc.queryForObject("SELECT atlas_replay_outbox(?,?,?)",Boolean.class,id,expected,reason));}
 private boolean replayConsumer(ConsumerRecord<String,String> record,int expected){return Boolean.TRUE.equals(jdbc.queryForObject("SELECT atlas_replay_consumer(?,?,?,?,?)",Boolean.class,record.topic(),record.partition(),record.offset(),expected,"FIX_DEPLOYED"));}
 private void overdue(UUID id){
  jdbc.update("UPDATE reservation SET created_at=clock_timestamp()-interval '3 seconds',updated_at=clock_timestamp()-interval '3 seconds',expires_at=clock_timestamp()-interval '1 second' WHERE id=?",id);
  jdbc.update("UPDATE outbox_event e SET created_at=r.created_at,next_attempt_at=r.created_at,payload=jsonb_set(jsonb_set(e.payload,'{occurredAt}',to_jsonb(r.created_at)),'{data,expiresAt}',to_jsonb(r.expires_at)) FROM reservation r WHERE r.id=e.aggregate_id AND r.id=?",id);
 }
 private void failPublishedMark(){jdbc.execute("CREATE FUNCTION phase12_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.status='PUBLISHED' THEN RAISE EXCEPTION 'test mark failure' USING ERRCODE='23514'; END IF; RETURN NEW; END $$");jdbc.execute("CREATE TRIGGER phase12_fault BEFORE UPDATE ON outbox_event FOR EACH ROW EXECUTE FUNCTION phase12_fault()");}
 private void failAllOutboxUpdates(){jdbc.execute("CREATE FUNCTION phase12_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test bookkeeping failure' USING ERRCODE='23514'; END $$");jdbc.execute("CREATE TRIGGER phase12_fault BEFORE UPDATE ON outbox_event FOR EACH ROW EXECUTE FUNCTION phase12_fault()");}
 private void failAuditInsert(){jdbc.execute("CREATE FUNCTION phase12_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test effect failure' USING ERRCODE='23514'; END $$");jdbc.execute("CREATE TRIGGER phase12_fault BEFORE INSERT ON reservation_audit FOR EACH ROW EXECUTE FUNCTION phase12_fault()");}
 private void gateReservationInsert(){jdbc.execute("CREATE FUNCTION phase12_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(121212); RETURN NEW; END $$");jdbc.execute("CREATE TRIGGER phase12_gate BEFORE INSERT ON reservation FOR EACH ROW EXECUTE FUNCTION phase12_gate()");}
 private void awaitInsertionGate(){await().atMost(Duration.ofSeconds(5)).until(()->jdbc.queryForObject("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE '%INSERT INTO reservation%'",Long.class)>0);}
 private void dropFaults(){for(String table:List.of("outbox_event","reservation_audit"))jdbc.execute("DROP TRIGGER IF EXISTS phase12_fault ON "+table);jdbc.execute("DROP TRIGGER IF EXISTS phase12_gate ON reservation");jdbc.execute("DROP FUNCTION IF EXISTS phase12_fault()");jdbc.execute("DROP FUNCTION IF EXISTS phase12_gate()");}
 private Connection connection() throws Exception{return DriverManager.getConnection(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword());}
 private long end() throws Exception{return admin.listOffsets(Map.of(PARTITION,OffsetSpec.latest())).partitionResult(PARTITION).get(10,TimeUnit.SECONDS).offset();}
 private long committedOffset() throws Exception {var offsets=admin.listConsumerGroupOffsets(KafkaMessagingConfiguration.GROUP).partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS);return offsets.containsKey(PARTITION)?offsets.get(PARTITION).offset():-1;}
 private List<ConsumerRecord<String,String>> records(int expected){
  try(var consumer=new KafkaConsumer<String,String>(Map.of("bootstrap.servers",KAFKA.getBootstrapServers(),"key.deserializer",StringDeserializer.class,"value.deserializer",StringDeserializer.class,"enable.auto.commit",false,"default.api.timeout.ms",5000))){
   consumer.assign(List.of(PARTITION));consumer.seek(PARTITION,start);var result=new java.util.ArrayList<ConsumerRecord<String,String>>();
   await().atMost(Duration.ofSeconds(15)).until(()->{consumer.poll(Duration.ofMillis(100)).forEach(result::add);return result.size()>=expected;});assertThat(result).hasSize(expected);return result;
  }
 }
 private org.springframework.context.ConfigurableApplicationContext application(boolean publish,boolean consume,String... extra){
  var args=new java.util.ArrayList<>(List.of("--server.port=0","--spring.datasource.url="+POSTGRES.getJdbcUrl(),"--spring.datasource.username="+POSTGRES.getUsername(),"--spring.datasource.password="+POSTGRES.getPassword(),"--atlas.kafka.enabled=true","--atlas.kafka.bootstrap-servers="+KAFKA.getBootstrapServers(),"--atlas.kafka.publisher-enabled="+publish,"--atlas.kafka.consumer-enabled="+consume,"--atlas.kafka.poll-ms=100","--atlas.recovery.max-attempts=3","--atlas.recovery.base-delay-ms=100","--atlas.recovery.max-delay-ms=400","--atlas.recovery.enabled=false"));for(String option:extra){String key=option.substring(0,option.indexOf('=')+1);args.removeIf(value->value.startsWith(key));args.add(option);}
  return new SpringApplicationBuilder(AtlasApplication.class).profiles("test").run(args.toArray(String[]::new));
 }
 private void awaitFailedConsumer(){await().atMost(Duration.ofSeconds(30)).until(()->jdbc.queryForObject("SELECT count(*) FROM consumer_failure WHERE status='FAILED' AND attempt_count=3",Long.class)==1);}
}
