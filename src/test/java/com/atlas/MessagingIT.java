package com.atlas;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.atlas.configuration.KafkaMessagingConfiguration;
import com.atlas.configuration.LocalIdentityConfiguration;
import com.atlas.messaging.ReservationAuditListener;
import com.atlas.messaging.ReservationAuditService;
import com.atlas.messaging.ReservationEventDecoder;
import com.atlas.outbox.OutboxPublisher;
import com.atlas.outbox.PublisherTransactions;
import com.atlas.reservation.CreateReservationRequest;
import com.atlas.reservation.ReservationService;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real PostgreSQL and Kafka. All destructive fixtures are confined to these disposable containers. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@Testcontainers(disabledWithoutDocker = false)
@Timeout(90)
class MessagingIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_messaging_test").withUsername("atlas_test").withPassword("testcontainers-only");
    @Container static final KafkaContainer KAFKA = new KafkaContainer(
            "apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false").withStartupTimeout(Duration.ofSeconds(120));
    static final String TOPIC = KafkaMessagingConfiguration.TOPIC;
    static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    static final UUID ALICE = LocalIdentityConfiguration.ALICE_ID;
    @Autowired JdbcTemplate jdbc;
    @Autowired ReservationService reservations;
    @Autowired OutboxPublisher publisher;
    @Autowired PublisherTransactions publisherTransactions;
    @Autowired com.atlas.reconciliation.RetryPolicy retry;
    @Autowired ReservationAuditListener listener;
    @Autowired ReservationAuditService audit;
    @Autowired ReservationEventDecoder decoder;
    @Autowired KafkaTemplate<String,String> kafka;
    @Autowired ConcurrentKafkaListenerContainerFactory<String,String> factory;
    @Autowired PlatformTransactionManager transactionManager;
    private Admin admin;
    private long startOffset;
    private final List<ConcurrentMessageListenerContainer<String,String>> containers = new ArrayList<>();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> 12);
        registry.add("atlas.hold.ttl-seconds", () -> 3600);
        registry.add("atlas.kafka.enabled", () -> true);
        registry.add("atlas.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("atlas.kafka.publisher-enabled", () -> false);
        registry.add("atlas.kafka.consumer-enabled", () -> false);
    }

    @BeforeEach
    void reset() throws Exception {
        dropFaults();
        for (String table : List.of("consumer_failure", "reservation_audit", "processed_event", "outbox_event", "idempotency_record", "reservation", "resource"))
            jdbc.update("DELETE FROM " + table);
        admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(), "default.api.timeout.ms", 5000, "request.timeout.ms", 3000));
        startOffset = endOffset();
        admin.deleteRecords(Map.of(PARTITION, RecordsToDelete.beforeOffset(startOffset))).all().get(10, TimeUnit.SECONDS);
    }

    @AfterEach
    void cleanup() {
        containers.forEach(ConcurrentMessageListenerContainer::stop);
        containers.clear();
        dropFaults();
        if (admin != null) admin.close(Duration.ofSeconds(5));
    }

    @Test
    void normalDeliveryMarksOnlyAcknowledgedEventAndCommitsAuditBeforeOffset() throws Exception {
        UUID reservation = hold();
        String group = group();
        consume(group, listener::onRecord);
        assertThat(publisher.publishOne()).isTrue();
        delivered(group, startOffset + 1, 1);
        var records = records(1);
        assertThat(records.getFirst().key()).isEqualTo(reservation.toString());
        assertThat(jdbc.queryForMap("SELECT status,attempt_count,published_at FROM outbox_event"))
                .containsEntry("status", "PUBLISHED").containsEntry("attempt_count", 1).doesNotContainValue(null);
        assertThat(publisher.publishOne()).isFalse();
        assertThat(count("processed_event")).isEqualTo(1);
        conservation();
        System.out.println("PHASE10 normal brokerRecords=1 published=1 markers=1 auditEffects=1 offsetCommitted=true");
    }

    @Test
    void failureAfterBrokerAckBeforeMarkRollsBackThenPublishesSameEventAgain() throws Exception {
        hold();
        failOn("outbox_event", "UPDATE");
        assertThatThrownBy(publisher::publishOne).isInstanceOf(RuntimeException.class);
        assertThat(records(1)).hasSize(1); // Real broker proves send succeeded despite failed database update.
        assertThat(jdbc.queryForMap("SELECT status,attempt_count,published_at FROM outbox_event"))
                .containsEntry("status", "PENDING").containsEntry("attempt_count", 0).containsEntry("published_at", null);
        dropFaults();
        // A fresh publisher object recovers durable work; no in-memory claim or ID is carried over.
        assertThat(new OutboxPublisher(publisherTransactions, kafka, retry, decoder).publishOne()).isTrue();
        var delivered = records(2);
        assertThat(delivered.get(0).value()).isEqualTo(delivered.get(1).value());
        String group = group();
        consume(group, listener::onRecord);
        delivered(group, startOffset + 2, 1);
        assertThat(count("processed_event")).isEqualTo(1);
        conservation();
        System.out.println("PHASE10 sendBeforeMarkFailure brokerRecords=2 stablePayload=true markers=1 auditEffects=1");
    }

    @Test
    void twoConcurrentKafkaConsumersWaitOnUniqueMarkerAndApplyOneEffect() throws Exception {
        hold();
        gateOn("reservation_audit", "INSERT");
        String first = group(), second = group();
        try (var gate = gate()) {
            consume(first, listener::onRecord);
            consume(second, listener::onRecord);
            assertThat(publisher.publishOne()).isTrue();
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(waiters("INSERT INTO reservation_audit", "advisory")).isEqualTo(1);
                assertThat(waiters("INSERT INTO processed_event", "transactionid")).isEqualTo(1);
            });
            assertThat(count("processed_event")).isZero();
            assertThat(count("reservation_audit")).isZero();
            unlock(gate);
            delivered(first, startOffset + 1, 1);
            delivered(second, startOffset + 1, 1);
        }
        assertThat(count("processed_event")).isEqualTo(1);
        conservation();
        System.out.println("PHASE10 concurrentConsumers=2 observedUniqueWait=true markers=1 auditEffects=1 bothOffsetsCommitted=true");
    }

    @Test
    void failureBetweenConsumerCommitAndAcknowledgmentReplaysAfterContainerRestart() throws Exception {
        hold();
        String group = group();
        CountDownLatch committed = new CountDownLatch(1);
        var first = consume(group, (record, acknowledgment) -> listener.onRecord(record, () -> {
            assertThat(count("reservation_audit")).isEqualTo(1);
            assertThat(count("processed_event")).isEqualTo(1);
            committed.countDown();
            throw new IllegalStateException("Injected crash after database commit before Kafka acknowledgment");
        }));
        assertThat(publisher.publishOne()).isTrue();
        assertThat(committed.await(20, TimeUnit.SECONDS)).isTrue();
        await().atMost(Duration.ofSeconds(15)).until(() -> !first.isRunning());
        first.stop();
        assertThat(committedOffset(group)).isLessThan(startOffset + 1);
        await().atMost(Duration.ofSeconds(3)).until(() -> Boolean.TRUE.equals(jdbc.queryForObject("SELECT bool_and(next_attempt_at<=clock_timestamp()) FROM consumer_failure WHERE status='RETRY_WAIT'",Boolean.class)));
        consume(group, listener::onRecord);
        delivered(group, startOffset + 1, 1);
        assertThat(count("processed_event")).isEqualTo(1);
        conservation();
        System.out.println("PHASE10 commitBeforeAckFailure restartRedelivered=true markers=1 auditEffects=1 finalOffsetCommitted=true");
    }

    @Test
    void auditFailureRollsBackMarkerAndDoesNotAcknowledgeThenRetriesOnRestart() throws Exception {
        hold();
        failOn("reservation_audit", "INSERT");
        String group = group();
        var failed = consume(group, listener::onRecord);
        assertThat(publisher.publishOne()).isTrue();
        await().atMost(Duration.ofSeconds(25)).until(() -> !failed.isRunning());
        failed.stop();
        assertThat(count("processed_event")).isZero();
        assertThat(count("reservation_audit")).isZero();
        assertThat(committedOffset(group)).isLessThan(startOffset + 1);
        dropFaults();
        await().atMost(Duration.ofSeconds(3)).until(() -> Boolean.TRUE.equals(jdbc.queryForObject("SELECT bool_and(next_attempt_at<=clock_timestamp()) FROM consumer_failure WHERE status='RETRY_WAIT'",Boolean.class)));
        consume(group, listener::onRecord);
        delivered(group, startOffset + 1, 1);
        conservation();
        System.out.println("PHASE10 effectFailure rolledBackMarker=true noEarlyAck=true retryAuditEffects=1");
    }

    @Test
    void skipLockedCanPublishTerminalBeforeHeldAndAuditToleratesReordering() throws Exception {
        UUID id = hold();
        reservations.confirm(ALICE, id);
        try (var locked = connection()) {
            locked.setAutoCommit(false);
            try (var statement = locked.createStatement()) {
                statement.executeQuery("SELECT event_id FROM outbox_event WHERE aggregate_sequence=1 FOR UPDATE").close();
            }
            assertThat(publisher.publishOne()).isTrue();
            assertThat(publisher.publishOne()).isFalse();
            locked.rollback();
        }
        assertThat(publisher.publishOne()).isTrue();
        assertThat(records(2).stream().map(r -> decoder.decode(r.key(), r.value()).aggregateSequence())).containsExactly(2L, 1L);
        String group = group();
        consume(group, listener::onRecord);
        delivered(group, startOffset + 2, 2);
        assertThat(jdbc.queryForList("SELECT aggregate_sequence FROM reservation_audit ORDER BY aggregate_sequence", Long.class)).containsExactly(1L, 2L);
        conservation();
        System.out.println("PHASE10 arrivalSequences=[2,1] auditSequences=[1,2] effects=2 capacityUnchanged=true");
    }

    @Test
    void concurrentPublishersSkipClaimedRowAndDoNotWaitForItsBrokerUpdate() throws Exception {
        hold();
        gateOn("outbox_event", "UPDATE");
        try (var threads = Executors.newSingleThreadExecutor(); var gate = gate()) {
            var first = threads.submit(publisher::publishOne);
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(waiters("UPDATE outbox_event", "advisory")).isEqualTo(1));
            assertThat(records(1)).hasSize(1);
            assertThat(publisher.publishOne()).isFalse();
            unlock(gate);
            assertThat(first.get(10, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(endOffset()).isEqualTo(startOffset + 1);
        assertThat(jdbc.queryForObject("SELECT status FROM outbox_event", String.class)).isEqualTo("PUBLISHED");
        System.out.println("PHASE10 concurrentPublishers lockedRowSkipped=true brokerRecords=1");
    }

    @Test
    void publisherCannotSeeUncommittedBusinessEventsAndRollbackLeavesNothingToSend() {
        var transaction = new TransactionTemplate(transactionManager);
        transaction.executeWithoutResult(status -> {
            hold();
            assertThat(publisher.publishOne()).isFalse();
            status.setRollbackOnly();
        });
        assertThat(count("outbox_event")).isZero();
        assertThat(count("reservation")).isZero();
        assertThat(publisher.publishOne()).isFalse();
    }

    @Test
    void unavailableBrokerHasBoundedWaitLeavesPendingAndDoesNotConsumeApiPool() throws Exception {
        hold();
        // An unused local port is a real network failure, not a mocked Kafka acknowledgment.
        int port;
        try (var socket = new java.net.ServerSocket(0)) { port = socket.getLocalPort(); }
        var badFactory = new DefaultKafkaProducerFactory<String,String>(Map.of(
                "bootstrap.servers", "127.0.0.1:" + port, "key.serializer", StringSerializer.class,
                "value.serializer", StringSerializer.class, "max.block.ms", 2000, "request.timeout.ms", 2000,
                "delivery.timeout.ms", 5000, "linger.ms", 0, "acks", "all", "enable.idempotence", true));
        badFactory.setPhysicalCloseTimeout(1);
        try {
            long before = System.nanoTime();
            assertThatThrownBy(new OutboxPublisher(publisherTransactions, new KafkaTemplate<>(badFactory), retry, decoder)::publishOne)
                    .isInstanceOf(RuntimeException.class);
            assertThat(Duration.ofNanos(System.nanoTime() - before)).isLessThan(Duration.ofSeconds(10));
            assertThat(jdbc.queryForObject("SELECT status FROM outbox_event", String.class)).isEqualTo("PENDING");
            assertThat(jdbc.queryForObject("SHOW idle_in_transaction_session_timeout", String.class)).isEqualTo("0");
            String publisherIdleTimeout = publisherTransactions.execute(sql -> sql.queryForObject("SHOW idle_in_transaction_session_timeout", String.class));
            assertThat(publisherIdleTimeout).isEqualTo("15s");
            hold(); // PostgreSQL business transactions still work after a broker failure.
            await().atMost(Duration.ofSeconds(3)).until(() -> Boolean.TRUE.equals(jdbc.queryForObject("SELECT bool_and(next_attempt_at<=clock_timestamp()) FROM outbox_event WHERE status='PENDING'",Boolean.class)));
            assertThat(publisher.publishOne()).isTrue();
            assertThat(publisher.publishOne()).isTrue();
            conservation();
        } finally { badFactory.destroy(); }
        System.out.println("PHASE10 brokerUnavailable boundedFailure=true pendingPreserved=true independentApiPool=true recovery=true");
    }

    @Test
    void poisonRecordStopsBeforeLaterOffsetAndChangedDuplicateIsRejected() throws Exception {
        UUID id = hold();
        String group = group();
        kafka.send(TOPIC, id.toString(), "{}").get(10, TimeUnit.SECONDS);
        assertThat(publisher.publishOne()).isTrue();
        var failed = consume(group, listener::onRecord);
        await().atMost(Duration.ofSeconds(25)).until(() -> !failed.isRunning());
        failed.stop();
        assertThat(count("processed_event")).isZero();
        assertThat(count("reservation_audit")).isZero();
        assertThat(committedOffset(group)).isLessThan(startOffset + 1);
        var valid = records(2).get(1);
        var event = decoder.decode(valid.key(), valid.value());
        assertThat(audit.apply(event, valid.value())).isTrue();
        String changed = jdbc.queryForObject("SELECT jsonb_set(payload,'{data,quantity}','2')::text FROM outbox_event", String.class);
        assertThatThrownBy(() -> audit.apply(decoder.decode(valid.key(), changed), changed))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThat(count("processed_event")).isEqualTo(1);
        assertThat(count("reservation_audit")).isEqualTo(1);
        System.out.println("PHASE10 poison stoppedWithoutOffsetSkip=true conflictingEventIdRejected=true");
    }

    @Test
    void automaticPublisherAndAnnotatedConsumerRecoverPendingWorkOnApplicationRestart() throws Exception {
        hold();
        try (var application = new SpringApplicationBuilder(AtlasApplication.class).profiles("test").run(
                "--server.port=0", "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(), "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--atlas.kafka.enabled=true", "--atlas.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--atlas.kafka.publisher-enabled=true", "--atlas.kafka.consumer-enabled=true", "--atlas.kafka.poll-ms=100")) {
            delivered(KafkaMessagingConfiguration.GROUP, startOffset + 1, 1);
            assertThat(jdbc.queryForObject("SELECT status FROM outbox_event", String.class)).isEqualTo("PUBLISHED");
            assertThat(application.containsBean("outboxScheduler")).isTrue();
            assertThat(application.getBean("outboxScheduler")).isNotSameAs(application.getBean("taskScheduler"));
        }
        conservation();
        System.out.println("PHASE10 applicationRestart scheduledPublisher=true annotatedConsumer=true recoveredPending=1 auditEffects=1");
    }

    private UUID hold() {
        UUID resource = jdbc.queryForObject("INSERT INTO resource(name,total_capacity,available_capacity) VALUES ('Messaging fixture',10,10) RETURNING id", UUID.class);
        return reservations.create(ALICE, new CreateReservationRequest(resource, 1)).id();
    }
    private String group() { return "phase10-" + UUID.randomUUID(); }
    private long count(String table) { return jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class); }
    private void conservation() {
        assertThat(jdbc.queryForObject("""
                SELECT count(*) FROM resource s WHERE s.available_capacity + COALESCE(
                (SELECT sum(r.quantity) FROM reservation r WHERE r.resource_id=s.id AND r.status IN ('HELD','CONFIRMED')),0) <> s.total_capacity
                """, Long.class)).isZero();
    }
    private ConcurrentMessageListenerContainer<String,String> consume(String group, AcknowledgingMessageListener<String,String> callback) {
        var container = factory.createContainer(TOPIC);
        container.getContainerProperties().setGroupId(group);
        container.getContainerProperties().setMessageListener(callback);
        container.setBeanName(group);
        containers.add(container);
        container.start();
        return container;
    }
    private void delivered(String group, long offset, long effects) {
        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            assertThat(committedOffset(group)).isEqualTo(offset);
            assertThat(count("reservation_audit")).isEqualTo(effects);
            assertThat(count("processed_event")).isEqualTo(effects);
        });
    }
    private long committedOffset(String group) throws Exception {
        var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        return offsets.containsKey(PARTITION) ? offsets.get(PARTITION).offset() : -1;
    }
    private long endOffset() throws Exception {
        return admin.listOffsets(Map.of(PARTITION, OffsetSpec.latest())).all().get(5, TimeUnit.SECONDS).get(PARTITION).offset();
    }
    private List<ConsumerRecord<String,String>> records(int expected) {
        try (var consumer = new KafkaConsumer<String,String>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.deserializer", StringDeserializer.class, "value.deserializer", StringDeserializer.class, "enable.auto.commit", false))) {
            consumer.assign(List.of(PARTITION));
            consumer.seek(PARTITION, startOffset);
            var rows = new ArrayList<ConsumerRecord<String,String>>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (rows.size() < expected && System.nanoTime() < deadline) consumer.poll(Duration.ofMillis(300)).forEach(rows::add);
            assertThat(rows).hasSize(expected);
            return rows;
        }
    }
    private void dropFaults() {
        jdbc.execute("DROP TRIGGER IF EXISTS phase10_fault ON outbox_event");
        jdbc.execute("DROP TRIGGER IF EXISTS phase10_fault ON reservation_audit");
        jdbc.execute("DROP FUNCTION IF EXISTS phase10_fault()");
    }
    private void failOn(String table, String operation) {
        jdbc.execute("CREATE FUNCTION phase10_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Injected Phase10 failure'; END $$");
        jdbc.execute("CREATE TRIGGER phase10_fault BEFORE " + operation + " ON " + table + " FOR EACH ROW EXECUTE FUNCTION phase10_fault()");
    }
    private void gateOn(String table, String operation) {
        jdbc.execute("CREATE FUNCTION phase10_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(101010); RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER phase10_fault BEFORE " + operation + " ON " + table + " FOR EACH ROW EXECUTE FUNCTION phase10_fault()");
    }
    private Connection connection() throws Exception { return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()); }
    private Connection gate() throws Exception {
        var gate = connection();
        gate.createStatement().execute("SELECT pg_advisory_lock(101010)");
        return gate;
    }
    private void unlock(Connection gate) throws Exception { try (var statement = gate.createStatement()) { statement.execute("SELECT pg_advisory_unlock(101010)"); } }
    private long waiters(String query, String event) {
        return jdbc.queryForObject("SELECT count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND query LIKE ? AND wait_event=?", Long.class, "%" + query + "%", event);
    }
}
