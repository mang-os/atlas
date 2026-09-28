package com.atlas;

import java.net.Socket;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.atlas.configuration.KafkaMessagingConfiguration;
import com.atlas.messaging.ReservationAuditListener;
import com.atlas.outbox.OutboxPublisher;
import com.atlas.reservation.ExpirationWorker;
import com.jayway.jsonpath.JsonPath;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** All faults, backend termination and container controls target only this suite's disposable infrastructure. */
@Testcontainers(disabledWithoutDocker = false)
@Timeout(120)
class FailureRecoveryIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_failure_test").withUsername("atlas_test").withPassword("testcontainers-only");
    @Container static final KafkaContainer KAFKA = new KafkaContainer(
            "apache/kafka@sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false").withStartupTimeout(Duration.ofSeconds(120));
    private static final int REDIS_PORT = freeLoopbackPort();
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>(
            "redis@sha256:0514fa59e3d84e2f3be66731c5401fc2a85b8ea71b49c0116c850f6a558076f4")
            .withExposedPorts(6379).withCommand("redis-server", "--save", "", "--appendonly", "no")
            // Docker can reassign an ephemeral published port on start. Recovery requires a stable endpoint.
            .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    new PortBinding(Ports.Binding.bindIpAndPort("127.0.0.1", REDIS_PORT), ExposedPort.tcp(6379))));
    private static final String TOPIC = KafkaMessagingConfiguration.TOPIC;
    private static final String GROUP = KafkaMessagingConfiguration.GROUP;
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final long GATE = 131313;
    private static final String PASSWORD = UUID.randomUUID().toString();
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final List<ConfigurableApplicationContext> applications = new ArrayList<>();
    private final List<ConcurrentMessageListenerContainer<String,String>> consumers = new ArrayList<>();
    private JdbcTemplate jdbc;
    private ExecutorService threads;
    private Admin admin;
    private long startOffset;
    private boolean kafkaPaused, redisStopped;

    @BeforeEach
    void prepareIsolatedInfrastructure() throws Exception {
        jdbc = new JdbcTemplate(new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        jdbc.setQueryTimeout(10);
        threads = Executors.newVirtualThreadPerTaskExecutor();
        admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(), "default.api.timeout.ms", 5000, "request.timeout.ms", 3000));
        if (!admin.listTopics().names().get(5, TimeUnit.SECONDS).contains(TOPIC))
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1).configs(Map.of("retention.ms", "-1"))))
                    .all().get(10, TimeUnit.SECONDS);
        // Migrate through the actual application before touching fixture tables.
        if (Boolean.TRUE.equals(jdbc.queryForObject("SELECT to_regclass('public.resource') IS NULL", Boolean.class)))
            close(application(false, false, false));
        dropFaults();
        for (String table : List.of("consumer_failure", "reservation_audit", "processed_event", "outbox_event", "idempotency_record", "reservation", "resource"))
            jdbc.update("DELETE FROM " + table);
        startOffset = endOffset();
        admin.deleteRecords(Map.of(PARTITION, RecordsToDelete.beforeOffset(startOffset))).all().get(10, TimeUnit.SECONDS);
        assertThat(REDIS.execInContainer("redis-cli", "FLUSHDB").getExitCode()).isZero();
    }

    @AfterEach
    void releaseFaultsAndAssertDurableInvariants() throws Exception {
        resumeKafka();
        startRedis();
        for (var consumer : consumers) consumer.stop();
        consumers.clear();
        for (var app : List.copyOf(applications)) close(app);
        if (threads != null) {
            threads.shutdownNow();
            assertThat(threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        dropFaults();
        if (admin != null) admin.close(Duration.ofSeconds(5));
        invariants();
    }

    @Test
    void terminatedPostgresBackendBeforeCommitRollsBackEveryWriteAndSameKeyRecovers() throws Exception {
        var app = application(false, false, false);
        UUID resource = resource(app, 10);
        String before = resourceSnapshot(resource);
        jdbc.execute("CREATE SEQUENCE phase13_seen");
        jdbc.execute("""
                CREATE FUNCTION phase13_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                  IF NEW.idempotency_key='db-before-commit' AND NEW.completed_at IS NOT NULL THEN
                    IF NOT EXISTS (SELECT 1 FROM reservation r JOIN resource s ON s.id=r.resource_id
                       JOIN outbox_event e ON e.aggregate_id=r.id
                       WHERE r.id=NEW.reservation_id AND r.status='HELD' AND s.available_capacity=8) THEN
                      RAISE EXCEPTION 'Required transaction writes not reached';
                    END IF;
                    PERFORM nextval('phase13_seen');
                    PERFORM pg_advisory_xact_lock(131313);
                  END IF;
                  RETURN NEW;
                END $$
                """);
        jdbc.execute("CREATE TRIGGER phase13_fault BEFORE UPDATE ON idempotency_record FOR EACH ROW EXECUTE FUNCTION phase13_fault()");
        try (Connection gate = gate()) {
            var request = threads.submit(() -> hold(app, resource, 2, "db-before-commit"));
            int pid = awaitBackend("UPDATE idempotency_record");
            assertThat(jdbc.queryForObject("SELECT is_called FROM phase13_seen", Boolean.class)).isTrue();
            assertThat(count("reservation")).isZero();
            assertThat(count("outbox_event")).isZero();
            assertThat(count("idempotency_record WHERE idempotency_key='db-before-commit'")).isZero();
            assertThat(resourceSnapshot(resource)).isEqualTo(before);
            terminateObservedBackend(pid, "UPDATE idempotency_record");
            var failed = request.get(15, TimeUnit.SECONDS);
            assertThat(failed.statusCode()).isEqualTo(503);
            assertThat(failed.body()).contains("TEMPORARILY_UNAVAILABLE").doesNotContain("131313", "SQL", "phase13_fault");
        }
        assertThat(resourceSnapshot(resource)).isEqualTo(before);
        assertThat(count("reservation")).isZero();
        assertThat(count("outbox_event")).isZero();
        assertThat(count("idempotency_record WHERE idempotency_key='db-before-commit'")).isZero();
        dropFaults();
        assertThat(hold(app, resource, 2, "db-before-commit").statusCode()).isEqualTo(201);
        assertThat(hold(app, resource, 2, "db-before-commit").headers().firstValue("Idempotency-Replayed")).contains("true");
        assertThat(count("reservation")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
        capacity(resource, 8);
        invariants();
        System.out.println("PHASE13 postgresBeforeCommit boundaryObserved=true backendTerminated=true rollback=all sameKeyRecovery=201 reservations=1 conservation=PASS");
    }

    @Test
    void lostHttpResponseAfterObservedCommitReplaysAcrossApplicationRestart() throws Exception {
        var before = application(false, false, false);
        UUID resource = resource(before, 10);
        String body = holdBody(resource, 3);
        try (Socket socket = new Socket("127.0.0.1", port(before))) {
            String wire = "POST /api/reservations HTTP/1.1\r\nHost: 127.0.0.1:" + port(before)
                    + "\r\nAuthorization: " + basic("alice") + "\r\nContent-Type: application/json"
                    + "\r\nIdempotency-Key: lost-response\r\nContent-Length: " + body.getBytes(StandardCharsets.UTF_8).length
                    + "\r\nConnection: close\r\n\r\n" + body;
            socket.getOutputStream().write(wire.getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            await().atMost(Duration.ofSeconds(10)).until(() -> count("idempotency_record WHERE idempotency_key='lost-response' AND completed_at IS NOT NULL") == 1);
            // No response byte is read. The independent database connection proves commit before disconnect.
        }
        var saved = jdbc.queryForMap("SELECT * FROM idempotency_record WHERE idempotency_key='lost-response'");
        close(before);
        var after = application(false, false, false);
        var replay = hold(after, resource, 3, "lost-response");
        assertThat(replay.statusCode()).isEqualTo(((Number) saved.get("response_status")).intValue());
        assertThat(replay.body()).isEqualTo(saved.get("response_body"));
        assertThat(replay.headers().firstValue("Location")).contains((String) saved.get("response_location"));
        assertThat(replay.headers().firstValue("Idempotency-Replayed")).contains("true");
        assertThat(count("reservation")).isEqualTo(1);
        assertThat(count("outbox_event")).isEqualTo(1);
        capacity(resource, 7);
        invariants();
        System.out.println("PHASE13 lostHttpResponse observedCommit=true responseBytesRead=0 applicationRestart=true originalResponseReplayed=true reservations=1 events=1");
    }

    @Test
    void actualKafkaOutageAllowsReservationsAndBacklogRecoversAfterBrokerResume() throws Exception {
        var app = application(false, false, false);
        UUID resource = resource(app, 10);
        pauseKafka();
        try {
            UUID confirmed = id(hold(app, resource, 2, "outage-confirmed"));
            assertThat(post(app, "/api/reservations/" + confirmed + "/confirm", null, "outage-confirm", "alice").statusCode()).isEqualTo(200);
            UUID cancelled = id(hold(app, resource, 3, "outage-cancelled"));
            assertThat(post(app, "/api/reservations/" + cancelled + "/cancel", null, "outage-cancel", "alice").statusCode()).isEqualTo(200);
            assertThat(hold(app, resource, 1, "outage-held").statusCode()).isEqualTo(201);
            assertThatThrownBy(app.getBean(OutboxPublisher.class)::publishOne).isInstanceOf(RuntimeException.class);
            assertThat(count("outbox_event WHERE status='PENDING'")).isEqualTo(5);
            assertThat(count("outbox_event WHERE last_error='BROKER_SEND_FAILED' AND attempt_count=1")).isEqualTo(1);
            assertThat(count("reservation_audit")).isZero();
            capacity(resource, 7);
            invariants();
            close(app);
        } finally { resumeKafka(); }
        var recovered = application(true, true, false);
        delivered(5, 35);
        assertThat(count("reservation WHERE status='CONFIRMED'")).isEqualTo(1);
        assertThat(count("reservation WHERE status='CANCELLED'")).isEqualTo(1);
        assertThat(count("reservation WHERE status='HELD'")).isEqualTo(1);
        capacity(resource, 7);
        assertThat(get(recovered, "/actuator/health").statusCode()).isEqualTo(200);
        invariants();
        System.out.println("PHASE13 kafkaOutage actualBrokerPaused=true httpMutations=5 pendingDuringOutage=5 recoveredEffects=5 conservation=PASS");
    }

    @Test
    void publisherConnectionLossAfterAcknowledgedSendRecoversInFreshApplicationWithoutDuplicateEffect() throws Exception {
        var first = application(false, false, false);
        UUID resource = resource(first, 10);
        id(hold(first, resource, 2, "publisher-crash"));
        UUID event = jdbc.queryForObject("SELECT event_id FROM outbox_event", UUID.class);
        String payload = jdbc.queryForObject("SELECT payload::text FROM outbox_event", String.class);
        markGate();
        try (Connection gate = gate()) {
            var publishing = threads.submit(first.getBean(OutboxPublisher.class)::publishOne);
            int pid = awaitBackend("UPDATE outbox_event");
            assertThat(records(1).getFirst().value()).isEqualTo(payload);
            assertThat(jdbc.queryForObject("SELECT status FROM outbox_event", String.class)).isEqualTo("PENDING");
            terminateObservedBackend(pid, "UPDATE outbox_event");
            assertThatThrownBy(() -> publishing.get(15, TimeUnit.SECONDS)).isInstanceOf(java.util.concurrent.ExecutionException.class);
        }
        assertThat(jdbc.queryForMap("SELECT status,attempt_count,published_at FROM outbox_event"))
                .containsEntry("status", "PENDING").containsEntry("attempt_count", 0).containsEntry("published_at", null);
        close(first);
        dropFaults();
        application(true, true, false);
        delivered(1, 35);
        var duplicate = records(2);
        assertThat(duplicate).extracting(ConsumerRecord::value).containsExactly(payload, payload);
        assertThat(jdbc.queryForObject("SELECT event_id FROM outbox_event", UUID.class)).isEqualTo(event);
        assertThat(count("processed_event")).isEqualTo(1);
        capacity(resource, 8);
        invariants();
        System.out.println("PHASE13 publisherSendBeforeMark observedBrokerAck=true backendTerminated=true applicationRestart=true brokerRecords=2 stableEventId=true auditEffects=1");
    }

    @Test
    void consumerApplicationRestartAfterEffectCommitBeforeAckRedeliversWithOneEffect() throws Exception {
        var first = application(false, false, false);
        UUID resource = resource(first, 10);
        hold(first, resource, 2, "consumer-crash");
        CountDownLatch committed = new CountDownLatch(1);
        var listener = first.getBean(ReservationAuditListener.class);
        var consumer = consume(first, (record, acknowledgment) -> listener.onRecord(record, () -> {
            // This callback executes inside the real listener after the independent audit transaction committed.
            assertThat(count("processed_event")).isEqualTo(1);
            assertThat(count("reservation_audit")).isEqualTo(1);
            committed.countDown();
            throw new IllegalStateException("Test failure after effect commit before acknowledgment");
        }));
        assertThat(first.getBean(OutboxPublisher.class).publishOne()).isTrue();
        assertThat(committed.await(15, TimeUnit.SECONDS)).isTrue();
        await().atMost(Duration.ofSeconds(15)).until(() -> !consumer.isRunning());
        assertThat(committedOffset()).isLessThan(startOffset + 1);
        assertThat(count("consumer_failure WHERE status='RETRY_WAIT' AND last_error='ACK_FAILED' AND attempt_count=1")).isEqualTo(1);
        String effect = jdbc.queryForObject("SELECT row_to_json(a)::text FROM reservation_audit a", String.class);
        consumer.stop();
        consumers.remove(consumer);
        close(first);
        application(false, true, false);
        delivered(1, 35);
        assertThat(committedOffset()).isEqualTo(startOffset + 1);
        assertThat(jdbc.queryForObject("SELECT row_to_json(a)::text FROM reservation_audit a", String.class)).isEqualTo(effect);
        assertThat(count("consumer_failure WHERE status='RESOLVED' AND attempt_count=1")).isEqualTo(1);
        capacity(resource, 8);
        invariants();
        System.out.println("PHASE13 consumerCommitBeforeAck observedCommit=true offsetNotAcknowledged=true applicationRestart=true redelivery=true unchangedAuditEffect=1");
    }

    @Test
    void stoppedRedisDoesNotFailCommittedHttpMutationsAndSameContainerRestartRefillsCache() throws Exception {
        var app = application(false, false, false);
        UUID resource = resource(app, 10);
        UUID cancelled = id(hold(app, resource, 2, "redis-first"));
        String availability = "/api/resources/" + resource + "/availability";
        String reservation = "/api/reservations/" + cancelled;
        await().atMost(Duration.ofSeconds(5)).until(() -> cached(get(app, availability)) && cached(get(app, reservation)));
        String containerId = REDIS.getContainerId();
        REDIS.getDockerClient().stopContainerCmd(containerId).withTimeout(1).exec();
        redisStopped = true;
        assertThat(REDIS.getDockerClient().inspectContainerCmd(containerId).exec().getState().getRunning()).isFalse();
        assertThat(post(app, reservation + "/cancel", null, "redis-cancel", "alice").statusCode()).isEqualTo(200);
        var held = hold(app, resource, 3, "redis-second");
        UUID confirmed = id(held);
        assertThat(post(app, "/api/reservations/" + confirmed + "/confirm", null, "redis-confirm", "alice").statusCode()).isEqualTo(200);
        var fallback = get(app, availability);
        assertThat(fallback.statusCode()).isEqualTo(200);
        assertThat(cached(fallback)).isFalse();
        assertThat(JsonPath.<Integer>read(fallback.body(), "$.availableCapacity")).isEqualTo(7);
        assertThat(JsonPath.<String>read(get(app, reservation).body(), "$.status")).isEqualTo("CANCELLED");
        capacity(resource, 7);
        invariants();
        startRedis();
        assertThat(REDIS.getContainerId()).isEqualTo(containerId);
        assertThat(REDIS.getDockerClient().inspectContainerCmd(containerId).exec().getNetworkSettings()
                .getPorts().getBindings().get(ExposedPort.tcp(6379))[0].getHostPortSpec()).isEqualTo(Integer.toString(REDIS_PORT));
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            var available = get(app, availability);
            var current = get(app, reservation);
            assertThat(cached(available)).isTrue();
            assertThat(cached(current)).isTrue();
            assertThat(JsonPath.<Integer>read(available.body(), "$.availableCapacity")).isEqualTo(7);
            assertThat(JsonPath.<String>read(current.body(), "$.status")).isEqualTo("CANCELLED");
        });
        var replay = hold(app, resource, 3, "redis-second");
        assertThat(replay.body()).isEqualTo(held.body());
        assertThat(replay.headers().firstValue("Idempotency-Replayed")).contains("true");
        assertThat(count("reservation")).isEqualTo(2);
        assertThat(count("outbox_event")).isEqualTo(4);
        invariants();
        System.out.println("PHASE13 redisOutage actualContainerStopped=true httpCommitsSurvived=true sameContainerRestart=true cacheRefilled=true capacity=7 replayNoDuplicate=true");
    }

    @Test
    void competingWorkersFromIndependentApplicationsReleaseOneHoldExactlyOnce() throws Exception {
        var first = application(false, false, false);
        UUID resource = resource(first, 10);
        UUID reservation = id(hold(first, resource, 3, "worker-hold"));
        overdue(reservation);
        var second = application(false, false, false);
        jdbc.execute("""
                CREATE FUNCTION phase13_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                  IF NEW.status='EXPIRED' THEN PERFORM pg_advisory_xact_lock(131313); END IF;
                  RETURN NEW;
                END $$
                """);
        jdbc.execute("CREATE TRIGGER phase13_fault BEFORE UPDATE ON reservation FOR EACH ROW EXECUTE FUNCTION phase13_fault()");
        try (Connection gate = gate()) {
            var winner = threads.submit(first.getBean(ExpirationWorker.class)::runBatch);
            awaitBackend("UPDATE reservation");
            var loser = threads.submit(second.getBean(ExpirationWorker.class)::runBatch).get(5, TimeUnit.SECONDS);
            assertThat(loser).isEqualTo(new ExpirationWorker.BatchResult(1, 0, 1, 0));
            assertThat(jdbc.queryForObject("SELECT status FROM reservation", String.class)).isEqualTo("HELD");
            capacity(resource, 7);
            invariants();
            unlock(gate);
            assertThat(winner.get(10, TimeUnit.SECONDS)).isEqualTo(new ExpirationWorker.BatchResult(1, 1, 0, 0));
        }
        assertThat(jdbc.queryForObject("SELECT status FROM reservation", String.class)).isEqualTo("EXPIRED");
        assertThat(count("outbox_event WHERE event_type='ReservationExpired'")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT version FROM resource", Long.class)).isEqualTo(2);
        capacity(resource, 10);
        assertThat(first.getBean(ExpirationWorker.class).runBatch().candidates()).isZero();
        assertThat(second.getBean(ExpirationWorker.class).runBatch().candidates()).isZero();
        invariants();
        System.out.println("PHASE13 competingExpiration applications=2 overlappingLockObserved=true winner=1 skipped=1 expirationEvents=1 capacityRestoredOnce=true");
    }

    @Test
    void applicationRestartDrainsPendingExpirationAndPublicationThenSecondRestartIsIdempotent() throws Exception {
        var before = application(false, false, false);
        UUID resource = resource(before, 10);
        var original = hold(before, resource, 3, "restart-overdue");
        UUID expired = id(original);
        UUID held = id(hold(before, resource, 2, "restart-future"));
        overdue(expired);
        assertThat(count("outbox_event WHERE status='PENDING'")).isEqualTo(2);
        assertThat(count("reservation WHERE status='HELD'")).isEqualTo(2);
        assertThat(count("reservation_audit")).isZero();
        close(before);
        var after = application(true, true, true);
        await().atMost(Duration.ofSeconds(35)).until(() -> count("reservation WHERE status='EXPIRED'") == 1);
        delivered(3, 35);
        capacity(resource, 8);
        assertThat(jdbc.queryForObject("SELECT status FROM reservation WHERE id=?", String.class, held)).isEqualTo("HELD");
        assertThat(count("outbox_event WHERE event_type='ReservationExpired'")).isEqualTo(1);
        invariants();
        close(after);
        var again = application(true, true, true);
        var replay = hold(again, resource, 3, "restart-overdue");
        assertThat(replay.body()).isEqualTo(original.body());
        assertThat(replay.headers().firstValue("Idempotency-Replayed")).contains("true");
        await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
            assertThat(JsonPath.<String>read(get(again, "/api/reservations/" + expired).body(), "$.status")).isEqualTo("EXPIRED");
            assertThat(count("reservation")).isEqualTo(2);
            assertThat(count("outbox_event")).isEqualTo(3);
            assertThat(count("reservation_audit")).isEqualTo(3);
            assertThat(count("processed_event")).isEqualTo(3);
        });
        capacity(resource, 8);
        invariants();
        System.out.println("PHASE13 applicationPendingRestart dueHoldExpired=1 futureHeld=1 backlogPublished=3 auditEffects=3 secondRestartNoDuplicate=true originalHttpReplayPreserved=true");
    }

    private ConfigurableApplicationContext application(boolean publish, boolean consume, boolean expire) {
        var app = new SpringApplicationBuilder(AtlasApplication.class).profiles("local", "test").run(
                "--server.port=0", "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(), "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.datasource.hikari.maximum-pool-size=8",
                "--atlas.local.alice-password=" + PASSWORD, "--atlas.local.bob-password=" + PASSWORD, "--atlas.local.admin-password=" + PASSWORD,
                "--atlas.hold.ttl-seconds=3600", "--atlas.kafka.enabled=true", "--atlas.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--atlas.kafka.publisher-enabled=" + publish, "--atlas.kafka.consumer-enabled=" + consume, "--atlas.kafka.poll-ms=100",
                "--atlas.expiration.enabled=" + expire, "--atlas.expiration.poll-ms=100", "--atlas.expiration.batch-size=2",
                "--atlas.recovery.enabled=" + expire, "--atlas.recovery.poll-ms=100", "--atlas.recovery.batch-size=2",
                "--atlas.recovery.max-attempts=5", "--atlas.recovery.base-delay-ms=100", "--atlas.recovery.max-delay-ms=400",
                "--atlas.redis.enabled=true", "--atlas.redis.host=" + REDIS.getHost(), "--atlas.redis.port=" + REDIS.getMappedPort(6379));
        applications.add(app);
        return app;
    }

    private void close(ConfigurableApplicationContext app) { app.close(); applications.remove(app); }
    private int port(ConfigurableApplicationContext app) { return ((WebServerApplicationContext) app).getWebServer().getPort(); }
    private UUID resource(ConfigurableApplicationContext app, int capacity) throws Exception {
        var response = post(app, "/api/resources", "{\"name\":\"Failure fixture\",\"totalCapacity\":" + capacity + "}", "resource", "admin");
        assertThat(response.statusCode()).isEqualTo(201);
        return id(response);
    }
    private UUID id(HttpResponse<String> response) {
        assertThat(response.statusCode()).isEqualTo(201);
        return UUID.fromString(JsonPath.read(response.body(), "$.id"));
    }
    private String holdBody(UUID resource, int quantity) { return "{\"resourceId\":\"" + resource + "\",\"quantity\":" + quantity + "}"; }
    private HttpResponse<String> hold(ConfigurableApplicationContext app, UUID resource, int quantity, String key) throws Exception {
        return post(app, "/api/reservations", holdBody(resource, quantity), key, "alice");
    }
    private String basic(String user) { return "Basic " + Base64.getEncoder().encodeToString((user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)); }
    private HttpResponse<String> post(ConfigurableApplicationContext app, String path, String body, String key, String user) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(app) + path))
                .timeout(Duration.ofSeconds(15)).header("Authorization", basic(user)).header("Idempotency-Key", key);
        if (body != null) request.header("Content-Type", "application/json");
        return http.send(request.POST(body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
    private HttpResponse<String> get(ConfigurableApplicationContext app, String path) throws Exception {
        return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port(app) + path)).timeout(Duration.ofSeconds(5))
                .header("Authorization", basic("alice")).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
    private boolean cached(HttpResponse<String> response) {
        assertThat(response.statusCode()).isEqualTo(200);
        return JsonPath.read(response.body(), "$.cached");
    }
    private long count(String table) { return jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class); }
    private void capacity(UUID resource, int expected) {
        assertThat(jdbc.queryForObject("SELECT available_capacity FROM resource WHERE id=?", Integer.class, resource)).isEqualTo(expected);
    }
    private String resourceSnapshot(UUID resource) { return jdbc.queryForObject("SELECT row_to_json(s)::text FROM resource s WHERE id=?", String.class, resource); }
    private void invariants() {
        assertThat(jdbc.queryForObject("""
                SELECT count(*) FROM resource s WHERE available_capacity<0 OR available_capacity>total_capacity OR
                  available_capacity+COALESCE((SELECT sum(quantity) FROM reservation r
                    WHERE r.resource_id=s.id AND r.status IN ('HELD','CONFIRMED')),0)<>total_capacity
                """, Long.class)).isZero();
        assertThat(count("reservation WHERE status NOT IN ('HELD','CONFIRMED','CANCELLED','EXPIRED') OR quantity<=0")).isZero();
        assertThat(jdbc.queryForObject("""
                SELECT count(*) FROM reservation r WHERE
                  (r.status='HELD' AND transition_sequence<>1) OR (r.status<>'HELD' AND transition_sequence<>2) OR
                  (SELECT count(*) FROM outbox_event e WHERE e.aggregate_id=r.id)<>r.transition_sequence
                """, Long.class)).isZero();
        assertThat(jdbc.queryForObject("""
                SELECT count(*) FROM processed_event p LEFT JOIN reservation_audit a
                  ON a.consumer_name=p.consumer_name AND a.event_id=p.event_id WHERE a.event_id IS NULL
                """, Long.class)).isZero();
        assertThat(count("idempotency_record WHERE completed_at IS NULL")).isZero();
    }
    private void overdue(UUID id) {
        jdbc.update("UPDATE reservation SET created_at=clock_timestamp()-interval '2 minutes', expires_at=clock_timestamp()-interval '1 minute' WHERE id=?", id);
    }
    private Connection gate() throws Exception {
        var gate = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        try (var statement = gate.createStatement()) { statement.execute("SELECT pg_advisory_lock(" + GATE + ")"); return gate; }
        catch (Exception failure) { gate.close(); throw failure; }
    }
    private void unlock(Connection gate) throws Exception { try (var statement = gate.createStatement()) { statement.execute("SELECT pg_advisory_unlock(" + GATE + ")"); } }
    private int awaitBackend(String query) {
        await().atMost(Duration.ofSeconds(5)).until(() -> !waitingBackends(query).isEmpty());
        var pids = waitingBackends(query);
        assertThat(pids).hasSize(1);
        return pids.getFirst();
    }
    private List<Integer> waitingBackends(String query) {
        return jdbc.queryForList("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory' AND query LIKE ?", Integer.class, "%" + query + "%");
    }
    private void terminateObservedBackend(int pid, String query) {
        assertThat(waitingBackends(query)).containsExactly(pid);
        assertThat(jdbc.queryForObject("SELECT pg_terminate_backend(?)", Boolean.class, pid)).isTrue();
    }
    private void markGate() {
        jdbc.execute("""
                CREATE FUNCTION phase13_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
                  IF NEW.status='PUBLISHED' THEN PERFORM pg_advisory_xact_lock(131313); END IF;
                  RETURN NEW;
                END $$
                """);
        jdbc.execute("CREATE TRIGGER phase13_fault BEFORE UPDATE ON outbox_event FOR EACH ROW EXECUTE FUNCTION phase13_fault()");
    }
    private void dropFaults() {
        for (String table : List.of("reservation", "idempotency_record", "outbox_event")) jdbc.execute("DROP TRIGGER IF EXISTS phase13_fault ON " + table);
        jdbc.execute("DROP FUNCTION IF EXISTS phase13_fault()");
        jdbc.execute("DROP SEQUENCE IF EXISTS phase13_seen");
    }
    private void pauseKafka() { KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec(); kafkaPaused = true; }
    private void resumeKafka() { if (kafkaPaused) { KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec(); kafkaPaused = false; } }
    private void startRedis() throws Exception {
        if (redisStopped) {
            REDIS.getDockerClient().startContainerCmd(REDIS.getContainerId()).exec(); redisStopped = false;
            await().atMost(Duration.ofSeconds(15)).until(() -> "PONG".equals(REDIS.execInContainer("redis-cli", "PING").getStdout().strip()));
        }
    }
    private static int freeLoopbackPort() {
        try (var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) { return socket.getLocalPort(); }
        catch (java.io.IOException failure) { throw new IllegalStateException("Cannot allocate a test Redis endpoint", failure); }
    }
    private long endOffset() throws Exception { return admin.listOffsets(Map.of(PARTITION, OffsetSpec.latest())).partitionResult(PARTITION).get(5, TimeUnit.SECONDS).offset(); }
    private long committedOffset() throws Exception {
        var offsets = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        return offsets.containsKey(PARTITION) ? offsets.get(PARTITION).offset() : -1;
    }
    private void delivered(int events, int seconds) {
        await().atMost(Duration.ofSeconds(seconds)).untilAsserted(() -> {
            assertThat(count("outbox_event WHERE status='PUBLISHED'")).isEqualTo(events);
            assertThat(count("outbox_event WHERE status<>'PUBLISHED'")).isZero();
            assertThat(count("processed_event")).isEqualTo(events);
            assertThat(count("reservation_audit")).isEqualTo(events);
            assertThat(committedOffset()).isEqualTo(endOffset());
        });
    }
    @SuppressWarnings("unchecked")
    private ConcurrentMessageListenerContainer<String,String> consume(ConfigurableApplicationContext app, AcknowledgingMessageListener<String,String> callback) {
        var factory = (ConcurrentKafkaListenerContainerFactory<String,String>) app.getBean("reservationAuditContainerFactory");
        var consumer = factory.createContainer(TOPIC);
        consumer.getContainerProperties().setGroupId(GROUP);
        consumer.getContainerProperties().setMessageListener(callback);
        consumer.setBeanName("phase13-ack-fault");
        consumers.add(consumer);
        consumer.start();
        return consumer;
    }
    private List<ConsumerRecord<String,String>> records(int expected) {
        try (var consumer = new KafkaConsumer<String,String>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.deserializer", StringDeserializer.class, "value.deserializer", StringDeserializer.class,
                "enable.auto.commit", false, "default.api.timeout.ms", 5000))) {
            consumer.assign(List.of(PARTITION)); consumer.seek(PARTITION, startOffset);
            var records = new ArrayList<ConsumerRecord<String,String>>();
            await().atMost(Duration.ofSeconds(15)).until(() -> { consumer.poll(Duration.ofMillis(100)).forEach(records::add); return records.size() >= expected; });
            assertThat(records).hasSize(expected);
            return records;
        }
    }
}
