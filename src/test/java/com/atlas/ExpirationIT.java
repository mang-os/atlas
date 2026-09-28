package com.atlas;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import com.atlas.common.ApiException;
import com.atlas.configuration.ExpirationScheduling;
import com.atlas.configuration.LocalIdentityConfiguration;
import com.atlas.reservation.CreateReservationRequest;
import com.atlas.reservation.ExpirationProcessor;
import com.atlas.reservation.ExpirationWorker;
import com.atlas.reservation.ReservationRepository;
import com.atlas.reservation.ReservationService;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

/** Real Spring transaction proxies and independent PostgreSQL connections; no mocked clock or repository. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@Testcontainers(disabledWithoutDocker = false)
@Timeout(90)
@org.springframework.test.context.TestExecutionListeners(listeners=RedisOutageEvidence.class,
        mergeMode=org.springframework.test.context.TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
class ExpirationIT {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_expiration_test").withUsername("atlas_test").withPassword("testcontainers-only")
            .withStartupTimeout(Duration.ofSeconds(90));
    @Autowired JdbcTemplate jdbc;
    @Autowired ReservationService reservations;
    @Autowired ReservationRepository repository;
    @Autowired ExpirationProcessor processor;
    @Autowired ExpirationWorker worker;
    @Autowired MeterRegistry meters;
    private ExecutorService threads;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("atlas.expiration.batch-size", () -> 2);
        registry.add("atlas.hold.ttl-seconds", () -> 3600);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> 6);
        registry.add("spring.datasource.hikari.data-source-properties.options", () -> "-c statement_timeout=20000 -c lock_timeout=15000");
        registry.add("spring.datasource.hikari.data-source-properties.socketTimeout", () -> 25);
    }

    @BeforeEach
    void resetDisposableFixtures() {
        jdbc.execute("DROP TRIGGER IF EXISTS test_expiration_release_failure ON resource");
        jdbc.execute("DROP FUNCTION IF EXISTS test_expiration_release_failure()");
        jdbc.execute("DROP SEQUENCE IF EXISTS test_expired_observed");
        jdbc.execute("DROP TRIGGER IF EXISTS test_terminal_gate ON reservation");
        jdbc.execute("DROP FUNCTION IF EXISTS test_terminal_gate()");
        jdbc.update("DELETE FROM outbox_event");
        jdbc.update("DELETE FROM idempotency_record");
        jdbc.update("DELETE FROM reservation");
        jdbc.update("DELETE FROM resource");
        threads = Executors.newVirtualThreadPerTaskExecutor();
    }

    @AfterEach
    void stopThreads() throws Exception {
        threads.shutdownNow();
        assertThat(threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        assertThat(jdbc.queryForObject("""
                SELECT count(*) FROM reservation r WHERE
                    (SELECT count(*) FROM outbox_event e WHERE e.aggregate_id=r.id) <> r.transition_sequence
                """, Integer.class)).isZero();
    }

    @Test
    void expiresDueHoldExactlyOnceWithDatabaseTimestampsAndCommittedMetric() {
        Fixture f = hold(); due(f);
        OffsetDateTime before = dbNow();
        double count = meters.counter("atlas.expiration.attempts", "outcome", "expired").count();
        var result = worker.runBatch();
        OffsetDateTime after = dbNow();
        assertThat(result).isEqualTo(new ExpirationWorker.BatchResult(1, 1, 0, 0));
        assertState(f, "EXPIRED");
        var row = reservationRow(f);
        assertThat(jdbc.queryForObject("SELECT expired_at FROM reservation WHERE id=?", OffsetDateTime.class, f.id))
                .isBetween(before, after);
        assertThat(row.get("updated_at")).isEqualTo(row.get("expired_at"));
        assertThat(resourceRow(f).get("updated_at")).isEqualTo(row.get("expired_at"));
        assertThat(row.get("confirmed_at")).isNull();
        assertThat(row.get("cancelled_at")).isNull();
        assertThat(meters.counter("atlas.expiration.attempts", "outcome", "expired").count()).isEqualTo(count + 1);
        assertThat(meters.get("atlas.expiration.decision.lag").timer().count()).isPositive();
        var resource = resourceRow(f);
        assertThat(processor.expire(f.id).expired()).isFalse();
        assertThat(worker.runBatch().candidates()).isZero();
        assertThat(reservationRow(f)).isEqualTo(row);
        assertThat(resourceRow(f)).isEqualTo(resource);
        assertState(f, "EXPIRED");
    }

    @Test
    void boundsCandidateBatchAndDrainsBacklogWithoutTouchingFutureHolds() {
        for (int i = 0; i < 5; i++) due(hold());
        Fixture future = hold();
        for (int expected : new int[]{2, 2, 1}) {
            assertThat(worker.runBatch()).isEqualTo(new ExpirationWorker.BatchResult(expected, expected, 0, 0));
            invariant();
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM reservation WHERE status='EXPIRED'", Integer.class)).isEqualTo(5);
        assertState(future, "HELD");
        assertThat(worker.runBatch().candidates()).isZero();
        System.out.println("PHASE7 bounded-batches=2,2,1 expired=5 futureHeld=1 conservation=PASS");
    }

    @Test
    void skipsLockedCandidateAndProcessesAnotherThenRecoversSkippedWork() throws Exception {
        Fixture locked = hold(); due(locked);
        Fixture free = hold(); due(free);
        try (Connection gate = connection()) {
            lock(gate, "reservation", locked.id);
            var result = threads.submit(worker::runBatch).get(3, TimeUnit.SECONDS);
            assertThat(result).isEqualTo(new ExpirationWorker.BatchResult(2, 1, 1, 0));
            assertState(locked, "HELD"); assertState(free, "EXPIRED");
            gate.rollback();
        }
        assertThat(worker.runBatch()).isEqualTo(new ExpirationWorker.BatchResult(1, 1, 0, 0));
        assertState(locked, "EXPIRED");
    }

    @Test
    void twoWorkersOnSameHoldReleaseExactlyOnce() throws Exception {
        Fixture f = hold(); due(f);
        try (Connection resourceGate = connection()) {
            lock(resourceGate, "resource", f.resource);
            Future<ExpirationWorker.BatchResult> first = threads.submit(worker::runBatch);
            await(() -> waiting("UPDATE resource%", null) >= 1, 8);
            // First worker owns the reservation and is waiting for its resource. Second must SKIP, not wait.
            var second = threads.submit(worker::runBatch).get(3, TimeUnit.SECONDS);
            assertThat(second).isEqualTo(new ExpirationWorker.BatchResult(1, 0, 1, 0));
            assertThat(first).isNotDone(); assertState(f, "HELD");
            resourceGate.rollback();
            assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo(new ExpirationWorker.BatchResult(1, 1, 0, 0));
        }
        assertState(f, "EXPIRED");
        assertThat(worker.runBatch().expired()).isZero();
        assertState(f, "EXPIRED");
        System.out.println("PHASE7 two-workers expired=1 skipped=1 releases=1 conservation=PASS");
    }

    @ParameterizedTest
    @ValueSource(strings = {"confirm", "cancel"})
    void expirationWinsReservationLockAgainstLifecycleRequest(String operation) throws Exception {
        Fixture f = hold(); due(f);
        try (Connection gate = connection()) {
            lock(gate, "resource", f.resource);
            var expiration = threads.submit(worker::runBatch);
            await(() -> waiting("UPDATE resource%", null) >= 1, 8);
            Future<String> lifecycle = threads.submit(() -> {
                try { act(f, operation); return "unexpected success"; }
                catch (ApiException failure) { assertThat(failure.status().value()).isEqualTo(409); return failure.code(); }
            });
            await(() -> waiting("SELECT * FROM reservation%FOR UPDATE%", null) >= 1, 8);
            assertState(f, "HELD");
            gate.rollback();
            assertThat(expiration.get(5, TimeUnit.SECONDS).expired()).isEqualTo(1);
            assertThat(lifecycle.get(5, TimeUnit.SECONDS)).isEqualTo("RESERVATION_EXPIRED");
        }
        assertState(f, "EXPIRED");
        System.out.println("PHASE7 expiration-vs-" + operation + " winner=EXPIRED lifecycle=409 releases=1 conservation=PASS");
    }

    @ParameterizedTest
    @ValueSource(strings = {"confirm", "cancel"})
    void validLifecycleDecisionBeforeDeadlineWinsEvenWhenCommitIsAfterDeadline(String operation) throws Exception {
        Fixture f = hold();
        jdbc.update("UPDATE reservation SET expires_at=clock_timestamp()+interval '3 seconds' WHERE id=?", f.id);
        jdbc.execute("""
                CREATE FUNCTION test_terminal_gate() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.status IN ('CONFIRMED','CANCELLED') THEN PERFORM pg_advisory_xact_lock(7007); END IF;
                    RETURN NEW;
                END $$
                """);
        jdbc.execute("CREATE TRIGGER test_terminal_gate BEFORE UPDATE ON reservation FOR EACH ROW EXECUTE FUNCTION test_terminal_gate()");
        try (Connection gate = connection()) {
            gate.setAutoCommit(false);
            try (var statement = gate.createStatement()) { statement.execute("SELECT pg_advisory_xact_lock(7007)"); }
            var lifecycle = threads.submit(() -> act(f, operation));
            await(() -> waiting("UPDATE reservation%", "advisory") >= 1, 8);
            assertThat(pastDeadline(f)).isFalse();
            await(() -> pastDeadline(f), 8);
            // Candidate now looks overdue to other snapshots, but the valid lifecycle owner still holds its lock.
            assertThat(worker.runBatch()).isEqualTo(new ExpirationWorker.BatchResult(1, 0, 1, 0));
            assertThat(lifecycle).isNotDone();
            gate.rollback();
            assertThat(lifecycle.get(5, TimeUnit.SECONDS)).isEqualTo(operation.equals("confirm") ? "CONFIRMED" : "CANCELLED");
            assertThat(worker.runBatch().candidates()).isZero();
            assertState(f, operation.equals("confirm") ? "CONFIRMED" : "CANCELLED");
            System.out.println("PHASE7 lifecycle-vs-expiration winner=" + operation + " workerSkipped=1 postDeadlineCommit=true conservation=PASS");
        } finally {
            jdbc.execute("DROP TRIGGER test_terminal_gate ON reservation");
            jdbc.execute("DROP FUNCTION test_terminal_gate()");
        }
    }

    @Test
    void rechecksStateAndDeadlineWhenDiscoveryBecomesStale() {
        Fixture f = hold(); due(f);
        assertThat(repository.dueCandidates(2)).containsExactly(f.id);
        jdbc.update("UPDATE reservation SET expires_at=clock_timestamp()+interval '1 hour' WHERE id=?", f.id);
        assertThat(processor.expire(f.id).expired()).isFalse();
        assertState(f, "HELD");
        reservations.confirm(LocalIdentityConfiguration.ALICE_ID, f.id);
        var before = reservationRow(f);
        assertThat(processor.expire(f.id).expired()).isFalse();
        assertThat(processor.expire(UUID.randomUUID()).expired()).isFalse();
        assertThat(reservationRow(f)).isEqualTo(before);
        assertState(f, "CONFIRMED");
    }

    @Test
    void failedReleaseRollsBackOneItemWhileOtherCandidateCommitsAndLaterPollRetries() {
        Fixture broken = hold(); due(broken);
        Fixture healthy = hold(); due(healthy);
        var beforeReservation = reservationRow(broken); var beforeResource = resourceRow(broken);
        double failed = meters.counter("atlas.expiration.attempts", "outcome", "failed").count();
        jdbc.execute("CREATE SEQUENCE test_expired_observed");
        jdbc.execute("""
                CREATE FUNCTION test_expiration_release_failure() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF OLD.id='%s'::uuid AND NEW.available_capacity>OLD.available_capacity THEN
                        IF NOT EXISTS (SELECT 1 FROM reservation WHERE resource_id=OLD.id AND status='EXPIRED') THEN
                            RAISE EXCEPTION 'state update missing';
                        END IF;
                        PERFORM nextval('test_expired_observed');
                        RAISE EXCEPTION 'test-only expiration release failure' USING ERRCODE='23514';
                    END IF;
                    RETURN NEW;
                END $$
                """.formatted(broken.resource));
        jdbc.execute("CREATE TRIGGER test_expiration_release_failure BEFORE UPDATE ON resource FOR EACH ROW EXECUTE FUNCTION test_expiration_release_failure()");
        try {
            assertThat(worker.runBatch()).isEqualTo(new ExpirationWorker.BatchResult(2, 1, 0, 1));
            assertThat(jdbc.queryForObject("SELECT is_called FROM test_expired_observed", Boolean.class)).isTrue();
            assertThat(reservationRow(broken)).isEqualTo(beforeReservation);
            assertThat(resourceRow(broken)).isEqualTo(beforeResource);
            assertThat(meters.counter("atlas.expiration.attempts", "outcome", "failed").count()).isEqualTo(failed + 1);
            assertState(broken, "HELD"); assertState(healthy, "EXPIRED");
        } finally {
            jdbc.execute("DROP TRIGGER test_expiration_release_failure ON resource");
            jdbc.execute("DROP FUNCTION test_expiration_release_failure()");
            jdbc.execute("DROP SEQUENCE test_expired_observed");
        }
        assertThat(worker.runBatch()).isEqualTo(new ExpirationWorker.BatchResult(1, 1, 0, 0));
        assertState(broken, "EXPIRED"); assertState(healthy, "EXPIRED");
        System.out.println("PHASE7 rollback failed=1 independentCommit=1 laterRetry=1 conservation=PASS");
    }

    @Test
    void discoveryFailureIsReportedAndNextPollRecovers() {
        Fixture f = hold(); due(f);
        var scheduler = new ExpirationScheduling(worker, meters);
        double failures = meters.counter("atlas.expiration.discovery.failures").count();
        jdbc.execute("ALTER TABLE reservation RENAME TO test_unavailable_reservation");
        try { scheduler.poll(); }
        finally { jdbc.execute("ALTER TABLE test_unavailable_reservation RENAME TO reservation"); }
        assertThat(meters.counter("atlas.expiration.discovery.failures").count()).isEqualTo(failures + 1);
        assertState(f, "HELD");
        scheduler.poll();
        assertState(f, "EXPIRED");
    }

    @Test
    void restartRediscoversPersistedOverdueHoldAndAutomaticSchedulerExpiresIt() {
        Fixture f;
        ConfigurableApplicationContext before = application(false);
        try {
            assertThat(before.getBeansOfType(ExpirationScheduling.class)).isEmpty();
            f = hold(before.getBean(JdbcTemplate.class), before.getBean(ReservationService.class));
            due(f);
            assertState(f, "HELD");
        } finally { before.close(); }
        assertThat(before.isActive()).isFalse();
        // No manual batch invocation after restart. A new application context/pool/scheduler uses the same durable DB.
        try (ConfigurableApplicationContext after = application(true)) {
            assertThat(after.getBeansOfType(ExpirationScheduling.class)).hasSize(1);
            await(() -> "EXPIRED".equals(reservationRow(f).get("status"))
                    && after.getBean(MeterRegistry.class).counter("atlas.expiration.attempts", "outcome", "expired").count() == 1, 10);
            assertState(f, "EXPIRED");
            assertThat(after.getBean(MeterRegistry.class).counter("atlas.expiration.attempts", "outcome", "expired").count()).isEqualTo(1);
        }
        assertState(f, "EXPIRED");
        System.out.println("PHASE7 restart newApplicationContext=true automaticScheduler=true expired=1 releases=1 conservation=PASS");
    }

    private ConfigurableApplicationContext application(boolean enabled) {
        return new SpringApplicationBuilder(AtlasApplication.class).profiles("test").run(
                "--server.port=0", "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(), "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--atlas.expiration.enabled=" + enabled, "--atlas.expiration.poll-ms=100", "--atlas.expiration.batch-size=2");
    }

    private String act(Fixture f, String operation) {
        return (operation.equals("confirm") ? reservations.confirm(LocalIdentityConfiguration.ALICE_ID, f.id)
                : reservations.cancel(LocalIdentityConfiguration.ALICE_ID, f.id)).status().name();
    }
    private Fixture hold() { return hold(jdbc, reservations); }
    private Fixture hold(JdbcTemplate database, ReservationService service) {
        UUID resource = database.queryForObject("INSERT INTO resource(name,total_capacity,available_capacity) VALUES ('Expiration fixture',10,10) RETURNING id", UUID.class);
        return new Fixture(resource, service.create(LocalIdentityConfiguration.ALICE_ID, new CreateReservationRequest(resource, 3)).id());
    }
    private void due(Fixture f) {
        jdbc.update("UPDATE reservation SET created_at=clock_timestamp()-interval '2 minutes', expires_at=clock_timestamp()-interval '1 minute' WHERE id=?", f.id);
    }
    private boolean pastDeadline(Fixture f) {
        return Boolean.TRUE.equals(jdbc.queryForObject("SELECT clock_timestamp()>=expires_at FROM reservation WHERE id=?", Boolean.class, f.id));
    }
    private OffsetDateTime dbNow() { return jdbc.queryForObject("SELECT clock_timestamp()", OffsetDateTime.class); }
    private Map<String, Object> reservationRow(Fixture f) { return jdbc.queryForMap("SELECT * FROM reservation WHERE id=?", f.id); }
    private Map<String, Object> resourceRow(Fixture f) { return jdbc.queryForMap("SELECT * FROM resource WHERE id=?", f.resource); }
    private record Fixture(UUID resource, UUID id) { }

    private void assertState(Fixture f, String status) {
        var row = jdbc.queryForMap("""
                SELECT r.status,s.available_capacity,s.version,
                    (SELECT coalesce(sum(quantity),0) FROM reservation WHERE resource_id=s.id AND status IN ('HELD','CONFIRMED')) allocated
                FROM reservation r JOIN resource s ON s.id=r.resource_id WHERE r.id=?
                """, f.id);
        boolean allocated = status.equals("HELD") || status.equals("CONFIRMED");
        assertThat(row.get("status")).isEqualTo(status);
        assertThat(((Number) row.get("available_capacity")).intValue()).isEqualTo(allocated ? 7 : 10);
        assertThat(((Number) row.get("allocated")).intValue()).isEqualTo(allocated ? 3 : 0);
        assertThat(((Number) row.get("version")).longValue()).isEqualTo(allocated ? 1 : 2);
        invariant();
    }
    private void invariant() {
        assertThat(jdbc.queryForObject("""
                SELECT count(*) FROM resource s WHERE available_capacity<0 OR available_capacity>total_capacity
                    OR available_capacity+(SELECT coalesce(sum(quantity),0) FROM reservation
                        WHERE resource_id=s.id AND status IN ('HELD','CONFIRMED'))<>total_capacity
                """, Integer.class)).isZero();
    }
    private Connection connection() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl() + "&connectTimeout=5&socketTimeout=25", POSTGRES.getUsername(), POSTGRES.getPassword());
    }
    private void lock(Connection connection, String table, UUID id) throws Exception {
        assertThat(table).isIn("resource", "reservation");
        connection.setAutoCommit(false);
        try (var statement = connection.prepareStatement("SELECT id FROM " + table + " WHERE id=? FOR UPDATE")) {
            statement.setObject(1, id); assertThat(statement.executeQuery().next()).isTrue();
        }
    }
    private int waiting(String queryPattern, String event) {
        return jdbc.queryForObject("""
                SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active'
                    AND wait_event_type='Lock' AND query LIKE ? AND (?::text IS NULL OR wait_event=?)
                """, Integer.class, queryPattern, event, event);
    }
    private void await(BooleanSupplier done, int seconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) if (done.getAsBoolean()) return;
        throw new AssertionError("Bounded wait for PostgreSQL state failed");
    }
}
