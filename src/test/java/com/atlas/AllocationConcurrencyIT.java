package com.atlas;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import com.atlas.configuration.LocalIdentityConfiguration;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

/** Real HTTP, separate service transactions and PostgreSQL sessions. No test transaction or mocks. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"local", "test"})
@Testcontainers(disabledWithoutDocker = false)
@Timeout(120)
@org.springframework.test.context.TestExecutionListeners(listeners=RedisOutageEvidence.class,
        mergeMode=org.springframework.test.context.TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
class AllocationConcurrencyIT {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_concurrency_test").withUsername("atlas_test").withPassword("testcontainers-only")
            .withStartupTimeout(Duration.ofSeconds(90));
    static final String PASSWORD = UUID.randomUUID().toString();
    static final long FAULT_GATE = 5042026;
    @LocalServerPort int port;
    @Autowired JdbcTemplate jdbc;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private int samples;
    private int overlappingSessions;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        for (String user : List.of("alice", "bob", "admin")) {
            registry.add("atlas.local." + user + "-password", () -> PASSWORD);
        }
        registry.add("atlas.hold.ttl-seconds", () -> 3600);
        // Test-only budgets: enough connections for independent-resource progress, never 100 barrier parties in a pool.
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> 32);
        registry.add("spring.datasource.hikari.connection-timeout", () -> 30000);
        registry.add("spring.datasource.hikari.data-source-properties.options",
                () -> "-c statement_timeout=30000 -c lock_timeout=20000");
        registry.add("spring.datasource.hikari.data-source-properties.socketTimeout", () -> 40);
    }

    @BeforeEach
    void resetDisposableDatabase() {
        jdbc.execute("DROP TRIGGER IF EXISTS test_contention_failure ON reservation");
        jdbc.execute("DROP FUNCTION IF EXISTS test_contention_failure()");
        jdbc.execute("DROP SEQUENCE IF EXISTS test_decrement_observed");
        jdbc.update("DELETE FROM outbox_event");
        jdbc.update("DELETE FROM idempotency_record");
        jdbc.update("DELETE FROM reservation");
        jdbc.update("DELETE FROM resource");
        samples = 0;
        overlappingSessions = 0;
    }

    @RepeatedTest(3)
    void hundredRequestsForTenUnits(RepetitionInfo repetition) throws Exception {
        UUID resource = resource(10);
        var results = contend(resource, requests(resource, 100, false));
        verify("capacity10-run" + repetition.getCurrentRepetition(), resource, results, 10, 90, 0);
    }

    @Test
    void hundredRequestsForOneUnit() throws Exception {
        UUID resource = resource(1);
        verify("capacity1", resource, contend(resource, requests(resource, 100, false)), 1, 99, 0);
    }

    @Test
    void mixedQuantitiesConserveUnitsRegardlessOfWinnerOrder() throws Exception {
        UUID resource = resource(17);
        var results = contend(resource, requests(resource, 100, true));
        // Winners are scheduler-dependent; check exact response-to-row quantities, not a guessed success count.
        verify("mixed", resource, results, -1, -1, -1);
        int available = jdbc.queryForObject("SELECT available_capacity FROM resource WHERE id=?", Integer.class, resource);
        results.stream().filter(r -> r.status == 409).forEach(r -> assertThat(r.request.quantity).isGreaterThan(available));
    }

    @Test
    void anotherResourceCompletesWhileFirstResourceRemainsLocked() throws Exception {
        UUID blocked = resource(3);
        UUID independent = resource(7);
        var attempts = new ArrayList<>(requests(blocked, 8, false));
        attempts.addAll(requests(independent, 20, false));
        // Eight blocked requests cannot occupy the whole 32-connection pool.
        try (Connection gate = connection(); Connection observer = connection(); Batch batch = new Batch(attempts)) {
            lockResource(gate, blocked);
            batch.start();
            await(observer, () -> batch.futures.subList(8, 28).stream().allMatch(Future::isDone), 15);
            var independentResults = batch.results(8, 28);
            assertThat(batch.futures.subList(0, 8)).allMatch(f -> !f.isDone());
            awaitOverlap(observer);
            verify("independent-B-before-A-unlock", independent, independentResults, 7, 13, 0);
            gate.rollback();
            batch.await(observer);
            verify("independent-A", blocked, batch.results(0, 8), 3, 5, 0);
        }
    }

    @Test
    void failedInsertionRollsBackDecrementWhileOtherTransactionsWait() throws Exception {
        UUID resource = resource(10);
        jdbc.execute("CREATE SEQUENCE test_decrement_observed");
        jdbc.execute("""
                CREATE FUNCTION test_contention_failure() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.user_id = '22222222-2222-4222-8222-222222222222'::uuid THEN
                        IF NOT EXISTS (SELECT 1 FROM resource WHERE id=NEW.resource_id
                                AND available_capacity=7 AND version=1) THEN
                            RAISE EXCEPTION 'fault did not see expected decrement';
                        END IF;
                        PERFORM nextval('test_decrement_observed');
                        PERFORM pg_advisory_xact_lock(5042026);
                        -- Prove the intentional failure site was reached, rather than a gate timeout.
                        PERFORM nextval('test_decrement_observed');
                        RAISE EXCEPTION 'test-only insertion failure' USING ERRCODE='23514';
                    END IF;
                    RETURN NEW;
                END $$
                """);
        jdbc.execute("CREATE TRIGGER test_contention_failure BEFORE INSERT ON reservation FOR EACH ROW EXECUTE FUNCTION test_contention_failure()");
        try (Connection gate = connection(); Connection observer = connection();
             Batch fault = new Batch(List.of(new Attempt(resource, 3, "bob")));
             Batch healthy = new Batch(requests(resource, 100, false))) {
            gate.setAutoCommit(false);
            gate.createStatement().execute("SELECT pg_advisory_xact_lock(" + FAULT_GATE + ")");
            fault.start();
            await(observer, () -> Boolean.TRUE.equals(jdbc.queryForObject("SELECT is_called FROM test_decrement_observed", Boolean.class)), 15);
            assertThat(fault.futures.getFirst()).isNotDone();
            healthy.start();
            awaitOverlap(observer);
            // The failed transaction has changed its own row, but snapshots must still see conserved committed state.
            assertInvariant(observer);
            gate.rollback();
            fault.await(observer);
            Outcome failed = fault.results(0, 1).getFirst();
            assertThat(failed.status).isEqualTo(503);
            assertThat(failed.body).containsEntry("code", "TEMPORARILY_UNAVAILABLE");
            healthy.await(observer);
            verify("rollback-healthy", resource, healthy.results(0, 100), 10, 90, 0);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM reservation WHERE user_id=?", Long.class,
                    LocalIdentityConfiguration.BOB_ID)).isZero();
            assertThat(jdbc.queryForObject("SELECT last_value FROM test_decrement_observed", Long.class)).isEqualTo(2);
            System.out.println("PHASE5 injectedFault=1 status=503 decrementObserved=true intentionalFailureReached=true failedOwnerRows=0");
        } finally {
            jdbc.execute("DROP TRIGGER test_contention_failure ON reservation");
            jdbc.execute("DROP FUNCTION test_contention_failure()");
            jdbc.execute("DROP SEQUENCE test_decrement_observed");
        }
    }

    private List<Outcome> contend(UUID resource, List<Attempt> attempts) throws Exception {
        try (Connection gate = connection(); Connection observer = connection(); Batch batch = new Batch(attempts)) {
            lockResource(gate, resource);
            batch.start();
            awaitOverlap(observer);
            gate.rollback();
            batch.await(observer);
            return batch.results(0, attempts.size());
        }
    }

    private void lockResource(Connection gate, UUID resource) throws Exception {
        gate.setAutoCommit(false);
        try (var statement = gate.prepareStatement("SELECT id FROM resource WHERE id=? FOR UPDATE")) {
            statement.setObject(1, resource);
            assertThat(statement.executeQuery().next()).isTrue();
        }
    }

    private void awaitOverlap(Connection observer) throws Exception {
        await(observer, () -> overlappingSessions >= 2, 15);
    }

    private void await(Connection observer, BooleanSupplier done, int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        do {
            assertInvariant(observer);
            try (var statement = observer.createStatement(); var rows = statement.executeQuery("""
                    SELECT count(DISTINCT pid) FROM pg_stat_activity
                    WHERE datname=current_database() AND pid<>pg_backend_pid()
                      AND state='active' AND wait_event_type='Lock'
                      AND ltrim(query) LIKE 'UPDATE resource%'
                    """)) {
                rows.next();
                overlappingSessions = Math.max(overlappingSessions, rows.getInt(1));
            }
            if (done.getAsBoolean()) return;
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Bounded wait failed; overlapping allocation sessions=" + overlappingSessions);
    }

    private void assertInvariant(Connection observer) throws Exception {
        // One MVCC snapshot across both tables, including every persisted HELD even if overdue.
        try (var statement = observer.createStatement(); var rows = statement.executeQuery("""
                SELECT s.total_capacity, s.available_capacity,
                    coalesce(sum(r.quantity) FILTER (WHERE r.status IN ('HELD','CONFIRMED')),0) allocated
                FROM resource s LEFT JOIN reservation r ON r.resource_id=s.id
                GROUP BY s.id
                """)) {
            while (rows.next()) {
                long total = rows.getLong(1), available = rows.getLong(2), allocated = rows.getLong(3);
                assertThat(allocated).isBetween(0L, total);
                assertThat(available).isBetween(0L, total);
                assertThat(available + allocated).isEqualTo(total);
            }
        }
        samples++;
    }

    private void verify(String scenario, UUID resource, List<Outcome> results, int expectedSuccess,
                        int expectedConflict, int expectedAvailable) throws Exception {
        var successes = results.stream().filter(r -> r.status == 201).toList();
        var conflicts = results.stream().filter(r -> r.status == 409).toList();
        for (Outcome result : results) {
            assertThat(result.status).as("Unexpected response: %s", result.body).isIn(201, 409);
            if (result.status == 409) assertThat(result.body).containsEntry("code", "INSUFFICIENT_CAPACITY");
        }
        if (expectedSuccess >= 0) assertThat(successes).hasSize(expectedSuccess);
        if (expectedConflict >= 0) assertThat(conflicts).hasSize(expectedConflict);
        var ids = new HashSet<UUID>();
        long quantity = 0;
        for (Outcome success : successes) {
            UUID id = UUID.fromString((String) success.body.get("id"));
            assertThat(ids.add(id)).isTrue();
            assertThat(success.body).containsEntry("resourceId", resource.toString())
                    .containsEntry("quantity", success.request.quantity).containsEntry("status", "HELD")
                    .containsEntry("userId", LocalIdentityConfiguration.ALICE_ID.toString());
            assertThat(jdbc.queryForObject("SELECT quantity FROM reservation WHERE id=? AND resource_id=?", Integer.class, id, resource))
                    .isEqualTo(success.request.quantity);
            quantity += success.request.quantity;
        }
        assertThat(jdbc.queryForList("SELECT id FROM reservation WHERE resource_id=?", UUID.class, resource))
                .containsExactlyInAnyOrderElementsOf(ids);
        var state = jdbc.queryForMap("SELECT total_capacity,available_capacity,version FROM resource WHERE id=?", resource);
        int available = ((Number) state.get("available_capacity")).intValue();
        if (expectedAvailable >= 0) assertThat(available).isEqualTo(expectedAvailable);
        assertThat(((Number) state.get("version")).longValue()).isEqualTo(successes.size());
        assertThat(quantity + available).isEqualTo(((Number) state.get("total_capacity")).longValue());
        try (Connection observer = connection()) { assertInvariant(observer); }
        assertThat(overlappingSessions).isGreaterThanOrEqualTo(2);
        String line = "PHASE5 scenario=" + scenario + " requests=" + results.size() + " success=" + successes.size()
                + " conflicts=" + conflicts.size() + " rows=" + ids.size() + " allocated=" + quantity
                + " available=" + available + " overlap=" + overlappingSessions + " snapshots=" + samples;
        System.out.println(line);
        Path directory = Path.of("target", "phase5-results");
        Files.createDirectories(directory);
        Files.writeString(directory.resolve(scenario + ".txt"), line + System.lineSeparator());
    }

    private UUID resource(int capacity) {
        return jdbc.queryForObject("INSERT INTO resource(name,total_capacity,available_capacity) VALUES ('Concurrency fixture',?,?) RETURNING id",
                UUID.class, capacity, capacity);
    }

    private List<Attempt> requests(UUID resource, int count, boolean mixed) {
        return IntStream.range(0, count).mapToObj(i -> new Attempt(resource, mixed ? i % 4 + 1 : 1, "alice")).toList();
    }

    private Connection connection() throws Exception {
        var properties = new java.util.Properties();
        properties.setProperty("user", POSTGRES.getUsername());
        properties.setProperty("password", POSTGRES.getPassword());
        properties.setProperty("connectTimeout", "5");
        properties.setProperty("socketTimeout", "40");
        properties.setProperty("options", "-c statement_timeout=30000 -c lock_timeout=20000");
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), properties);
    }

    private Outcome send(Attempt attempt) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/reservations"))
                .timeout(Duration.ofSeconds(60))
                .header("Authorization", "Basic " + Base64.getEncoder().encodeToString((attempt.user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)))
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .POST(HttpRequest.BodyPublishers.ofString("{\"resourceId\":\"" + attempt.resource + "\",\"quantity\":" + attempt.quantity + "}"))
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        UUID.fromString(response.headers().firstValue("X-Request-Id").orElseThrow());
        return new Outcome(attempt, response.statusCode(), JsonPath.read(response.body(), "$"));
    }

    private record Attempt(UUID resource, int quantity, String user) { }
    private record Outcome(Attempt request, int status, Map<String, Object> body) { }

    private class Batch implements AutoCloseable {
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        private final CyclicBarrier barrier;
        private final List<Future<Outcome>> futures = new ArrayList<>();

        Batch(List<Attempt> attempts) {
            barrier = new CyclicBarrier(attempts.size() + 1);
            for (Attempt attempt : attempts) {
                futures.add(executor.submit(() -> {
                    // Synchronize clients BEFORE HTTP or database connection acquisition.
                    barrier.await(15, TimeUnit.SECONDS);
                    return send(attempt);
                }));
            }
        }

        void start() throws Exception { barrier.await(15, TimeUnit.SECONDS); }
        void await(Connection observer) throws Exception {
            AllocationConcurrencyIT.this.await(observer, () -> futures.stream().allMatch(Future::isDone), 75);
        }
        List<Outcome> results(int from, int to) throws Exception {
            var outcomes = new ArrayList<Outcome>();
            for (Future<Outcome> future : futures.subList(from, to)) outcomes.add(future.get(1, TimeUnit.SECONDS));
            return outcomes;
        }
        @Override public void close() throws Exception {
            futures.forEach(f -> f.cancel(true));
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
}
