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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import com.atlas.configuration.LocalIdentityConfiguration;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"local", "test"})
@Testcontainers(disabledWithoutDocker = false)
@Timeout(90)
@org.springframework.test.context.TestExecutionListeners(listeners=RedisOutageEvidence.class,
        mergeMode=org.springframework.test.context.TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
class ReservationLifecycleIT {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_lifecycle_test").withUsername("atlas_test").withPassword("testcontainers-only")
            .withStartupTimeout(Duration.ofSeconds(90));
    static final String PASSWORD = UUID.randomUUID().toString();
    @LocalServerPort int port;
    @Autowired JdbcTemplate jdbc;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        for (String user : List.of("alice", "bob", "admin")) registry.add("atlas.local." + user + "-password", () -> PASSWORD);
        registry.add("atlas.hold.ttl-seconds", () -> 3600);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> 10);
        registry.add("spring.datasource.hikari.data-source-properties.options", () -> "-c statement_timeout=20000 -c lock_timeout=15000");
        registry.add("spring.datasource.hikari.data-source-properties.socketTimeout", () -> 25);
    }

    @BeforeEach
    void resetOwnDatabase() {
        jdbc.execute("DROP TRIGGER IF EXISTS test_release_failure ON resource");
        jdbc.execute("DROP FUNCTION IF EXISTS test_release_failure()");
        jdbc.execute("DROP SEQUENCE IF EXISTS test_cancel_observed");
        jdbc.update("DELETE FROM outbox_event");
        jdbc.update("DELETE FROM idempotency_record");
        jdbc.update("DELETE FROM reservation");
        jdbc.update("DELETE FROM resource");
    }

    @ParameterizedTest
    @ValueSource(strings = {"confirm", "cancel"})
    void repeatedOperationHasExactlyOneEffectEvenAfterOldDeadline(String operation) throws Exception {
        Fixture f = hold();
        var beforeResource = resourceRow(f);
        var result = action(f.id, operation, "alice", null);
        assertThat(result.statusCode()).isEqualTo(200);
        var body = json(result);
        String state = operation.equals("confirm") ? "CONFIRMED" : "CANCELLED";
        assertThat(body).containsEntry("status", state).containsEntry("id", f.id.toString())
                .containsEntry("resourceId", f.resource.toString()).containsEntry("quantity", 3)
                .containsEntry("userId", LocalIdentityConfiguration.ALICE_ID.toString());
        assertThat(body.get(operation.equals("confirm") ? "confirmedAt" : "cancelledAt")).isEqualTo(body.get("updatedAt"));
        assertThat(body.get(operation.equals("confirm") ? "cancelledAt" : "confirmedAt")).isNull();
        assertThat(body.get("expiredAt")).isNull();
        assertState(f, state);
        if (operation.equals("confirm")) assertThat(resourceRow(f)).isEqualTo(beforeResource);
        var after = reservationRow(f);
        var resourceAfter = resourceRow(f);
        var repeated = action(f.id, operation, "alice", null);
        assertThat(repeated.statusCode()).isEqualTo(200);
        assertThat(json(repeated)).isEqualTo(body);
        assertThat(reservationRow(f)).isEqualTo(after);
        assertThat(resourceRow(f)).isEqualTo(resourceAfter);
        // Move the entire valid terminal history into the past, preserving row timestamp constraints.
        jdbc.update("""
                UPDATE reservation SET created_at=created_at-interval '2 hours', updated_at=updated_at-interval '2 hours',
                    expires_at=expires_at-interval '2 hours', confirmed_at=confirmed_at-interval '2 hours',
                    cancelled_at=cancelled_at-interval '2 hours' WHERE id=?
                """, f.id);
        var historical = reservationRow(f);
        assertThat(action(f.id, operation, "alice", null).statusCode()).isEqualTo(200);
        assertThat(reservationRow(f)).isEqualTo(historical);
        assertThat(resourceRow(f)).isEqualTo(resourceAfter);
        assertState(f, state);
        record(operation, result);
    }

    @ParameterizedTest
    @CsvSource({"confirm,cancel", "cancel,confirm"})
    void oppositeTerminalOperationConflicts(String first, String second) throws Exception {
        Fixture f = hold();
        assertThat(action(f.id, first, "alice", null).statusCode()).isEqualTo(200);
        var before = reservationRow(f);
        error(action(f.id, second, "alice", null), 409, "INVALID_STATE_TRANSITION");
        assertThat(reservationRow(f)).isEqualTo(before);
        assertState(f, first.equals("confirm") ? "CONFIRMED" : "CANCELLED");
    }

    @ParameterizedTest
    @ValueSource(strings = {"confirm", "cancel"})
    void overdueHeldRemainsAllocatedAndPersistedExpiredIsRejected(String operation) throws Exception {
        Fixture f = hold();
        jdbc.update("UPDATE reservation SET created_at=clock_timestamp()-interval '2 minutes', expires_at=clock_timestamp()-interval '1 minute' WHERE id=?", f.id);
        var before = reservationRow(f);
        var expired = action(f.id, operation, "alice", null);
        error(expired, 409, "RESERVATION_EXPIRED");
        assertThat(reservationRow(f)).isEqualTo(before);
        assertState(f, "HELD");
        // A consistent EXPIRED fixture exercises the matrix only; no worker is implemented.
        // Use one dedicated connection for fixture atomicity (not the application's pool transaction).
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("UPDATE reservation SET status='EXPIRED',transition_sequence=2,expired_at=clock_timestamp() WHERE id=?")) {
                statement.setObject(1, f.id); statement.executeUpdate();
            }
            try (var statement = connection.prepareStatement("UPDATE resource SET available_capacity=10,version=2 WHERE id=?")) {
                statement.setObject(1, f.resource); statement.executeUpdate();
            }
            connection.commit();
        }
        error(action(f.id, operation, "alice", null), 409, "RESERVATION_EXPIRED");
        assertState(f, "EXPIRED");
        record(operation + "-expired", expired);
    }

    @ParameterizedTest
    @ValueSource(strings = {"confirm", "cancel"})
    void authenticatesOwnerAndValidatesEmptyBodyAndIdentifier(String operation) throws Exception {
        Fixture f = hold();
        var before = reservationRow(f);
        for (String user : List.of("bob", "admin")) error(action(f.id, operation, user, null), 404, "RESERVATION_NOT_FOUND");
        error(action(f.id, operation, null, null), 401, "AUTHENTICATION_REQUIRED");
        error(action(UUID.randomUUID(), operation, "alice", null), 404, "RESERVATION_NOT_FOUND");
        error(send("/api/reservations/not-a-uuid/" + operation, "alice", null), 400, "INVALID_REQUEST");
        for (String body : List.of("{}", "null", " ", "{\"userId\":\"" + LocalIdentityConfiguration.ALICE_ID + "\"}")) {
            error(action(f.id, operation, "alice", body), 400, "INVALID_REQUEST");
        }
        // Forged identity headers are ignored for lifecycle authorization too.
        var forged = client.send(request("/api/reservations/" + f.id + "/" + operation, "bob", null)
                .header("X-User-Id", LocalIdentityConfiguration.ALICE_ID.toString()).header("X-Role", "ADMIN").build(), HttpResponse.BodyHandlers.ofString());
        error(forged, 404, "RESERVATION_NOT_FOUND");
        assertThat(reservationRow(f)).isEqualTo(before);
        assertState(f, "HELD");
    }

    @RepeatedTest(3)
    void confirmVersusCancelHasOneWinner() throws Exception {
        Fixture f = hold();
        var results = race(f, List.of("confirm", "cancel"));
        assertThat(results.stream().map(HttpResponse::statusCode)).containsExactlyInAnyOrder(200, 409);
        var winner = results.stream().filter(r -> r.statusCode() == 200).findFirst().orElseThrow();
        error(results.stream().filter(r -> r.statusCode() == 409).findFirst().orElseThrow(), 409, "INVALID_STATE_TRANSITION");
        String state = (String) json(winner).get("status");
        assertState(f, state);
        System.out.println("PHASE6 race=confirm-cancel winner=" + state + " success=1 conflict=1 conservation=PASS");
    }

    @ParameterizedTest
    @ValueSource(strings = {"confirm", "cancel"})
    void simultaneousSameOperationsHaveOneMutation(String operation) throws Exception {
        Fixture f = hold();
        var results = race(f, java.util.Collections.nCopies(16, operation));
        assertThat(results).allMatch(r -> r.statusCode() == 200);
        var first = json(results.getFirst());
        results.forEach(r -> assertThat(json(r)).isEqualTo(first));
        assertState(f, operation.equals("confirm") ? "CONFIRMED" : "CANCELLED");
        System.out.println("PHASE6 race=" + operation + "-x16 success=16 mutations=1 conservation=PASS");
    }

    @ParameterizedTest
    @ValueSource(strings = {"confirm", "cancel"})
    void waitsAcrossDeadlineThenRejectsUsingPostLockTime(String operation) throws Exception {
        Fixture f = hold();
        try (Connection gate = connection(); var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            lock(gate, f.id);
            // The committed deadline will be crossed while an HTTP transaction is demonstrably blocked.
            // Set using the lock owner to avoid blocking fixture setup on its own row.
            try (var statement = gate.prepareStatement("UPDATE reservation SET expires_at=clock_timestamp()+interval '3 seconds' WHERE id=?")) {
                statement.setObject(1, f.id); statement.executeUpdate();
            }
            Future<HttpResponse<String>> future = workers.submit(() -> action(f.id, operation, "alice", null));
            await(() -> waitingSessions() >= 1, 8);
            // Read the gate's own uncommitted deadline; application must re-read after lock acquisition.
            try (var statement = gate.prepareStatement("SELECT clock_timestamp()>=expires_at FROM reservation WHERE id=?")) {
                statement.setObject(1, f.id);
                try (var row = statement.executeQuery()) { row.next(); assertThat(row.getBoolean(1)).isFalse(); }
                await(() -> {
                    try (var row = statement.executeQuery()) { row.next(); return row.getBoolean(1); }
                    catch (java.sql.SQLException e) { throw new IllegalStateException(e); }
                }, 8);
            }
            assertThat(future).isNotDone();
            gate.commit();
            error(future.get(10, TimeUnit.SECONDS), 409, "RESERVATION_EXPIRED");
            assertState(f, "HELD");
            System.out.println("PHASE6 deadline-wait=" + operation + " response=409 state=HELD available=7 conservation=PASS");
        }
    }

    @Test
    void releaseFailureRollsBackStateAndCapacityThenRetrySucceeds() throws Exception {
        Fixture f = hold();
        var beforeReservation = reservationRow(f);
        var beforeResource = resourceRow(f);
        jdbc.execute("CREATE SEQUENCE test_cancel_observed");
        jdbc.execute("""
                CREATE FUNCTION test_release_failure() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.available_capacity > OLD.available_capacity THEN
                        IF NOT EXISTS (SELECT 1 FROM reservation WHERE resource_id=OLD.id AND status='CANCELLED') THEN
                            RAISE EXCEPTION 'state transition not visible';
                        END IF;
                        PERFORM nextval('test_cancel_observed');
                        RAISE EXCEPTION 'test-only release failure' USING ERRCODE='23514';
                    END IF;
                    RETURN NEW;
                END $$
                """);
        jdbc.execute("CREATE TRIGGER test_release_failure BEFORE UPDATE ON resource FOR EACH ROW EXECUTE FUNCTION test_release_failure()");
        try {
            var failure = action(f.id, "cancel", "alice", null);
            error(failure, 503, "TEMPORARILY_UNAVAILABLE");
            assertThat(jdbc.queryForObject("SELECT is_called FROM test_cancel_observed", Boolean.class)).isTrue();
            assertThat(reservationRow(f)).isEqualTo(beforeReservation);
            assertThat(resourceRow(f)).isEqualTo(beforeResource);
            assertState(f, "HELD");
            record("release-rollback", failure);
        } finally {
            jdbc.execute("DROP TRIGGER test_release_failure ON resource");
            jdbc.execute("DROP FUNCTION test_release_failure()");
            jdbc.execute("DROP SEQUENCE test_cancel_observed");
        }
        assertThat(action(f.id, "cancel", "alice", null).statusCode()).isEqualTo(200);
        assertState(f, "CANCELLED");
    }

    private List<HttpResponse<String>> race(Fixture f, List<String> operations) throws Exception {
        var workers = Executors.newVirtualThreadPerTaskExecutor();
        var futures = new ArrayList<Future<HttpResponse<String>>>();
        try (Connection gate = connection()) {
            lock(gate, f.id);
            CyclicBarrier barrier = new CyclicBarrier(operations.size() + 1);
            for (String operation : operations) futures.add(workers.submit(() -> {
                barrier.await(10, TimeUnit.SECONDS);
                return action(f.id, operation, "alice", null);
            }));
            barrier.await(10, TimeUnit.SECONDS);
            await(() -> waitingSessions() >= 2, 8);
            gate.rollback();
            var results = new ArrayList<HttpResponse<String>>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            for (var future : futures) results.add(future.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS));
            return results;
        } finally {
            futures.forEach(future -> future.cancel(true));
            workers.shutdownNow();
            assertThat(workers.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private int waitingSessions() {
        // A separate connection remains usable even when every application pool slot is blocked.
        try (Connection observer = connection(); var statement = observer.createStatement(); var rows = statement.executeQuery("""
                SELECT count(DISTINCT pid) FROM pg_stat_activity WHERE datname=current_database()
                    AND state='active' AND wait_event_type='Lock' AND query LIKE 'SELECT * FROM reservation%FOR UPDATE%'
                """)) {
            rows.next(); return rows.getInt(1);
        } catch (java.sql.SQLException e) { throw new IllegalStateException(e); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }

    private void await(BooleanSupplier condition, int seconds) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) if (condition.getAsBoolean()) return;
        throw new AssertionError("Bounded database-state wait failed");
    }

    private void lock(Connection connection, UUID id) throws Exception {
        connection.setAutoCommit(false);
        try (var statement = connection.prepareStatement("SELECT id FROM reservation WHERE id=? FOR UPDATE")) {
            statement.setObject(1, id); assertThat(statement.executeQuery().next()).isTrue();
        }
    }

    private Connection connection() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl() + "&connectTimeout=5&socketTimeout=25", POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private Fixture hold() throws Exception {
        UUID resource = jdbc.queryForObject("INSERT INTO resource(name,total_capacity,available_capacity) VALUES ('Lifecycle fixture',10,10) RETURNING id", UUID.class);
        var response = send("/api/reservations", "alice", "{\"resourceId\":\"" + resource + "\",\"quantity\":3}");
        assertThat(response.statusCode()).isEqualTo(201);
        return new Fixture(resource, UUID.fromString((String) json(response).get("id")));
    }

    private void assertState(Fixture f, String expected) {
        // State and conserved quantities from the SAME committed database statement snapshot after every race.
        var row = jdbc.queryForMap("""
                SELECT r.status,s.available_capacity,s.version,s.total_capacity,
                    (SELECT coalesce(sum(quantity),0) FROM reservation WHERE resource_id=s.id AND status IN ('HELD','CONFIRMED')) allocated
                FROM reservation r JOIN resource s ON s.id=r.resource_id WHERE r.id=?
                """, f.id);
        assertThat(row.get("status")).isEqualTo(expected);
        boolean allocated = expected.equals("HELD") || expected.equals("CONFIRMED");
        assertThat(((Number) row.get("available_capacity")).intValue()).isEqualTo(allocated ? 7 : 10);
        assertThat(((Number) row.get("allocated")).longValue()).isEqualTo(allocated ? 3 : 0);
        assertThat(((Number) row.get("version")).longValue()).isEqualTo(allocated ? 1 : 2);
        assertThat(((Number) row.get("allocated")).longValue() + ((Number) row.get("available_capacity")).longValue())
                .isEqualTo(((Number) row.get("total_capacity")).longValue());
    }

    private Map<String, Object> resourceRow(Fixture f) { return jdbc.queryForMap("SELECT * FROM resource WHERE id=?", f.resource); }
    private Map<String, Object> reservationRow(Fixture f) { return jdbc.queryForMap("SELECT * FROM reservation WHERE id=?", f.id); }
    private record Fixture(UUID resource, UUID id) { }
    private HttpResponse<String> action(UUID id, String operation, String user, String body) throws Exception {
        return send("/api/reservations/" + id + "/" + operation, user, body);
    }
    private HttpResponse<String> send(String path, String user, String body) throws Exception {
        return client.send(request(path, user, body).build(), HttpResponse.BodyHandlers.ofString());
    }
    private HttpRequest.Builder request(String path, String user, String body) {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).timeout(Duration.ofSeconds(30));
        if (user != null) request.header("Authorization", "Basic " + Base64.getEncoder().encodeToString((user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)));
        if (body != null) request.header("Content-Type", "application/json");
        request.setHeader("Idempotency-Key", UUID.randomUUID().toString());
        return request.POST(body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
    }
    private Map<String, Object> json(HttpResponse<String> response) { return JsonPath.read(response.body(), "$"); }
    private void error(HttpResponse<String> response, int status, String code) {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(status);
        assertThat(json(response)).containsEntry("code", code)
                .containsEntry("requestId", response.headers().firstValue("X-Request-Id").orElseThrow());
        assertThat(response.body()).doesNotContain(PASSWORD, "UPDATE resource", "test-only", "SQLException");
    }
    private void record(String name, HttpResponse<String> response) throws Exception {
        Path directory = Path.of("target", "phase6-http"); Files.createDirectories(directory);
        Files.writeString(directory.resolve(name + ".txt"), "HTTP " + response.statusCode() + "\nX-Request-Id: "
                + response.headers().firstValue("X-Request-Id").orElseThrow() + "\n" + response.body() + "\n");
    }
}
