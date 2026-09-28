package com.atlas;

import java.net.Socket;
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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import com.atlas.configuration.LocalIdentityConfiguration;
import com.atlas.idempotency.IdempotentMutations;
import com.atlas.idempotency.IdempotencyExecutor;
import com.atlas.common.ApiException;
import org.springframework.http.HttpStatus;
import com.atlas.idempotency.StoredResponse;
import com.atlas.reservation.CreateReservationRequest;
import com.atlas.reservation.ExpirationProcessor;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
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
class IdempotencyIT {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_idempotency_test").withUsername("atlas_test").withPassword("testcontainers-only")
            .withStartupTimeout(Duration.ofSeconds(90));
    static final String PASSWORD = UUID.randomUUID().toString();
    static final UUID ALICE = LocalIdentityConfiguration.ALICE_ID;
    private static final long GATE = 880081;
    @Autowired JdbcTemplate jdbc;
    @Autowired IdempotencyExecutor executor;
    @Autowired ExpirationProcessor expiration;
    @LocalServerPort int port;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private ExecutorService threads;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("atlas.local.alice-password", () -> PASSWORD);
        registry.add("atlas.local.bob-password", () -> PASSWORD);
        registry.add("atlas.local.admin-password", () -> PASSWORD);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> 20);
        registry.add("atlas.hold.ttl-seconds", () -> 3600);
    }

    @BeforeEach
    void resetDisposableDatabase() {
        for (String table : List.of("resource", "reservation", "idempotency_record"))
            jdbc.execute("DROP TRIGGER IF EXISTS test_idempotency_fault ON " + table);
        jdbc.execute("DROP FUNCTION IF EXISTS test_idempotency_fault()");
        jdbc.execute("DROP SEQUENCE IF EXISTS test_idempotency_seen");
        jdbc.update("DELETE FROM outbox_event");
        jdbc.update("DELETE FROM idempotency_record");
        jdbc.update("DELETE FROM reservation");
        jdbc.update("DELETE FROM resource");
        threads = Executors.newVirtualThreadPerTaskExecutor();
    }
    @AfterEach
    void closeThreadsAndCheckNoIncompleteClaims() throws Exception {
        threads.shutdownNow();
        assertThat(threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        assertThat(count("idempotency_record WHERE completed_at IS NULL")).isZero();
        invariant();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event", Long.class))
                .isEqualTo(jdbc.queryForObject("SELECT coalesce(sum(transition_sequence),0) FROM reservation", Long.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {"resource", "reservation", "confirm", "cancel"})
    void concurrentDuplicatesWaitOnUniqueClaimAndReplayOriginal(String kind) throws Exception {
        UUID resource = fixture(10);
        boolean lifecycle = kind.equals("confirm") || kind.equals("cancel");
        UUID heldId = lifecycle ? id(hold(resource, 2, "fixture")) : null;
        String path = lifecycle ? "/api/reservations/" + heldId + "/" + kind : kind.equals("resource") ? "/api/resources" : "/api/reservations";
        String body = kind.equals("resource") ? "{\"name\":\"Unicode café 東京\",\"totalCapacity\":10}" : lifecycle ? null : holdBody(resource, 2);
        String user = kind.equals("resource") ? "admin" : "alice";
        trigger(lifecycle ? "reservation" : kind, "PERFORM pg_advisory_xact_lock(" + GATE + "); RETURN NEW;", lifecycle ? "UPDATE" : "INSERT");
        List<Future<HttpResponse<String>>> pending = new ArrayList<>();
        try (Connection gate = gate()) {
            pending.add(threads.submit(() -> post(path, user, "duplicate", body)));
            await(() -> advisoryWaiters() >= 1);
            CyclicBarrier start = new CyclicBarrier(13);
            for (int i = 0; i < 12; i++) pending.add(threads.submit(() -> {
                start.await(5, TimeUnit.SECONDS);
                return post(path, user, "duplicate", body);
            }));
            start.await(5, TimeUnit.SECONDS);
            await(() -> claimWaiters() >= 2);
            unlock(gate);
        }
        List<HttpResponse<String>> results = new ArrayList<>();
        for (var future : pending) results.add(future.get(15, TimeUnit.SECONDS));
        var originals = results.stream().filter(r -> r.headers().firstValue("Idempotency-Replayed").isEmpty()).toList();
        assertThat(originals).hasSize(1);
        assertThat(originals.getFirst().statusCode()).isEqualTo(lifecycle ? 200 : 201);
        for (var result : results) {
            assertThat(result.statusCode()).isEqualTo(lifecycle ? 200 : 201);
            assertThat(result.body()).isEqualTo(originals.getFirst().body());
            assertThat(result.headers().firstValue("Location")).isEqualTo(originals.getFirst().headers().firstValue("Location"));
            assertThat(result.headers().firstValue("Content-Type")).contains("application/json");
        }
        assertThat(results.stream().map(r -> r.headers().firstValue("X-Request-Id").orElseThrow()).distinct()).hasSize(13);
        assertThat(count("idempotency_record")).isEqualTo(lifecycle ? 2 : 1);
        if (kind.equals("cancel")) capacity(resource, 10, 2, 1);
        else if (kind.equals("reservation") || kind.equals("confirm")) capacity(resource, 8, 1, 1);
        else assertThat(count("resource WHERE name='Unicode café 東京'")).isEqualTo(1);
        System.out.println("PHASE8 duplicates kind=" + kind + " requests=13 originals=1 replays=12 uniqueWaiters>=2 conservation=PASS");
    }

    @Test
    void simultaneousMismatchWaitsForOriginalCommit() throws Exception {
        UUID resource = fixture(10);
        trigger("reservation", "PERFORM pg_advisory_xact_lock(" + GATE + "); RETURN NEW;", "INSERT");
        Future<HttpResponse<String>> first;
        Future<HttpResponse<String>> second;
        try (Connection gate = gate()) {
            first = threads.submit(() -> hold(resource, 2, "same"));
            await(() -> advisoryWaiters() >= 1);
            second = threads.submit(() -> hold(resource, 3, "same"));
            await(() -> claimWaiters() >= 1);
            unlock(gate);
        }
        assertThat(first.get(10, TimeUnit.SECONDS).statusCode()).isEqualTo(201);
        error(second.get(10, TimeUnit.SECONDS), 409, "IDEMPOTENCY_KEY_MISMATCH");
        capacity(resource, 8, 1, 1);
        assertThat(count("idempotency_record")).isEqualTo(1);
    }

    @Test
    void canonicalRequestsIgnoreJsonOrderSpacingTrimAndUuidCase() throws Exception {
        var first = post("/api/resources", "admin", "canonical", "{\"name\":\"  café 東京  \",\"totalCapacity\":10}");
        replay(first, post("/api/resources", "admin", "canonical", "{ \"totalCapacity\" : 10, \"name\" : \"café 東京\" }"));
        UUID resource = id(first);
        var held = hold(resource, 2, "canonical");
        replay(held, post("/api/reservations", "alice", "canonical", "{\"quantity\":2,\"resourceId\":\"" + resource.toString().toUpperCase() + "\"}"));
        error(post("/api/resources", "admin", "canonical", "{\"name\":\"different\",\"totalCapacity\":10}"), 409, "IDEMPOTENCY_KEY_MISMATCH");
        error(hold(resource, 3, "canonical"), 409, "IDEMPOTENCY_KEY_MISMATCH");
        error(hold(fixture(10), 2, "canonical"), 409, "IDEMPOTENCY_KEY_MISMATCH");
        capacity(resource, 8, 1, 1);
        assertThat(count("idempotency_record")).isEqualTo(2);
    }

    @Test
    void scopeIsCallerAndOperationWhilePathIsPartOfHash() throws Exception {
        UUID resource = fixture(10);
        var alice = hold(resource, 1, "shared");
        var bob = post("/api/reservations", "bob", "shared", holdBody(resource, 1));
        assertThat(alice.statusCode()).isEqualTo(201);
        assertThat(bob.statusCode()).isEqualTo(201);
        assertThat(id(alice)).isNotEqualTo(id(bob));
        replay(alice, hold(resource, 1, "shared"));
        var confirmed = action(id(alice), "confirm", "shared");
        assertThat(confirmed.statusCode()).isEqualTo(200);
        replay(confirmed, action(id(alice), "confirm", "shared"));
        error(action(id(alice), "cancel", "shared"), 409, "INVALID_STATE_TRANSITION");
        UUID other = id(hold(resource, 1, "other-hold"));
        error(action(other, "confirm", "shared"), 409, "IDEMPOTENCY_KEY_MISMATCH");
        capacity(resource, 7, 3, 3);
        assertThat(count("idempotency_record")).isEqualTo(5);
        assertThat(jdbc.queryForObject("SELECT status FROM reservation WHERE id=?", String.class, other)).isEqualTo("HELD");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "a b", "a,b", "a/b", "a@b", "oversized"})
    void invalidMissingOrOversizedKeysAreNotStored(String invalid) throws Exception {
        String key = invalid.equals("oversized") ? "a".repeat(129) : invalid;
        error(post("/api/reservations", "alice", key.isEmpty() ? null : key, holdBody(fixture(10), 1)), 400, "INVALID_IDEMPOTENCY_KEY");
        assertThat(count("idempotency_record")).isZero();
        assertThat(count("reservation")).isZero();
    }

    @Test
    void duplicateHeaderRejectedAndBoundaryLengthsAccepted() throws Exception {
        UUID resource = fixture(10);
        var duplicate = request("/api/reservations", "alice", "one", holdBody(resource, 1)).header("Idempotency-Key", "two").build();
        error(client.send(duplicate, HttpResponse.BodyHandlers.ofString()), 400, "INVALID_IDEMPOTENCY_KEY");
        assertThat(count("idempotency_record")).isZero();
        assertThat(hold(resource, 1, "x").statusCode()).isEqualTo(201);
        assertThat(hold(resource, 1, "A0._:-" + "z".repeat(122)).statusCode()).isEqualTo(201);
        capacity(resource, 8, 2, 2);
    }

    @Test
    void invalidUnauthenticatedAndForeignOwnerRequestsDoNotConsumeKeys() throws Exception {
        UUID resource = fixture(10);
        error(post("/api/reservations", "alice", "bad", holdBody(resource, 0)), 400, "INVALID_REQUEST");
        error(post("/api/reservations", "alice", "bad", "{}"), 400, "INVALID_REQUEST");
        error(post("/api/reservations", null, "bad", holdBody(resource, 1)), 401, "AUTHENTICATION_REQUIRED");
        error(post("/api/resources", "alice", "bad", "{\"name\":\"Room\",\"totalCapacity\":10}"), 403, "FORBIDDEN");
        assertThat(count("idempotency_record")).isZero();
        UUID id = id(hold(resource, 1, "bad"));
        for (String user : List.of("bob", "admin")) {
            error(post("/api/reservations/" + id + "/confirm", user, "foreign", null), 404, "RESERVATION_NOT_FOUND");
            error(post("/api/reservations/" + id + "/cancel", user, "foreign", null), 404, "RESERVATION_NOT_FOUND");
        }
        error(post("/api/reservations/" + id + "/confirm", "alice", "body", "{}"), 400, "INVALID_REQUEST");
        assertThat(count("idempotency_record")).isEqualTo(1);
        capacity(resource, 9, 1, 1);
    }

    @Test
    void replayRechecksAccessIncludingStoredCreationReference() throws Exception {
        UUID resource = fixture(10);
        var held = hold(resource, 1, "owner");
        assertThat(action(id(held), "confirm", "owner").statusCode()).isEqualTo(200);
        // No owner-changing API exists. Test-only tampering proves replay still checks PostgreSQL ownership.
        jdbc.update("UPDATE reservation SET user_id=? WHERE id=?", LocalIdentityConfiguration.BOB_ID, id(held));
        error(hold(resource, 1, "owner"), 404, "RESERVATION_NOT_FOUND");
        error(action(id(held), "confirm", "owner"), 404, "RESERVATION_NOT_FOUND");
        assertThat(count("idempotency_record")).isEqualTo(2);
        capacity(resource, 9, 1, 1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"confirm", "cancel", "expire"})
    void originalHeldResponseReplaysAfterStateChanges(String target) throws Exception {
        UUID resource = fixture(10);
        var held = hold(resource, 2, "create");
        if (target.equals("expire")) {
            due(id(held));
            assertThat(expiration.expire(id(held)).expired()).isTrue();
        } else {
            var changed = action(id(held), target, "change");
            assertThat(changed.statusCode()).isEqualTo(200);
            replay(changed, action(id(held), target, "change"));
        }
        replay(held, hold(resource, 2, "create"));
        assertThat(json(held).get("status")).isEqualTo("HELD");
        assertThat(jdbc.queryForObject("SELECT status FROM reservation WHERE id=?", String.class, id(held)))
                .isEqualTo(switch (target) { case "confirm" -> "CONFIRMED"; case "cancel" -> "CANCELLED"; default -> "EXPIRED"; });
        capacity(resource, target.equals("confirm") ? 8 : 10, target.equals("confirm") ? 1 : 2, 1);
    }

    @Test
    void storedCapacityConflictReplaysAfterCapacityBecomesFree() throws Exception {
        UUID resource = fixture(1);
        var held = hold(resource, 1, "occupy");
        var rejected = hold(resource, 1, "conflict");
        error(rejected, 409, "INSUFFICIENT_CAPACITY");
        assertThat(json(rejected).get("requestId")).isEqualTo(rejected.headers().firstValue("X-Request-Id").orElseThrow());
        assertThat(action(id(held), "cancel", "release").statusCode()).isEqualTo(200);
        var again = hold(resource, 1, "conflict");
        replay(rejected, again);
        assertThat(json(again).get("requestId")).isNotEqualTo(again.headers().firstValue("X-Request-Id").orElseThrow());
        capacity(resource, 1, 2, 1);
        assertThat(hold(resource, 1, "new-intention").statusCode()).isEqualTo(201);
        capacity(resource, 0, 3, 2);
    }

    @Test
    void deterministicMissingAndExpiredResultsPersist() throws Exception {
        UUID missing = UUID.randomUUID();
        var noResource = hold(missing, 1, "missing-resource");
        error(noResource, 404, "RESOURCE_NOT_FOUND");
        replay(noResource, hold(missing, 1, "missing-resource"));
        var noReservation = action(missing, "confirm", "missing-reservation");
        error(noReservation, 404, "RESERVATION_NOT_FOUND");
        replay(noReservation, action(missing, "confirm", "missing-reservation"));
        UUID resource = fixture(10);
        UUID id = id(hold(resource, 1, "held"));
        due(id);
        var expired = action(id, "confirm", "expired");
        error(expired, 409, "RESERVATION_EXPIRED");
        assertThat(expiration.expire(id).expired()).isTrue();
        replay(expired, action(id, "confirm", "expired"));
        assertThat(count("idempotency_record")).isEqualTo(4);
        capacity(resource, 10, 2, 1);
    }

    @Test
    void responseStorageFailureRollsBackClaimAndMutation() throws Exception {
        UUID resource = fixture(10);
        jdbc.execute("CREATE SEQUENCE test_idempotency_seen");
        trigger("idempotency_record", """
                IF EXISTS (SELECT 1 FROM reservation WHERE id=NEW.reservation_id)
                    AND EXISTS (SELECT 1 FROM resource WHERE id=NEW.resource_id AND available_capacity=8) THEN
                    PERFORM nextval('test_idempotency_seen');
                END IF;
                RAISE EXCEPTION 'test completion failure';
                """, "UPDATE");
        error(hold(resource, 2, "retry"), 503, "TEMPORARILY_UNAVAILABLE");
        assertThat(jdbc.queryForObject("SELECT is_called FROM test_idempotency_seen", Boolean.class)).isTrue();
        capacity(resource, 10, 0, 0);
        assertThat(count("idempotency_record")).isZero();
        jdbc.execute("DROP TRIGGER test_idempotency_fault ON idempotency_record");
        assertThat(hold(resource, 2, "retry").statusCode()).isEqualTo(201);
        capacity(resource, 8, 1, 1);
    }

    @Test
    void waitingDuplicateClaimsAfterFirstRollsBack() throws Exception {
        UUID resource = fixture(10);
        jdbc.execute("CREATE SEQUENCE test_idempotency_seen");
        trigger("reservation", """
                IF nextval('test_idempotency_seen')=1 THEN
                    PERFORM pg_advisory_xact_lock(880081);
                    RAISE EXCEPTION 'test first insertion failure';
                END IF;
                RETURN NEW;
                """, "INSERT");
        Future<HttpResponse<String>> first;
        Future<HttpResponse<String>> second;
        try (Connection gate = gate()) {
            first = threads.submit(() -> hold(resource, 2, "retry"));
            await(() -> advisoryWaiters() >= 1);
            second = threads.submit(() -> hold(resource, 2, "retry"));
            await(() -> claimWaiters() >= 1);
            unlock(gate);
        }
        error(first.get(10, TimeUnit.SECONDS), 503, "TEMPORARILY_UNAVAILABLE");
        var winner = second.get(10, TimeUnit.SECONDS);
        assertThat(winner.statusCode()).isEqualTo(201);
        assertThat(winner.headers().firstValue("Idempotency-Replayed")).isEmpty();
        replay(winner, hold(resource, 2, "retry"));
        capacity(resource, 8, 1, 1);
        assertThat(count("idempotency_record")).isEqualTo(1);
        System.out.println("PHASE8 rollback-winner first=503 waiter=201 reservations=1 decrements=1 conservation=PASS");
    }

    @Test
    void claimLockTimeoutIsRetryableWithoutBusinessMutation() throws Exception {
        UUID resource = fixture(10);
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            try (var sql = connection.prepareStatement("INSERT INTO idempotency_record(caller_id,operation,idempotency_key,request_hash) VALUES (?,'v1.reservation.create','timeout',?)")) {
                sql.setObject(1, ALICE); sql.setString(2, "0".repeat(64)); sql.executeUpdate();
            }
            long start = System.nanoTime();
            error(hold(resource, 1, "timeout"), 503, "TEMPORARILY_UNAVAILABLE");
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isBetween(Duration.ofSeconds(4), Duration.ofSeconds(12));
            capacity(resource, 10, 0, 0);
            assertThat(count("idempotency_record")).isZero();
            connection.rollback();
        }
        assertThat(hold(resource, 1, "timeout").statusCode()).isEqualTo(201);
        capacity(resource, 9, 1, 1);
    }

    @Test
    void lostResponseAfterCommitReplaysWithoutAnotherAllocation() throws Exception {
        UUID resource = fixture(10);
        String body = holdBody(resource, 2);
        // Never read the HTTP response; a separate PostgreSQL connection observes the committed result.
        try (Socket socket = new Socket("127.0.0.1", port)) {
            String wire = "POST /api/reservations HTTP/1.1\r\nHost: 127.0.0.1:" + port
                    + "\r\nAuthorization: " + basic("alice") + "\r\nContent-Type: application/json"
                    + "\r\nIdempotency-Key: lost\r\nContent-Length: " + body.getBytes(StandardCharsets.UTF_8).length
                    + "\r\nConnection: close\r\n\r\n" + body;
            socket.getOutputStream().write(wire.getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            await(() -> count("idempotency_record WHERE idempotency_key='lost' AND completed_at IS NOT NULL") == 1);
        }
        var retry = hold(resource, 2, "lost");
        assertThat(retry.statusCode()).isEqualTo(201);
        assertThat(retry.headers().firstValue("Idempotency-Replayed")).contains("true");
        var stored = jdbc.queryForMap("SELECT * FROM idempotency_record WHERE idempotency_key='lost'");
        assertThat(retry.body()).isEqualTo(stored.get("response_body"));
        assertThat(retry.headers().firstValue("Location")).contains((String) stored.get("response_location"));
        capacity(resource, 8, 1, 1);
        System.out.println("PHASE8 lost-response observedCommit=true responseRead=false replay=201 reservations=1 decrements=1");
    }

    @Test
    void restartApplicationContextRetainsReplay() {
        UUID resource = fixture(10);
        var request = new CreateReservationRequest(resource, 2);
        StoredResponse original;
        try (ConfigurableApplicationContext before = application()) {
            original = before.getBean(IdempotentMutations.class).hold(ALICE, "restart", "before", request);
            assertThat(original.status()).isEqualTo(201);
            assertThat(original.replayed()).isFalse();
        }
        try (ConfigurableApplicationContext after = application()) {
            var replayed = after.getBean(IdempotentMutations.class).hold(ALICE, "restart", "after", request);
            assertThat(replayed.replayed()).isTrue();
            assertThat(replayed.body()).isEqualTo(original.body());
            assertThat(replayed.status()).isEqualTo(original.status());
            assertThat(replayed.location()).isEqualTo(original.location());
        }
        capacity(resource, 8, 1, 1);
        assertThat(count("idempotency_record")).isEqualTo(1);
        System.out.println("PHASE8 context-restart original=201 replay=201 reservations=1 decrements=1");
    }

    @Test
    void incompleteCommittedClaimFailsClosedAndHealthIsDown() throws Exception {
        UUID resource = fixture(10);
        jdbc.update("INSERT INTO idempotency_record(caller_id,operation,idempotency_key,request_hash) VALUES (?,'v1.reservation.create','corrupt',?)", ALICE, "0".repeat(64));
        try {
            error(hold(resource, 1, "corrupt"), 503, "TEMPORARILY_UNAVAILABLE");
            var health = client.send(HttpRequest.newBuilder(URI.create(base() + "/actuator/health")).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(health.statusCode()).isEqualTo(503);
            assertThat(json(health)).containsEntry("status", "DOWN").hasSize(1);
            capacity(resource, 10, 0, 0);
        } finally {
            // Deliberately corrupt fixture only. Production has no deletion path.
            jdbc.update("DELETE FROM idempotency_record WHERE idempotency_key='corrupt'");
        }
    }

    @Test
    void storedDomainRejectionRollsBackItsSavepointBeforeCommittingResponse() {
        UUID resource = fixture(10);
        var result = executor.execute(ALICE, "v1.reservation.create", "savepoint", List.of("savepoint-test"), null, "original-request", () -> {
            // Test-only future business rejection after a write: the outer claim must survive, the write must not.
            jdbc.update("UPDATE resource SET available_capacity=8,version=1 WHERE id=?", resource);
            throw new ApiException(HttpStatus.CONFLICT, "INSUFFICIENT_CAPACITY", "Test domain rejection");
        });
        assertThat(result.status()).isEqualTo(409);
        assertThat(result.replayed()).isFalse();
        capacity(resource, 10, 0, 0);
        assertThat(count("idempotency_record WHERE completed_at IS NOT NULL")).isEqualTo(1);
        var replayed = executor.execute(ALICE, "v1.reservation.create", "savepoint", List.of("savepoint-test"), null, "retry-request", () -> {
            throw new AssertionError("Replay must never invoke mutation");
        });
        assertThat(replayed.replayed()).isTrue();
        assertThat(replayed.body()).isEqualTo(result.body());
    }

    private ConfigurableApplicationContext application() {
        return new SpringApplicationBuilder(AtlasApplication.class).profiles("test").run("--server.port=0",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(), "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(), "--atlas.expiration.enabled=false");
    }
    private UUID fixture(int capacity) {
        return jdbc.queryForObject("INSERT INTO resource(name,total_capacity,available_capacity) VALUES ('Idempotency fixture',?,?) RETURNING id", UUID.class, capacity, capacity);
    }
    private String base() { return "http://127.0.0.1:" + port; }
    private String holdBody(UUID resource, int quantity) { return "{\"resourceId\":\"" + resource + "\",\"quantity\":" + quantity + "}"; }
    private String basic(String user) { return "Basic " + Base64.getEncoder().encodeToString((user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8)); }
    private HttpRequest.Builder request(String path, String user, String key, String body) {
        var request = HttpRequest.newBuilder(URI.create(base() + path)).timeout(Duration.ofSeconds(25));
        if (user != null) request.header("Authorization", basic(user));
        if (key != null) request.header("Idempotency-Key", key);
        if (body != null) request.header("Content-Type", "application/json");
        return request.POST(body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
    }
    private HttpResponse<String> post(String path, String user, String key, String body) throws Exception {
        return client.send(request(path, user, key, body).build(), HttpResponse.BodyHandlers.ofString());
    }
    private HttpResponse<String> hold(UUID resource, int quantity, String key) throws Exception { return post("/api/reservations", "alice", key, holdBody(resource, quantity)); }
    private HttpResponse<String> action(UUID id, String operation, String key) throws Exception { return post("/api/reservations/" + id + "/" + operation, "alice", key, null); }
    private Map<String, Object> json(HttpResponse<String> response) { return JsonPath.read(response.body(), "$"); }
    private UUID id(HttpResponse<String> response) { return UUID.fromString((String) json(response).get("id")); }
    private int count(String table) { return jdbc.queryForObject("SELECT count(*) FROM " + table, Integer.class); }
    private void error(HttpResponse<String> response, int status, String code) {
        assertThat(response.statusCode()).as(response.body()).isEqualTo(status);
        assertThat(json(response)).containsEntry("code", code);
        assertThat(response.body()).doesNotContain(PASSWORD, "SQLException", "SELECT ", "INSERT ");
    }
    private void replay(HttpResponse<String> original, HttpResponse<String> replayed) {
        assertThat(replayed.statusCode()).isEqualTo(original.statusCode());
        assertThat(replayed.body()).isEqualTo(original.body());
        assertThat(replayed.headers().firstValue("Content-Type")).isEqualTo(original.headers().firstValue("Content-Type"));
        assertThat(replayed.headers().firstValue("Location")).isEqualTo(original.headers().firstValue("Location"));
        assertThat(replayed.headers().firstValue("Idempotency-Replayed")).contains("true");
        assertThat(replayed.headers().firstValue("X-Request-Id")).isNotEqualTo(original.headers().firstValue("X-Request-Id"));
    }
    private void capacity(UUID resource, int available, long version, int rows) {
        var row = jdbc.queryForMap("SELECT available_capacity,version FROM resource WHERE id=?", resource);
        assertThat(row.get("available_capacity")).isEqualTo(available);
        assertThat(row.get("version")).isEqualTo(version);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM reservation WHERE resource_id=?", Integer.class, resource)).isEqualTo(rows);
        invariant();
    }
    private void invariant() {
        assertThat(jdbc.queryForObject("""
                SELECT count(*) FROM resource s WHERE s.available_capacity + coalesce(
                  (SELECT sum(r.quantity) FROM reservation r WHERE r.resource_id=s.id AND r.status IN ('HELD','CONFIRMED')),0) <> s.total_capacity
                """, Integer.class)).isZero();
    }
    private void due(UUID id) {
        jdbc.update("UPDATE reservation SET created_at=clock_timestamp()-interval '2 minutes', expires_at=clock_timestamp()-interval '1 minute' WHERE id=?", id);
    }
    private void trigger(String table, String body, String event) {
        jdbc.execute("CREATE FUNCTION test_idempotency_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN " + body + " END $$");
        jdbc.execute("CREATE TRIGGER test_idempotency_fault BEFORE " + event + " ON " + table + " FOR EACH ROW EXECUTE FUNCTION test_idempotency_fault()");
    }
    private Connection connection() throws Exception { return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()); }
    private Connection gate() throws Exception {
        var connection = connection();
        try (var statement = connection.createStatement()) { statement.execute("SELECT pg_advisory_lock(" + GATE + ")"); }
        return connection;
    }
    private void unlock(Connection gate) throws Exception {
        try (var statement = gate.createStatement()) { statement.execute("SELECT pg_advisory_unlock(" + GATE + ")"); }
    }
    private int advisoryWaiters() {
        return jdbc.queryForObject("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory'", Integer.class);
    }
    private int claimWaiters() {
        return jdbc.queryForObject("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%INSERT INTO idempotency_record%'", Integer.class);
    }
    private void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(4).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("Timed out waiting for observed PostgreSQL condition");
            Thread.onSpinWait();
        }
    }
}
