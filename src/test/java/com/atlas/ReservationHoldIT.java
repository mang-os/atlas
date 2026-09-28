package com.atlas;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import com.atlas.configuration.LocalIdentityConfiguration;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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

/** Real HTTP transactions; fixture SQL and faults affect only this class's disposable PostgreSQL database. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"local", "test"})
@Testcontainers(disabledWithoutDocker = false)
@org.springframework.test.context.TestExecutionListeners(listeners=RedisOutageEvidence.class,
        mergeMode=org.springframework.test.context.TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
class ReservationHoldIT {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_hold_test").withUsername("atlas_test").withPassword("testcontainers-only")
            .withStartupTimeout(Duration.ofSeconds(90));
    static final String ALICE_PASSWORD = UUID.randomUUID().toString();
    static final String BOB_PASSWORD = UUID.randomUUID().toString();
    static final String ADMIN_PASSWORD = UUID.randomUUID().toString();
    static final int TTL_SECONDS = 17;
    @LocalServerPort int port;
    @Autowired JdbcTemplate jdbc;
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("atlas.local.alice-password", () -> ALICE_PASSWORD);
        registry.add("atlas.local.bob-password", () -> BOB_PASSWORD);
        registry.add("atlas.local.admin-password", () -> ADMIN_PASSWORD);
        registry.add("atlas.hold.ttl-seconds", () -> TTL_SECONDS);
    }

    @BeforeEach
    void cleanOwnFixtures() {
        jdbc.execute("DROP TRIGGER IF EXISTS test_fail_insert ON reservation");
        jdbc.execute("DROP FUNCTION IF EXISTS test_fail_insert()");
        jdbc.execute("DROP SEQUENCE IF EXISTS test_insert_observed");
        jdbc.update("DELETE FROM outbox_event");
        jdbc.update("DELETE FROM idempotency_record");
        jdbc.update("DELETE FROM reservation");
        jdbc.update("DELETE FROM resource");
    }

    @Test
    void allocatesInOneTransactionAndUsesAuthenticatedOwnerAndDatabaseTime() throws Exception {
        String resource = resource(10);
        Instant dbBefore = databaseTime();
        var response = create("alice", resource, 3);
        Instant dbAfter = databaseTime();
        assertThat(response.statusCode()).isEqualTo(201);
        var body = json(response);
        assertThat(body.keySet()).containsExactlyInAnyOrder("id", "userId", "resourceId", "quantity", "status", "expiresAt",
                "confirmedAt", "cancelledAt", "expiredAt", "createdAt", "updatedAt");
        UUID id = UUID.fromString((String) body.get("id"));
        assertThat(body).containsEntry("resourceId", resource).containsEntry("userId", LocalIdentityConfiguration.ALICE_ID.toString())
                .containsEntry("quantity", 3).containsEntry("status", "HELD")
                .containsEntry("confirmedAt", null).containsEntry("cancelledAt", null).containsEntry("expiredAt", null);
        assertThat(response.headers().firstValue("Location")).contains("/api/reservations/" + id);
        Instant created = Instant.parse((String) body.get("createdAt"));
        Instant expires = Instant.parse((String) body.get("expiresAt"));
        assertThat(created).isBetween(dbBefore, dbAfter);
        assertThat(Duration.between(created, expires)).isEqualTo(Duration.ofSeconds(TTL_SECONDS));
        assertThat(body.get("updatedAt")).isEqualTo(body.get("createdAt"));
        assertThat(jdbc.queryForObject("SELECT user_id FROM reservation WHERE id=?", UUID.class, id)).isEqualTo(LocalIdentityConfiguration.ALICE_ID);
        assertCapacity(resource, 7, 1, 1);
        var read = send("GET", "/api/reservations/" + id, "alice", null);
        assertThat(read.statusCode()).isEqualTo(200);
        assertThat(json(read)).containsAllEntriesOf(body).containsEntry("cached",false);
        assertThat(Instant.parse((String)json(read).get("observedAt"))).isAfterOrEqualTo(created);
        record("create-hold", response);
        record("owned-read", read);
        record("owned-list", send("GET", userPath("alice"), "alice", null));
    }

    @Test
    void fillsExactlyThenRejectsInsufficientCapacityWithoutMutation() throws Exception {
        String resource = resource(2);
        assertThat(create("alice", resource, 2).statusCode()).isEqualTo(201);
        var before = resourceJson(resource);
        var failed = create("bob", resource, 1);
        error(failed, 409, "INSUFFICIENT_CAPACITY");
        assertThat(resourceJson(resource)).isEqualTo(before);
        assertCapacity(resource, 0, 1, 1);
        record("capacity-conflict", failed);
    }

    @Test
    void rejectsQuantityAboveAvailableOrTotalAndDistinguishesUnknownResource() throws Exception {
        String resource = resource(10);
        error(create("alice", resource, 11), 409, "INSUFFICIENT_CAPACITY");
        assertCapacity(resource, 10, 0, 0);
        assertThat(create("alice", resource, 7).statusCode()).isEqualTo(201);
        error(create("bob", resource, 4), 409, "INSUFFICIENT_CAPACITY");
        error(create("alice", UUID.randomUUID().toString(), 1), 404, "RESOURCE_NOT_FOUND");
        assertCapacity(resource, 3, 1, 1);
    }

    static Stream<String> invalidRequests() {
        String resource = "11111111-1111-4111-8111-111111111111";
        String prefix = "{\"resourceId\":\"" + resource + "\",";
        return Stream.of("{}", "null", "[]", "", "{", "{\"quantity\":1}",
                "{\"resourceId\":null,\"quantity\":1}", "{\"resourceId\":\"bad\",\"quantity\":1}",
                "{\"resourceId\":\"1-1-1-1-1\",\"quantity\":1}", "{\"resourceId\":1,\"quantity\":1}",
                prefix + "\"quantity\":null}", prefix + "\"quantity\":0}", prefix + "\"quantity\":-1}",
                prefix + "\"quantity\":1.5}", prefix + "\"quantity\":1.0}", prefix + "\"quantity\":2147483648}",
                prefix + "\"quantity\":\"1\"}", prefix + "\"quantity\":true}",
                prefix + "\"quantity\":1,\"userId\":\"" + LocalIdentityConfiguration.BOB_ID + "\"}",
                prefix + "\"quantity\":1,\"expiresAt\":\"2030-01-01T00:00:00Z\"}",
                prefix + "\"quantity\":1,\"status\":\"CONFIRMED\"}", prefix + "\"quantity\":1,\"id\":\"" + resource + "\"}",
                prefix + "\"quantity\":1,\"quantity\":2}", prefix + "\"quantity\":1} {}");
    }

    @ParameterizedTest
    @MethodSource("invalidRequests")
    void invalidRequestsHaveNoAllocationOrReservation(String body) throws Exception {
        String resource = resource(10);
        var before = resourceJson(resource);
        error(send("POST", "/api/reservations", "alice", body), 400, "INVALID_REQUEST");
        assertThat(resourceJson(resource)).isEqualTo(before);
        assertCapacity(resource, 10, 0, 0);
    }

    @Test
    void supportsMaxIntegerQuantityWithoutOverflow() throws Exception {
        String resource = resource(Integer.MAX_VALUE);
        assertThat(create("alice", resource, Integer.MAX_VALUE).statusCode()).isEqualTo(201);
        assertCapacity(resource, 0, 1, 1);
        error(create("bob", resource, 1), 409, "INSUFFICIENT_CAPACITY");
        assertCapacity(resource, 0, 1, 1);
    }

    @Test
    void ownerReadsOnlyTheirReservationAndAdminHasNoBypass() throws Exception {
        String resource = resource(10);
        String id = (String) json(create("alice", resource, 1)).get("id");
        for (String other : List.of("bob", "admin")) {
            error(send("GET", "/api/reservations/" + id, other, null), 404, "RESERVATION_NOT_FOUND");
        }
        error(send("GET", "/api/reservations/" + UUID.randomUUID(), "alice", null), 404, "RESERVATION_NOT_FOUND");
        var forged = client.send(request("GET", "/api/reservations/" + id, "bob", null)
                .header("X-User-Id", LocalIdentityConfiguration.ALICE_ID.toString()).header("X-Role", "ADMIN").build(), HttpResponse.BodyHandlers.ofString());
        error(forged, 404, "RESERVATION_NOT_FOUND");
        assertCapacity(resource, 9, 1, 1);
    }

    @Test
    void callerIdComesFromCredentialsEvenWithForgedHeadersAndAdminCanHoldForSelf() throws Exception {
        String resource = resource(10);
        var forged = client.send(request("POST", "/api/reservations", "alice", body(resource, 1))
                .header("X-User-Id", LocalIdentityConfiguration.BOB_ID.toString()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(forged.statusCode()).isEqualTo(201);
        assertThat(json(forged)).containsEntry("userId", LocalIdentityConfiguration.ALICE_ID.toString());
        var admin = create("admin", resource, 1);
        assertThat(admin.statusCode()).isEqualTo(201);
        assertThat(json(admin)).containsEntry("userId", LocalIdentityConfiguration.ADMIN_ID.toString());
        String adminId = (String) json(admin).get("id");
        assertThat(send("GET", "/api/reservations/" + adminId, "admin", null).statusCode()).isEqualTo(200);
        assertCapacity(resource, 8, 2, 2);
    }

    @Test
    void listingsAreOwnerScopedAndCrossUserPathsForbidden() throws Exception {
        String resource = resource(10);
        create("alice", resource, 1);
        create("bob", resource, 2);
        for (String user : List.of("alice", "bob", "admin")) {
            var response = send("GET", userPath(user), user, null);
            assertThat(response.statusCode()).isEqualTo(200);
            var result = json(response);
            assertThat(result.keySet()).containsExactlyInAnyOrder("items", "nextCursor");
            assertThat(result.get("nextCursor")).isNull();
            var items = items(response);
            assertThat(items).hasSize(user.equals("admin") ? 0 : 1);
            assertThat(items).allSatisfy(item -> assertThat(item.get("userId")).isEqualTo(owner(user).toString()));
        }
        error(send("GET", userPath("alice"), "bob", null), 403, "FORBIDDEN");
        error(send("GET", userPath("alice"), "admin", null), 403, "FORBIDDEN");
        error(send("GET", "/api/users/" + UUID.randomUUID() + "/reservations", "alice", null), 403, "FORBIDDEN");
        assertCapacity(resource, 7, 2, 2);
    }

    @Test
    void cursorPaginationUsesTimestampAndIdTieBreakerAndCannotLeakOtherOwners() throws Exception {
        String resource = resource(10);
        for (int index = 0; index < 3; index++) { create("alice", resource, 1); }
        create("bob", resource, 1);
        jdbc.update("UPDATE reservation SET created_at='2026-01-01T00:00:00Z',expires_at='2026-01-01T00:00:17Z'");
        var expected = jdbc.queryForList("SELECT id::text FROM reservation WHERE user_id=? ORDER BY created_at,id", String.class, owner("alice"));
        var first = send("GET", userPath("alice") + "?limit=2", "alice", null);
        assertThat(items(first)).extracting(row -> row.get("id")).containsExactlyElementsOf(expected.subList(0, 2));
        String cursor = (String) json(first).get("nextCursor");
        assertThat(cursor).isNotBlank();
        var next = send("GET", userPath("alice") + "?limit=2&cursor=" + cursor, "alice", null);
        assertThat(items(next)).extracting(row -> row.get("id")).containsExactly(expected.getLast());
        assertThat(json(next).get("nextCursor")).isNull();
        var bob = send("GET", userPath("bob") + "?limit=2&cursor=" + cursor, "bob", null);
        assertThat(items(bob)).allSatisfy(row -> assertThat(row.get("userId")).isEqualTo(owner("bob").toString()));
        assertCapacity(resource, 6, 4, 4);
    }

    @Test
    void defaultTwentyAndMaximumHundredPageSizes() throws Exception {
        String resource = resource(30);
        for (int index = 0; index < 21; index++) { create("alice", resource, 1); }
        var first = send("GET", userPath("alice"), "alice", null);
        assertThat(items(first)).hasSize(20);
        assertThat(json(first).get("nextCursor")).isInstanceOf(String.class);
        var next = send("GET", userPath("alice") + "?cursor=" + json(first).get("nextCursor"), "alice", null);
        assertThat(items(next)).hasSize(1);
        assertThat(json(next).get("nextCursor")).isNull();
        assertThat(items(send("GET", userPath("alice") + "?limit=100", "alice", null))).hasSize(21);
        assertCapacity(resource, 9, 21, 21);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "101", "2147483648", "abc", "1.5", ""})
    void invalidListLimitsReturn400(String limit) throws Exception {
        error(send("GET", userPath("alice") + "?limit=" + limit, "alice", null), 400, "INVALID_REQUEST");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "not-base64!", "YWJj", "a"})
    void invalidCursorReturns400(String cursor) throws Exception {
        error(send("GET", userPath("alice") + "?cursor=" + cursor, "alice", null), 400, "INVALID_REQUEST");
    }

    @ParameterizedTest
    @ValueSource(strings = {"bad|11111111-1111-4111-8111-111111111111", "2026-01-01T00:00:00Z|bad",
            "2026-01-01T00:00:00.000000001Z|11111111-1111-4111-8111-111111111111", "+10000-01-01T00:00:00Z|11111111-1111-4111-8111-111111111111"})
    void invalidCursorPayloadCannotCauseSqlErrors(String payload) throws Exception {
        String cursor = Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        error(send("GET", userPath("alice") + "?cursor=" + cursor, "alice", null), 400, "INVALID_REQUEST");
    }

    @ParameterizedTest
    @ValueSource(strings = {"/api/reservations/bad", "/api/reservations/1-1-1-1-1", "/api/users/bad/reservations"})
    void invalidPathIdsReturn400(String path) throws Exception {
        error(send("GET", path, "alice", null), 400, "INVALID_REQUEST");
    }

    @Test
    void unauthenticatedRequestsCannotCreateReadOrList() throws Exception {
        String resource = resource(10);
        error(send("POST", "/api/reservations", null, body(resource, 1)), 401, "AUTHENTICATION_REQUIRED");
        error(send("GET", "/api/reservations/" + UUID.randomUUID(), null, null), 401, "AUTHENTICATION_REQUIRED");
        error(send("GET", userPath("alice"), null, null), 401, "AUTHENTICATION_REQUIRED");
        assertCapacity(resource, 10, 0, 0);
    }

    @Test
    void insertFaultAfterDecrementRollsBackCapacityVersionAndTimestamp() throws Exception {
        String resource = resource(10);
        var before = resourceJson(resource);
        jdbc.execute("CREATE SEQUENCE test_insert_observed");
        jdbc.execute("""
                CREATE FUNCTION test_fail_insert() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM resource WHERE id=NEW.resource_id AND available_capacity=7 AND version=1) THEN
                        PERFORM nextval('test_insert_observed');
                    END IF;
                    RAISE EXCEPTION 'test-only injected insert failure' USING ERRCODE='23514';
                END $$
                """);
        jdbc.execute("CREATE TRIGGER test_fail_insert BEFORE INSERT ON reservation FOR EACH ROW EXECUTE FUNCTION test_fail_insert()");
        try {
            var failed = create("alice", resource, 3);
            error(failed, 503, "TEMPORARILY_UNAVAILABLE");
            // Sequence advancement is nontransactional and proves the fault saw the already-decremented row.
            assertThat(jdbc.queryForObject("SELECT is_called FROM test_insert_observed", Boolean.class)).isTrue();
            assertThat(resourceJson(resource)).isEqualTo(before);
            assertCapacity(resource, 10, 0, 0);
            record("rollback-error", failed);
        } finally {
            jdbc.execute("DROP TRIGGER test_fail_insert ON reservation");
            jdbc.execute("DROP FUNCTION test_fail_insert()");
            jdbc.execute("DROP SEQUENCE test_insert_observed");
        }
        assertThat(create("alice", resource, 3).statusCode()).isEqualTo(201);
        assertCapacity(resource, 7, 1, 1);
    }

    @Test
    void decrementFailureNeverInsertsReservationOrPartiallyUpdatesResource() throws Exception {
        String resource = resource(10);
        jdbc.update("UPDATE resource SET version=9223372036854775807 WHERE id=?", UUID.fromString(resource));
        var before = resourceJson(resource);
        error(create("alice", resource, 1), 503, "TEMPORARILY_UNAVAILABLE");
        assertThat(resourceJson(resource)).isEqualTo(before);
        assertCapacity(resource, 10, Long.MAX_VALUE, 0);
    }

    @Test
    void retriesWithSameHeaderReplayOneAllocation() throws Exception {
        String resource = resource(10);
        var request = request("POST", "/api/reservations", "alice", body(resource, 2)).setHeader("Idempotency-Key", "hold-retry").build();
        var first = client.send(request, HttpResponse.BodyHandlers.ofString());
        var second = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(first.statusCode()).isEqualTo(201);
        assertThat(second.statusCode()).isEqualTo(201);
        assertThat(second.body()).isEqualTo(first.body());
        assertThat(second.headers().firstValue("Idempotency-Replayed")).contains("true");
        assertCapacity(resource, 8, 1, 1);
    }

    @Test
    void overdueHeldRemainsAllocatedUntilFutureExpirationWorkerExists() throws Exception {
        String resource = resource(10);
        String id = (String) json(create("alice", resource, 2)).get("id");
        jdbc.update("UPDATE reservation SET created_at=clock_timestamp()-interval '1 hour',expires_at=clock_timestamp()-interval '30 minutes' WHERE id=?", UUID.fromString(id));
        var read = send("GET", "/api/reservations/" + id, "alice", null);
        assertThat(json(read)).containsEntry("status", "HELD");
        assertThat(Instant.parse((String) json(read).get("expiresAt"))).isBefore(databaseTime());
        assertThat(items(send("GET", userPath("alice"), "alice", null))).hasSize(1);
        assertCapacity(resource, 8, 1, 1);
    }

    @Test
    void lifecycleEndpointsRejectUnexpectedBodiesWithoutChangingHold() throws Exception {
        String resource = resource(10);
        String id = (String) json(create("alice", resource, 1)).get("id");
        for (String operation : List.of("confirm", "cancel")) {
            error(send("POST", "/api/reservations/" + id + "/" + operation, "alice", "{}"), 400, "INVALID_REQUEST");
        }
        assertCapacity(resource, 9, 1, 1);
    }

    private void assertCapacity(String resource, int available, long version, int rows) {
        UUID id = UUID.fromString(resource);
        assertThat(jdbc.queryForObject("SELECT available_capacity FROM resource WHERE id=?", Integer.class, id)).isEqualTo(available);
        assertThat(jdbc.queryForObject("SELECT version FROM resource WHERE id=?", Long.class, id)).isEqualTo(version);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM reservation WHERE resource_id=?", Integer.class, id)).isEqualTo(rows);
        // Single consistent statement; HELD counts even if its deadline has passed.
        assertThat(jdbc.queryForObject("""
                SELECT s.available_capacity + coalesce((SELECT sum(r.quantity) FROM reservation r
                    WHERE r.resource_id=s.id AND r.status IN ('HELD','CONFIRMED')),0) - s.total_capacity
                FROM resource s WHERE s.id=?
                """, Long.class, id)).isZero();
    }

    private String resource(int capacity) throws Exception {
        var created = send("POST", "/api/resources", "admin", "{\"name\":\"Hold resource\",\"totalCapacity\":" + capacity + "}");
        assertThat(created.statusCode()).isEqualTo(201);
        return (String) json(created).get("id");
    }

    private Map<String, Object> resourceJson(String id) throws Exception { return json(send("GET", "/api/resources/" + id, "alice", null)); }
    private String body(String resource, int quantity) { return "{\"resourceId\":\"" + resource + "\",\"quantity\":" + quantity + "}"; }
    private HttpResponse<String> create(String user, String resource, int quantity) throws Exception { return send("POST", "/api/reservations", user, body(resource, quantity)); }
    private UUID owner(String user) { return switch (user) { case "alice" -> LocalIdentityConfiguration.ALICE_ID; case "bob" -> LocalIdentityConfiguration.BOB_ID; default -> LocalIdentityConfiguration.ADMIN_ID; }; }
    private String userPath(String user) { return "/api/users/" + owner(user) + "/reservations"; }
    private Map<String, Object> json(HttpResponse<String> response) { return JsonPath.read(response.body(), "$"); }
    private List<Map<String, Object>> items(HttpResponse<String> response) { return JsonPath.read(response.body(), "$.items"); }
    private Instant databaseTime() { return jdbc.queryForObject("SELECT clock_timestamp()", java.time.OffsetDateTime.class).toInstant(); }

    private HttpResponse<String> send(String method, String path, String user, String body) throws Exception {
        var response = client.send(request(method, path, user, body).build(), HttpResponse.BodyHandlers.ofString());
        UUID.fromString(response.headers().firstValue("X-Request-Id").orElseThrow());
        return response;
    }

    private HttpRequest.Builder request(String method, String path, String user, String body) {
        var builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).timeout(Duration.ofSeconds(10));
        if (user != null) {
            String password = switch (user) { case "alice" -> ALICE_PASSWORD; case "bob" -> BOB_PASSWORD; default -> ADMIN_PASSWORD; };
            builder.header("Authorization", "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8)));
        }
        if (body != null) { builder.header("Content-Type", "application/json"); }
        if (method.equals("POST")) builder.setHeader("Idempotency-Key", UUID.randomUUID().toString());
        return builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
    }

    private void error(HttpResponse<String> response, int status, String code) {
        assertThat(response.statusCode()).isEqualTo(status);
        var error = json(response);
        assertThat(error.keySet()).containsExactlyInAnyOrder("code", "message", "requestId", "details");
        assertThat(error).containsEntry("code", code).containsEntry("requestId", response.headers().firstValue("X-Request-Id").orElseThrow());
        assertThat(response.body()).doesNotContain(ALICE_PASSWORD, BOB_PASSWORD, ADMIN_PASSWORD, "INSERT INTO", "UPDATE resource", "SQLException", "test-only injected", "stackTrace");
    }

    private void record(String name, HttpResponse<String> response) throws Exception {
        var folder = java.nio.file.Path.of("target", "phase4-http");
        java.nio.file.Files.createDirectories(folder);
        java.nio.file.Files.writeString(folder.resolve(name + ".txt"), "HTTP " + response.statusCode() + "\nX-Request-Id: "
                + response.headers().firstValue("X-Request-Id").orElseThrow() + "\nLocation: " + response.headers().firstValue("Location").orElse("")
                + "\n\n" + response.body() + "\n");
    }
}
