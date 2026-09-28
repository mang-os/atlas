package com.atlas;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.atlas.configuration.LocalIdentityConfiguration;
import com.atlas.idempotency.IdempotentMutations;
import com.atlas.outbox.OutboxWriter;
import com.atlas.reservation.ReservationRepository;
import org.springframework.transaction.IllegalTransactionStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
class OutboxIT {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_outbox_test").withUsername("atlas_test").withPassword("testcontainers-only")
            .withStartupTimeout(Duration.ofSeconds(90));
    static final String PASSWORD = UUID.randomUUID().toString();
    static final UUID ALICE = LocalIdentityConfiguration.ALICE_ID;
    @Autowired JdbcTemplate jdbc;
    @Autowired OutboxWriter outbox;
    @Autowired ReservationRepository reservations;
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
        for (String table : List.of("resource", "reservation", "idempotency_record", "outbox_event"))
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
    @ValueSource(strings = {"confirm", "cancel", "expire"})
    void emitsVersionedEnvelopeAndExactlyOneEventPerTransition(String operation) throws Exception {
        UUID resource = fixture(10);
        var held = hold(resource, 2, "hold");
        UUID id = id(held);
        var created = events(id).getFirst();
        envelope(created, id, resource, "HELD", 1, 0);
        Map<String,Object> heldPayload = JsonPath.read(created.get("payload").toString(), "$");
        assertThat(heldPayload.get("correlationId")).isEqualTo(held.headers().firstValue("X-Request-Id").orElseThrow());
        replay(held, hold(resource, 2, "hold"));
        HttpResponse<String> terminal = null;
        if (operation.equals("expire")) {
            due(id);
            assertThat(expiration.expire(id).expired()).isTrue();
            assertThat(expiration.expire(id).expired()).isFalse();
        } else {
            terminal = action(id, operation, "terminal");
            assertThat(terminal.statusCode()).isEqualTo(200);
            replay(terminal, action(id, operation, "terminal"));
            assertThat(action(id, operation, "new-key-same-terminal").statusCode()).isEqualTo(200);
        }
        replay(held, hold(resource, 2, "hold"));
        var events = events(id);
        assertThat(events).hasSize(2);
        assertThat(events.getFirst()).isEqualTo(created);
        assertThat(events.get(1).get("event_id")).isNotEqualTo(created.get("event_id"));
        String state = operation.equals("confirm") ? "CONFIRMED" : operation.equals("cancel") ? "CANCELLED" : "EXPIRED";
        envelope(events.get(1), id, resource, state, 2, operation.equals("confirm") ? 0 : 2);
        if (terminal != null) {
            Map<String,Object> payload = JsonPath.read(events.get(1).get("payload").toString(), "$");
            assertThat(payload.get("correlationId")).isEqualTo(terminal.headers().firstValue("X-Request-Id").orElseThrow());
        }
        capacity(resource, operation.equals("confirm") ? 8 : 10, operation.equals("confirm") ? 1 : 2, 1);
        eventInvariant();
        System.out.println("PHASE9 transition=" + state + " events=2 sequences=1,2 released=" + (operation.equals("confirm") ? 0 : 2) + " replayExtraEvents=0");
    }

    @ParameterizedTest
    @ValueSource(strings = {"create", "confirm", "cancel", "expire"})
    void failedOutboxInsertRollsBackBusinessStateCapacityAndIdempotency(String operation) throws Exception {
        UUID resource = fixture(10);
        UUID id = operation.equals("create") ? null : id(hold(resource, 2, "fixture"));
        if (operation.equals("expire")) due(id);
        var beforeResource = resourceRow(resource);
        var beforeReservation = id == null ? null : reservationRow(id);
        var beforeEvents = jdbc.queryForList("SELECT * FROM outbox_event ORDER BY event_id");
        int beforeKeys = count("idempotency_record");
        jdbc.execute("CREATE SEQUENCE test_idempotency_seen");
        trigger("outbox_event", """
                IF EXISTS (SELECT 1 FROM reservation r JOIN resource s ON s.id=r.resource_id
                    WHERE r.id=NEW.aggregate_id AND r.transition_sequence=NEW.aggregate_sequence
                    AND r.status=NEW.payload->'data'->>'status'
                    AND s.available_capacity=CASE WHEN r.status IN ('CANCELLED','EXPIRED') THEN 10 ELSE 8 END) THEN
                    PERFORM nextval('test_idempotency_seen');
                END IF;
                RAISE EXCEPTION 'injected outbox insertion failure';
                """, "INSERT");
        if (operation.equals("expire")) assertThatThrownBy(() -> expiration.expire(id)).isInstanceOf(org.springframework.dao.DataAccessException.class);
        else error(operation.equals("create") ? hold(resource, 2, "fault") : action(id, operation, "fault"), 503, "TEMPORARILY_UNAVAILABLE");
        assertThat(jdbc.queryForObject("SELECT is_called FROM test_idempotency_seen", Boolean.class)).isTrue();
        assertThat(resourceRow(resource)).isEqualTo(beforeResource);
        if (id != null) assertThat(reservationRow(id)).isEqualTo(beforeReservation);
        else assertThat(count("reservation")).isZero();
        assertThat(jdbc.queryForList("SELECT * FROM outbox_event ORDER BY event_id")).isEqualTo(beforeEvents);
        assertThat(count("idempotency_record")).isEqualTo(beforeKeys);
        jdbc.execute("DROP TRIGGER test_idempotency_fault ON outbox_event");
        if (operation.equals("expire")) assertThat(expiration.expire(id).expired()).isTrue();
        else assertThat((operation.equals("create") ? hold(resource, 2, "fault") : action(id, operation, "fault")).statusCode()).isEqualTo(operation.equals("create") ? 201 : 200);
        eventInvariant();
        System.out.println("PHASE9 outbox-failure operation=" + operation + " observedBusinessWrite=true atomicRollback=PASS retry=PASS");
    }

    @Test
    void responseStorageFailureAfterEventInsertRollsBackBoth() throws Exception {
        UUID resource = fixture(10);
        jdbc.execute("CREATE SEQUENCE test_idempotency_seen");
        trigger("idempotency_record", """
                IF EXISTS (SELECT 1 FROM outbox_event WHERE aggregate_id=NEW.reservation_id) THEN
                    PERFORM nextval('test_idempotency_seen');
                END IF;
                RAISE EXCEPTION 'injected response failure after outbox';
                """, "UPDATE");
        error(hold(resource, 2, "response"), 503, "TEMPORARILY_UNAVAILABLE");
        assertThat(jdbc.queryForObject("SELECT is_called FROM test_idempotency_seen", Boolean.class)).isTrue();
        capacity(resource, 10, 0, 0);
        assertThat(count("outbox_event")).isZero();
        assertThat(count("idempotency_record")).isZero();
        jdbc.execute("DROP TRIGGER test_idempotency_fault ON idempotency_record");
        assertThat(hold(resource, 2, "response").statusCode()).isEqualTo(201);
        assertThat(count("outbox_event")).isEqualTo(1);
    }

    @Test
    void businessRejectionsAndInsertionFailureCreateNoEvents() throws Exception {
        UUID resource = fixture(1);
        error(hold(resource, 2, "conflict"), 409, "INSUFFICIENT_CAPACITY");
        error(hold(UUID.randomUUID(), 1, "missing"), 404, "RESOURCE_NOT_FOUND");
        assertThat(count("outbox_event")).isZero();
        trigger("reservation", "RAISE EXCEPTION 'injected reservation insertion failure';", "INSERT");
        error(hold(resource, 1, "insertion"), 503, "TEMPORARILY_UNAVAILABLE");
        capacity(resource, 1, 0, 0);
        assertThat(count("outbox_event")).isZero();
        assertThat(count("idempotency_record")).isEqualTo(2);
    }

    @Test
    void concurrentOppositeTransitionsHaveOneTerminalEventAndConsistentCapacity() throws Exception {
        UUID resource = fixture(10);
        UUID id = id(hold(resource, 2, "hold"));
        CyclicBarrier barrier = new CyclicBarrier(3);
        var confirm = threads.submit(() -> { barrier.await(5, TimeUnit.SECONDS); return action(id, "confirm", "confirm"); });
        var cancel = threads.submit(() -> { barrier.await(5, TimeUnit.SECONDS); return action(id, "cancel", "cancel"); });
        barrier.await(5, TimeUnit.SECONDS);
        var results = List.of(confirm.get(10, TimeUnit.SECONDS), cancel.get(10, TimeUnit.SECONDS));
        assertThat(results.stream().map(HttpResponse::statusCode)).containsExactlyInAnyOrder(200,409);
        String status = (String) reservationRow(id).get("status");
        assertThat(events(id)).hasSize(2);
        assertThat(events(id).get(1).get("event_type")).isEqualTo(status.equals("CONFIRMED") ? "ReservationConfirmed" : "ReservationCancelled");
        capacity(resource, status.equals("CONFIRMED") ? 8 : 10, status.equals("CONFIRMED") ? 1 : 2, 1);
        eventInvariant();
    }

    @Test
    void mandatoryTransactionPreventsDetachedOutboxCommit() throws Exception {
        UUID id = id(hold(fixture(10), 1, "hold"));
        var row = reservations.findOwned(id, ALICE).orElseThrow();
        assertThatThrownBy(() -> outbox.append(row)).isInstanceOf(IllegalTransactionStateException.class);
        assertThat(count("outbox_event")).isEqualTo(1);
    }

    @Test
    void restartRetainsEventIdentityAndReplayAddsNothing() {
        UUID resource = fixture(10);
        var request = new CreateReservationRequest(resource, 2);
        StoredResponse original;
        try (var context = application()) {
            original = context.getBean(IdempotentMutations.class).hold(ALICE, "restart", "original", request);
        }
        var before = jdbc.queryForList("SELECT * FROM outbox_event ORDER BY event_id");
        try (var context = application()) {
            var replay = context.getBean(IdempotentMutations.class).hold(ALICE, "restart", "retry", request);
            assertThat(replay.replayed()).isTrue();
            assertThat(replay.body()).isEqualTo(original.body());
        }
        assertThat(jdbc.queryForList("SELECT * FROM outbox_event ORDER BY event_id")).isEqualTo(before).hasSize(1);
        capacity(resource, 8, 1, 1);
    }

    private void envelope(Map<String,Object> event, UUID id, UUID resource, String status, long sequence, int released) {
        Map<String,Object> payload = JsonPath.read(event.get("payload").toString(), "$");
        assertThat(payload.keySet()).containsExactlyInAnyOrder("eventId","schemaVersion","eventType","aggregateType","aggregateId","aggregateSequence","occurredAt","correlationId","data");
        assertThat(payload).containsEntry("eventId", event.get("event_id").toString()).containsEntry("schemaVersion", 1)
                .containsEntry("eventType", event.get("event_type")).containsEntry("aggregateType", "RESERVATION")
                .containsEntry("aggregateId", id.toString());
        assertThat(((Number) payload.get("aggregateSequence")).longValue()).isEqualTo(sequence);
        assertThat(event.get("aggregate_sequence")).isEqualTo(sequence);
        UUID.fromString((String) payload.get("correlationId"));
        var occurred = java.time.Instant.parse((String) payload.get("occurredAt"));
        assertThat(occurred).isEqualTo(((java.sql.Timestamp)event.get("created_at")).toInstant());
        assertThat(event.get("next_attempt_at")).isEqualTo(event.get("created_at"));
        assertThat(event).containsEntry("status","PENDING").containsEntry("attempt_count",0).containsEntry("published_at",null).containsEntry("last_error",null);
        Map<String,Object> data = JsonPath.read(event.get("payload").toString(), "$.data");
        assertThat(data.keySet()).containsExactlyInAnyOrder("resourceId","userId","quantity","status","expiresAt","releasedQuantity");
        assertThat(data).containsEntry("resourceId",resource.toString()).containsEntry("userId",ALICE.toString())
                .containsEntry("quantity",2).containsEntry("status",status).containsEntry("releasedQuantity",released);
        java.time.Instant.parse((String)data.get("expiresAt"));
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
    private void eventInvariant() {
        assertThat(jdbc.queryForObject("""
                SELECT count(*) FROM reservation r WHERE
                    (SELECT count(*) FROM outbox_event e WHERE e.aggregate_id=r.id) <> r.transition_sequence
                    OR NOT EXISTS (SELECT 1 FROM outbox_event e WHERE e.aggregate_id=r.id AND e.aggregate_sequence=1 AND e.event_type='ReservationHeld')
                """, Integer.class)).isZero();
    }
    private List<Map<String,Object>> events(UUID id) {
        return jdbc.queryForList("SELECT * FROM outbox_event WHERE aggregate_id=? ORDER BY aggregate_sequence", id);
    }
    private Map<String,Object> resourceRow(UUID id) { return jdbc.queryForMap("SELECT * FROM resource WHERE id=?", id); }
    private Map<String,Object> reservationRow(UUID id) { return jdbc.queryForMap("SELECT * FROM reservation WHERE id=?", id); }
}
