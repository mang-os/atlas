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
import com.atlas.configuration.LocalPrincipal;
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
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.core.userdetails.UserDetailsService;
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
class ResourceApiIT {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_resource_api_test").withUsername("atlas_test").withPassword("testcontainers-only")
            .withStartupTimeout(Duration.ofSeconds(90));
    static final String ALICE_PASSWORD = UUID.randomUUID().toString();
    static final String BOB_PASSWORD = UUID.randomUUID().toString();
    static final String ADMIN_PASSWORD = UUID.randomUUID().toString();
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    @LocalServerPort int port;
    @Autowired JdbcTemplate jdbc;
    @Autowired UserDetailsService users;
    @Autowired AuthenticationConfiguration authentication;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("atlas.local.alice-password", () -> ALICE_PASSWORD);
        registry.add("atlas.local.bob-password", () -> BOB_PASSWORD);
        registry.add("atlas.local.admin-password", () -> ADMIN_PASSWORD);
    }

    @BeforeEach
    void resetOwnDisposableDatabase() {
        jdbc.update("DELETE FROM outbox_event");
        jdbc.update("DELETE FROM idempotency_record");
        jdbc.update("DELETE FROM resource");
    }

    @Test
    void adminCreatesAndAllUsersReadAnExplicitDtoWithServerFields() throws Exception {
        var created = send("POST", "/api/resources", "admin", "{\"name\":\"  Demo room  \",\"totalCapacity\":10}");
        assertThat(created.statusCode()).isEqualTo(201);
        recordResponse("create", created);
        var body = json(created);
        assertThat(body.keySet()).containsExactlyInAnyOrder("id", "name", "totalCapacity", "availableCapacity", "version", "createdAt", "updatedAt");
        var id = UUID.fromString((String) body.get("id"));
        assertThat(body).containsEntry("name", "Demo room").containsEntry("totalCapacity", 10).containsEntry("availableCapacity", 10);
        assertThat(((Number) body.get("version")).longValue()).isZero();
        assertThat(Instant.parse((String) body.get("createdAt"))).isBeforeOrEqualTo(Instant.now());
        assertThat(body.get("updatedAt")).isEqualTo(body.get("createdAt"));
        assertThat(created.headers().firstValue("Location")).contains("/api/resources/" + id);
        assertThat(jdbc.queryForObject("SELECT available_capacity FROM resource WHERE id=?", Integer.class, id)).isEqualTo(10);
        for (var user : List.of("alice", "bob", "admin")) {
            var lookup = send("GET", "/api/resources/" + id, user, null);
            assertThat(lookup.statusCode()).isEqualTo(200);
            assertThat(json(lookup)).isEqualTo(body);
            recordResponse("lookup-" + user, lookup);
        }
    }

    @Test
    void availabilityIsDatabaseObservationAndMayChangeAfterRead() throws Exception {
        String id = create("Demo", 10);
        var first = send("GET", "/api/resources/" + id + "/availability", "alice", null);
        assertThat(first.statusCode()).isEqualTo(200);
        recordResponse("availability", first);
        assertThat(json(first).keySet()).containsExactlyInAnyOrder("resourceId", "availableCapacity", "observedAt", "cached");
        assertThat(json(first)).containsEntry("resourceId", id).containsEntry("availableCapacity", 10).containsEntry("cached", false);
        Instant.parse((String) json(first).get("observedAt"));
        assertThat(first.headers().firstValue("Cache-Control").orElseThrow()).contains("no-store");
        // Test-only simulation of a later capacity mutation; no allocation service is implemented.
        jdbc.update("UPDATE resource SET available_capacity=9 WHERE id=?", UUID.fromString(id));
        var second = send("GET", "/api/resources/" + id + "/availability", "bob", null);
        assertThat(json(second)).containsEntry("availableCapacity", 9);
        assertThat(json(first)).containsEntry("availableCapacity", 10);
    }

    static Stream<String> invalidBodies() {
        return Stream.of("{}", "null", "[]", "{", "",
                "{\"name\":\"\\u0000Room\",\"totalCapacity\":1}",
                "{\"name\":\"Room\\u0000\",\"totalCapacity\":1}",
                "{\"name\":null,\"totalCapacity\":1}",
                "{\"name\":\"\",\"totalCapacity\":1}",
                "{\"name\":\"   \",\"totalCapacity\":1}",
                "{\"name\":\"" + "x".repeat(121) + "\",\"totalCapacity\":1}",
                "{\"name\":\"Room\"}", "{\"name\":\"Room\",\"totalCapacity\":null}",
                "{\"name\":\"Room\",\"totalCapacity\":0}", "{\"name\":\"Room\",\"totalCapacity\":-1}",
                "{\"name\":\"Room\",\"totalCapacity\":2147483648}",
                "{\"name\":\"Room\",\"totalCapacity\":1.5}", "{\"name\":\"Room\",\"totalCapacity\":1.0}",
                "{\"name\":\"Room\",\"totalCapacity\":\"10\"}", "{\"name\":123,\"totalCapacity\":1}",
                "{\"name\":\"Room\",\"totalCapacity\":true}",
                "{\"name\":\"Room\",\"totalCapacity\":1,\"quantity\":1}",
                "{\"name\":\"Room\",\"totalCapacity\":1,\"id\":\"11111111-1111-4111-8111-111111111111\"}",
                "{\"name\":\"Room\",\"totalCapacity\":1,\"availableCapacity\":99}",
                "{\"name\":\"Room\",\"totalCapacity\":1,\"userId\":\"33333333-3333-4333-8333-333333333333\"}",
                "{\"name\":\"Room\",\"totalCapacity\":1,\"role\":\"ADMIN\"}",
                "{\"name\":\"Room\",\"totalCapacity\":1,\"totalCapacity\":2}",
                "{\"name\":\"Room\",\"totalCapacity\":1} {}");
    }

    @ParameterizedTest
    @MethodSource("invalidBodies")
    void invalidJsonValidationAndProtectedFieldsReturnSafeErrorsWithoutInsert(String body) throws Exception {
        checkError(send("POST", "/api/resources", "admin", body), 400, "INVALID_REQUEST");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM resource", Integer.class)).isZero();
    }

    @Test
    void supportsPositiveCapacityBoundariesAndDuplicateNames() throws Exception {
        create("x".repeat(120), 1);
        var first = create("Room", Integer.MAX_VALUE);
        var second = create("Room", Integer.MAX_VALUE);
        assertThat(first).isNotEqualTo(second);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM resource", Integer.class)).isEqualTo(3);
    }

    @Test
    void creationRetriesReplayOriginalResult() throws Exception {
        var request = request("POST", "/api/resources", "admin", "{\"name\":\"Retry\",\"totalCapacity\":1}")
                .setHeader("Idempotency-Key", "resource-retry").build();
        var first = client.send(request, HttpResponse.BodyHandlers.ofString());
        var second = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(first.statusCode()).isEqualTo(201);
        assertThat(second.statusCode()).isEqualTo(201);
        assertThat(second.body()).isEqualTo(first.body());
        assertThat(second.headers().firstValue("Idempotency-Replayed")).contains("true");
        assertThat(first.headers().firstValue("Idempotency-Replayed")).isEmpty();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM resource", Integer.class)).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"alice", "bob"})
    void usersCannotCreateEvenWithForgedIdentityHeaders(String user) throws Exception {
        var response = client.send(request("POST", "/api/resources", user, "{\"name\":\"Room\",\"totalCapacity\":1}")
                .header("X-User-Id", LocalIdentityConfiguration.ADMIN_ID.toString()).header("X-Role", "ADMIN").build(),
                HttpResponse.BodyHandlers.ofString());
        checkError(response, 403, "FORBIDDEN");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM resource", Integer.class)).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"POST", "GET", "availability"})
    void credentialsRequiredForEveryResourceRoute(String route) throws Exception {
        String id = create("Room", 1);
        String path = route.equals("POST") ? "/api/resources" : "/api/resources/" + id + (route.equals("availability") ? "/availability" : "");
        checkError(send(route.equals("POST") ? "POST" : "GET", path, null,
                route.equals("POST") ? "{\"name\":\"Room\",\"totalCapacity\":1}" : null), 401, "AUTHENTICATION_REQUIRED");
    }

    @ParameterizedTest
    @ValueSource(strings = {"wrong-password", "unknown-user", "malformed-basic"})
    void badCredentialsHaveSameSafeAuthenticationError(String mode) throws Exception {
        var builder = request("GET", "/api/resources/" + UUID.randomUUID(), null, null);
        String header = mode.equals("malformed-basic") ? "Basic invalid!" : basic(mode.equals("unknown-user") ? "unknown" : "admin", "wrong");
        checkError(client.send(builder.header("Authorization", header).build(), HttpResponse.BodyHandlers.ofString()), 401, "AUTHENTICATION_REQUIRED");
    }

    @ParameterizedTest
    @ValueSource(strings = {"/api/resources/not-a-uuid", "/api/resources/1-1-1-1-1", "/api/resources/not-a-uuid/availability"})
    void rejectsMalformedIdentifiers(String path) throws Exception {
        checkError(send("GET", path, "alice", null), 400, "INVALID_REQUEST");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "/availability"})
    void missingResourcesReturnDomain404(String suffix) throws Exception {
        checkError(send("GET", "/api/resources/" + UUID.randomUUID() + suffix, "bob", null), 404, "RESOURCE_NOT_FOUND");
    }

    @Test
    void missingIdentifierAndMissingReservationReturnStructuredErrors() throws Exception {
        checkError(send("GET", "/api/resources", "alice", null), 405, "METHOD_NOT_ALLOWED");
        checkError(send("GET", "/api/reservations/" + UUID.randomUUID(), "alice", null), 404, "RESERVATION_NOT_FOUND");
    }

    @Test
    void permissionsProtectMetricsAndHealthRemainsMinimal() throws Exception {
        var health = send("GET", "/actuator/health", null, null);
        assertThat(health.statusCode()).isEqualTo(200);
        assertThat(json(health)).containsExactlyEntriesOf(Map.of("status", "UP"));
        checkError(send("GET", "/actuator/metrics", null, null), 401, "AUTHENTICATION_REQUIRED");
        checkError(send("GET", "/actuator/metrics", "alice", null), 403, "FORBIDDEN");
        checkError(send("GET", "/actuator/metrics", "admin", null), 404, "NOT_FOUND");
    }

    @Test
    void requestIdsAreFreshServerGeneratedAndErrorsMatchHeaders() throws Exception {
        var one = client.send(request("GET", "/api/resources/not-valid", "alice", null)
                .header("X-Request-Id", "client-controlled").build(), HttpResponse.BodyHandlers.ofString());
        checkError(one, 400, "INVALID_REQUEST");
        var two = send("GET", "/api/resources/not-valid", "alice", null);
        assertThat(one.headers().firstValue("X-Request-Id")).isNotEqualTo(two.headers().firstValue("X-Request-Id"));
        assertThat(one.headers().firstValue("X-Request-Id").orElseThrow()).isNotEqualTo("client-controlled");
        assertThat(one.headers().firstValue("Set-Cookie")).isEmpty();
    }

    @Test
    void validationDetailsAreFieldReasonsWithoutRejectedValues() throws Exception {
        var response = send("POST", "/api/resources", "admin", "{\"name\":\"\",\"totalCapacity\":0}");
        checkError(response, 400, "INVALID_REQUEST");
        recordResponse("validation-error", response);
        List<Map<String, String>> details = JsonPath.read(response.body(), "$.details");
        assertThat(details).hasSize(2);
        assertThat(details).allSatisfy(detail -> assertThat(detail.keySet()).containsExactlyInAnyOrder("field", "reason"));
    }

    @Test
    void methodAndMediaErrorsUseTheErrorContract() throws Exception {
        checkError(send("DELETE", "/api/resources/" + UUID.randomUUID(), "admin", null), 405, "METHOD_NOT_ALLOWED");
        var plain = client.send(HttpRequest.newBuilder(URI.create(base() + "/api/resources"))
                .header("Authorization", basic("admin", ADMIN_PASSWORD)).header("Content-Type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString("text")).build(), HttpResponse.BodyHandlers.ofString());
        checkError(plain, 415, "UNSUPPORTED_MEDIA_TYPE");
        var incompatible = client.send(request("GET", "/api/resources/" + create("Accept", 1), "alice", null)
                .header("Accept", "text/plain").build(), HttpResponse.BodyHandlers.ofString());
        checkError(incompatible, 406, "NOT_ACCEPTABLE");
    }

    @Test
    void verifiedPrincipalHasStableIdRolesAndOnlyPasswordHash() throws Exception {
        var manager = authentication.getAuthenticationManager();
        var result = manager.authenticate(UsernamePasswordAuthenticationToken.unauthenticated("alice", ALICE_PASSWORD));
        assertThat(result.getPrincipal()).isInstanceOf(LocalPrincipal.class);
        assertThat(((LocalPrincipal) result.getPrincipal()).id()).isEqualTo(LocalIdentityConfiguration.ALICE_ID);
        assertThat(result.getAuthorities()).extracting("authority").containsExactlyInAnyOrder("ROLE_USER", "FACTOR_PASSWORD");
        assertThat(((LocalPrincipal) users.loadUserByUsername("bob")).id()).isEqualTo(LocalIdentityConfiguration.BOB_ID);
        var admin = (LocalPrincipal) users.loadUserByUsername("admin");
        assertThat(admin.id()).isEqualTo(LocalIdentityConfiguration.ADMIN_ID);
        assertThat(admin.getAuthorities()).extracting("authority").containsExactlyInAnyOrder("ROLE_USER", "ROLE_ADMIN");
        assertThat(admin.getPassword()).startsWith("$2a$").doesNotContain(ADMIN_PASSWORD);
        // Repeated HTTP requests above also prove credential erasure does not destroy stored hashes.
    }

    private String create(String name, int capacity) throws Exception {
        var response = send("POST", "/api/resources", "admin", "{\"name\":\"" + name + "\",\"totalCapacity\":" + capacity + "}");
        assertThat(response.statusCode()).isEqualTo(201);
        return (String) json(response).get("id");
    }

    private HttpResponse<String> send(String method, String path, String user, String body) throws Exception {
        var response = client.send(request(method, path, user, body).build(), HttpResponse.BodyHandlers.ofString());
        UUID.fromString(response.headers().firstValue("X-Request-Id").orElseThrow());
        return response;
    }

    private HttpRequest.Builder request(String method, String path, String user, String body) {
        var builder = HttpRequest.newBuilder(URI.create(base() + path)).timeout(Duration.ofSeconds(10));
        if (user != null) {
            String password = switch (user) { case "alice" -> ALICE_PASSWORD; case "bob" -> BOB_PASSWORD; default -> ADMIN_PASSWORD; };
            builder.header("Authorization", basic(user, password));
        }
        if (body != null) { builder.header("Content-Type", "application/json"); }
        if (method.equals("POST")) builder.setHeader("Idempotency-Key", UUID.randomUUID().toString());
        return builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
    }

    private String basic(String user, String password) {
        return "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    private void recordResponse(String label, HttpResponse<String> response) throws Exception {
        var directory = java.nio.file.Path.of("target", "phase3-http");
        java.nio.file.Files.createDirectories(directory);
        String headers = "HTTP " + response.statusCode() + "\nContent-Type: "
                + response.headers().firstValue("Content-Type").orElse("") + "\nX-Request-Id: "
                + response.headers().firstValue("X-Request-Id").orElse("") + "\nLocation: "
                + response.headers().firstValue("Location").orElse("") + "\n\n";
        java.nio.file.Files.writeString(directory.resolve(label + ".txt"), headers + response.body() + "\n");
    }

    private String base() { return "http://127.0.0.1:" + port; }
    private Map<String, Object> json(HttpResponse<String> response) { return JsonPath.read(response.body(), "$"); }

    private void checkError(HttpResponse<String> response, int status, String code) {
        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(response.headers().firstValue("Content-Type").orElseThrow()).contains("application/json");
        var error = json(response);
        assertThat(error.keySet()).containsExactlyInAnyOrder("code", "message", "requestId", "details");
        assertThat(error).containsEntry("code", code).containsEntry("requestId", response.headers().firstValue("X-Request-Id").orElseThrow());
        UUID.fromString((String) error.get("requestId"));
        assertThat(response.body()).doesNotContain(ALICE_PASSWORD, BOB_PASSWORD, ADMIN_PASSWORD,
                "Authorization", "stackTrace", "SQLException", "jdbc:postgresql", "INSERT INTO", "invalid!");
    }
}
