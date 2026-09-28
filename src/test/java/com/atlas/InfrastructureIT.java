package com.atlas;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import javax.sql.DataSource;

import com.jayway.jsonpath.JsonPath;
import jakarta.persistence.EntityManagerFactory;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
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
@ActiveProfiles("test")
@Testcontainers(disabledWithoutDocker = false)
class InfrastructureIT {
    // Same verified postgres:17.11-bookworm digest as Compose; digest-only for Testcontainers parsing.
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_test")
            .withUsername("atlas_test")
            .withPassword("testcontainers-only")
            .withStartupTimeout(Duration.ofSeconds(90));

    @DynamicPropertySource
    static void databaseProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired DataSource dataSource;
    @Autowired JdbcTemplate jdbc;
    @Autowired Flyway flyway;
    @Autowired EntityManagerFactory entityManagerFactory;
    @Autowired org.springframework.context.ApplicationContext context;
    @LocalServerPort int port;

    @Test
    void kafkaClientsAndWorkersAreAbsentWithoutExplicitOptIn() {
        assertThat(context.getBeansOfType(org.springframework.kafka.core.KafkaTemplate.class)).isEmpty();
        assertThat(context.getBeansOfType(com.atlas.outbox.OutboxPublisher.class)).isEmpty();
        assertThat(context.getBeansOfType(com.atlas.messaging.ReservationAuditListener.class)).isEmpty();
        assertThat(context.getBeansOfType(com.atlas.common.RedisCacheStore.class)).isEmpty();
    }

    @Test
    void connectsToRealPostgresAndInitializesJpa() throws Exception {
        try (var connection = dataSource.getConnection()) {
            assertThat(connection.getMetaData().getDatabaseProductName()).isEqualTo("PostgreSQL");
            assertThat(connection.getMetaData().getDatabaseMajorVersion()).isEqualTo(17);
        }
        assertThat(jdbc.queryForObject("select 1", Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("show timezone", String.class)).isEqualTo("UTC");
        assertThat(entityManagerFactory.isOpen()).isTrue();
    }

    @Test
    void executesAndValidatesFlywayAgainstAnEmptyDatabase() {
        flyway.validate();
        assertThat(flyway.info().pending()).isEmpty();
        assertThat(flyway.info().applied()).isNotEmpty();
        assertThat(jdbc.queryForObject(
                "select description from infrastructure_probe where id = 1", String.class))
                .isEqualTo("Flyway executed");
        assertThat(jdbc.queryForObject(
                "select count(*) from flyway_schema_history where success = false", Integer.class))
                .isZero();
    }

    @Test
    void servesMinimalHealthAndDoesNotExposeMetrics() throws Exception {
        var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        var response = client.send(request("/actuator/health"), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        Map<String, Object> health = JsonPath.read(response.body(), "$");
        assertThat(health).containsExactlyEntriesOf(Map.of("status", "UP"));
        var metrics = client.send(request("/actuator/metrics"), HttpResponse.BodyHandlers.ofString());
        assertThat(metrics.statusCode()).isEqualTo(401);
    }

    @Test
    void localIdentityIsDisabledWithoutExplicitLocalProfile() throws Exception {
        var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/resources"))
                .header("Authorization", "Basic " + java.util.Base64.getEncoder().encodeToString("admin:unused".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(401);
        Map<String, Object> error = JsonPath.read(response.body(), "$");
        assertThat(error).containsEntry("code", "AUTHENTICATION_REQUIRED")
                .containsEntry("requestId", response.headers().firstValue("X-Request-Id").orElseThrow());
    }

    private HttpRequest request(String path) {
        return HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(10)).GET().build();
    }
}
