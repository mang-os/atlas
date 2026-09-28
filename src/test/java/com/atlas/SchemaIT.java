package com.atlas;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Production migrations only, real PostgreSQL; every fixture transaction rolls back. */
@Testcontainers(disabledWithoutDocker = false)
@org.junit.jupiter.api.Timeout(90)
class SchemaIT {
    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_schema_test")
            .withUsername("atlas_test")
            .withPassword("testcontainers-only")
            .withStartupTimeout(Duration.ofSeconds(90));

    private static final String CREATED = "'2026-01-01T00:00:00Z'::timestamptz";
    private static final String DEADLINE = "'2026-01-01T00:05:00Z'::timestamptz";
    private static Flyway flyway;
    private Connection connection;
    private UUID resourceId;
    private UUID reservationId;

    @BeforeAll
    static void freshMigration() {
        flyway = migrations("public", "classpath:db/migration");
        assertThat(flyway.migrate().migrationsExecuted).isEqualTo(5);
        flyway.validate();
    }

    static Flyway migrations(String schema, String location) {
        return Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations(location)
                .validateMigrationNaming(true).failOnMissingLocations(true).cleanDisabled(true).load();
    }

    @BeforeEach
    void fixture() throws SQLException {
        connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        connection.setAutoCommit(false);
        execute("SET LOCAL TIME ZONE 'UTC'");
        resourceId = uuid("INSERT INTO resource(name,total_capacity,available_capacity) VALUES ('Room',10,8) RETURNING id");
        reservationId = uuid("INSERT INTO reservation(user_id,resource_id,quantity,status,created_at,expires_at) "
                + "VALUES (gen_random_uuid(),'" + resourceId + "',2,'HELD'," + CREATED + "," + DEADLINE + ") RETURNING id");
    }

    @AfterEach
    void rollbackFixture() throws SQLException {
        if (connection != null) {
            try { connection.rollback(); } finally { connection.close(); }
        }
    }

    @Test
    void freshSchemaContainsOnlyFoundationTablesAndRerunIsNoOp() throws SQLException {
        assertThat(flyway.info().current().getVersion().getVersion()).isEqualTo("5");
        assertThat(flyway.info().pending()).isEmpty();
        assertThat(flyway.migrate().migrationsExecuted).isZero();
        assertThat(strings("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"))
                .containsExactly("consumer_failure", "flyway_schema_history", "idempotency_record", "outbox_event", "processed_event", "reservation", "reservation_audit", "resource");
        assertThat(scalar("SELECT count(*) FROM flyway_schema_history WHERE success = false")).isZero();
    }

    @Test
    void upgradesPhaseOneEmptyHistoryWithoutBaselineOrClean(@TempDir Path emptyLocation) throws SQLException {
        // PostgreSQL already provided public in Phase 1. Pre-create the isolated equivalent
        // so Flyway does not add its unrelated schema-creation marker to the history.
        try (var setup = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = setup.createStatement()) {
            statement.execute("CREATE SCHEMA phase1_upgrade");
        }
        var phaseOne = migrations("phase1_upgrade", "filesystem:" + emptyLocation.toAbsolutePath());
        assertThat(phaseOne.migrate().migrationsExecuted).isZero();
        assertThat(phaseOne.info().applied()).isEmpty();
        assertThat(strings("SELECT tablename FROM pg_tables WHERE schemaname='phase1_upgrade'"))
                .containsExactly("flyway_schema_history");
        var upgrade = migrations("phase1_upgrade", "classpath:db/migration");
        assertThat(upgrade.migrate().migrationsExecuted).isEqualTo(5);
        upgrade.validate();
        assertThat(upgrade.migrate().migrationsExecuted).isZero();
        assertThat(strings("SELECT tablename FROM pg_tables WHERE schemaname='phase1_upgrade' ORDER BY tablename"))
                .containsExactly("consumer_failure", "flyway_schema_history", "idempotency_record", "outbox_event", "processed_event", "reservation", "reservation_audit", "resource");
        assertThat(scalar("SELECT count(*) FROM phase1_upgrade.resource")).isZero();
        assertThat(scalar("SELECT count(*) FROM phase1_upgrade.reservation")).isZero();
    }

    static Stream<Arguments> invalidResources() {
        return Stream.of(
                Arguments.of("name=''", "23514"), Arguments.of("name=' '", "23514"),
                Arguments.of("name=' leading'", "23514"), Arguments.of("name='trailing '", "23514"),
                Arguments.of("name=E'\\tRoom'", "23514"), Arguments.of("name=E'Room\\n'", "23514"),
                Arguments.of("name=repeat('a',121)", "22001"),
                Arguments.of("total_capacity=0", "23514"), Arguments.of("total_capacity=-1", "23514"),
                Arguments.of("available_capacity=-1", "23514"), Arguments.of("available_capacity=11", "23514"),
                Arguments.of("version=-1", "23514"));
    }

    @ParameterizedTest
    @MethodSource("invalidResources")
    void rejectsInvalidResourceValues(String assignment, String sqlState) throws SQLException {
        reject("UPDATE resource SET " + assignment, sqlState);
    }

    @ParameterizedTest
    @ValueSource(strings = {"id", "name", "total_capacity", "available_capacity", "version", "created_at", "updated_at"})
    void resourceRequiredFieldsCannotBeNull(String column) throws SQLException {
        reject("UPDATE resource SET " + column + "=NULL", "23502");
    }

    @ParameterizedTest
    @ValueSource(strings = {"id", "user_id", "resource_id", "quantity", "status", "expires_at", "created_at", "updated_at"})
    void reservationRequiredFieldsCannotBeNull(String column) throws SQLException {
        reject("UPDATE reservation SET " + column + "=NULL", "23502");
    }

    static Stream<Arguments> invalidReservations() {
        return Stream.of(
                Arguments.of("quantity=0"), Arguments.of("quantity=-1"),
                Arguments.of("status='PENDING'"), Arguments.of("status='held'"), Arguments.of("status=''"),
                Arguments.of("expires_at=" + CREATED),
                Arguments.of("expires_at=" + CREATED + " - interval '1 microsecond'"),
                Arguments.of("status='CONFIRMED',confirmed_at=" + CREATED + " - interval '1 microsecond'"),
                Arguments.of("status='CONFIRMED',confirmed_at=" + DEADLINE),
                Arguments.of("status='CONFIRMED',confirmed_at=" + DEADLINE + " + interval '1 microsecond'"),
                Arguments.of("status='CANCELLED',cancelled_at=" + CREATED + " - interval '1 microsecond'"),
                Arguments.of("status='CANCELLED',cancelled_at=" + DEADLINE),
                Arguments.of("status='CANCELLED',cancelled_at=" + DEADLINE + " + interval '1 microsecond'"),
                Arguments.of("status='EXPIRED',expired_at=" + DEADLINE + " - interval '1 microsecond'"));
    }

    @ParameterizedTest
    @MethodSource("invalidReservations")
    void rejectsInvalidQuantityStatusAndTimeBoundaries(String assignment) throws SQLException {
        reject("UPDATE reservation SET " + assignment + (assignment.contains("status=") && !assignment.contains("PENDING") && !assignment.contains("held") && !assignment.contains("status=''") ? ",transition_sequence=2" : ""), "23514");
    }

    @Test
    void enforcesAllThirtyTwoStatusAndTerminalTimestampShapes() throws SQLException {
        String[] statuses = {"HELD", "CONFIRMED", "CANCELLED", "EXPIRED"};
        for (int state = 0; state < statuses.length; state++) {
            for (int mask = 0; mask < 8; mask++) {
                String update = "UPDATE reservation SET status='" + statuses[state] + "',transition_sequence=" + (state == 0 ? 1 : 2) + ",confirmed_at="
                        + ((mask & 1) != 0 ? CREATED : "NULL") + ",cancelled_at="
                        + ((mask & 2) != 0 ? CREATED : "NULL") + ",expired_at="
                        + ((mask & 4) != 0 ? DEADLINE : "NULL");
                int validMask = state == 0 ? 0 : 1 << (state - 1);
                if (mask == validMask) { execute(update); }
                else { reject(update, "23514"); }
            }
        }
    }

    @Test
    void acceptsInclusiveCreationAndExclusiveDeadlineBoundaries() throws SQLException {
        for (String status : List.of("CONFIRMED", "CANCELLED")) {
            String timestampColumn = status.equals("CONFIRMED") ? "confirmed_at" : "cancelled_at";
            for (String time : List.of(CREATED, DEADLINE + " - interval '1 microsecond'")) {
                execute("UPDATE reservation SET status='HELD',transition_sequence=1,confirmed_at=NULL,cancelled_at=NULL,expired_at=NULL");
                execute("UPDATE reservation SET status='" + status + "',transition_sequence=2," + timestampColumn + "=" + time);
            }
        }
        execute("UPDATE reservation SET status='EXPIRED',transition_sequence=2,cancelled_at=NULL,expired_at=" + DEADLINE);
        execute("UPDATE reservation SET expired_at=" + DEADLINE + " + interval '1 microsecond'");
    }

    @Test
    void acceptsCapacityExtremesNonUniqueNamesAndDatabaseDefaults() throws SQLException {
        execute("INSERT INTO resource(name,total_capacity,available_capacity) VALUES ('Room',1,0)");
        execute("INSERT INTO resource(name,total_capacity,available_capacity) VALUES (repeat('x',120),2147483647,2147483647)");
        assertThat(scalar("SELECT count(*) FROM resource WHERE version=0 AND created_at IS NOT NULL AND updated_at IS NOT NULL")).isEqualTo(3);
        assertThat(scalar("SELECT count(DISTINCT id) FROM resource")).isEqualTo(3);
        execute("UPDATE reservation SET quantity=2147483647");
        reject("UPDATE reservation SET quantity=2147483648", "22003");
        reject("UPDATE resource SET total_capacity=2147483648", "22003");
    }

    @Test
    void generatesReservationIdentityAndDatabaseTimestamps() throws SQLException {
        UUID generated = uuid("INSERT INTO reservation(user_id,resource_id,quantity,status,expires_at) "
                + "VALUES (gen_random_uuid(),'" + resourceId + "',1,'HELD',statement_timestamp() + interval '5 minutes') RETURNING id");
        assertThat(generated).isNotEqualTo(reservationId);
        assertThat(scalar("SELECT count(*) FROM reservation WHERE id='" + generated + "' "
                + "AND updated_at=created_at AND expires_at=created_at + interval '5 minutes' "
                + "AND created_at BETWEEN transaction_timestamp() AND clock_timestamp()")).isEqualTo(1);
    }

    @Test
    void enforcesKeysAndRestrictsParentDeletionAndOrphans() throws SQLException {
        reject("INSERT INTO resource SELECT * FROM resource", "23505");
        reject("INSERT INTO reservation SELECT * FROM reservation", "23505");
        reject("UPDATE reservation SET resource_id=gen_random_uuid()", "23503");
        reject("DELETE FROM resource WHERE id='" + resourceId + "'", "23503");
        assertThat(scalar("SELECT count(*) FROM reservation r JOIN resource s ON r.resource_id=s.id")).isEqualTo(1);
        assertThat(scalar("SELECT count(*) FROM reservation WHERE id='" + reservationId + "'")).isEqualTo(1);
        // There is deliberately no local users table / user_id foreign key.
        execute("UPDATE reservation SET user_id=gen_random_uuid()");
    }

    @Test
    void suppliesRequiredIndexesAndTimestampTypes() throws SQLException {
        var indexes = strings("SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND tablename='reservation'");
        assertThat(indexes).hasSize(4);
        assertThat(indexes).anySatisfy(index -> assertThat(index).contains("(user_id, created_at, id)"));
        assertThat(indexes).anySatisfy(index -> assertThat(index).contains("(resource_id, status)"));
        assertThat(indexes).anySatisfy(index -> assertThat(index).contains("(expires_at, id)", "WHERE", "HELD"));
        assertThat(scalar("SELECT count(*) FROM pg_indexes WHERE schemaname='public' AND tablename='resource'")).isEqualTo(1);
        assertThat(strings("SELECT DISTINCT data_type FROM information_schema.columns WHERE table_schema='public' "
                + "AND table_name IN ('resource','reservation') AND column_name LIKE '%_at'"))
                .containsExactly("timestamp with time zone");
    }

    @Test
    void failedReservationInsertCanRollBackResourceChangeInSameTransaction() throws SQLException {
        var beforeChange = connection.setSavepoint();
        execute("UPDATE resource SET available_capacity=7,version=version+1");
        var error = assertThrows(SQLException.class, () -> execute("INSERT INTO reservation(user_id,resource_id,quantity,status,created_at,expires_at) "
                + "VALUES (gen_random_uuid(),'" + resourceId + "',0,'HELD'," + CREATED + "," + DEADLINE + ")"));
        assertThat(error.getSQLState()).isEqualTo("23514");
        connection.rollback(beforeChange);
        assertThat(scalar("SELECT available_capacity FROM resource")).isEqualTo(8);
        assertThat(scalar("SELECT version FROM resource")).isZero();
        assertThat(scalar("SELECT count(*) FROM reservation")).isEqualTo(1);
    }

    @Test
    void documentsChecksDoNotEnforceHistoryOrCrossTableConservation() throws SQLException {
        execute("UPDATE reservation SET status='CONFIRMED',transition_sequence=2,confirmed_at=" + CREATED);
        assertThat(scalar("SELECT available_capacity FROM resource")).isEqualTo(8);
        assertThat(scalar("SELECT available_capacity + (SELECT sum(quantity) FROM reservation "
                + "WHERE status IN ('HELD','CONFIRMED')) - total_capacity FROM resource")).isZero();
        // A resulting row can be valid even though this historical transition is forbidden.
        execute("UPDATE reservation SET status='HELD',transition_sequence=1,confirmed_at=NULL,quantity=11");
        assertThat(scalar("SELECT available_capacity + (SELECT sum(quantity) FROM reservation "
                + "WHERE status IN ('HELD','CONFIRMED')) - total_capacity FROM resource")).isEqualTo(9);
        // No updated_at monotonicity assumption: wall clocks may move backwards.
        execute("UPDATE reservation SET updated_at=created_at - interval '1 second'");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "idempotency_key=''", "idempotency_key='invalid space'", "idempotency_key=repeat('x',129)",
            "operation='v1.unknown'", "request_hash=repeat('g',64)", "request_hash='short'",
            "response_status=201", "response_body='{}'", "response_content_type='application/json'",
            "response_location='/orphan'", "completed_at=clock_timestamp()"
    })
    void idempotencyRejectsMalformedKeysHashesAndPartialResponses(String assignment) throws SQLException {
        insertClaim();
        reject("UPDATE idempotency_record SET " + assignment, assignment.contains("129") ? "22001" : "23514");
    }

    @Test
    void idempotencyScopeResponseShapeAndForeignKeysAreEnforced() throws SQLException {
        insertClaim();
        reject("INSERT INTO idempotency_record SELECT * FROM idempotency_record", "23505");
        execute("UPDATE idempotency_record SET response_status=201,response_body='{\"exact\": 1}',"
                + "response_content_type='application/json',response_location='/api/reservations/" + reservationId
                + "',completed_at=clock_timestamp(),reservation_id='" + reservationId + "',resource_id='" + resourceId + "'");
        assertThat(strings("SELECT response_body FROM idempotency_record")).containsExactly("{\"exact\": 1}");
        reject("UPDATE idempotency_record SET response_status=503", "23514");
        reject("UPDATE idempotency_record SET response_content_type='text/plain'", "23514");
        reject("UPDATE idempotency_record SET response_body=NULL", "23514");
        reject("UPDATE idempotency_record SET reservation_id=gen_random_uuid()", "23503");
        reject("UPDATE idempotency_record SET resource_id=gen_random_uuid()", "23503");
        reject("DELETE FROM reservation WHERE id='" + reservationId + "'", "23503");
        reject("DELETE FROM resource WHERE id='" + resourceId + "'", "23503");
        execute("INSERT INTO idempotency_record(caller_id,operation,idempotency_key,request_hash) "
                + "SELECT gen_random_uuid(),operation,idempotency_key,request_hash FROM idempotency_record");
        execute("INSERT INTO idempotency_record(caller_id,operation,idempotency_key,request_hash) "
                + "SELECT caller_id,'v1.reservation.cancel',idempotency_key,request_hash FROM idempotency_record");
        assertThat(scalar("SELECT count(*) FROM idempotency_record")).isEqualTo(4);
    }

    @Test
    void v2UpgradePreservesExistingResourceAndHeldReservation() throws SQLException {
        String schema = "phase_seven_upgrade";
        var v1 = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration").target("1").load();
        assertThat(v1.migrate().migrationsExecuted).isEqualTo(1);
        try (var setup = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var sql = setup.createStatement()) {
            sql.execute("SET search_path TO " + schema);
            sql.execute("INSERT INTO resource(id,name,total_capacity,available_capacity) VALUES ('" + resourceId + "','Existing',10,8)");
            sql.execute("INSERT INTO reservation(id,user_id,resource_id,quantity,status,expires_at) VALUES ('" + reservationId
                    + "',gen_random_uuid(),'" + resourceId + "',2,'HELD',clock_timestamp()+interval '1 hour')");
        }
        var v2 = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration").target("2").load();
        assertThat(v2.migrate().migrationsExecuted).isEqualTo(1);
        v2.validate();
        assertThat(v2.migrate().migrationsExecuted).isZero();
        assertThat(scalar("SELECT available_capacity FROM " + schema + ".resource WHERE id='" + resourceId + "'")).isEqualTo(8);
        assertThat(strings("SELECT status FROM " + schema + ".reservation WHERE id='" + reservationId + "'")).containsExactly("HELD");
        assertThat(scalar("SELECT count(*) FROM " + schema + ".idempotency_record")).isZero();
        System.out.println("PHASE8 V1-to-V2 upgrade existingHeld=1 available=8 retained=true emptyIdempotency=true");
    }

    @ParameterizedTest
    @ValueSource(strings = {"HELD", "CONFIRMED", "CANCELLED", "EXPIRED"})
    void phaseNineRefusesAnyHistoricalReservationAndLeavesV2Untouched(String state) throws SQLException {
        String schema = "phase8_nonempty_" + state.toLowerCase();
        var old = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration").target("2").load();
        old.migrate();
        try (var setup = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var sql = setup.createStatement()) {
            sql.execute("SET search_path TO " + schema);
            sql.execute("INSERT INTO resource(id,name,total_capacity,available_capacity) VALUES ('" + resourceId + "','Historical',10,8)");
            String terminal = state.equals("HELD") ? "" : "," + state.toLowerCase() + "_at";
            String time = state.equals("HELD") ? "" : "," + (state.equals("EXPIRED") ? DEADLINE : CREATED);
            sql.execute("INSERT INTO reservation(id,user_id,resource_id,quantity,status,created_at,expires_at" + terminal + ") VALUES ('"
                    + reservationId + "',gen_random_uuid(),'" + resourceId + "',2,'" + state + "'," + CREATED + "," + DEADLINE + time + ")");
        }
        var upgrade = migrations(schema, "classpath:db/migration");
        var error = assertThrows(org.flywaydb.core.api.FlywayException.class, upgrade::migrate);
        assertThat(error.getMessage()).contains("Phase 9 requires an empty reservation dataset");
        assertThat(strings("SELECT status FROM " + schema + ".reservation")).containsExactly(state);
        assertThat(scalar("SELECT available_capacity FROM " + schema + ".resource")).isEqualTo(8);
        assertThat(scalar("SELECT count(*) FROM information_schema.columns WHERE table_schema='" + schema + "' AND column_name='transition_sequence'")).isZero();
        assertThat(scalar("SELECT count(*) FROM pg_tables WHERE schemaname='" + schema + "' AND tablename='outbox_event'")).isZero();
        assertThat(upgrade.info().current().getVersion().getVersion()).isEqualTo("2");
        System.out.println("PHASE9 activation nonemptyStatus=" + state + " refused=true historicalRowsPreserved=true migrationRolledBack=true");
    }

    @Test
    void phaseNineUpgradesEmptyReservationsWhilePreservingResourceAndKeys() throws SQLException {
        String schema = "phase8_empty";
        var old = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration").target("2").load();
        old.migrate();
        try (var setup = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var sql = setup.createStatement()) {
            sql.execute("SET search_path TO " + schema);
            sql.execute("INSERT INTO resource(id,name,total_capacity,available_capacity) VALUES ('" + resourceId + "','Preserved',10,10)");
            sql.execute("INSERT INTO idempotency_record(caller_id,operation,idempotency_key,request_hash,response_status,response_body,response_content_type,completed_at,resource_id) "
                    + "VALUES (gen_random_uuid(),'v1.resource.create','existing',repeat('a',64),201,'{}','application/json',clock_timestamp(),'" + resourceId + "')");
        }
        var upgrade = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration").target("3").load();
        assertThat(upgrade.migrate().migrationsExecuted).isEqualTo(1);
        upgrade.validate();
        assertThat(upgrade.migrate().migrationsExecuted).isZero();
        assertThat(scalar("SELECT available_capacity FROM " + schema + ".resource")).isEqualTo(10);
        assertThat(scalar("SELECT count(*) FROM " + schema + ".idempotency_record")).isEqualTo(1);
        assertThat(scalar("SELECT count(*) FROM " + schema + ".outbox_event")).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"aggregate_sequence=0", "aggregate_sequence=2", "event_type='CapacityReleased'", "schema_version=0",
            "schema_version=2", "aggregate_type='RESOURCE'", "status='FAILED'", "status='PUBLISHED'", "published_at=clock_timestamp()",
            "attempt_count=-1", "payload='{}'::jsonb", "payload='[]'::jsonb", "last_error=repeat('x',1025)"})
    void outboxConstraintsRejectInvalidRows(String assignment) throws SQLException {
        insertOutbox();
        reject("UPDATE outbox_event SET " + assignment, "23514");
    }

    @Test
    void outboxKeysForeignKeysSequenceShapeAndPendingIndexAreEnforced() throws SQLException {
        insertOutbox();
        reject("INSERT INTO outbox_event SELECT * FROM outbox_event", "23505");
        reject("INSERT INTO outbox_event(event_id,aggregate_id,aggregate_type,aggregate_sequence,event_type,schema_version,payload,next_attempt_at,created_at) "
                + "SELECT gen_random_uuid(),aggregate_id,aggregate_type,aggregate_sequence,event_type,schema_version,payload,next_attempt_at,created_at FROM outbox_event", "23505");
        reject("UPDATE outbox_event SET aggregate_id=gen_random_uuid()", "23503");
        reject("DELETE FROM reservation WHERE id='" + reservationId + "'", "23503");
        reject("UPDATE reservation SET transition_sequence=2", "23514");
        execute("UPDATE outbox_event SET status='PUBLISHED',published_at=clock_timestamp()");
        assertThat(strings("SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND tablename='outbox_event'"))
                .anySatisfy(index -> assertThat(index).contains("next_attempt_at, created_at, event_id", "PENDING"));
    }

    @Test
    void auditMarkerKeysRelationshipsAndShapeAreEnforcedWithoutProducerDependency() throws SQLException {
        execute("INSERT INTO processed_event VALUES ('" + reservationId + "','audit',clock_timestamp())");
        reject("INSERT INTO processed_event SELECT * FROM processed_event", "23505");
        reject("UPDATE processed_event SET consumer_name=''", "23514");
        execute("INSERT INTO reservation_audit(event_id,consumer_name,reservation_id,aggregate_sequence,event_type,payload) VALUES ('"
                + reservationId + "','audit',gen_random_uuid(),1,'ReservationHeld','{}')");
        reject("INSERT INTO reservation_audit SELECT * FROM reservation_audit", "23505");
        reject("UPDATE reservation_audit SET event_id=gen_random_uuid()", "23503");
        reject("UPDATE reservation_audit SET consumer_name='other'", "23503");
        reject("DELETE FROM processed_event", "23503");
        reject("UPDATE reservation_audit SET aggregate_sequence=2", "23514");
        reject("UPDATE reservation_audit SET event_type='CapacityReleased'", "23514");
        reject("UPDATE reservation_audit SET payload='[]'", "23514");
        execute("INSERT INTO processed_event SELECT event_id,'independent-consumer',processed_at FROM processed_event");
        assertThat(scalar("SELECT count(*) FROM processed_event")).isEqualTo(2);
        assertThat(strings("SELECT indexdef FROM pg_indexes WHERE tablename='reservation_audit' AND schemaname='public'"))
                .anySatisfy(index -> assertThat(index).contains("reservation_id, aggregate_sequence"));
    }

    @Test
    void phaseTenUpgradePreservesExistingReservationsOutboxAndResponseKeys() throws SQLException {
        String schema = "phase9_upgrade";
        var old = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration").target("3").load();
        old.migrate();
        try (var setup = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var sql = setup.createStatement()) {
            sql.execute("SET search_path TO " + schema + ",public");
            sql.execute("INSERT INTO resource(id,name,total_capacity,available_capacity) VALUES ('" + resourceId + "','Retained',10,8)");
            sql.execute("INSERT INTO reservation(id,user_id,resource_id,quantity,status,expires_at) VALUES ('" + reservationId
                    + "',gen_random_uuid(),'" + resourceId + "',2,'HELD',clock_timestamp()+interval '1 hour')");
            sql.execute("INSERT INTO idempotency_record(caller_id,operation,idempotency_key,request_hash,response_status,response_body,response_content_type,completed_at,reservation_id) "
                    + "VALUES (gen_random_uuid(),'v1.reservation.create','existing',repeat('a',64),201,'{}','application/json',clock_timestamp(),'" + reservationId + "')");
        }
        // Reuse the established valid outbox fixture, copying its committed value into the isolated upgrade schema.
        insertOutbox();
        try (var setup = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var sql = setup.prepareStatement("INSERT INTO " + schema + ".outbox_event(event_id,aggregate_id,aggregate_type,aggregate_sequence,event_type,schema_version,payload,next_attempt_at,created_at) "
                     + "VALUES (gen_random_uuid(),?,'RESERVATION',1,'ReservationHeld',1,CAST(? AS jsonb),clock_timestamp(),clock_timestamp())")) {
            sql.setObject(1, reservationId);
            sql.setString(2, strings("SELECT payload::text FROM outbox_event").getFirst());
            sql.executeUpdate();
        }
        var upgrade = Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration").target("4").load();
        assertThat(upgrade.migrate().migrationsExecuted).isEqualTo(1);
        upgrade.validate();
        assertThat(upgrade.migrate().migrationsExecuted).isZero();
        assertThat(scalar("SELECT available_capacity FROM " + schema + ".resource")).isEqualTo(8);
        assertThat(strings("SELECT status FROM " + schema + ".reservation")).containsExactly("HELD");
        assertThat(strings("SELECT status FROM " + schema + ".outbox_event")).containsExactly("PENDING");
        assertThat(scalar("SELECT count(*) FROM " + schema + ".idempotency_record")).isEqualTo(1);
        assertThat(scalar("SELECT count(*) FROM " + schema + ".processed_event")).isZero();
        assertThat(scalar("SELECT count(*) FROM " + schema + ".reservation_audit")).isZero();
        System.out.println("PHASE10 V3-to-V4 upgrade retainedReservation=true retainedPendingEvent=true retainedKey=true emptyAudit=true");
    }

    @ParameterizedTest
    @ValueSource(strings={"failed_at=clock_timestamp()", "replay_count=-1", "replay_count=1", "last_replayed_at=clock_timestamp()", "last_replay_reason='FIX_DEPLOYED'", "last_error='private message'"})
    void phaseTwelveOutboxRetryConstraintsRejectInvalidShapes(String assignment) throws SQLException {
        insertOutbox(); reject("UPDATE outbox_event SET "+assignment,"23514");
    }

    @ParameterizedTest
    @ValueSource(strings={"partition=-1","record_offset=-1","attempt_count=-1","status='PENDING'","status='FAILED'","payload_hash='bad'","last_error='secret'","replay_count=1","next_attempt_at=NULL"})
    void phaseTwelveConsumerFailureConstraintsRejectInvalidShapes(String assignment) throws SQLException {
        execute("INSERT INTO consumer_failure(consumer_name,topic,partition,record_offset,payload_hash,attempt_count,status,next_attempt_at,last_error) VALUES ('reservation-audit-v1','atlas.reservation-events.v1',0,0,repeat('a',64),1,'RETRY_WAIT',clock_timestamp(),'EFFECT_FAILED')");
        reject("UPDATE consumer_failure SET "+assignment,"23514");
    }

    @Test
    void phaseTwelveUpgradePreservesV4StateAndPayloadAndRerunIsNoOp() throws SQLException {
        String schema="phase11_upgrade";
        var old=Flyway.configure().dataSource(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword())
            .schemas(schema).defaultSchema(schema).locations("classpath:db/migration").target("4").load();
        assertThat(old.migrate().migrationsExecuted).isEqualTo(4);
        insertOutbox();
        String body=strings("SELECT payload::text FROM outbox_event").getFirst();
        try(var setup=DriverManager.getConnection(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword());var sql=setup.createStatement()) {
            sql.execute("SET search_path TO "+schema+",public");
            sql.execute("INSERT INTO resource(id,name,total_capacity,available_capacity) VALUES ('"+resourceId+"','V4 retained',10,8)");
            sql.execute("INSERT INTO reservation(id,user_id,resource_id,quantity,status,expires_at) VALUES ('"+reservationId+"',gen_random_uuid(),'"+resourceId+"',2,'HELD',clock_timestamp()+interval '1 hour')");
            try(var insert=setup.prepareStatement("INSERT INTO outbox_event(event_id,aggregate_id,aggregate_type,aggregate_sequence,event_type,schema_version,payload,status,attempt_count,next_attempt_at,created_at,published_at) VALUES (? ,?,'RESERVATION',1,'ReservationHeld',1,?::jsonb,'PUBLISHED',2,clock_timestamp(),clock_timestamp(),clock_timestamp())")) {
                insert.setObject(1,UUID.randomUUID());insert.setObject(2,reservationId);insert.setString(3,body);insert.executeUpdate();
            }
            sql.execute("INSERT INTO processed_event(event_id,consumer_name) SELECT event_id,'reservation-audit-v1' FROM outbox_event");
            sql.execute("INSERT INTO reservation_audit(event_id,consumer_name,reservation_id,aggregate_sequence,event_type,payload) SELECT event_id,'reservation-audit-v1',aggregate_id,aggregate_sequence,event_type,payload FROM outbox_event");
        }
        String before;
        try(var observed=DriverManager.getConnection(POSTGRES.getJdbcUrl(),POSTGRES.getUsername(),POSTGRES.getPassword());var query=observed.createStatement()) {
            query.execute("SET TIME ZONE 'UTC'");
            try(var rows=query.executeQuery("SELECT row_to_json(e)::text FROM "+schema+".outbox_event e")) {
                rows.next();before=rows.getString(1);
            }
        }
        var upgrade=migrations(schema,"classpath:db/migration");assertThat(upgrade.migrate().migrationsExecuted).isEqualTo(1);upgrade.validate();assertThat(upgrade.migrate().migrationsExecuted).isZero();
        // Compare the original fields, excluding only newly introduced metadata.
        assertThat(scalar("SELECT count(*) FROM "+schema+".outbox_event e WHERE (to_jsonb(e)-ARRAY['failed_at','replay_count','last_replayed_at','last_replay_reason'])='"+before.replace("'","''")+"'::jsonb")).isEqualTo(1);
        assertThat(scalar("SELECT available_capacity FROM "+schema+".resource")).isEqualTo(8);
        assertThat(scalar("SELECT count(*) FROM "+schema+".reservation_audit")).isEqualTo(1);
        assertThat(scalar("SELECT count(*) FROM "+schema+".consumer_failure")).isZero();
        assertThat(strings("SELECT status FROM "+schema+".outbox_event")).containsExactly("PUBLISHED");
        System.out.println("PHASE12 V4-to-V5 preservedState=true preservedEnvelope=true preservedAudit=true rerunNoOp=true");
    }

    private void insertOutbox() throws SQLException {
        execute("INSERT INTO outbox_event(event_id,aggregate_id,aggregate_type,aggregate_sequence,event_type,schema_version,payload,next_attempt_at,created_at) "
                + "VALUES (gen_random_uuid(),'" + reservationId + "','RESERVATION',1,'ReservationHeld',1,"
                + "'{\"eventId\":\"fixture\",\"schemaVersion\":1,\"eventType\":\"ReservationHeld\",\"aggregateType\":\"RESERVATION\","
                + "\"aggregateId\":\"fixture\",\"aggregateSequence\":1,\"occurredAt\":\"fixture\",\"correlationId\":\"fixture\",\"data\":{}}'::jsonb,clock_timestamp(),clock_timestamp())");
    }

    private void insertClaim() throws SQLException {
        execute("INSERT INTO idempotency_record(caller_id,operation,idempotency_key,request_hash) "
                + "VALUES (gen_random_uuid(),'v1.reservation.create','schema-key',repeat('a',64))");
    }

    private void reject(String sql, String sqlState) throws SQLException {
        var savepoint = connection.setSavepoint();
        try {
            var error = assertThrows(SQLException.class, () -> execute(sql), sql);
            assertThat(error.getSQLState()).as(sql).isEqualTo(sqlState);
        } finally {
            connection.rollback(savepoint);
            connection.releaseSavepoint(savepoint);
        }
    }

    private void execute(String sql) throws SQLException {
        try (var statement = connection.createStatement()) { statement.execute(sql); }
    }

    private UUID uuid(String sql) throws SQLException {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
            assertThat(rows.next()).isTrue();
            return rows.getObject(1, UUID.class);
        }
    }

    private long scalar(String sql) throws SQLException {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private List<String> strings(String sql) throws SQLException {
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(sql)) {
            var result = new ArrayList<String>();
            while (rows.next()) { result.add(rows.getString(1)); }
            return result;
        }
    }
}
