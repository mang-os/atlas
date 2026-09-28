package com.atlas.idempotency;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import javax.sql.DataSource;
import com.atlas.common.ApiError;
import com.atlas.common.ApiException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Service
public class IdempotencyExecutor {
    private static final Set<String> STORED_FAILURES = Set.of("RESOURCE_NOT_FOUND", "RESERVATION_NOT_FOUND",
            "INSUFFICIENT_CAPACITY", "RESERVATION_EXPIRED", "INVALID_STATE_TRANSITION");
    private final JdbcTemplate jdbc;
    private final DataSource dataSource;
    private final JsonMapper json;

    public IdempotencyExecutor(JdbcTemplate jdbc, DataSource dataSource, JsonMapper json) {
        this.jdbc = jdbc;
        this.dataSource = dataSource;
        this.json = json;
    }

    public record Mutation(int status, Object body, String location, UUID reservationId, UUID resourceId) {}

    @Transactional(isolation = Isolation.READ_COMMITTED, rollbackFor = Exception.class)
    public StoredResponse execute(UUID caller, String operation, String key, List<?> canonicalRequest,
                                  UUID ownedPath, String requestId, Supplier<Mutation> mutation) {
        // An unlocked ownership read precedes the claim, including on replay. Ownership is immutable in v1.
        checkOwnership(caller, ownedPath);
        String hash = hash(json.writeValueAsString(canonicalRequest));
        boolean claimed = !jdbc.queryForList("""
                INSERT INTO idempotency_record(caller_id, operation, idempotency_key, request_hash)
                VALUES (?, ?, ?, ?) ON CONFLICT (caller_id, operation, idempotency_key) DO NOTHING
                RETURNING idempotency_key
                """, String.class, caller, operation, key, hash).isEmpty();
        if (!claimed) {
            // Separate READ COMMITTED statement sees the winner after its unique-index wait commits.
            var rows = jdbc.queryForList("""
                    SELECT * FROM idempotency_record WHERE caller_id=? AND operation=? AND idempotency_key=?
                    """, caller, operation, key);
            if (rows.size() != 1 || rows.getFirst().get("completed_at") == null) {
                throw new DataIntegrityViolationException("Missing or incomplete committed idempotency result");
            }
            var row = rows.getFirst();
            if (!hash.equals(row.get("request_hash"))) {
                throw new ApiException(HttpStatus.CONFLICT, "IDEMPOTENCY_KEY_MISMATCH",
                        "This Idempotency-Key was already used for a different request");
            }
            checkOwnership(caller, (UUID) row.get("reservation_id"));
            return new StoredResponse(((Number) row.get("response_status")).intValue(),
                    (String) row.get("response_body"), (String) row.get("response_content_type"),
                    (String) row.get("response_location"), true);
        }

        Mutation result;
        // JdbcTemplate and JpaTransactionManager share this transaction-bound JDBC connection.
        Connection connection = DataSourceUtils.getConnection(dataSource);
        try {
            var savepoint = connection.setSavepoint();
            try {
                result = mutation.get();
            } catch (ApiException rejection) {
                if (!STORED_FAILURES.contains(rejection.code())) throw rejection;
                connection.rollback(savepoint);
                result = new Mutation(rejection.status().value(),
                        new ApiError(rejection.code(), rejection.getMessage(), requestId, List.of()), null, null, null);
            }
            connection.releaseSavepoint(savepoint);
        } catch (SQLException failure) {
            throw new DataAccessResourceFailureException("Could not isolate idempotent mutation", failure);
        } finally {
            DataSourceUtils.releaseConnection(connection, dataSource);
        }

        String body = json.writeValueAsString(result.body());
        int changed = jdbc.update("""
                UPDATE idempotency_record SET response_status=?, response_body=?, response_content_type='application/json',
                    response_location=?, reservation_id=?, resource_id=?, completed_at=clock_timestamp()
                WHERE caller_id=? AND operation=? AND idempotency_key=? AND completed_at IS NULL
                """, result.status(), body, result.location(), result.reservationId(), result.resourceId(), caller, operation, key);
        if (changed != 1) throw new DataIntegrityViolationException("Could not complete idempotency result");
        return new StoredResponse(result.status(), body, "application/json", result.location(), false);
    }

    private void checkOwnership(UUID caller, UUID id) {
        if (id == null) return;
        var owners = jdbc.queryForList("SELECT user_id FROM reservation WHERE id=?", UUID.class, id);
        if (!owners.isEmpty() && !owners.getFirst().equals(caller)) {
            // Outside the stored-domain-error handler: ownership failures never consume a key.
            throw new ApiException(HttpStatus.NOT_FOUND, "RESERVATION_NOT_FOUND", "Reservation not found");
        }
    }

    private String hash(String canonicalJson) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonicalJson.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
