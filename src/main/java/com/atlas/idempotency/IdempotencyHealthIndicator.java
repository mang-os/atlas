package com.atlas.idempotency;

import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** MVCC excludes ordinary uncommitted claims. Any visible incomplete claim needs operator investigation. */
@Component
public class IdempotencyHealthIndicator extends AbstractHealthIndicator {
    private final JdbcTemplate jdbc;

    public IdempotencyHealthIndicator(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        boolean corrupt = Boolean.TRUE.equals(jdbc.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM idempotency_record WHERE completed_at IS NULL)", Boolean.class));
        if (corrupt) builder.down();
        else builder.up();
    }
}
