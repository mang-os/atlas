package com.atlas.inventory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ResourceRepository {
    private final JdbcTemplate jdbc;

    public ResourceRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public Resource create(String name, int capacity) {
        return jdbc.queryForObject("INSERT INTO resource (name,total_capacity,available_capacity) VALUES (?,?,?) RETURNING *",
                this::map, name, capacity, capacity);
    }

    public Optional<Resource> find(UUID id) {
        return jdbc.query("SELECT * FROM resource WHERE id=?", this::map, id).stream().findFirst();
    }

    public Optional<AvailabilityResponse> availability(UUID id) {
        return jdbc.query("SELECT id,available_capacity,clock_timestamp() AS observed_at FROM resource WHERE id=?",
                (row, index) -> new AvailabilityResponse(row.getObject("id", UUID.class), row.getInt("available_capacity"),
                        row.getObject("observed_at", OffsetDateTime.class).toInstant(), false), id).stream().findFirst();
    }

    private Resource map(ResultSet row, int index) throws SQLException {
        return new Resource(row.getObject("id", UUID.class), row.getString("name"), row.getInt("total_capacity"),
                row.getInt("available_capacity"), row.getLong("version"),
                row.getObject("created_at", OffsetDateTime.class).toInstant(),
                row.getObject("updated_at", OffsetDateTime.class).toInstant());
    }
}
