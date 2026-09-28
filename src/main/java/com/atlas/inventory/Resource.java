package com.atlas.inventory;

import java.time.Instant;
import java.util.UUID;

/**
 * Internal immutable resource snapshot, not an API DTO or a managed JPA entity.
 * Capacity mutations must use a database transaction, never save a stale snapshot.
 */
public record Resource(
        UUID id,
        String name,
        int totalCapacity,
        int availableCapacity,
        long version,
        Instant createdAt,
        Instant updatedAt) {
}
