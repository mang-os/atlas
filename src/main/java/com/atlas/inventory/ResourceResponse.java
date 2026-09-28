package com.atlas.inventory;

import java.time.Instant;
import java.util.UUID;

/** Deliberate public DTO; never return the internal snapshot directly. */
public record ResourceResponse(UUID id, String name, int totalCapacity, int availableCapacity,
                               long version, Instant createdAt, Instant updatedAt) {
    static ResourceResponse from(Resource resource) {
        return new ResourceResponse(resource.id(), resource.name(), resource.totalCapacity(), resource.availableCapacity(),
                resource.version(), resource.createdAt(), resource.updatedAt());
    }
}
