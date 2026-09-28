package com.atlas.inventory;

import java.time.Instant;
import java.util.UUID;

/** Informational database observation, never authorization for a later allocation. */
public record AvailabilityResponse(UUID resourceId, int availableCapacity, Instant observedAt, boolean cached) {}
