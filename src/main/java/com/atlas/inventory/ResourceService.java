package com.atlas.inventory;

import java.util.UUID;
import com.atlas.common.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ResourceService {
    private final ResourceRepository resources;
    private final com.atlas.common.ReadCache cache;
    private final com.atlas.common.CacheInvalidation invalidation;

    public ResourceService(ResourceRepository resources, com.atlas.common.ReadCache cache, com.atlas.common.CacheInvalidation invalidation) {
        this.resources = resources; this.cache=cache; this.invalidation=invalidation;
    }

    @Transactional
    public ResourceResponse create(CreateResourceRequest request) {
        var created=resources.create(request.name(), request.totalCapacity());
        invalidation.afterCommit(created.id(),null);
        return ResourceResponse.from(created);
    }

    @Transactional(readOnly = true)
    public ResourceResponse find(UUID id) {
        return ResourceResponse.from(resources.find(id).orElseThrow(this::missing));
    }

    @Transactional(readOnly = true)
    public AvailabilityResponse availability(UUID id) {
        return cache.availability(id, () -> resources.availability(id).orElseThrow(this::missing));
    }

    private ApiException missing() { return new ApiException(HttpStatus.NOT_FOUND, "RESOURCE_NOT_FOUND", "Resource not found"); }
}
