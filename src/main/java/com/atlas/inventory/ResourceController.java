package com.atlas.inventory;

import java.util.UUID;
import com.atlas.common.ApiException;
import com.atlas.common.RequestIdFilter;
import com.atlas.configuration.LocalPrincipal;
import com.atlas.idempotency.IdempotencyKey;
import com.atlas.idempotency.IdempotentMutations;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import jakarta.validation.Valid;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/resources")
public class ResourceController {
    private final ResourceService resources;

    private final IdempotentMutations mutations;

    public ResourceController(ResourceService resources, IdempotentMutations mutations) {
        this.resources = resources;
        this.mutations = mutations;
    }

    @PostMapping
    ResponseEntity<byte[]> create(@AuthenticationPrincipal LocalPrincipal caller,
                                  @Valid @RequestBody CreateResourceRequest request, HttpServletRequest http) {
        return mutations.resource(caller.id(), IdempotencyKey.from(http), RequestIdFilter.id(http), request).toHttp();
    }

    @GetMapping("/{id}")
    ResourceResponse find(@PathVariable String id) { return resources.find(uuid(id)); }

    @GetMapping("/{id}/availability")
    ResponseEntity<AvailabilityResponse> availability(@PathVariable String id) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(resources.availability(uuid(id)));
    }

    private UUID uuid(String id) {
        if (!id.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "Resource ID must be a UUID");
        }
        return UUID.fromString(id);
    }
}
