package com.atlas.idempotency;

import java.util.List;
import java.util.UUID;
import com.atlas.inventory.CreateResourceRequest;
import com.atlas.inventory.ResourceService;
import com.atlas.reservation.CreateReservationRequest;
import com.atlas.reservation.ReservationResponse;
import com.atlas.reservation.ReservationService;
import org.springframework.stereotype.Service;

/** Typed, validated HTTP mutations enter one claim/business/response transaction. Workers do not use keys. */
@Service
public class IdempotentMutations {
    private final IdempotencyExecutor executor;
    private final ResourceService resources;
    private final ReservationService reservations;

    public IdempotentMutations(IdempotencyExecutor executor, ResourceService resources, ReservationService reservations) {
        this.executor = executor;
        this.resources = resources;
        this.reservations = reservations;
    }

    public StoredResponse resource(UUID caller, String key, String requestId, CreateResourceRequest request) {
        String operation = "v1.resource.create";
        return executor.execute(caller, operation, key, List.of(operation, request.name(), request.totalCapacity()), null, requestId, () -> {
            var result = resources.create(request);
            return new IdempotencyExecutor.Mutation(201, result, "/api/resources/" + result.id(), null, result.id());
        });
    }

    public StoredResponse hold(UUID caller, String key, String requestId, CreateReservationRequest request) {
        String operation = "v1.reservation.create";
        return executor.execute(caller, operation, key, List.of(operation, request.resourceId().toString(), request.quantity()), null, requestId,
                () -> response(reservations.create(caller, request), true));
    }

    public StoredResponse confirm(UUID caller, String key, String requestId, UUID id) {
        String operation = "v1.reservation.confirm";
        return executor.execute(caller, operation, key, List.of(operation, id.toString()), id, requestId,
                () -> response(reservations.confirm(caller, id), false));
    }

    public StoredResponse cancel(UUID caller, String key, String requestId, UUID id) {
        String operation = "v1.reservation.cancel";
        return executor.execute(caller, operation, key, List.of(operation, id.toString()), id, requestId,
                () -> response(reservations.cancel(caller, id), false));
    }

    private IdempotencyExecutor.Mutation response(ReservationResponse result, boolean created) {
        return new IdempotencyExecutor.Mutation(created ? 201 : 200, result,
                created ? "/api/reservations/" + result.id() : null, result.id(), result.resourceId());
    }
}
