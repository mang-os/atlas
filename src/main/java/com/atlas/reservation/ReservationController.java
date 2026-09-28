package com.atlas.reservation;

import java.util.UUID;
import com.atlas.common.ApiException;
import com.atlas.common.RequestIdFilter;
import com.atlas.idempotency.IdempotencyKey;
import com.atlas.idempotency.IdempotentMutations;
import jakarta.servlet.http.HttpServletRequest;
import com.atlas.configuration.LocalPrincipal;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ReservationController {
    private final ReservationService reservations;

    private final IdempotentMutations mutations;

    public ReservationController(ReservationService reservations, IdempotentMutations mutations) {
        this.reservations = reservations;
        this.mutations = mutations;
    }

    @PostMapping("/api/reservations")
    ResponseEntity<byte[]> create(@AuthenticationPrincipal LocalPrincipal caller,
                                              @Valid @RequestBody CreateReservationRequest request, HttpServletRequest http) {
        return mutations.hold(caller.id(), IdempotencyKey.from(http), RequestIdFilter.id(http), request).toHttp();
    }

    @GetMapping("/api/reservations/{id}")
    ResponseEntity<ReservationReadResponse> find(@AuthenticationPrincipal LocalPrincipal caller, @PathVariable String id) {
        return ResponseEntity.ok().cacheControl(org.springframework.http.CacheControl.noStore())
                .body(reservations.find(caller.id(), uuid(id, "Reservation")));
    }

    @PostMapping("/api/reservations/{id}/confirm")
    ResponseEntity<byte[]> confirm(@AuthenticationPrincipal LocalPrincipal caller, @PathVariable String id,
                                @RequestBody(required = false) byte[] body, HttpServletRequest http) {
        requireEmptyBody(body);
        return mutations.confirm(caller.id(), IdempotencyKey.from(http), RequestIdFilter.id(http), uuid(id, "Reservation")).toHttp();
    }

    @PostMapping("/api/reservations/{id}/cancel")
    ResponseEntity<byte[]> cancel(@AuthenticationPrincipal LocalPrincipal caller, @PathVariable String id,
                               @RequestBody(required = false) byte[] body, HttpServletRequest http) {
        requireEmptyBody(body);
        return mutations.cancel(caller.id(), IdempotencyKey.from(http), RequestIdFilter.id(http), uuid(id, "Reservation")).toHttp();
    }

    private void requireEmptyBody(byte[] body) {
        if (body != null && body.length != 0) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "This operation requires an empty body");
        }
    }

    @GetMapping("/api/users/{userId}/reservations")
    ReservationPage list(@AuthenticationPrincipal LocalPrincipal caller, @PathVariable String userId,
                         @RequestParam(required = false) String limit,
                         @RequestParam(required = false) String cursor) {
        limit = limit == null ? "20" : limit;
        if (!limit.matches("[0-9]{1,3}") || Integer.parseInt(limit) < 1 || Integer.parseInt(limit) > 100) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "Limit must be an integer from 1 to 100");
        }
        return reservations.list(caller.id(), uuid(userId, "User"), Integer.parseInt(limit), cursor);
    }

    private UUID uuid(String value, String label) {
        if (!value.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", label + " ID must be a UUID");
        }
        return UUID.fromString(value);
    }
}
