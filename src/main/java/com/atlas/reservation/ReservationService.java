package com.atlas.reservation;

import java.util.UUID;
import com.atlas.common.ApiException;
import com.atlas.outbox.OutboxWriter;
import com.atlas.configuration.HoldProperties;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ReservationService {
    private final ReservationRepository reservations;
    private final HoldProperties holds;
    private final OutboxWriter outbox;
    private final com.atlas.common.ReadCache cache;
    private final com.atlas.common.CacheInvalidation invalidation;

    public ReservationService(ReservationRepository reservations, HoldProperties holds, OutboxWriter outbox,
            com.atlas.common.ReadCache cache, com.atlas.common.CacheInvalidation invalidation) {
        this.reservations = reservations;
        this.holds = holds;
        this.outbox = outbox;
        this.cache=cache; this.invalidation=invalidation;
    }

    // Expected domain rejections precede writes. The API executor rolls back its savepoint before storing them;
    // infrastructure exceptions still mark the whole shared transaction for rollback.
    @Transactional(isolation = Isolation.READ_COMMITTED, rollbackFor = Exception.class, noRollbackFor = ApiException.class)
    public ReservationResponse create(UUID ownerId, CreateReservationRequest request) {
        if (!reservations.allocate(request.resourceId(), request.quantity())) {
            if (!reservations.resourceExists(request.resourceId())) {
                throw new ApiException(HttpStatus.NOT_FOUND, "RESOURCE_NOT_FOUND", "Resource not found");
            }
            throw new ApiException(HttpStatus.CONFLICT, "INSUFFICIENT_CAPACITY", "The requested quantity is no longer available");
        }
        // Let insertion errors escape the transaction proxy; never swallow them or return partial success.
        var created = reservations.insert(ownerId, request.resourceId(), request.quantity(), holds.ttlSeconds());
        outbox.append(created);
        invalidation.afterCommit(created.resourceId(),created.id());
        return ReservationResponse.from(created);
    }

    @Transactional(readOnly = true)
    public ReservationReadResponse find(UUID ownerId, UUID id) {
        var authorizedAt=reservations.ownedReadTime(id,ownerId).orElseThrow(() ->
                new ApiException(HttpStatus.NOT_FOUND, "RESERVATION_NOT_FOUND", "Reservation not found"));
        return cache.reservation(id,ownerId,authorizedAt, () -> reservations.observedOwned(id,ownerId).orElseThrow(() ->
                new ApiException(HttpStatus.NOT_FOUND, "RESERVATION_NOT_FOUND", "Reservation not found")));
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, rollbackFor = Exception.class, noRollbackFor = ApiException.class)
    public ReservationResponse confirm(UUID ownerId, UUID id) {
        return transition(ownerId, id, ReservationStatus.CONFIRMED);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, rollbackFor = Exception.class, noRollbackFor = ApiException.class)
    public ReservationResponse cancel(UUID ownerId, UUID id) {
        return transition(ownerId, id, ReservationStatus.CANCELLED);
    }

    private ReservationResponse transition(UUID ownerId, UUID id, ReservationStatus target) {
        // The API first claims its idempotency key. Shared business order: one existing reservation, then its resource if releasing capacity.
        // ExpirationProcessor uses the same order; hold creation never locks an existing reservation.
        var row = reservations.lockOwned(id, ownerId).orElseThrow(() ->
                new ApiException(HttpStatus.NOT_FOUND, "RESERVATION_NOT_FOUND", "Reservation not found"));
        var decidedAt = reservations.decisionTime();
        if (!ReservationLifecycle.requiresTransition(row.status(), target, row.expiresAt(), decidedAt)) {
            return ReservationResponse.from(row);
        }
        var updated = reservations.transition(id, target, decidedAt);
        if (target == ReservationStatus.CANCELLED) reservations.release(row.resourceId(), row.quantity(), decidedAt);
        outbox.append(updated);
        invalidation.afterCommit(updated.resourceId(),updated.id());
        return ReservationResponse.from(updated);
    }

    @Transactional(readOnly = true)
    public ReservationPage list(UUID callerId, UUID requestedOwner, int limit, String cursorText) {
        if (!callerId.equals(requestedOwner)) {
            throw new ApiException(HttpStatus.FORBIDDEN, "FORBIDDEN", "This operation is not permitted");
        }
        var rows = reservations.listOwned(callerId, limit + 1, ReservationCursor.decode(cursorText));
        boolean hasMore = rows.size() > limit;
        var page = rows.stream().limit(limit).map(ReservationResponse::from).toList();
        String next = hasMore ? new ReservationCursor(page.getLast().createdAt(), page.getLast().id()).encode() : null;
        return new ReservationPage(page, next);
    }
}
