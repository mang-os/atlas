package com.atlas.reservation;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import com.atlas.outbox.OutboxWriter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Internal worker operation; no public HTTP route. Each call commits or rolls back one reservation. */
@Service
public class ExpirationProcessor {
    private final ReservationRepository reservations;

    private final OutboxWriter outbox;
    private final com.atlas.common.CacheInvalidation invalidation;

    public ExpirationProcessor(ReservationRepository reservations, OutboxWriter outbox, com.atlas.common.CacheInvalidation invalidation) {
        this.reservations = reservations;
        this.outbox = outbox;
        this.invalidation=invalidation;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED, rollbackFor = Exception.class)
    public Result expire(UUID id) {
        return expireInternal(id,false);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED, rollbackFor = Exception.class)
    public Result expireVerified(UUID id) { return expireInternal(id,true); }

    private Result expireInternal(UUID id,boolean verified) {
        var locked = reservations.lockForExpiration(id);
        if (locked.isEmpty()) return new Result(false, Duration.ZERO);
        var row = locked.get();
        // Discovery is only a hint. State and deadline are authoritative only after this lock.
        var decidedAt = reservations.decisionTime();
        if (!isDue(row.status(), row.expiresAt(), decidedAt)) return new Result(false, Duration.ZERO);
        if (verified && !reservations.integrityForExpiration(row.resourceId()))
            throw new IllegalStateException("Uncertain reservation/resource integrity; repair refused");
        var expired = reservations.transition(id, ReservationStatus.EXPIRED, decidedAt);
        reservations.release(row.resourceId(), row.quantity(), decidedAt);
        outbox.append(expired);
        invalidation.afterCommit(expired.resourceId(),expired.id());
        return new Result(true, Duration.between(row.expiresAt(), decidedAt));
    }

    static boolean isDue(ReservationStatus state, Instant expiresAt, Instant decidedAt) {
        return state == ReservationStatus.HELD && !decidedAt.isBefore(expiresAt);
    }

    public record Result(boolean expired, Duration decisionLag) { }
}
