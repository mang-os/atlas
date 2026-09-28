package com.atlas.reservation;

import java.time.Instant;
import com.atlas.common.ApiException;
import org.springframework.http.HttpStatus;

/** Evaluated only while holding the owned reservation lock, using a post-lock database time. */
final class ReservationLifecycle {
    private ReservationLifecycle() { }

    static boolean requiresTransition(ReservationStatus state, ReservationStatus target, Instant expiresAt, Instant decisionTime) {
        if (target != ReservationStatus.CONFIRMED && target != ReservationStatus.CANCELLED) {
            throw new IllegalArgumentException("Unsupported request transition");
        }
        // Repeat success is independent of the old hold deadline, and produces no new write.
        if (state == target) return false;
        if (state == ReservationStatus.EXPIRED || (state == ReservationStatus.HELD && !decisionTime.isBefore(expiresAt))) {
            throw new ApiException(HttpStatus.CONFLICT, "RESERVATION_EXPIRED", "The reservation hold has expired");
        }
        if (state != ReservationStatus.HELD) {
            throw new ApiException(HttpStatus.CONFLICT, "INVALID_STATE_TRANSITION", "This operation is not valid for the reservation state");
        }
        return true;
    }
}
