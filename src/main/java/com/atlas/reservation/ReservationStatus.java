package com.atlas.reservation;

/** Persisted lifecycle values; confirmation retains allocated capacity. */
public enum ReservationStatus {
    HELD,
    CONFIRMED,
    CANCELLED,
    EXPIRED;

    /** Describes the state graph only; locks, ownership and deadline checks belong to transactions. */
    public boolean canTransitionTo(ReservationStatus target) {
        return this == HELD && (target == CONFIRMED || target == CANCELLED || target == EXPIRED);
    }
}
