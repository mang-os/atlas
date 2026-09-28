package com.atlas.reservation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReservationStatusTest {
    @Test
    void onlyHeldCanTransitionToOneOfTheThreeTerminalStates() {
        for (var source : ReservationStatus.values()) {
            for (var target : ReservationStatus.values()) {
                boolean allowed = source == ReservationStatus.HELD && target != ReservationStatus.HELD;
                assertThat(source.canTransitionTo(target)).as("%s -> %s", source, target).isEqualTo(allowed);
            }
            assertThat(source.canTransitionTo(null)).isFalse();
        }
    }
}
