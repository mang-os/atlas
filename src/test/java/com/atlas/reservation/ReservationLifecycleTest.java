package com.atlas.reservation;

import java.time.Instant;
import com.atlas.common.ApiException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReservationLifecycleTest {
    private static final Instant DEADLINE = Instant.parse("2026-09-27T12:00:00Z");

    @ParameterizedTest
    @EnumSource(value = ReservationStatus.class, names = {"CONFIRMED", "CANCELLED"})
    void acceptsOneMicrosecondBeforeDeadline(ReservationStatus target) {
        assertThat(ReservationLifecycle.requiresTransition(ReservationStatus.HELD, target, DEADLINE,
                DEADLINE.minusNanos(1000))).isTrue();
    }

    @ParameterizedTest
    @EnumSource(value = ReservationStatus.class, names = {"CONFIRMED", "CANCELLED"})
    void rejectsExactDeadline(ReservationStatus target) {
        assertThatThrownBy(() -> ReservationLifecycle.requiresTransition(ReservationStatus.HELD, target, DEADLINE, DEADLINE))
                .isInstanceOfSatisfying(ApiException.class, e -> assertThat(e.code()).isEqualTo("RESERVATION_EXPIRED"));
    }

    @ParameterizedTest
    @EnumSource(value = ReservationStatus.class, names = {"CONFIRMED", "CANCELLED"})
    void rejectsOneMicrosecondAfterDeadline(ReservationStatus target) {
        assertThatThrownBy(() -> ReservationLifecycle.requiresTransition(ReservationStatus.HELD, target, DEADLINE,
                DEADLINE.plusNanos(1000)))
                .isInstanceOfSatisfying(ApiException.class, e -> assertThat(e.code()).isEqualTo("RESERVATION_EXPIRED"));
    }
}
