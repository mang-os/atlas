package com.atlas.reservation;

import java.time.Instant;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class ExpirationBoundaryTest {
    @ParameterizedTest
    @CsvSource({"-1000,false", "0,true", "1000,true"})
    void dueAtEqualityUsingSuppliedDatabaseDecisionTime(long nanos, boolean due) {
        Instant deadline = Instant.parse("2026-09-27T12:00:00Z");
        assertThat(ExpirationProcessor.isDue(ReservationStatus.HELD, deadline, deadline.plusNanos(nanos))).isEqualTo(due);
    }

    @ParameterizedTest
    @EnumSource(value = ReservationStatus.class, names = {"CONFIRMED", "CANCELLED", "EXPIRED"})
    void terminalStateNeverExpiresAgain(ReservationStatus status) {
        assertThat(ExpirationProcessor.isDue(status, Instant.EPOCH, Instant.EPOCH.plusSeconds(10))).isFalse();
    }
}
