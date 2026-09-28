package com.atlas.reservation;

import java.util.UUID;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/** Owner, deadline, state and ID are server controlled, never input fields. */
public record CreateReservationRequest(
        @NotNull(message = "is required") UUID resourceId,
        @NotNull(message = "is required") @Positive(message = "must be a positive integer") Integer quantity) {}
