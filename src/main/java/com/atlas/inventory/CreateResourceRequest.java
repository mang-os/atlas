package com.atlas.inventory;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/** Only fields clients may supply. IDs, capacity counters and timestamps are server controlled. */
public record CreateResourceRequest(
        @NotBlank(message = "must not be blank") @Size(max = 120, message = "must be at most 120 characters") @Pattern(regexp = "[^\\x00]*", message = "must not contain NUL characters") String name,
        @NotNull(message = "is required") @Positive(message = "must be a positive integer") Integer totalCapacity) {
    public CreateResourceRequest {
        if (name != null) { name = name.strip(); }
    }
}
