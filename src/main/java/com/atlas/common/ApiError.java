package com.atlas.common;

import java.util.List;

/** Public error contract. Never include rejected values, SQL or authentication credentials. */
public record ApiError(String code, String message, String requestId, List<FieldError> details) {
    public record FieldError(String field, String reason) {}
}
