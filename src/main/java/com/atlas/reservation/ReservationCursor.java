package com.atlas.reservation;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import com.atlas.common.ApiException;
import org.springframework.http.HttpStatus;

/** Opaque pagination position only. Ownership is always enforced independently in the SQL query. */
record ReservationCursor(Instant createdAt, UUID id) {
    String encode() {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(
                (createdAt.toString() + "|" + id).getBytes(StandardCharsets.UTF_8));
    }

    static ReservationCursor decode(String encoded) {
        if (encoded == null) { return null; }
        try {
            if (encoded.isEmpty() || encoded.length() > 180) { throw new IllegalArgumentException(); }
            String[] fields = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8).split("\\|", -1);
            if (fields.length != 2) { throw new IllegalArgumentException(); }
            var cursor = new ReservationCursor(Instant.parse(fields[0]), UUID.fromString(fields[1]));
            if (!cursor.encode().equals(encoded)) { throw new IllegalArgumentException(); }
            // Bound cursor timestamps to supported API years (1-9999) and stored microsecond precision.
            if (cursor.createdAt().getNano() % 1000 != 0 || cursor.createdAt().isBefore(Instant.parse("0001-01-01T00:00:00Z"))
                    || cursor.createdAt().isAfter(Instant.parse("9999-12-31T23:59:59.999999Z"))) {
                throw new IllegalArgumentException();
            }
            return cursor;
        } catch (IllegalArgumentException | java.time.DateTimeException exception) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "Invalid reservation cursor");
        }
    }
}
