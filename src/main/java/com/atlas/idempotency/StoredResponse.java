package com.atlas.idempotency;

import java.nio.charset.StandardCharsets;
import org.springframework.http.ResponseEntity;

/** JSON is serialized once, before commit; MVC writes bytes without serializing a second time. */
public record StoredResponse(int status, String body, String contentType, String location, boolean replayed) {
    public ResponseEntity<byte[]> toHttp() {
        var response = ResponseEntity.status(status).header("Content-Type", contentType);
        if (location != null) response.header("Location", location);
        if (replayed) response.header("Idempotency-Replayed", "true");
        return response.body(body.getBytes(StandardCharsets.UTF_8));
    }
}
