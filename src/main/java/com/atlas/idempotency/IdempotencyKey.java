package com.atlas.idempotency;

import java.util.Collections;
import com.atlas.common.ApiException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;

public final class IdempotencyKey {
    private IdempotencyKey() {}

    public static String from(HttpServletRequest request) {
        var values = Collections.list(request.getHeaders("Idempotency-Key"));
        if (values.size() != 1 || !values.getFirst().matches("[A-Za-z0-9._:-]{1,128}")) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "INVALID_IDEMPOTENCY_KEY",
                    "Supply one Idempotency-Key containing 1 to 128 letters, digits, dots, underscores, colons or hyphens");
        }
        return values.getFirst();
    }
}
