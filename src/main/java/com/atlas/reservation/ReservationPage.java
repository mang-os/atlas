package com.atlas.reservation;

import java.util.List;

public record ReservationPage(List<ReservationResponse> items, String nextCursor) {
    public ReservationPage { items = List.copyOf(items); }
}
