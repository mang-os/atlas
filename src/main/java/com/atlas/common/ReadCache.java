package com.atlas.common;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;
import com.atlas.configuration.RedisCacheProperties;
import com.atlas.inventory.AvailabilityResponse;
import com.atlas.reservation.ReservationReadResponse;
import com.atlas.reservation.ReservationResponse;
import com.atlas.reservation.ReservationStatus;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Informational snapshots only. All ownership/mutation/expiry decisions remain outside this helper in PostgreSQL. */
@Component
public class ReadCache {
    public static final long AVAILABILITY_TTL_MS=2000, RESERVATION_TTL_MS=5000;
    private final RedisCacheStore store;
    private final RedisCacheProperties properties;
    private final JsonMapper json;
    private final MeterRegistry meters;
    public ReadCache(ObjectProvider<RedisCacheStore> store, RedisCacheProperties properties, JsonMapper json, MeterRegistry meters) {
        this.store=store.getIfAvailable(); this.properties=properties; this.json=json; this.meters=meters;
    }
    public static String availabilityKey(UUID id) { return "atlas:v1:availability:"+id; }
    public static String reservationKey(UUID id) { return "atlas:v1:reservation:"+id; }
    public record ReservationSnapshot(ReservationResponse state, Instant observedAt) { }

    public AvailabilityResponse availability(UUID id, Supplier<AvailabilityResponse> database) {
        if (store==null) return database.get();
        var lookup=store.read(availabilityKey(id),AVAILABILITY_TTL_MS);
        long began=System.nanoTime();
        try {
            if (lookup.value()!=null) {
                var tree=json.readTree(lookup.value());
                var cached=tree!=null && tree.hasNonNull("resourceId") && tree.hasNonNull("availableCapacity")
                        && tree.hasNonNull("observedAt") && tree.hasNonNull("cached")
                        ? json.treeToValue(tree,AvailabilityResponse.class) : null;
                if (cached!=null && id.equals(cached.resourceId()) && cached.availableCapacity()>=0 && cached.observedAt()!=null) {
                    hit("availability");
                    return new AvailabilityResponse(id,cached.availableCapacity(),cached.observedAt(),true);
                }
            }
        } catch (RuntimeException malformed) { invalid("availability"); }
        var loaded=database.get(); // Domain/database errors propagate; never cache a rejection.
        try { if (withinBudget(began)) store.fill(availabilityKey(id),json.writeValueAsString(loaded),lookup,AVAILABILITY_TTL_MS,-1); }
        catch (RuntimeException ignored) { invalid("availability"); }
        return loaded;
    }

    public ReservationReadResponse reservation(UUID id, UUID owner, Instant databaseNow, Supplier<ReservationSnapshot> database) {
        if (store==null) { var row=database.get(); return ReservationReadResponse.from(row.state(),row.observedAt(),false); }
        var lookup=store.read(reservationKey(id),RESERVATION_TTL_MS);
        long began=System.nanoTime();
        try {
            if (lookup.value()!=null) {
                var cached=json.readValue(lookup.value(),ReservationSnapshot.class);
                if (valid(cached,id,owner) && (cached.state().status()!=ReservationStatus.HELD || databaseNow.isBefore(cached.state().expiresAt()))) {
                    hit("reservation");
                    return ReservationReadResponse.from(cached.state(),cached.observedAt(),true);
                }
            }
        } catch (RuntimeException malformed) { invalid("reservation"); }
        var loaded=database.get();
        try {
            long remaining=loaded.state().status()==ReservationStatus.HELD
                    ? Math.max(0,Duration.between(loaded.observedAt(),loaded.state().expiresAt()).toMillis()) : -1;
            if (withinBudget(began)) store.fill(reservationKey(id),json.writeValueAsString(loaded),lookup,RESERVATION_TTL_MS,remaining);
        } catch (RuntimeException ignored) { invalid("reservation"); }
        return ReservationReadResponse.from(loaded.state(),loaded.observedAt(),false);
    }
    private boolean valid(ReservationSnapshot cached, UUID id, UUID owner) {
        if (cached==null || cached.observedAt()==null || cached.state()==null) return false;
        var row=cached.state();
        return id.equals(row.id()) && owner.equals(row.userId()) && row.resourceId()!=null && row.quantity()>0
                && row.status()!=null && row.expiresAt()!=null && row.createdAt()!=null && row.updatedAt()!=null
                && (row.status()==ReservationStatus.HELD ? row.confirmedAt()==null && row.cancelledAt()==null && row.expiredAt()==null
                : row.status()==ReservationStatus.CONFIRMED ? row.confirmedAt()!=null && row.cancelledAt()==null && row.expiredAt()==null
                : row.status()==ReservationStatus.CANCELLED ? row.cancelledAt()!=null && row.confirmedAt()==null && row.expiredAt()==null
                : row.expiredAt()!=null && row.confirmedAt()==null && row.cancelledAt()==null);
    }
    private boolean withinBudget(long began) { return System.nanoTime()-began <= Duration.ofMillis(properties.fillBudgetMs()).toNanos(); }
    private void hit(String kind) { meters.counter("atlas.cache.read","kind",kind,"outcome","hit").increment(); }
    private void invalid(String kind) { meters.counter("atlas.cache.read","kind",kind,"outcome","invalid").increment(); }
}
