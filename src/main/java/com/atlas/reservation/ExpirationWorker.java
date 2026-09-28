package com.atlas.reservation;

import com.atlas.configuration.ExpirationProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Discovery holds no mutation locks; the separate processor proxy owns each item's transaction. */
@Component
public class ExpirationWorker {
    private static final Logger LOG = LoggerFactory.getLogger(ExpirationWorker.class);
    private final ReservationRepository reservations;
    private final ExpirationProcessor processor;
    private final ExpirationProperties properties;
    private final MeterRegistry meters;

    public ExpirationWorker(ReservationRepository reservations, ExpirationProcessor processor,
                            ExpirationProperties properties, MeterRegistry meters) {
        this.reservations = reservations;
        this.processor = processor;
        this.properties = properties;
        this.meters = meters;
    }

    public BatchResult runBatch() {
        var candidates = reservations.dueCandidates(properties.batchSize());
        int expired = 0, skipped = 0, failed = 0;
        for (var id : candidates) {
            try {
                var result = processor.expire(id); // Returns only after the independent transaction commits.
                if (result.expired()) {
                    expired++;
                    meters.counter("atlas.expiration.attempts", "outcome", "expired").increment();
                    meters.timer("atlas.expiration.decision.lag").record(result.decisionLag());
                } else {
                    skipped++;
                    meters.counter("atlas.expiration.attempts", "outcome", "skipped").increment();
                }
            } catch (RuntimeException failure) {
                failed++;
                meters.counter("atlas.expiration.attempts", "outcome", "failed").increment();
                LOG.warn("Expiration failed reservationId={} category={}", id, failure.getClass().getSimpleName());
                // Failed item's transaction has ended; other candidates can progress. The row stays eligible next poll.
            }
        }
        return new BatchResult(candidates.size(), expired, skipped, failed);
    }

    public record BatchResult(int candidates, int expired, int skipped, int failed) { }
}
