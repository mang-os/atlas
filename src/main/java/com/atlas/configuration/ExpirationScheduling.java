package com.atlas.configuration;

import com.atlas.reservation.ExpirationWorker;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(prefix = "atlas.expiration", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ExpirationScheduling {
    private static final Logger LOG = LoggerFactory.getLogger(ExpirationScheduling.class);
    private final ExpirationWorker worker;
    private final MeterRegistry meters;

    public ExpirationScheduling(ExpirationWorker worker, MeterRegistry meters) { this.worker = worker; this.meters = meters; }

    @Scheduled(fixedDelayString = "${atlas.expiration.poll-ms:1000}", initialDelayString = "${atlas.expiration.poll-ms:1000}")
    public void poll() {
        try {
            var result = worker.runBatch();
            if (result.expired() > 0) LOG.info("Expiration batch expired={} skipped={} failed={}", result.expired(), result.skipped(), result.failed());
        } catch (RuntimeException failure) {
            meters.counter("atlas.expiration.discovery.failures").increment();
            LOG.warn("Expiration discovery failed category={}", failure.getClass().getSimpleName());
            // No in-memory queue is lost. A later fixed-delay poll rediscovers committed overdue rows.
        }
    }
}
