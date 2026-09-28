package com.atlas.configuration;

import com.atlas.outbox.OutboxPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "atlas.kafka", name = {"enabled", "publisher-enabled"}, havingValue = "true")
public class OutboxScheduling {
    private static final Logger LOG = LoggerFactory.getLogger(OutboxScheduling.class);
    private final OutboxPublisher publisher;
    private final KafkaProperties properties;
    private final MeterRegistry meters;

    public OutboxScheduling(OutboxPublisher publisher, KafkaProperties properties, MeterRegistry meters) {
        this.publisher = publisher; this.properties = properties; this.meters = meters;
    }

    @Scheduled(scheduler = "outboxScheduler", fixedDelayString = "${atlas.kafka.poll-ms:1000}", initialDelayString = "${atlas.kafka.poll-ms:1000}")
    public void poll() {
        for (int i = 0; i < properties.batchSize(); i++) {
            try {
                if (!publisher.publishOne()) return;
                meters.counter("atlas.outbox.publication", "outcome", "published").increment();
            } catch (RuntimeException failure) {
                meters.counter("atlas.outbox.publication", "outcome", "failed").increment();
                LOG.warn("Outbox publication failed category={}; inspect durable event schedule/status", failure.getClass().getSimpleName());
                return; // Durable per-event schedule controls eligibility; a later poll can process other rows.
            }
        }
    }
}
