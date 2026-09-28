package com.atlas.messaging;

import com.atlas.configuration.KafkaMessagingConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "atlas.kafka", name = "enabled", havingValue = "true")
public class ReservationAuditListener {
    private final ReservationEventDecoder decoder;
    private final ReservationAuditService audit;
    private final MeterRegistry meters;
    private final ConsumerFailures failures;

    public ReservationAuditListener(ReservationEventDecoder decoder, ReservationAuditService audit, MeterRegistry meters, ConsumerFailures failures) {
        this.decoder = decoder; this.audit = audit; this.meters = meters; this.failures=failures;
    }

    @KafkaListener(id = "reservationAudit", topics = KafkaMessagingConfiguration.TOPIC, groupId = KafkaMessagingConfiguration.GROUP,
            containerFactory = "reservationAuditContainerFactory", autoStartup = "${atlas.kafka.consumer-enabled:true}")
    public void onRecord(ConsumerRecord<String,String> record, Acknowledgment acknowledgment) {
        failures.check(record); // A blocked retry never runs the business effect or acknowledges.
        java.util.UUID eventId=null;
        String category="INVALID_ENVELOPE";
        try {
            var event=decoder.decode(record.key(),record.value());
            eventId=event.eventId();
            category="EFFECT_FAILED";
            boolean applied=audit.apply(event,record.value()); // Proxy returns only after DB commit.
            meters.counter("atlas.audit.events","outcome",applied?"applied":"duplicate").increment();
            failures.resolved(record); // Bookkeeping failure also leaves the Kafka offset unacknowledged.
            category="ACK_FAILED";
            acknowledgment.acknowledge();
        } catch (RuntimeException failure) {
            // Independent transaction AFTER the effect transaction ended. Never logs payload/exception messages.
            failures.failed(record,eventId,category);
            meters.counter("atlas.audit.failures","category",category).increment();
            throw new IllegalStateException("Consumer record failed category="+category);
        }
    }
}
