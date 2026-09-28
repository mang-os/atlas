package com.atlas.configuration;

import com.atlas.messaging.ConsumerFailures;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.Scheduled;

/** Single-topic/partition local consumer. Exhaustion stops the whole container; no offset is skipped. */
@Configuration(proxyBeanMethods=false)
@ConditionalOnProperty(prefix="atlas.kafka",name={"enabled","consumer-enabled"},havingValue="true")
public class ConsumerRecoveryScheduling {
 private final ConsumerFailures failures;
 private final KafkaListenerEndpointRegistry registry;
 public ConsumerRecoveryScheduling(ConsumerFailures failures,KafkaListenerEndpointRegistry registry) { this.failures=failures;this.registry=registry; }
 @Scheduled(scheduler="outboxScheduler",fixedDelayString="${atlas.kafka.poll-ms:1000}",initialDelayString="${atlas.kafka.poll-ms:1000}")
 public void poll() {
  var container=registry.getListenerContainer("reservationAudit");
  if (container!=null && !container.isRunning() && failures.canResume()) container.start();
 }
}
