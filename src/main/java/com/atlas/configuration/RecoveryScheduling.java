package com.atlas.configuration;

import com.atlas.reconciliation.ReconciliationWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration(proxyBeanMethods=false)
@EnableScheduling
@ConditionalOnProperty(prefix="atlas.recovery",name="enabled",havingValue="true",matchIfMissing=true)
public class RecoveryScheduling {
 private static final Logger LOG=LoggerFactory.getLogger(RecoveryScheduling.class);
 private final ReconciliationWorker worker;
 public RecoveryScheduling(ReconciliationWorker worker) { this.worker=worker; }
 @Scheduled(fixedDelayString="${atlas.recovery.poll-ms:10000}",initialDelayString="${atlas.recovery.poll-ms:10000}")
 public void poll() {
  try {
   var result=worker.runBatch();
   if (!result.before().counts().isEmpty() || result.refused()>0)
    LOG.warn("Reconciliation counts={} expired={} skipped={} refused={}",result.before().counts(),result.expired(),result.skipped(),result.refused());
  } catch (RuntimeException failure) { LOG.warn("Reconciliation unavailable category={}",failure.getClass().getSimpleName()); }
 }
}
