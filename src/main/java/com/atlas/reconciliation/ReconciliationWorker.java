package com.atlas.reconciliation;

import com.atlas.reservation.ExpirationProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;

/** Inspection transaction ends before each independent guarded repair. No snapshot-derived capacity write exists. */
@Service
public class ReconciliationWorker {
 private static final org.slf4j.Logger LOG=org.slf4j.LoggerFactory.getLogger(ReconciliationWorker.class);
 private final ReconciliationService checks;
 private final ExpirationProcessor expiration;
 private final MeterRegistry meters;
 public ReconciliationWorker(ReconciliationService checks,ExpirationProcessor expiration,MeterRegistry meters) {
  this.checks=checks;this.expiration=expiration;this.meters=meters;
 }
 public Result runBatch() {
  var before=checks.inspect();
  int expired=0,skipped=0,refused=0;
  for (var id:checks.overdueCandidates()) {
   try { if (expiration.expireVerified(id).expired()) expired++; else skipped++; }
   catch (RuntimeException failure) { refused++; LOG.warn("Reconciliation repair refused reservationId={} category={}",id,failure.getClass().getSimpleName()); }
  }
  meters.counter("atlas.reconciliation.repairs","outcome","expired").increment(expired);
  meters.counter("atlas.reconciliation.repairs","outcome","skipped").increment(skipped);
  meters.counter("atlas.reconciliation.repairs","outcome","refused").increment(refused);
  return new Result(before,expired,skipped,refused);
 }
 public record Result(ReconciliationService.Report before,int expired,int skipped,int refused) { }
}
