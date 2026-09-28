package com.atlas.reconciliation;

import java.util.concurrent.ThreadLocalRandom;
import com.atlas.configuration.RecoveryProperties;
import org.springframework.stereotype.Component;

/** Equal jitter: half of the exponential cap through the cap inclusive, never a zero-delay retry. */
@Component
public class RetryPolicy {
 private final RecoveryProperties properties;
 public RetryPolicy(RecoveryProperties properties) { this.properties=properties; }
 public int maxAttempts() { return properties.maxAttempts(); }
 public long cap(int failureNumber) {
  if (failureNumber<1) throw new IllegalArgumentException("Failure number must be positive");
  long cap=properties.baseDelayMs();
  for (int i=1;i<failureNumber && cap<properties.maxDelayMs();i++) cap=Math.min(cap*2,properties.maxDelayMs());
  return cap;
 }
 public long delayMs(int failureNumber) {
  long cap=cap(failureNumber);
  return ThreadLocalRandom.current().nextLong(cap/2,cap+1);
 }
}
