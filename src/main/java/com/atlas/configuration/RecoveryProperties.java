package com.atlas.configuration;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("atlas.recovery")
public record RecoveryProperties(@DefaultValue("true") boolean enabled,
 @DefaultValue("10000") @Min(100) @Max(60000) int pollMs,
 @DefaultValue("100") @Min(1) @Max(100) int batchSize,
 @DefaultValue("60000") @Min(1000) @Max(3600000) int stuckMs,
 @DefaultValue("5") @Min(1) @Max(20) int maxAttempts,
 @DefaultValue("1000") @Min(100) @Max(60000) int baseDelayMs,
 @DefaultValue("60000") @Min(100) @Max(300000) int maxDelayMs) {
 @AssertTrue(message="maxDelayMs must be at least baseDelayMs")
 public boolean isDelayRangeValid() { return maxDelayMs>=baseDelayMs; }
}
