package com.atlas.configuration;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("atlas.expiration")
public record ExpirationProperties(@DefaultValue("true") boolean enabled,
                                   @DefaultValue("1000") @Min(100) @Max(60000) int pollMs,
                                   @DefaultValue("100") @Min(1) @Max(100) int batchSize) { }
