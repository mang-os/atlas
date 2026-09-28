package com.atlas.configuration;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("atlas.redis")
public record RedisCacheProperties(@DefaultValue("false") boolean enabled,
        @DefaultValue("localhost") @NotBlank String host,
        @DefaultValue("6379") @Min(1) @Max(65535) int port,
        @DefaultValue("100") @Min(10) @Max(1000) int connectTimeoutMs,
        @DefaultValue("100") @Min(10) @Max(1000) int commandTimeoutMs,
        @DefaultValue("1000") @Min(1) @Max(1000) int fillBudgetMs) { }
