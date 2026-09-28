package com.atlas.configuration;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("atlas.hold")
@Validated
public record HoldProperties(@DefaultValue("300") @Min(1) @Max(3600) int ttlSeconds) {}
