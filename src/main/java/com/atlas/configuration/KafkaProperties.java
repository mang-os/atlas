package com.atlas.configuration;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("atlas.kafka")
public record KafkaProperties(@DefaultValue("false") boolean enabled,
                              @DefaultValue("localhost:9092") @NotBlank String bootstrapServers,
                              @DefaultValue("true") boolean publisherEnabled,
                              @DefaultValue("true") boolean consumerEnabled,
                              @DefaultValue("1000") @Min(100) @Max(60000) int pollMs,
                              @DefaultValue("20") @Min(1) @Max(100) int batchSize) { }
