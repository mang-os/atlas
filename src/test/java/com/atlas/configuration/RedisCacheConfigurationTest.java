package com.atlas.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.assertj.core.api.Assertions.assertThat;

class RedisCacheConfigurationTest {
    private final ApplicationContextRunner runner=new ApplicationContextRunner().withUserConfiguration(RedisCacheConfiguration.class)
            .withInitializer(context -> context.getEnvironment().getPropertySources()
                    .remove(org.springframework.core.env.StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME));
    @Test void validatesDefaults() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            var properties=context.getBean(RedisCacheProperties.class);
            assertThat(properties.enabled()).isFalse();
            assertThat(properties.connectTimeoutMs()).isEqualTo(100);
            assertThat(properties.commandTimeoutMs()).isEqualTo(100);
            assertThat(properties.fillBudgetMs()).isEqualTo(1000);
        });
    }
    @ParameterizedTest
    @ValueSource(strings={"port=0","port=65536","connect-timeout-ms=9","connect-timeout-ms=1001","command-timeout-ms=0",
            "command-timeout-ms=1001","fill-budget-ms=0","fill-budget-ms=1001","fill-budget-ms=abc","host="})
    void rejectsUnboundedOrInvalidConfiguration(String assignment) {
        runner.withPropertyValues("atlas.redis."+assignment).run(context -> assertThat(context).hasFailed());
    }
}
