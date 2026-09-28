package com.atlas.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class HoldConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner().withUserConfiguration(HoldConfiguration.class);

    @Test
    void defaultsToThreeHundredSeconds() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(HoldProperties.class).ttlSeconds()).isEqualTo(300);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "3600"})
    void acceptsDocumentedBounds(String value) {
        runner.withPropertyValues("atlas.hold.ttl-seconds=" + value).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(HoldProperties.class).ttlSeconds()).isEqualTo(Integer.parseInt(value));
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "3601", "abc", "1.5"})
    void rejectsInvalidDurationAtStartup(String value) {
        runner.withPropertyValues("atlas.hold.ttl-seconds=" + value).run(context -> assertThat(context).hasFailed());
    }
}
