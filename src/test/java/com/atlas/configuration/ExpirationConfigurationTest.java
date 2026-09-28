package com.atlas.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class ExpirationConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner().withUserConfiguration(ExpirationConfiguration.class);

    @Test
    void defaultsEnableOneSecondPollWithOneHundredCandidates() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(ExpirationProperties.class)).isEqualTo(new ExpirationProperties(true, 1000, 100));
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"atlas.expiration.poll-ms=100", "atlas.expiration.poll-ms=60000",
            "atlas.expiration.batch-size=1", "atlas.expiration.batch-size=100", "atlas.expiration.enabled=false"})
    void acceptsBoundsAndDisable(String setting) {
        runner.withPropertyValues(setting).run(context -> assertThat(context).hasNotFailed());
    }

    @ParameterizedTest
    @ValueSource(strings = {"atlas.expiration.poll-ms=99", "atlas.expiration.poll-ms=60001",
            "atlas.expiration.poll-ms=abc", "atlas.expiration.batch-size=0", "atlas.expiration.batch-size=101",
            "atlas.expiration.batch-size=1.5", "atlas.expiration.enabled=perhaps"})
    void invalidConfigurationFailsStartup(String setting) {
        runner.withPropertyValues(setting).run(context -> assertThat(context).hasFailed());
    }
}
