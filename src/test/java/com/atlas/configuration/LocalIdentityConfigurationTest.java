package com.atlas.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class LocalIdentityConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(context -> context.getEnvironment().setActiveProfiles("local"))
            .withUserConfiguration(LocalIdentityConfiguration.class);

    @ParameterizedTest
    @ValueSource(strings = {"alice", "bob", "admin"})
    void startupFailsWhenAnyRequiredCredentialIsMissing(String missing) {
        runner.withPropertyValues(
                "atlas.local.alice-password=" + (missing.equals("alice") ? "" : "test-only-alice"),
                "atlas.local.bob-password=" + (missing.equals("bob") ? "" : "test-only-bob"),
                "atlas.local.admin-password=" + (missing.equals("admin") ? "" : "test-only-admin"))
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void rejectsBlankAndOverlongBcryptCredentialsWithoutPrintingTheirValues() {
        for (String password : new String[] {"   ", "x".repeat(73)}) {
            runner.withPropertyValues("atlas.local.alice-password=" + password,
                    "atlas.local.bob-password=test-only-bob", "atlas.local.admin-password=test-only-admin")
                    .run(context -> {
                        assertThat(context).hasFailed();
                        assertThat(context.getStartupFailure()).hasStackTraceContaining("atlas.local.alice-password");
                        assertThat(context.getStartupFailure().getMessage()).doesNotContain("x".repeat(73));
                    });
        }
    }
}
