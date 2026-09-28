package com.atlas.configuration;
import com.atlas.reconciliation.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
class RecoveryConfigurationTest {
 private final ApplicationContextRunner runner=new ApplicationContextRunner().withUserConfiguration(RecoveryConfiguration.class)
  .withInitializer(context->context.getEnvironment().getPropertySources().remove(org.springframework.core.env.StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME));
 @Test void defaultsAndEqualJitterAreBoundedThroughSaturation() {
  runner.run(context->{
   assertThat(context).hasNotFailed();var properties=context.getBean(RecoveryProperties.class);var retry=new RetryPolicy(properties);
   assertThat(properties.enabled()).isTrue();assertThat(retry.maxAttempts()).isEqualTo(5);
   for (int attempt=1;attempt<=30;attempt++) {
    long cap=Math.min(1000L*(1L<<Math.min(attempt-1,20)),60000L);
    assertThat(retry.cap(attempt)).isEqualTo(cap);
    for(int sample=0;sample<100;sample++) assertThat(retry.delayMs(attempt)).isBetween(cap/2,cap);
   }
   assertThatThrownBy(()->retry.delayMs(0)).isInstanceOf(IllegalArgumentException.class);
  });
 }
 @ParameterizedTest @ValueSource(strings={"max-attempts=0","max-attempts=21","base-delay-ms=99","base-delay-ms=60001","max-delay-ms=99","max-delay-ms=300001","max-delay-ms=500","poll-ms=99","poll-ms=60001","batch-size=0","batch-size=101","stuck-ms=999","stuck-ms=3600001"})
 void invalidBoundsFailStartup(String assignment) { runner.withPropertyValues("atlas.recovery."+assignment).run(context->assertThat(context).hasFailed()); }
}
