package com.atlas;

import com.atlas.common.RedisCacheStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;
import static org.assertj.core.api.Assertions.assertThat;

/** Checks that the opt-in outage regression actually attempted Redis invalidation and used its fallback. */
public class RedisOutageEvidence extends AbstractTestExecutionListener {
    @Override public void afterTestClass(TestContext test) {
        var context=test.getApplicationContext();
        var environment=context.getEnvironment();
        if (environment.getProperty("atlas.redis.enabled",Boolean.class,false)
                && environment.getProperty("atlas.redis.port",Integer.class,6379)==1) {
            assertThat(context.getBeansOfType(RedisCacheStore.class)).hasSize(1);
            var failures=context.getBean(MeterRegistry.class).find("atlas.cache.failure").tag("operation","invalidation").counter();
            assertThat(failures).isNotNull();
            assertThat(failures.count()).isPositive();
            System.out.println("PHASE11 RedisUnavailable coreSuite="+test.getTestClass().getSimpleName()
                    +" enabled=true unreachablePort=1 invalidationFallbackCount="+failures.count());
        }
    }
}
