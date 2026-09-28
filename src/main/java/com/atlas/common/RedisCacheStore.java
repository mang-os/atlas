package com.atlas.common;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import com.atlas.configuration.RedisCacheProperties;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.ConnectionFuture;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.resource.DefaultClientResources;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Optional, lazy shared connection. No Redis failure escapes into the authoritative transaction. */
@Component
@ConditionalOnProperty(prefix = "atlas.redis", name = "enabled", havingValue = "true")
public class RedisCacheStore implements AutoCloseable {
    private static final String READ = """
            local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000)
            return {redis.call('GET',KEYS[1]) or '', tostring(now), tostring(redis.call('PTTL',KEYS[1]))}
            """;
    // Redis checks elapsed time atomically with SET, even if the client pauses or its command times out.
    private static final String FILL = """
            local t=redis.call('TIME'); local now=t[1]*1000+math.floor(t[2]/1000)
            local elapsed=now-tonumber(ARGV[2])
            if elapsed<0 or elapsed>tonumber(ARGV[3]) then return 0 end
            local ttl=tonumber(ARGV[4]); local remaining=tonumber(ARGV[5])
            if remaining>=0 then ttl=math.min(ttl,remaining-elapsed) end
            if ttl<1 then return 0 end
            redis.call('SET',KEYS[1],ARGV[1],'PX',ttl); return 1
            """;
    private final RedisCacheProperties properties;
    private final MeterRegistry meters;
    private final DefaultClientResources resources;
    private final RedisClient client;
    private final RedisURI uri;
    private final AtomicReference<ConnectionFuture<StatefulRedisConnection<String,String>>> connection = new AtomicReference<>();

    public RedisCacheStore(RedisCacheProperties properties, MeterRegistry meters) {
        this.properties=properties; this.meters=meters;
        resources=DefaultClientResources.builder().ioThreadPoolSize(2).computationThreadPoolSize(2).build();
        uri=RedisURI.Builder.redis(properties.host(),properties.port()).withTimeout(Duration.ofMillis(properties.commandTimeoutMs())).build();
        client=RedisClient.create(resources,uri);
        client.setOptions(ClientOptions.builder().autoReconnect(false).requestQueueSize(32)
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .socketOptions(SocketOptions.builder().connectTimeout(Duration.ofMillis(properties.connectTimeoutMs())).build())
                .timeoutOptions(TimeoutOptions.enabled(Duration.ofMillis(properties.commandTimeoutMs()))).build());
        org.slf4j.LoggerFactory.getLogger(getClass()).info("Optional Redis cache enabled connectTimeoutMs={} commandTimeoutMs={} fillBudgetMs={}",
                properties.connectTimeoutMs(),properties.commandTimeoutMs(),properties.fillBudgetMs());
    }

    public record Lookup(String value, long redisTimeMs, boolean available) { }
    public Lookup read(String key, long maximumTtlMs) {
        try {
            List<Object> result=waitFor(connected().async().eval(READ,ScriptOutputType.MULTI,new String[]{key}));
            String value=result.get(0).toString();
            long ttl=Long.parseLong(result.get(2).toString());
            return new Lookup(!value.isEmpty() && ttl>0 && ttl<=maximumTtlMs ? value : null,
                    Long.parseLong(result.get(1).toString()),true);
        } catch (Exception failure) { failed("read",failure); return new Lookup(null,0,false); }
    }

    public boolean fill(String key, String value, Lookup lookup, long ttlMs, long remainingHoldMs) {
        if (!lookup.available()) return false;
        try {
            Long result=waitFor(connected().async().eval(FILL,ScriptOutputType.INTEGER,new String[]{key},value,
                    Long.toString(lookup.redisTimeMs()),Integer.toString(properties.fillBudgetMs()),
                    Long.toString(ttlMs),Long.toString(remainingHoldMs)));
            return result==1;
        } catch (Exception failure) { failed("fill",failure); return false; }
    }

    public void invalidate(String... keys) {
        try { waitFor(connected().async().del(keys)); meters.counter("atlas.cache.invalidation","outcome","success").increment(); }
        catch (Exception failure) { failed("invalidation",failure); }
    }

    private StatefulRedisConnection<String,String> connected() throws Exception {
        var pending=connection.get();
        if (pending==null || pending.toCompletableFuture().isCompletedExceptionally() || pending.isCancelled()
                || pending.isDone() && !pending.toCompletableFuture().getNow(null).isOpen()) {
            synchronized (connection) {
                if (connection.get()==pending) {
                    var created=client.connectAsync(StringCodec.UTF8,uri);
                    connection.set(created);
                    created.whenComplete((ready,failure) -> { if (ready!=null && connection.get()!=created) ready.closeAsync(); });
                }
                pending=connection.get();
            }
        }
        try { return pending.get(properties.connectTimeoutMs(),TimeUnit.MILLISECONDS); }
        catch (Exception failure) { discard(pending); throw failure; }
    }
    private <T> T waitFor(Future<T> future) throws Exception {
        try { return future.get(properties.commandTimeoutMs(),TimeUnit.MILLISECONDS); }
        catch (Exception failure) { future.cancel(true); throw failure; }
    }
    private void failed(String operation, Exception failure) {
        if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
        discard(connection.get());
        meters.counter("atlas.cache.failure","operation",operation).increment();
    }
    private void discard(ConnectionFuture<StatefulRedisConnection<String,String>> pending) {
        if (pending!=null && connection.compareAndSet(pending,null)) {
            if (pending.isDone() && !pending.toCompletableFuture().isCompletedExceptionally() && !pending.isCancelled()) pending.toCompletableFuture().getNow(null).closeAsync();
            else pending.cancel(true);
        }
    }
    @Override public void close() {
        discard(connection.get());
        client.shutdown(0,1,TimeUnit.SECONDS);
        resources.shutdown(0,1,TimeUnit.SECONDS);
    }
}
