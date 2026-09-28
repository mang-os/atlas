package com.atlas;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.atlas.common.ApiException;
import com.atlas.common.ReadCache;
import com.atlas.common.RedisCacheStore;
import com.atlas.configuration.LocalIdentityConfiguration;
import com.atlas.idempotency.IdempotentMutations;
import com.atlas.inventory.ResourceRepository;
import com.atlas.inventory.ResourceService;
import com.atlas.reservation.CreateReservationRequest;
import com.atlas.reservation.ExpirationProcessor;
import com.atlas.reservation.ReservationRepository;
import com.atlas.reservation.ReservationService;
import com.atlas.reservation.ReservationStatus;
import com.jayway.jsonpath.JsonPath;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.resource.DefaultClientResources;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real Redis/PostgreSQL, test-scoped pauses/ACLs/gates. Redis is never allocation or ownership authority. */
@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"local","test"})
@Testcontainers(disabledWithoutDocker=false)
@Timeout(60)
class CacheIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer(
            "postgres@sha256:639ab7ceb90e13123085b741fb31ef493fba25463002f6da665352e7b534b652")
            .withDatabaseName("atlas_cache_test").withUsername("atlas_test").withPassword("testcontainers-only");
    @Container static final GenericContainer<?> REDIS=new GenericContainer<>(
            "redis@sha256:0514fa59e3d84e2f3be66731c5401fc2a85b8ea71b49c0116c850f6a558076f4").withExposedPorts(6379);
    static final UUID ALICE=LocalIdentityConfiguration.ALICE_ID, BOB=LocalIdentityConfiguration.BOB_ID;
    static final String PASSWORD=UUID.randomUUID().toString();
    @Autowired JdbcTemplate jdbc;
    @Autowired ResourceRepository resourceRepository;
    @Autowired ResourceService resources;
    @Autowired ReservationRepository repository;
    @Autowired ReservationService reservations;
    @Autowired ExpirationProcessor expiration;
    @Autowired IdempotentMutations mutations;
    @Autowired ReadCache cache;
    @Autowired RedisCacheStore store;
    @Autowired MeterRegistry meters;
    @Autowired PlatformTransactionManager transactionManager;
    @LocalServerPort int port;
    private DefaultClientResources adminResources;
    private RedisClient adminClient;
    private StatefulRedisConnection<String,String> admin;
    private ExecutorService threads;
    private boolean paused;

    @DynamicPropertySource static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username",POSTGRES::getUsername);
        registry.add("spring.datasource.password",POSTGRES::getPassword);
        registry.add("spring.datasource.hikari.maximum-pool-size",()->10);
        registry.add("atlas.hold.ttl-seconds",()->3600);
        for (String user:List.of("alice","bob","admin")) registry.add("atlas.local."+user+"-password",()->PASSWORD);
        registry.add("atlas.redis.enabled",()->true);
        registry.add("atlas.redis.host",REDIS::getHost);
        registry.add("atlas.redis.port",()->REDIS.getMappedPort(6379));
    }
    @BeforeEach void reset() {
        adminResources=DefaultClientResources.builder().ioThreadPoolSize(2).computationThreadPoolSize(2).build();
        adminClient=RedisClient.create(adminResources,RedisURI.Builder.redis(REDIS.getHost(),REDIS.getMappedPort(6379)).withTimeout(Duration.ofSeconds(3)).build());
        admin=adminClient.connect();
        admin.sync().aclSetuser("default",new io.lettuce.core.AclSetuserArgs().allCommands());
        admin.sync().flushdb();
        for (String table:List.of("reservation_audit","processed_event","outbox_event","idempotency_record","reservation","resource")) jdbc.update("DELETE FROM "+table);
        threads=Executors.newVirtualThreadPerTaskExecutor();
        warm();
    }
    @AfterEach void close() {
        resume();
        if (threads!=null) threads.close();
        if (admin!=null) { admin.sync().aclSetuser("default",new io.lettuce.core.AclSetuserArgs().allCommands()); admin.close(); }
        if (adminClient!=null) adminClient.shutdown(0,1,TimeUnit.SECONDS);
        if (adminResources!=null) adminResources.shutdown(0,1,TimeUnit.SECONDS);
        conservation();
    }

    @Test void cachesOnlyDocumentedReadsAndPreservesObservationMetadata() throws Exception {
        UUID resource=fixture();
        var first=resources.availability(resource);
        var second=resources.availability(resource);
        assertThat(first.cached()).isFalse();
        assertThat(second.cached()).isTrue();
        assertThat(second.observedAt()).isEqualTo(first.observedAt());
        assertThat(admin.sync().pttl(ReadCache.availabilityKey(resource))).isBetween(1L,2000L);
        UUID id=hold(resource);
        var firstReservation=reservations.find(ALICE,id);
        var cached=reservations.find(ALICE,id);
        assertThat(firstReservation.cached()).isFalse();
        assertThat(cached.cached()).isTrue();
        assertThat(cached.observedAt()).isEqualTo(firstReservation.observedAt());
        assertThat(admin.sync().pttl(ReadCache.reservationKey(id))).isBetween(1L,5000L);
        var http=get(id,"alice");
        assertThat(http.statusCode()).isEqualTo(200);
        assertThat(http.headers().firstValue("Cache-Control")).contains("no-store");
        assertThat(JsonPath.<Boolean>read(http.body(),"$.cached")).isTrue();
        var mutation=mutations.confirm(ALICE,"confirm","request",id);
        assertThat(mutation.body()).doesNotContain("observedAt","cached");
        assertThat(reservations.list(ALICE,ALICE,20,null).items().getFirst().status()).isEqualTo(ReservationStatus.CONFIRMED);
        System.out.println("PHASE11 normal availabilityTtlMs<=2000 reservationTtlMs<=5000 observedAtPreserved=true readOnlyMetadata=true");
    }

    @Test void evictionIsAnOrdinaryDatabaseMiss() {
        UUID resource=fixture(), id=hold(resource);
        resources.availability(resource); reservations.find(ALICE,id);
        admin.sync().del(ReadCache.availabilityKey(resource),ReadCache.reservationKey(id));
        assertThat(resources.availability(resource).cached()).isFalse();
        assertThat(reservations.find(ALICE,id).cached()).isFalse();
        assertThat(resources.availability(resource).cached()).isTrue();
        assertThat(reservations.find(ALICE,id).cached()).isTrue();
    }

    @Test void postgresOwnershipIsCheckedBeforeEveryCacheHitIncludingAdminAndChangedOwner() throws Exception {
        UUID id=hold(fixture());
        assertThat(reservations.find(ALICE,id).cached()).isFalse();
        assertThat(get(id,"bob").statusCode()).isEqualTo(404);
        assertThat(get(id,"admin").statusCode()).isEqualTo(404);
        jdbc.update("UPDATE reservation SET user_id=? WHERE id=?",BOB,id); // Privileged fixture corruption challenges cached identity.
        assertThat(get(id,"alice").statusCode()).isEqualTo(404);
        var owner=reservations.find(BOB,id);
        assertThat(owner.userId()).isEqualTo(BOB);
        assertThat(owner.cached()).isFalse();
        assertThatThrownBy(()->reservations.list(ALICE,BOB,20,null)).isInstanceOf(ApiException.class);
    }

    @Test void invalidCachePayloadAndWrongRedisTypeFallBackToPostgres() {
        UUID resource=fixture(), id=hold(resource);
        admin.sync().set(ReadCache.availabilityKey(resource),"{broken",io.lettuce.core.SetArgs.Builder.px(2000));
        assertThat(resources.availability(resource).availableCapacity()).isEqualTo(9);
        admin.sync().set(ReadCache.availabilityKey(resource),"{\"resourceId\":\""+resource+"\",\"observedAt\":\"2026-09-28T00:00:00Z\",\"cached\":false}",io.lettuce.core.SetArgs.Builder.px(2000));
        var incomplete=resources.availability(resource);
        assertThat(incomplete.cached()).isFalse();
        assertThat(incomplete.availableCapacity()).isEqualTo(9);
        admin.sync().del(ReadCache.availabilityKey(resource));
        admin.sync().lpush(ReadCache.availabilityKey(resource),"wrong-type");
        assertThat(resources.availability(resource).cached()).isFalse();
        admin.sync().set(ReadCache.reservationKey(id),"{}",io.lettuce.core.SetArgs.Builder.px(5000));
        assertThat(reservations.find(ALICE,id).cached()).isFalse();
    }

    @Test void invalidationHappensAfterCommitAndClearsBothKeys() {
        UUID resource=fixture(), id=hold(resource);
        resources.availability(resource); reservations.find(ALICE,id);
        new TransactionTemplate(transactionManager).executeWithoutResult(status->{
            reservations.cancel(ALICE,id);
            assertThat(admin.sync().get(ReadCache.availabilityKey(resource))).isNotNull();
            assertThat(admin.sync().get(ReadCache.reservationKey(id))).isNotNull();
        });
        assertThat(admin.sync().get(ReadCache.availabilityKey(resource))).isNull();
        assertThat(admin.sync().get(ReadCache.reservationKey(id))).isNull();
        assertThat(resources.availability(resource).availableCapacity()).isEqualTo(10);
        assertThat(reservations.find(ALICE,id).status()).isEqualTo(ReservationStatus.CANCELLED);
    }

    @Test void rolledBackMutationDoesNotInvalidateOrChangeDatabaseState() {
        UUID resource=fixture(), id=hold(resource);
        resources.availability(resource); reservations.find(ALICE,id);
        String availability=admin.sync().get(ReadCache.availabilityKey(resource)), reservation=admin.sync().get(ReadCache.reservationKey(id));
        new TransactionTemplate(transactionManager).executeWithoutResult(status->{reservations.cancel(ALICE,id); status.setRollbackOnly();});
        assertThat(admin.sync().get(ReadCache.availabilityKey(resource))).isEqualTo(availability);
        assertThat(admin.sync().get(ReadCache.reservationKey(id))).isEqualTo(reservation);
        assertThat(jdbc.queryForObject("SELECT status FROM reservation",String.class)).isEqualTo("HELD");
        assertThat(jdbc.queryForObject("SELECT available_capacity FROM resource",Integer.class)).isEqualTo(9);
    }

    @Test void redisOutageCannotFailCommittedHoldReplayReadsOrHealth() throws Exception {
        UUID resource=fixture();
        pause();
        long before=System.nanoTime();
        var committed=mutations.hold(ALICE,"outage","request",new CreateReservationRequest(resource,1));
        UUID id=UUID.fromString(JsonPath.read(committed.body(),"$.id"));
        assertThat(committed.status()).isEqualTo(201);
        assertThat(resources.availability(resource).cached()).isFalse();
        assertThat(resources.availability(resource).availableCapacity()).isEqualTo(9);
        assertThat(reservations.find(ALICE,id).cached()).isFalse();
        var replay=mutations.hold(ALICE,"outage","retry",new CreateReservationRequest(resource,1));
        assertThat(replay.replayed()).isTrue();
        assertThat(replay.body()).isEqualTo(committed.body());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM reservation",Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_event",Long.class)).isEqualTo(1);
        assertThat(Duration.ofNanos(System.nanoTime()-before)).isLessThan(Duration.ofSeconds(4));
        var health=HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/actuator/health")).GET().build(),HttpResponse.BodyHandlers.ofString());
        assertThat(health.statusCode()).isEqualTo(200);
        assertThat(meters.get("atlas.cache.failure").tag("operation","invalidation").counter().count()).isPositive();
        resume(); warm();
        assertThat(resources.availability(resource).cached()).isFalse();
        assertThat(resources.availability(resource).cached()).isTrue();
        System.out.println("PHASE11 redisUnavailable committedHold=true exactReplay=true databaseFallback=true healthUP=true recovery=true");
    }

    @Test void staleAvailabilityCannotAuthorizeAllocationAndExpires() {
        UUID resource=fixture();
        resources.availability(resource);
        admin.sync().aclSetuser("default",new io.lettuce.core.AclSetuserArgs().removeCommand(io.lettuce.core.protocol.CommandType.DEL));
        hold(resource);
        admin.sync().aclSetuser("default",new io.lettuce.core.AclSetuserArgs().addCommand(io.lettuce.core.protocol.CommandType.DEL));
        warm();
        var stale=resources.availability(resource);
        assertThat(stale.cached()).isTrue();
        assertThat(stale.availableCapacity()).isEqualTo(10);
        assertThat(resources.find(resource).availableCapacity()).isEqualTo(9);
        assertThatThrownBy(()->reservations.create(ALICE,new CreateReservationRequest(resource,10)))
                .isInstanceOf(ApiException.class).satisfies(error->assertThat(((ApiException)error).code()).isEqualTo("INSUFFICIENT_CAPACITY"));
        expired(ReadCache.availabilityKey(resource),Duration.ofSeconds(3));
        assertThat(resources.availability(resource).availableCapacity()).isEqualTo(9);
        System.out.println("PHASE11 staleAvailability shown=10 postgres=9 rejectedQuantity=10 expiredWithinTtl=true");
    }

    @Test void staleHeldStateCannotAuthorizeCancellationAndExpires() {
        UUID resource=fixture(), id=hold(resource);
        reservations.find(ALICE,id);
        admin.sync().aclSetuser("default",new io.lettuce.core.AclSetuserArgs().removeCommand(io.lettuce.core.protocol.CommandType.DEL));
        reservations.confirm(ALICE,id);
        admin.sync().aclSetuser("default",new io.lettuce.core.AclSetuserArgs().addCommand(io.lettuce.core.protocol.CommandType.DEL)); warm();
        var stale=reservations.find(ALICE,id);
        assertThat(stale.cached()).isTrue();
        assertThat(stale.status()).isEqualTo(ReservationStatus.HELD);
        assertThatThrownBy(()->reservations.cancel(ALICE,id)).isInstanceOf(ApiException.class);
        assertThat(jdbc.queryForObject("SELECT status FROM reservation",String.class)).isEqualTo("CONFIRMED");
        assertThat(jdbc.queryForObject("SELECT available_capacity FROM resource",Integer.class)).isEqualTo(9);
        expired(ReadCache.reservationKey(id),Duration.ofSeconds(6));
        assertThat(reservations.find(ALICE,id).status()).isEqualTo(ReservationStatus.CONFIRMED);
        System.out.println("PHASE11 staleHeld shown=HELD postgres=CONFIRMED cancellationRejected=true retainedCapacity=true ttlRecovery=true");
    }

    @Test void heldTtlIsCappedAndAuthoritativeExpirationInvalidatesResourceAndReservation() {
        UUID resource=fixture(), id=hold(resource);
        jdbc.update("UPDATE reservation SET expires_at=clock_timestamp()+interval '800 milliseconds' WHERE id=?",id);
        reservations.find(ALICE,id);
        assertThat(admin.sync().pttl(ReadCache.reservationKey(id))).isBetween(1L,800L);
        expired(ReadCache.reservationKey(id),Duration.ofSeconds(2));
        resources.availability(resource);
        assertThatThrownBy(()->reservations.confirm(ALICE,id)).isInstanceOf(ApiException.class);
        assertThat(expiration.expire(id).expired()).isTrue();
        assertThat(admin.sync().get(ReadCache.availabilityKey(resource))).isNull();
        assertThat(reservations.find(ALICE,id).status()).isEqualTo(ReservationStatus.EXPIRED);
        assertThat(resources.availability(resource).availableCapacity()).isEqualTo(10);
    }

    @ParameterizedTest @ValueSource(strings={"availability","reservation"})
    void refillAfterInvalidationMayRestoreOldSnapshotButTtlBoundsIt(String kind) throws Exception {
        UUID resource=fixture(), id=hold(resource);
        CountDownLatch loaded=new CountDownLatch(1), release=new CountDownLatch(1);
        String key=kind.equals("availability")?ReadCache.availabilityKey(resource):ReadCache.reservationKey(id);
        var delayed=threads.submit(()->{
            if (kind.equals("availability")) return cache.availability(resource,()->{
                var row=resourceRepository.availability(resource).orElseThrow(); signal(loaded,release); return row;
            });
            return cache.reservation(id,ALICE,repository.decisionTime(),()->{
                var row=repository.observedOwned(id,ALICE).orElseThrow(); signal(loaded,release); return row;
            });
        });
        assertThat(loaded.await(5,TimeUnit.SECONDS)).isTrue();
        try {
            reservations.cancel(ALICE,id);
            assertThat(admin.sync().get(key)).isNull();
        } finally { release.countDown(); }
        delayed.get(5,TimeUnit.SECONDS);
        assertThat(admin.sync().get(key)).isNotNull();
        if (kind.equals("availability")) assertThat(resources.availability(resource).availableCapacity()).isEqualTo(9);
        else assertThat(reservations.find(ALICE,id).status()).isEqualTo(ReservationStatus.HELD);
        assertThat(jdbc.queryForObject("SELECT status FROM reservation",String.class)).isEqualTo("CANCELLED");
        assertThat(admin.sync().pttl(key)).isBetween(1L,kind.equals("availability")?2000L:5000L);
        expired(key,Duration.ofSeconds(kind.equals("availability")?3:6));
        if (kind.equals("availability")) assertThat(resources.availability(resource).availableCapacity()).isEqualTo(10);
        else assertThat(reservations.find(ALICE,id).status()).isEqualTo(ReservationStatus.CANCELLED);
        System.out.println("PHASE11 refillRace kind="+kind+" staleRestored=true ttlBounded=true authoritativeStateAndCapacityCorrect=true");
    }

    @ParameterizedTest @ValueSource(strings={"availability","reservation"})
    void fillsOlderThanOneSecondAreRejected(String kind) throws Exception {
        UUID resource=fixture(), id=hold(resource);
        CountDownLatch loaded=new CountDownLatch(1), release=new CountDownLatch(1);
        long began=redisTime();
        var delayed=threads.submit(()->kind.equals("availability")?
                cache.availability(resource,()->{var row=resourceRepository.availability(resource).orElseThrow();signal(loaded,release);return row;}):
                cache.reservation(id,ALICE,repository.decisionTime(),()->{var row=repository.observedOwned(id,ALICE).orElseThrow();signal(loaded,release);return row;}));
        assertThat(loaded.await(5,TimeUnit.SECONDS)).isTrue();
        try { await().atMost(Duration.ofSeconds(3)).until(()->redisTime()>began+1100); }
        finally { release.countDown(); }
        delayed.get(5,TimeUnit.SECONDS);
        String key=kind.equals("availability")?ReadCache.availabilityKey(resource):ReadCache.reservationKey(id);
        assertThat(admin.sync().get(key)).isNull();
    }

    @Test void serverRejectsLateWriteEvenWhenClientTimedOutAfterFetchingSnapshot() {
        UUID resource=fixture();
        long began=System.nanoTime();
        var result=cache.availability(resource,()->{var row=resourceRepository.availability(resource).orElseThrow();pause();return row;});
        assertThat(result.cached()).isFalse();
        assertThat(Duration.ofNanos(System.nanoTime()-began)).isLessThan(Duration.ofSeconds(2));
        try { await().atMost(Duration.ofSeconds(3)).until(()->System.nanoTime()-began>TimeUnit.MILLISECONDS.toNanos(1200)); }
        finally { resume(); }
        assertThat(admin.sync().get(ReadCache.availabilityKey(resource))).isNull();
        // Also prove the actual atomic script refuses an old token without relying on cancellation semantics.
        warm();
        var token=store.read(ReadCache.availabilityKey(resource),2000);
        var old=new RedisCacheStore.Lookup(null,token.redisTimeMs()-1100,true);
        assertThat(store.fill(ReadCache.availabilityKey(resource),"stale",old,2000,-1)).isFalse();
        assertThat(admin.sync().get(ReadCache.availabilityKey(resource))).isNull();
        System.out.println("PHASE11 lateServerWrite clientTimeoutFallback=true serverFillGuardRejected=true");
    }

    @Test void missingRowsAndOverdueHeldAreNeverNegativelyCached() {
        UUID missing=UUID.randomUUID();
        assertThatThrownBy(()->resources.availability(missing)).isInstanceOf(ApiException.class);
        assertThatThrownBy(()->reservations.find(ALICE,missing)).isInstanceOf(ApiException.class);
        assertThat(admin.sync().get(ReadCache.availabilityKey(missing))).isNull();
        assertThat(admin.sync().get(ReadCache.reservationKey(missing))).isNull();
        UUID resource=fixture(), id=hold(resource);
        jdbc.update("UPDATE reservation SET expires_at=created_at+interval '1 microsecond' WHERE id=?",id);
        assertThat(reservations.find(ALICE,id).status()).isEqualTo(ReservationStatus.HELD);
        assertThat(admin.sync().get(ReadCache.reservationKey(id))).isNull();
    }

    private UUID fixture() { return jdbc.queryForObject("INSERT INTO resource(name,total_capacity,available_capacity) VALUES ('Cache fixture',10,10) RETURNING id",UUID.class); }
    private UUID hold(UUID resource) { return reservations.create(ALICE,new CreateReservationRequest(resource,1)).id(); }
    private void warm() { await().atMost(Duration.ofSeconds(5)).until(()->store.read("phase11-warm",2000).available()); }
    private long redisTime() { var time=admin.sync().time(); return Long.parseLong(time.get(0))*1000+Long.parseLong(time.get(1))/1000; }
    private void expired(String key,Duration budget) { await().atMost(budget).until(()->admin.sync().get(key)==null); }
    private void pause() { REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec(); paused=true; }
    private void resume() { if (paused) { REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec(); paused=false; } }
    private void signal(CountDownLatch loaded,CountDownLatch release) {
        loaded.countDown();
        try { if (!release.await(5,TimeUnit.SECONDS)) throw new IllegalStateException("Cache test gate timed out"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
    private HttpResponse<String> get(UUID id,String user) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/api/reservations/"+id))
                .header("Authorization","Basic "+Base64.getEncoder().encodeToString((user+":"+PASSWORD).getBytes(StandardCharsets.UTF_8)))
                .timeout(Duration.ofSeconds(5)).GET().build(),HttpResponse.BodyHandlers.ofString());
    }
    private void conservation() {
        assertThat(jdbc.queryForObject("SELECT count(*) FROM resource s WHERE available_capacity+COALESCE((SELECT sum(quantity) FROM reservation r WHERE r.resource_id=s.id AND r.status IN ('HELD','CONFIRMED')),0)<>total_capacity",Long.class)).isZero();
    }
}
