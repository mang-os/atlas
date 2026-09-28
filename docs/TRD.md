# Atlas - Technical Requirements and Decisions

Current implementation and final local review/handoff cover Phases 1-15, with unresolved/deferred items explicitly listed. [Final evidence and limitations](VERIFICATION.md#phase-15). Historical phase sections retain the changes made at that phase; current behavior is summarized below and in README. Requirements/source authority: [PRD](PRD.md).

## Architecture and stack

Requirement [U/S]: Java 21, Maven, Spring Boot, PostgreSQL, Redis, Kafka, Docker Compose, Flyway, JUnit, Testcontainers, and k6. Phase 1 includes Spring Web, Validation, Data JPA, and Actuator under `com.atlas`. Redis and Kafka start behind optional Compose profiles.

Decision [D]: the target is one modular Spring Boot application. HTTP handlers, transaction services, expiration, outbox and optional publisher/audit consumer, cache and guarded reconciliation are implemented through Phase 12. Packages: inventory, reservation, idempotency, outbox, messaging, reconciliation, configuration, common. Controllers use request/response DTOs; entities never form the public API. Use explicit SQL for conditional capacity updates and locking where JPA abstractions obscure behavior. Prevent stale JPA entities from overwriting changes made by native updates.

```text
Authenticated client -> REST API -> PostgreSQL transaction
                                  | resource + reservation
                                  | idempotency + transactional outbox
                                  v
[Implemented, opt-in] Redis <- bounded cache reads / post-commit invalidation

[Phase 10, opt-in] PostgreSQL outbox -> publisher -> Kafka -> consumer
                                        -> PostgreSQL marker + audit effect
Expiration (implemented) ---------------> PostgreSQL
Reconciliation (implemented) ----------------> PostgreSQL
```

PostgreSQL decides allocation, ownership, lifecycle, idempotency, and expiration. Kafka and Redis never participate in the reservation transaction. Kafka outage accumulates durable outbox work; Redis outage falls back to PostgreSQL. PostgreSQL failure blocks new durable mutations and must not produce false success.

## Version gate

Phase 1 selected **Spring Boot 4.1.1** from the official stable documentation and resolved its parent/BOM from Maven Central. The official requirements accept Java 17-26 and Maven >=3.6.3; Atlas deliberately compiles/runs on Java 21 and wraps Maven 3.9.11. The supported 4.1 line was released in June 2026; support policy and compatibility references were checked on 2026-09-27. Compilation/package and all three real-PostgreSQL infrastructure tests passed on Java 21. PostgreSQL 17.11, Flyway 12.4.0 and Hibernate 7.4.5.Final initialized successfully together.

| Component | Pinned version / policy |
| --- | --- |
| Java language/runtime major | 21; Maven Enforcer rejects other majors |
| Host runtime used for verification | Microsoft OpenJDK 21.0.12.1+ 1-LTS |
| Spring Boot parent, starters and packaging plugin | 4.1.1 |
| Maven / official wrapper | 3.9.11 / 3.3.4; distribution SHA-256 recorded in wrapper properties |
| Flyway core and PostgreSQL module | 12.4.0, via Boot BOM |
| PostgreSQL JDBC | 42.7.13, via Boot BOM |
| Hibernate ORM | 7.4.5.Final, via Boot BOM |
| JUnit Jupiter | 6.0.3, via Boot BOM; this uses the Jupiter API, not an override to JUnit 5 |
| Testcontainers core, Jupiter extension, PostgreSQL | 2.0.5, via Boot BOM |
| Maven Enforcer | 3.6.3, managed by Boot parent |
| Surefire / Failsafe | 3.5.6 / 3.5.6, managed by Boot parent |
| Compiler / Resources / JAR plugins | 3.15.0 / 3.5.0 / 3.5.1, managed by Boot parent |
| PostgreSQL image | postgres:17.11-bookworm plus verified immutable digest |
| Redis optional image | redis:8.4.7-alpine3.22 plus verified immutable digest |
| Kafka optional image | apache/kafka:4.3.1 plus verified immutable digest |
| Application runtime image | eclipse-temurin:21.0.12.1_1-jre-noble plus verified immutable digest |
| Compose CLI verification | Initial syntax check:2.39.4; actual startup/build: Docker Desktop Compose 5.4.0 |
| Docker engine used | 29.7.2, Linux amd64 on Docker Desktop 4.88.1 |
| k6 | 2.3.0; immutable image digest in [load toolchain](../scripts/load/toolchain.json), introduced in Phase 14 |

No dependency version overrides are required; the fixed parent resolves exact versions. The authoritative [resolved dependency tree](VERIFICATION.md#phase-1), [effective plugin versions](VERIFICATION.md#phase-1), and [registry digests](VERIFICATION.md#phase-1) accompany the build. A rejected draft runtime tag is retained in the manifest-check evidence; only the subsequently verified tag is used. All selected images pulled successfully; PostgreSQL, Redis and Kafka passed their health checks. The application image built and ran as UID/GID 10001 and served minimal HTTP health. These are startup checks, not Kafka/Redis business integration. Reverify optional integration compatibility in Phases 10/11.

Official references checked on 2026-09-27:

- [Spring Boot system requirements](https://docs.spring.io/spring-boot/system-requirements.html) and [4.1 release announcement](https://spring.io/blog/2026/06/10/spring-boot-4/).
- [Spring Boot support policy](https://github.com/spring-projects/spring-boot/wiki/Supported-Versions) and [managed dependencies](https://docs.spring.io/spring-boot/appendix/dependency-versions/coordinates.html).
- [Flyway startup integration](https://docs.spring.io/spring-boot/how-to/data-initialization.html) requires the Flyway starter plus the PostgreSQL-specific module.
- [Maven wrapper](https://maven.apache.org/tools/wrapper/) and [Testcontainers PostgreSQL module](https://java.testcontainers.org/modules/databases/postgres/).
- [PostgreSQL support](https://www.postgresql.org/support/versioning/), [Redis official images](https://hub.docker.com/_/redis), [Kafka image quickstart](https://kafka.apache.org/quickstart/), and [Temurin official image manifest](https://github.com/docker-library/official-images/blob/master/library/eclipse-temurin).

### Phase 1 implementation boundaries

`pom.xml` uses Boot 4's `spring-boot-starter-webmvc` and `spring-boot-starter-flyway` names. Maven `verify` binds Failsafe; `InfrastructureIT` is mandatory and never disabled without Docker. There are no unit/business tests yet, so `test` alone is not acceptance. An explicit `-DskipTests package` checks compilation/packaging only.

`application.yml` provides shared bounds and safe migration settings; `application-local.yml` adds local URL/user defaults but no password default. The test profile is test-resource-only and receives isolated Testcontainers connection properties. Flyway's production location has a README and no SQL; a repeatable test-only probe verifies the migration pipeline without taking Phase 2's V1 number. JPA validates, never generates schema. Actual PostgreSQL connectivity, migration execution/validation, JPA initialization and live health are verified. Hikari now executes `SET TIME ZONE 'UTC'` after creating each JDBC connection; the driver otherwise used the JVM time zone despite the earlier startup options. `management.endpoint.health.probes.enabled=false` keeps the exact minimal response and prevents Boot 4.1 probe groups from appearing. A negative startup probe against an intentionally closed port exited with a connection error, not a false healthy startup.

Only health is exposed, without components/details. Domain authentication and restricted metrics are Phase 3; no domain endpoints exist in Phase 1. The host API and published container ports bind loopback by default. No production profile is supplied. Redis/Kafka have optional Compose profiles and no application client integration. The optional app container packages the already-built JAR and runs as UID/GID 10001.

Windows verification required the process-local setting `-Djdk.net.unixdomain.tmpdir=S:\Atlas\.tools\java-sockets`; a standalone NIO selector probe reproduced the default temporary-directory failure and passed with this setting. IPv4 preference alone failed. The underlying Windows temporary-path cause was not further narrowed; no OS/global JDK settings were changed. Containerized Linux Java needed no such setting. See [final verification evidence](VERIFICATION.md#phase-1).

Configuration details and exact startup commands are maintained in [README](../README.md). Proposed future variables below do not imply an implemented feature.

## Decision register

| ID | Ambiguity | Adopted planning decision |
| --- | --- | --- |
| D1 | Hold duration unspecified in S | 300 seconds, configured by `ATLAS_HOLD_TTL_SECONDS`, positive integer bounded to 1-3600; persisted deadline never changes when configuration changes |
| D2 | Identify versus authenticate | Local-only HTTP Basic via Spring Security; stable configured UUID principals, USER and ADMIN roles; no request userId as authority |
| D3 | Replay status examples differ | Preserve original status and serialized body, including 201 on create replay; add replay header; U's original-result contract takes priority over S's suggested 200 replay |
| D4 | Confirmed allocation lifetime | Retained permanently in this version, as explicitly required by U Prompt 2 |
| D5 | Deadline boundary | Database time sampled after acquiring the reservation lock; valid iff `decisionTime < expires_at`; equality is expired |
| D6 | Lock order | Idempotency claim first if present, then one existing reservation, then its resource, then new outbox row; never lock an existing reservation while holding its resource |
| D7 | Event ordering | Logical per-reservation sequence; no global or strict arrival order promise; consumers tolerate duplicates and reordering |
| D8 | Separate CapacityReleased event | Omit it; cancellation/expiration events carry releasedQuantity, avoiding two events for one release |
| D9 | Idempotency retention | Retain full committed responses for project lifetime; no automatic purge or key reuse in v1 |
| D10 | Cache scope | Resource availability and reservation status DTOs only; TTL 2 seconds and at most 5 seconds respectively |
| D11 | Reservation/account scope | One resource per reservation; configured local users; no accounts table or dynamic user provisioning |
| D12 | Repeated lifecycle commands | Same terminal operation returns 200 current state, no new event/release; incompatible terminal operation returns 409 |

## Phase 2 database foundation (implemented)

`V1__resource_and_reservation.sql` creates only resource/reservation, their named row CHECKs, UUID keys, restrictive relationship and three reservation indexes. UUIDs default to PostgreSQL generation; insert timestamps default to `statement_timestamp()`. Future mutation services must explicitly set the sampled post-lock decision timestamp and increment the resource version where appropriate. At Phase 2 there were no update triggers or lifecycle/capacity services; Phase 4 now adds the hold transaction below.

Internal Resource/Reservation records and ReservationStatus model persistence-independent snapshots and the allowed transition graph. These are not public DTOs or JPA entities; dedicated adapters/DTOs arrive with the corresponding API phases. SchemaIT tests production SQL directly against PostgreSQL, while InfrastructureIT regresses application startup/Flyway/JPA initialization/health with V1 present. See [schema enforcement boundary](BACKEND_SCHEMA.md#phase-2-implementation-boundary) and [executed evidence](VERIFICATION.md#phase-2). Pinned dependency/container versions are unchanged; no Redis/Kafka integration was introduced.

## Phase 3 resource/identity implementation

ResourceController maps dedicated DTOs to a transactional ResourceService and JdbcTemplate repository; it never returns persistence entities or internal records as the API. Native INSERT RETURNING uses V1 defaults for IDs/timestamps. Availability uses one PostgreSQL statement with clock_timestamp() and never mutates capacity. No new migration was needed.

Spring Boot's security starter is pinned to 4.1.1 by the parent and resolves Spring Security **7.1.1**, as recorded in [dependency evidence](VERIFICATION.md#phase-3). The implementation uses stateless HTTP Basic, explicit USER/ADMIN rules, BCrypt strength 10, disabled form login/sessions/request cache, and fresh UserDetails objects so credential erasure does not destroy the configured hashes. Local identity runs only with `local`; all three environment passwords are nonblank, at most 72 UTF-8 bytes, with no default. Invalid credentials fail startup. In other profiles a rejecting UserDetailsService suppresses Boot's generated user, Basic authentication is disabled, and resource requests are denied.

Stable identity mapping: alice=`11111111-1111-4111-8111-111111111111` (USER), bob=`22222222-2222-4222-8222-222222222222` (USER), admin=`33333333-3333-4333-8333-333333333333` (USER+ADMIN). These immutable IDs live in LocalIdentityConfiguration and survive password changes/restarts. There is no users table. Authentication produces LocalPrincipal with UUID; headers/body userId never supply identity. Phase 4 now uses the authenticated UUID for hold creation and owned reservation SQL/listing checks.

The local scheme is for localhost CLI development, with no TLS/production provider, registration/recovery, persisted accounts or rate limiting. CSRF protection is disabled for this CLI API, not a supported browser credential workflow; form login, sessions and cross-origin browser access are not enabled. The optional app container binds publicly inside the container but Compose publishes only on loopback. Credentials are hashes in memory after startup; environment secrets still require private local handling and rotation requires restart.

Strict Jackson 3 parsing rejects extra/duplicate properties, trailing tokens, scalar coercion and float-to-int conversion. An explicit textual coercion rule is necessary because numeric-to-string coercion otherwise survived disabling ALLOW_COERCION_OF_SCALARS. Names use strip() and bean validation (nonblank/max 120 UTF-16 code units); capacity is required/positive Integer. The database's VARCHAR length is code-point based, so the public validation is deliberately a stricter bound for supplementary characters.

RequestIdFilter runs before security and generates UUIDs; ApiErrorWriter and advice share one structured error record. Public minimal health remains; other Actuator routes require admin and metrics stay unexposed. This changes anonymous metrics from Phase 1's 404 to 401; authenticated USER gets403 and admin404. InfrastructureIT was updated for that intentional permission boundary and tests nonlocal access denial.

Official [Basic authentication](https://docs.spring.io/spring-security/reference/servlet/authentication/passwords/basic.html), [authentication architecture](https://docs.spring.io/spring-security/reference/servlet/authentication/architecture.html), and Boot [Jackson support](https://docs.spring.io/spring-boot/reference/features/json.html) / [customizer API](https://docs.spring.io/spring-boot/api/java/org/springframework/boot/jackson/autoconfigure/JsonMapperBuilderCustomizer.html) were checked on 2026-09-27; local resolved APIs and compilation/tests verified compatibility. See [Phase 3 evidence](VERIFICATION.md#phase-3) for 106 integration + 5 unit tests and current limits.

## Phase 4 atomic holds (implemented, intermediate)

ReservationService uses `@Transactional(isolation=READ_COMMITTED, rollbackFor=Exception.class)` around two JDBC operations. ReservationRepository.allocate performs a native conditional decrement and resource version/timestamp update. No SELECT precedes it. If it affects zero rows, a subsequent existence lookup distinguishes missing resource from insufficient capacity. Unsupported resource deletion/resizing is not introduced. On success, a separate INSERT uses a MATERIALIZED CTE to sample clock_timestamp() exactly once after allocation and derives creation/update/expiry timestamps from it. IDs are still V1 database defaults; no schema migration was changed or added.

HoldProperties binds `atlas.hold.ttl-seconds` from ATLAS_HOLD_TTL_SECONDS, default 300, startup validated1-3600. Startup/default/boundary tests pass; the integration suite uses 17 seconds to prove a configured value is persisted, rather than hardcoding five minutes. Request ownership comes exclusively from LocalPrincipal.id(). GET queries id+user_id; listing first requires path userId==callerId and then uses user_id-filtered `(created_at,id)` keyset SQL. Admin has no foreign-data bypass. Dedicated DTOs stay separate from internal snapshots/JDBC rows.

The test-only PostgreSQL trigger raises an insertion error after observing available 7/version 1 from the same transaction. A test sequence advances only on that observation (sequence effects are nontransactional), proving the fault occurred after the decrement. The returned 503 contains no SQL; capacity/version/updated_at then exactly match the pre-call snapshot and no reservation exists. A separate version-overflow fixture tests failed decrement with no insert. No fault hooks are in production code, migrations or HTTP routes.

Phase 4 does not implement lifecycle, expiration or idempotency. Retried POSTs allocate separately even with an identical Idempotency-Key, and overdue HELD rows remain allocated. Confirmed capacity remains retained by contract; no confirm/release logic was added. These sequential/rollback/ownership tests do not constitute Phase 5 concurrent allocation proof. See [Phase 4 evidence](VERIFICATION.md#phase-4) for 57 new integration cases and the full 163 integration + 13 unit gate.

Technical references rechecked 2026-09-27: PostgreSQL 17 [UPDATE](https://www.postgresql.org/docs/17/sql-update.html) / [database clock functions](https://www.postgresql.org/docs/17/functions-datetime.html), and Spring [transaction proxy behavior](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/tx-decl-explained.html) / [rollback rules](https://docs.spring.io/spring-framework/reference/7.1/data-access/transaction/declarative.html). Runtime evidence is the real PostgreSQL suite, not those documentation claims alone.

## Caller identity and access

D2 adds Spring Security in Phase 3. The `local` profile configures `alice`, `bob`, and `admin` with documented stable UUIDs and credentials supplied through local environment variables. The admin also has USER role for its own reservations. Startup fails if required local credentials are absent; no hardcoded fallback password. Password hashes are used in the authentication configuration, credentials are never logged, and generated secrets stay outside Git. HTTP Basic over plain HTTP is limited to localhost development; a production profile must not enable this scheme by default. There is no production auth implementation in scope.

All domain endpoints require authentication. Only ADMIN creates resources. USER/ADMIN may read resources and manage their own reservations. Admin has no implicit access to other users' reservations. Foreign-owned reservation IDs return 404; a user-list path targeting another identity returns 403. Health exposes minimal aggregate application/database status and incomplete-claim detection; detailed components are hidden and metrics are not exposed. The service identity, not a client-supplied role/header/body, determines permissions.

## Transactions, locks, and time

Allocate with a READ COMMITTED transaction containing an atomic conditional resource decrement and new reservation insert. An update affecting zero rows is distinguished as missing resource or insufficient capacity using a follow-up lookup; resource deletion/resizing is unsupported. An insertion failure aborts the whole transaction. No availability pre-read is authoritative.

Existing-reservation paths take its row lock, then sample database `clock_timestamp()` into one decision timestamp. Confirm before the deadline; cancel before the deadline; overdue requests return `RESERVATION_EXPIRED` without changing HELD state. The worker later materializes EXPIRED and restores capacity. Before Phase 7 there is no automatic recovery of overdue capacity; document this intermediate limitation.

Confirmation updates only the reservation. Cancellation/expiration lock the reservation before updating its resource. Hold creation locks the resource and inserts a fresh, externally invisible reservation; it does not lock existing reservations. Workers discover at most 100 candidate IDs, then process one reservation per transaction using `FOR UPDATE SKIP LOCKED`, rechecking status and time after the lock. Never hold locks on several resources in one worker transaction. This avoids cycles introduced by batches with different resource orders.

Use UTC instants and PostgreSQL TIMESTAMPTZ. For holds, sample database time after successful allocation and use it for createdAt and expiresAt. Deadline validity is decided at the post-lock check, not HTTP arrival or eventual commit time. A confirmation checked just before the deadline may commit after it; worker contention cannot reverse a committed terminal state.

Technical basis: PostgreSQL rechecks an UPDATE predicate after a concurrent updater commits, supporting the conditional decrement design. Its READ COMMITTED snapshots differ between statements. See [transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html). `CURRENT_TIMESTAMP` reflects transaction start; use the wall-clock database function for post-wait deadline checks. See [date/time functions](https://www.postgresql.org/docs/current/functions-datetime.html). `SKIP LOCKED` suits competing queue workers, not a complete inventory view; see [SELECT locking clauses](https://www.postgresql.org/docs/current/sql-select.html). These sources were checked during planning; the design still requires real tests.

## Durable idempotency, introduced in Phase 8

Every state-changing domain API, including resource creation, requires `Idempotency-Key`. Scope is `(caller_id, operation, key)`, where operation is a stable API-versioned name, not a concrete reservation ID. Canonical SHA-256 input includes operation, normalized typed body, and relevant path UUIDs. Reject unknown fields; canonical JSON uses fixed field ordering and normalized UUIDs/integers/trimmed names. Missing versus empty bodies is normalized explicitly per operation. Transport metadata, passwords, and request IDs are excluded.

After authentication, validation, and any ownership check, start a READ COMMITTED transaction. Attempt `INSERT ... ON CONFLICT DO NOTHING RETURNING ...` for the unique scope. An insert winner performs the mutation and stores the serialized response before committing. A loser reads the committed row in a subsequent statement: matching hash replays, differing hash returns 409. An uncommitted duplicate claim waits with a bounded timeout; return retryable 503 if that wait expires. If the winner rolls back, another contender may claim. Do not catch a unique-constraint exception inside an already aborted transaction and continue.

The claim and response never commit separately. Unexpected failures roll back the claim and mutation together. A savepoint may isolate a deliberately handled business rejection; never commit partial allocation. Store successful 2xx and authenticated, validated deterministic domain 404/409 results, including insufficient capacity and expired holds. Do not store malformed input, auth/ownership failures, key mismatch, contention timeout, or infrastructure 5xx. Retrying a stored capacity conflict returns that conflict even if capacity later becomes free; a new intention needs a new key.

Persist status, exact body bytes as UTF-8 text, content type, and Location. Replay preserves those and the body's original requestId if present; the HTTP `X-Request-Id` is new for tracing this attempt. Add `Idempotency-Replayed: true`. Authenticate and recheck access before replay. A saved HELD response may now be stale; GET gives current state. Retention is indefinite in v1, so no expiry race is introduced; document storage growth and prohibit manual cleanup without a new contract.

## Events and publication

Each reservation starts at transition sequence 1 (HELD); a terminal change increments it to 2. One outbox row per successful transition, unique `(aggregate_id, aggregate_sequence)`. Envelope: eventId, schemaVersion=1, eventType, aggregateType=RESERVATION, aggregateId, aggregateSequence, occurredAt, correlationId, and data with resourceId, userId, quantity, status, expiresAt, releasedQuantity. Released quantity is zero for hold/confirm and quantity for cancel/expire. No separate CapacityReleased event.

Phase 10 uses topic `atlas.reservation-events.v1`, key reservation UUID. For a simple demonstrator, publisher locks one due outbox row per transaction with SKIP LOCKED, sends with bounded broker-ack timeout, and marks PUBLISHED only after acknowledgment. A crash rolls back the database transaction and unlocks the row. Network ambiguity can produce duplicates; it cannot prove exactly-once transport. Holding a DB connection during send is an explicit throughput cost; isolate publisher concurrency and timeouts from API pool capacity.

Polling order is best effort. Even a shared Kafka key does not impose database sequence across independent publishers/retries. Logical sequence describes causality; physical arrival may be 2, 1, 1. The demonstrable consumer writes an immutable audit effect per event and a processed-event marker in one PostgreSQL transaction. It is order independent. Any future state projection must use sequence checks and gap handling before claiming ordered effects. There is no resource-wide event sequence.

Acknowledge a Kafka record only after the consumer DB commit. A crash before acknowledgment replays the event; the unique marker suppresses the second effect. Never advance a partition offset past uncommitted earlier records. Retain markers at least as long as events can be replayed; both are indefinite in v1.

## Caching, retry, and recovery

Phase 11 uses `atlas:v1:availability:{resourceId}` (2-second TTL) and `atlas:v1:reservation:{reservationId}` (5-second TTL, capped by remaining hold lifetime for HELD). Authenticate and verify reservation ownership against PostgreSQL before using a cached status. Stale cached terminal/HELD values may be shown only as informational status. Confirm/cancel and replay always use PostgreSQL. Post-commit invalidation is best effort and cannot turn committed success into HTTP failure.

Use 100 ms Redis connect/command budgets as proposed tunables. A failed read falls back to PostgreSQL. Cache refill racing invalidation can restore stale data: enforce a maximum 1-second fill budget and do not cache older fetched results. With working Redis expiry, staleness can extend up to fill budget plus TTL after a mutation; this is an application bound to test, not perfect coherence or a measured latency guarantee. Return observedAt and cached indicators. Do not cache business rejections.

Phase 12 adopts equal-jitter exponential retry: random integer delay from half the cap through `min(60s, 1s * 2^(attempt-1))`, with 5 recorded attempts by default, then FAILED. This replaces the planning proposal of full jitter/10 attempts; positive minimum waits and a smaller default budget bound repeated failures. Both caps and attempt limits are validated configuration. Phase 10 must already use bounded send/poll timeouts; Phase 12 adds durable scheduling, exhaustion, and controlled replay. Outbox crashes may leave a send uncounted when its transaction rolls back; attempt limits cover recorded failures, not unknowable network attempts. Replay resets scheduling for the same eventId and records operator reason; it never creates a replacement event.

Consumer failures keep the record unacknowledged. Persist retry/failure state separately after rolling back the failed effect transaction, stop the single-partition listener until nextAttemptAt, and leave it stopped on exhaustion for controlled recovery. Process sequentially per partition in v1. Restart consults durable failure records before attempting the effect. Poison records without a valid event ID are tracked by topic/partition/offset. No automatic skip or dead-letter acknowledgment silently loses the required effect.

Reconciliation detects overdue HELD rows, old pending/failed outbox rows, missing expected reservation events, and capacity drift. Capacity comparison uses one consistent SQL statement or a read-only REPEATABLE READ transaction. All persisted HELD rows count. Repair overdue holds through the integrity-checked locked expiry path. Publisher/consumer retry existing work independently; controlled replay of FAILED work is an operator action. Successful expiry invalidates cache after commit. Missing event history or capacity drift requires investigation; never synthesize unverifiable history or overwrite capacity from a stale snapshot. Long-lived outbox locks are reported; operators terminate a stuck worker through a documented procedure rather than taking over its live lock.

## Configuration and operations plan

Profiles: `local` (PostgreSQL required), `test` (isolated Testcontainers), optional Compose `redis`/`kafka`, production disabled until separately designed. Phase 1 writes exact executable configuration and `.env.example`; later phases add only their settings.

| Implemented variables | Purpose |
| --- | --- |
| JAVA_HOME, SERVER_PORT | Java installation and local HTTP listener |
| SPRING_DATASOURCE_URL, SPRING_DATASOURCE_USERNAME, SPRING_DATASOURCE_PASSWORD | Application DB connection; credentials outside Git |
| POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD | Local Compose database configuration |
| ATLAS_LOCAL_ALICE_PASSWORD, ATLAS_LOCAL_BOB_PASSWORD, ATLAS_LOCAL_ADMIN_PASSWORD | Local authentication setup in Phase 3 |
| ATLAS_HOLD_TTL_SECONDS, ATLAS_EXPIRATION_POLL_MS, ATLAS_EXPIRATION_BATCH_SIZE | Hold/worker policy; proposed polling 1000 ms, candidate limit 100 |
| ATLAS_KAFKA_ENABLED, SPRING_KAFKA_BOOTSTRAP_SERVERS | Optional publisher and consumer infrastructure |
| ATLAS_REDIS_ENABLED, ATLAS_REDIS_HOST, ATLAS_REDIS_PORT | Optional cache; SPRING_DATA_REDIS_HOST/PORT aliases accepted |
| ATLAS_RECOVERY_MAX_ATTEMPTS, ATLAS_RECOVERY_BASE_DELAY_MS, ATLAS_RECOVERY_MAX_DELAY_MS | Durable retry policy; the earlier ATLAS_RETRY_* proposal is not implemented |

Record request IDs, reservation/event IDs, outcomes, retry age, and sanitized errors. Do not log credentials or full idempotency bodies. Existing process-local meters cover expiry, publication, audit, cache and reconciliation outcomes. Only health is exposed; backlog/publication delay are sampled with SQL in Phase 14. Uniform success logging, JSON logs, request timers, metric export and tracing remain unresolved/deferred observability work, not an implemented endpoint contract. Bound DB locks, queries, connection acquisition, HTTP requests, and broker sends; Phase 1/each integration phase must record chosen budgets and verify fail-fast behavior.

## Phase 5 allocation concurrency verification (implemented)

AllocationConcurrencyIT uses actual HTTP requests against the unchanged Phase 4 application
and PostgreSQL 17.11. It has no enclosing test transaction, mocked repository or shared JDBC
connection for application requests. A client CyclicBarrier releases all attempts before
connection acquisition. A separate test session locks the resource until at least two distinct
PostgreSQL backend PIDs are observed executing allocation UPDATEs and waiting on locks.
This verifies actual transaction overlap, beyond simultaneous client scheduling.

A separate autocommit observer samples capacity and allocated sums in one statement snapshot.
Each sample and the final state must satisfy bounds and conservation; final successful IDs,
quantities, row count and resource version must match committed data. Only HTTP 409 with
INSUFFICIENT_CAPACITY is a capacity conflict. Unexpected 5xx, authentication errors, SQL errors,
connection failures, missing overlap and bounded-wait failures fail the test; there are no retries.

Test-only settings: pool maximum 32; connection acquisition 30s; PostgreSQL lock/statement
limits 20s/30s; JDBC connect/socket 5s/40s; HTTP connect/request 5s/60s; barrier and overlap
budgets 15s; batch completion 75s; each test 120s; worker shutdown 5s. Hold TTL is 3600s,
with no expiration worker. The observer uses separate connections so pool saturation cannot
prevent invariant checks or unlocking. These are correctness-test budgets, not production tuning.

The independent-resource case reserves pool headroom and requires resource B's 20 requests
to finish while A is still locked. The rollback case installs a test-only insertion trigger:
it observes a quantity-3 decrement, advances a nontransactional sequence, waits behind an
advisory gate while healthy requests contend, advances the sequence again and raises 23514.
The second marker proves the intentional exception site was reached. One expected 503 is
checked separately; healthy requests must still produce exactly 10 successes and 90 conflicts.
No test hook, migration, dependency, Redis client or Kafka client was added to production.

Seven cases passed, including three repeated capacity-10 rounds; full verify passed 170
integration and 13 unit tests. [Evidence](VERIFICATION.md#phase-5) records outcomes,
repeat policy, environment and limitations. Mocks cannot execute PostgreSQL row locking,
MVCC snapshots, UPDATE predicate rechecks or commit/rollback visibility. This finite suite
is evidence for these scenarios, not a mathematical proof of every schedule or a benchmark.

Official PostgreSQL 17 references checked on 2026-09-27: [READ COMMITTED behavior and
sequence rollback semantics](https://www.postgresql.org/docs/17/transaction-iso.html) and
[pg_stat_activity wait observations](https://www.postgresql.org/docs/17/monitoring-stats.html).

## Phase 6 confirmation and cancellation (implemented)

ReservationController now exposes owner-only POST confirm/cancel with an empty body. The
service uses READ COMMITTED/rollbackFor=Exception transactions. ReservationRepository selects
by ID and authenticated owner FOR UPDATE; a separate statement samples clock_timestamp only
after the lock returns. ReservationLifecycle applies the documented state/deadline matrix.
Same-target terminal repeats return the existing DTO without writes, irrespective of the old
deadline. HELD at equality or after expiry is rejected and remains allocated. Foreign IDs
return the same 404 as missing IDs; admin has no bypass.

Confirmation updates only status, confirmedAt and updatedAt. Cancellation updates status,
cancelledAt and updatedAt, then increments resource availability by quantity and resource
version by one, with the same sampled time. Both statements share the transaction. Release
failure propagates to roll back the terminal state too; capacity bounds are never clamped.
No lifecycle migration, new dependency or runtime configuration was needed.

The complete shared order is idempotency claim -> one existing reservation -> its
resource if releasing -> outbox. After the API key claim, confirmation needs only the reservation;
cancellation uses reservation then resource. A new hold updates a resource then inserts a
previously invisible reservation, never locking an existing reservation. Phase 7 workers
process one locked reservation per transaction in that same order without an API key.
No external network operation occurs while these transaction locks are held.

Deadline eligibility is based on the post-reservation-lock sample, not eventual commit.
The Phase 6 same-state policy remains distinct from Phase 8 stored-response replay. Current
create retries use the key contract, and Phase 7 workers reclaim overdue HELD capacity.
Exact microsecond-before/equal/after comparisons are tested directly against the production
policy. Real PostgreSQL HTTP tests separately prove requests waiting across the deadline
use the new post-lock time, with no mocked clock. See [Phase 6 evidence](VERIFICATION.md#phase-6).

## Phase 7 database expiration (implemented)

ExpirationProperties validates enabled=true, poll-ms=1000 (100-60000), batch-size=100 (1-100).
ExpirationScheduling uses Spring fixed-delay scheduling, including the same initial delay.
It is conditional on enabled, and calls ExpirationWorker without a surrounding transaction.
The worker discovers up to batchSize IDs ordered by expires_at/id using HELD and
expires_at <= statement_timestamp(), matching the existing partial index. It invokes the
separate ExpirationProcessor proxy for each ID; REQUIRES_NEW/READ_COMMITTED guarantees one
reservation transaction even if a future caller has an outer transaction.

The processor locks by ID FOR UPDATE SKIP LOCKED, then samples clock_timestamp() in a
separate statement. It explicitly rechecks state and decisionTime >= expiresAt, because a
candidate snapshot may be stale. It uses the shared repository transition and release paths:
EXPIRED/expired_at/updated_at and resource availability/version/update time commit together.
The common reservation -> resource order is unchanged. Worker authority is internal with no
HTTP route; caller ownership checks on public APIs remain unchanged.

Per-item RuntimeException failures are caught only outside the transaction proxy, logged
with reservation ID/category and counted, allowing subsequent candidates to commit. Discovery
failures are caught by the scheduling boundary and retried on a later scheduled invocation.
No claim/lease table, migration, Redis or Kafka dependency is required. Pending work survives
restart in HELD rows. Existing connection/statement/lock bounds still apply (production
pool acquisition 5s, statement 10s, lock 5s, JDBC socket 15s). Batch size bounds candidate
count, not total wall time. Fixed delay begins after the prior batch finishes.

Committed worker outcomes feed atlas.expiration.attempts by expired/skipped/failed;
discovery failures feed atlas.expiration.discovery.failures. The timer
atlas.expiration.decision.lag measures persisted decision time minus expiresAt only after
success. It excludes any later resource wait/commit delay. It is not completion-lag telemetry
or an SLO. Metrics remain in the registry with public exposure limited to health.

Expiration lag is expected: a deadline can pass before discovery, lock acquisition and
commit. During that interval, the persisted HELD row still counts in conservation. Backlog,
locks, failures, scheduling disablement and outages can make lag unbounded. A whole oldest
candidate batch occupied by long locks or repeated failures can postpone newer due work;
this phase does not promise fairness or implement the Phase 12 repair/retry ledger. A valid
pre-deadline request can commit later, while a worker skips its locked reservation. Terminal
states prevent repeated release. Redis notifications never decide or trigger authoritative
expiry.

The test profile disables automatic scheduling globally to preserve older overdue fixtures.
ExpirationIT invokes real proxied transactions and separately verifies scheduling enabled in
a newly started application context after closing a prior context against the same PostgreSQL
container. This is application-context/pool/scheduler restart evidence, not an OS kill or host
crash simulation. Exact microsecond boundary tests exercise the production predicate; race
and clock-wait tests use real PostgreSQL deadlines. [Evidence](VERIFICATION.md#phase-7)
records the test-side timestamp conversion failure and successful correction/reruns.

References checked 2026-09-27: [PostgreSQL 17 SELECT locking clauses](https://www.postgresql.org/docs/17/sql-select.html)
and [Spring scheduling](https://docs.spring.io/spring-framework/reference/integration/scheduling.html).

## Phase 8 implementation details

`IdempotentMutations` defines the four stable operations. Controllers authenticate and validate typed DTOs/path/body and require exactly one key header. `IdempotencyExecutor` owns a READ COMMITTED transaction spanning the unique claim, business services and response completion. `V2__durable_idempotency.sql` adds the composite primary key and response storage; V1 is unchanged. Expiration workers keep their existing independent transactions and need no API key.

Canonical v1 SHA-256 input is UTF-8 compact JSON of a fixed-order array, serialized by the pinned configured JsonMapper:

- `["v1.resource.create", strippedName, totalCapacity]`
- `["v1.reservation.create", lowercaseResourceUuid, quantity]`
- `["v1.reservation.confirm", lowercaseReservationUuid]`
- `["v1.reservation.cancel", lowercaseReservationUuid]`

The array represents validated typed fields, so JSON property order/whitespace and UUID letter case do not change the digest. String normalization is exactly existing Java strip(), with no Unicode normalization. Confirm/cancel have no body; zero bytes or absent body mean the same request, while any nonempty body is invalid. Reject unknown/duplicate fields as before. Caller is in the scope rather than the digest. Canonical encoding and existing operation names must remain stable across upgrades while their records are retained.

Authentication/roles run on every HTTP attempt. Existing-reservation ownership is checked without a lock before the claim; after claiming, the business path locks and checks the owned row again. Replay also checks the stored reservation reference. Owner is immutable through all v1 APIs. A missing path can produce a stored domain 404; a known foreign owner returns indistinguishable 404 but consumes no key. No admin bypass exists.

Business methods join the outer transaction. Expected ApiException rejections do not mark the shared transaction rollback-only; the executor rolls back a JDBC savepoint taken after the claim before storing an eligible domain rejection. Infrastructure exceptions escape and roll back everything. JdbcTemplate and JpaTransactionManager use the same transaction-bound connection; there are no managed entities to become stale after JDBC savepoint rollback. A serialization or response-update failure also rolls back the business effect. MVC gets a StoredResponse only after the transaction proxy commits and writes its already serialized UTF-8 bytes.

The persisted failure allowlist is RESOURCE_NOT_FOUND, RESERVATION_NOT_FOUND (missing, not foreign ownership), INSUFFICIENT_CAPACITY, RESERVATION_EXPIRED, INVALID_STATE_TRANSITION. Invalid requests, authentication/role/ownership failures, key mismatch and infrastructure failures are never newly persisted. Bound duplicate waits with the existing 5s PostgreSQL lock timeout, 10s statement timeout, 5s pool acquisition and 15s JDBC socket timeout; contention uses sanitized 503 TEMPORARILY_UNAVAILABLE. These are individual bounds, not an end-to-end SLA. Retry after backoff with the original key/request when the outcome is uncertain.

IdempotencyHealthIndicator uses an indexed EXISTS probe for committed incomplete claims. Health becomes DOWN without disclosing record contents; replay of such a claim returns retryable 503 and never takes over or mutates it. This is corruption detection, not automated repair. Ordinary uncommitted claims are invisible to the probe. Full responses remain retained indefinitely; no cleanup or claim-expiry setting is introduced. Restrict database access and investigate corruption before changing data.

Reviewed references: [PostgreSQL 17 INSERT / ON CONFLICT](https://www.postgresql.org/docs/17/sql-insert.html) and [Spring transaction rollback rules](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/rolling-back.html). Finite real-PostgreSQL race tests and evidence are in [Phase 8 evidence](VERIFICATION.md#phase-8); outbox creation is added in Phase 9; publication/cache behavior remains planned.

## Phase 9 transactional outbox implementation

V3 adds reservation.transition_sequence and outbox_event. The activation migration takes an ACCESS EXCLUSIVE reservation-table lock, then rejects any existing reservation row before changing schema. This closes the inspection/write race during the migration. Stop every old-version API/worker before migrating and start only the Phase 9 version afterward: mixed-version writers could otherwise create unrecorded history. The migration never deletes data, resets keys or backfills fabricated events. An empty reservation dataset may retain resource rows and their idempotency records. Existing historical reservations require a separately designed import or explicitly authorized reset of a known disposable database. Tests cover both the allowed upgrade and rejection of every existing status.

Architecture now implemented: HTTP authentication/validation -> scoped idempotency transaction -> reservation/capacity changes -> OutboxWriter -> serialized idempotency response -> PostgreSQL commit -> HTTP response. The expiration processor uses reservation -> resource -> outbox within its existing per-reservation transaction. OutboxWriter uses Spring MANDATORY propagation, so a detached call fails instead of opening a separate commit. No Kafka call, asynchronous listener, after-commit event write, publisher or extra dependency was added. The planned PostgreSQL outbox -> Kafka publisher -> consumer path remains Phase 10 work.

ReservationHeld is sequence 1. ReservationConfirmed, ReservationCancelled or ReservationExpired is sequence 2. State updates increment the stored sequence under the existing reservation lock. The database checks resulting state/sequence shape and uniquely constrains (aggregate_id,aggregate_sequence); application transactions enforce complete history. A same-terminal no-op, failed request or idempotent replay emits nothing. Resource creation is not a reservation transition and emits no event. Confirm retains allocated capacity.

The version 1 ReservationEvent envelope has eventId, schemaVersion, eventType, aggregateType, aggregateId, aggregateSequence, occurredAt, correlationId and data. Data includes resourceId, userId, quantity, status, expiresAt and releasedQuantity. A server UUID is generated once for a newly attempted event and retained in the committed row/payload. A rolled-back attempt has no durable event identity; a later successful attempt may use a new UUID. Once committed, a retry/restart preserves it. HTTP events carry the server X-Request-Id as correlationId; internal/worker transitions generate a separate correlation UUID. Replay creates neither a new event nor a new event correlation.

occurredAt and outbox created_at are the transition's sampled PostgreSQL decision time, not commit, publication or arrival time. Initial next_attempt_at equals that time. Events begin PENDING with attempt_count=0, published_at and last_error null. Phase 9 does not drain the backlog. The immutable JSONB envelope has semantic JSON equality, unlike the byte-preserving TEXT idempotency response; event consumers must not depend on object key order.

There is no CapacityReleased event. ReservationCancelled/ReservationExpired alone carry releasedQuantity=quantity; HELD/CONFIRMED carry zero. A future consumer deduplicates by eventId before applying a release, so it must not also infer a second release event. Sequence gives logical ordering only within one reservation; UUID order, timestamps, resource IDs and PENDING index order do not promise arrival order. Future publication may deliver sequence 2 before 1 and duplicates; no resource-wide or global order or exactly-once delivery is claimed.

Outbox envelopes are immutable through this application and retained indefinitely. Database administrators can still violate history through direct writes: constraints do not equate the JSON envelope with table values, ensure every reservation has its events, or enforce conservation. Integration tests validate envelope/table/business agreement and rollback; control database write access. Publisher/retry metadata updates, delivery evidence, retention changes and automated reconciliation remain later work.

Reviewed on 2026-09-28: [PostgreSQL 17 lock modes](https://www.postgresql.org/docs/17/explicit-locking.html) and [Spring transaction propagation](https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/tx-propagation.html). Actual verification belongs to [Phase 9 evidence](VERIFICATION.md#phase-9).

## Phase 10 publication and audit implementation

Architecture: reservation transaction -> committed outbox -> dedicated publisher transaction -> Kafka acknowledgment -> outbox PUBLISHED commit. Independently: Kafka record -> validated v1 envelope -> processed_event claim + reservation_audit insert -> PostgreSQL commit -> synchronous Kafka acknowledgment. HTTP mutation success never waits for Kafka. These are independent database/broker commits; there is no exactly-once delivery claim.

**Pins.** Boot 4.1.1's fixed BOM resolves Spring Kafka **4.1.1**, kafka-clients **4.2.1**, and Testcontainers Kafka **2.0.5**. Apache Kafka **4.3.1**, PostgreSQL **17.11** and their immutable image digests remain unchanged. MessagingIT exercises the real client/broker combination. Spring Kafka is an explicit dependency without the Boot Kafka starter; disabled messaging creates no producer/consumer/Admin clients. Java remains 21. References checked for this implementation: [manual acknowledgment](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html), [container-stopping error handler](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html), [producer configuration](https://kafka.apache.org/42/configuration/producer-configs/), [Testcontainers Kafka](https://java.testcontainers.org/modules/kafka/). Resolved versions and actual checks are in Phase 10 evidence.

**Publisher.** One READ COMMITTED / REQUIRES_NEW transaction selects one due PENDING row in `(next_attempt_at,created_at,event_id)` order, using `FOR UPDATE SKIP LOCKED`. Keep its row lock through the bounded send; send the persisted payload unchanged, keyed by reservation UUID. Set PUBLISHED/published_at and commit only after the send future reports acknowledgment. Failure after send but before DB commit can leave a broker record with a PENDING row; retry resends the same eventId. Producer idempotence helps within a producer session; it cannot close this DB/broker commit gap.

Each app has a dedicated two-connection Hikari publisher pool, with copied connection properties so the API/consumer pool is unaffected. One dedicated scheduler polls every 1000ms by default, up to 20 independent one-row transactions per poll; first failure ends the batch. A separate scheduler prevents broker waits from blocking expiration. Other publishers skip a locked row and can publish other rows. No PROCESSING lease exists: rollback/connection loss unlocks abandoned work. The database idle-in-transaction timeout bounds abandoned sessions; an ambiguous network result can still duplicate delivery.

| Individual bound | Value |
| --- | --- |
| Producer acks / idempotence / max in-flight | all / true / 5 |
| Producer metadata/buffer block / request / delivery / future wait | 2s / 2s / 5s / 6s; future wait starts after send returns |
| Publisher pool acquire / JDBC transaction | 2s / 12s |
| Publisher SQL statement / lock / idle-in-transaction | 10s / 5s / 15s |
| JDBC connect / socket | inherited 5s / 15s |
| Admin request / API / operation | 3s / 5s / 5s |
| Consumer poll / maximum poll interval / session | 500ms / 60s / 10s |
| Synchronous offset commit | 3s; zero additional application retries |

These are individual bounds, not a total SLA. Holding a DB connection and row lock during broker I/O is a deliberate throughput cost. The original Phase 10 publisher counted only committed publication. Phase 12, described below, also persists failed attempts, retry schedules, fixed error categories and exhaustion using a savepoint. Failed bookkeeping or connection loss can still leave a send uncounted. Process-local counters/log categories supplement the durable ledger.

**Consumer.** Strict string decoding validates required fields, v1 schema, key/aggregate identity, type/status/sequence, quantities/releases and deadlines. `ReservationAuditService.apply` uses a Spring proxy with REQUIRES_NEW/READ COMMITTED. An INSERT ON CONFLICT DO NOTHING RETURNING on `(event_id,consumer_name)` arbitrates duplicates, including uncommitted concurrent claims. The winner inserts the audit effect. A loser checks the committed audit JSONB in a subsequent statement/snapshot; missing effect or changed content for an existing eventId fails. Any effect/commit error rolls back both marker and effect. Acknowledgment follows the transaction proxy's successful return.

The container uses one record per poll and one listener thread, auto-commit disabled, MANUAL_IMMEDIATE and synchronous offset commits. No asynchronous processing can advance past an earlier failed record. CommonContainerStoppingErrorHandler stops on listener/DB/acknowledgment failure; no recoverer skips a record. After correcting the cause, restart the app to retry from the committed group offset. A failure after DB commit but before acknowledgment therefore replays safely. Stable consumer name `reservation-audit-v1` differs from group `atlas.reservation-audit.v1`; changing/resetting a group does not grant a second effect in the same database. An uninitialized group starts at earliest.

**Ordering and retention.** Topic `atlas.reservation-events.v1`: one partition, replication factor 1 locally, cleanup.policy=delete, retention.ms=-1, retention.bytes=-1. Broker auto-creation is disabled; KafkaAdmin or the README CLI creates the topic. A reservation key chooses its partition but does not impose PostgreSQL causal order across publishers/retries. Arrival can be 2,1,1. The append-only audit inserts one row per event and never updates reservation/capacity or a latest-state projection. Inspect `(reservation_id,aggregate_sequence)` for logical order. There is no resource/global sequence. No separate CapacityReleased exists; terminal events already carry releasedQuantity and the audit does not release capacity.

Outbox, markers, audit and local Kafka records are retained indefinitely; no purge job is added. Deleting markers while replay remains possible breaks deduplication. The named Compose kafka-data volume retains logs across recreation. A single local PLAINTEXT broker is not replicated durability/security. If Kafka is unavailable at startup, create the topic or restart the app after recovery. Failed consumers need operator correction/restart; automated recovery remains later work. Existing health alone is not a delivery/progress guarantee.

## Phase 11 optional cache implementation

Only resource availability and individual owned reservation GETs use cache-aside reads. Full resource lookup and reservation listings remain database observations. Mutation DTOs and stored replay bodies are unchanged; reservation GET adds observedAt/cached. PostgreSQL always checks ownership by reservation primary key and caller UUID before Redis access. Confirmation/cancellation, allocation, idempotency and expiration never consult cached state. A cached HELD/terminal value is informational; a cached HELD value is rejected if the ownership query's database time has reached its cached deadline.

Keys: atlas:v1:availability:{canonical-resource-UUID}, fixed 2000ms TTL; atlas:v1:reservation:{canonical-reservation-UUID}, fixed 5000ms TTL. HELD fills are additionally capped by the positive remaining lifetime measured from the database snapshot time. Overdue HELD and domain rejections are not cached. Each snapshot preserves its original PostgreSQL observedAt; cache hits set cached=true without refreshing TTL. Corrupt payloads, wrong IDs/owners/types and keys without a valid bounded TTL fall back to PostgreSQL. Cache contents never grant identity or mutation permission.

Lettuce **7.5.2.RELEASE** and Netty **4.2.17.Final** are pinned by Boot 4.1.1's BOM. Direct Lettuce integration avoids a Redis auto-configuration/health dependency when caching is disabled. Redis **8.4.7**, with the existing immutable image digest, is verified with GenericContainer and local Compose. No new PostgreSQL migration is needed. [Lettuce client options](https://github.com/redis/lettuce/wiki/Client-Options) and [Redis SET expiration](https://redis.io/docs/latest/commands/set/) describe the underlying client/TTL features; [Phase 11 evidence](VERIFICATION.md#phase-11) records actual compatibility and behavior.

Default Redis connect and command budgets are each **100ms**, configurable integer 10-1000ms. A lazy asynchronous shared connection has bounded waits, auto-reconnect disabled, disconnected commands rejected, queue limit 32 and two I/O/two completion threads. Failed/timed-out operations discard the connection for a later bounded reconnect attempt. No startup connection or optional Redis health indicator blocks PostgreSQL-backed operation. A lookup can spend one connect plus one command budget (200ms default); a successful miss may add another bounded fill operation. Invalidation is one DEL for both affected keys, with at most one connect/command wait. These are individual Redis waiting budgets, not a total HTTP/DB/scheduling SLA.

**Refill guard and staleness.** A Redis Lua lookup returns a snapshot and Redis TIME. A miss records that server time before the DB fetch. The client permits fill only within its monotonic fill budget (default/max 1000ms); the fill Lua script independently checks elapsed Redis time against the original token atomically with SET PX. Thus a client pause or queued command that exceeds the budget cannot install an old snapshot, even if the client already timed out. HELD TTL subtracts elapsed server time conservatively from the database-measured remaining lifetime. Negative elapsed time also rejects a fill. This relies on normally progressing Redis/DB clocks and working expiry; Redis is not authoritative time for business decisions.

A fetch racing a mutation may read the old snapshot, then commit invalidation may delete the key before that fetch fills it again. An accepted fill can occur up to 1s after its lookup and retain a 2s/5s TTL: stale entries can remain up to **3s availability / 6s reservation** after mutation, often less (HELD is capped). A hit fetched just before expiry can additionally spend up to the command-wait budget delivering that snapshot (100ms default). HTTP network delays, process suspension and authoritative expiration-worker lag have no freshness SLA. This is bounded storage under the stated clocks/expiry assumptions, not perfect coherence. Tests deliberately demonstrate stale refill and subsequent expiry; they reject delayed fills at both client and server boundaries.

**Invalidation.** Register a transaction synchronization only after successful creation/transition/event insertion. Resource creation clears its availability key; hold/confirm/cancel/expire clear resource availability and reservation keys after PostgreSQL commits. Outer idempotency/worker transactions govern that commit; rollback produces no invalidation. Redis errors after commit are contained and cannot turn durable success into failure. Replay and repeated terminal no-ops do not mutate or refresh the cache. A commit-to-invalidation failure/process crash leaves stale entries until expiry; a refill can still race successful invalidation. No distributed lease, version-coherence protocol or durable invalidation retry is introduced. Process-local cache hit/invalid/failure/invalidation counters support inspection; existing Actuator health remains database/application health rather than a Redis availability promise.

Optional enablement: ATLAS_REDIS_ENABLED=true or Spring profile redis. ATLAS_REDIS_HOST/PORT are preferred; SPRING_DATA_REDIS_HOST/PORT remain compatible aliases. Additional controls are ATLAS_REDIS_CONNECT_TIMEOUT_MS, ATLAS_REDIS_COMMAND_TIMEOUT_MS and ATLAS_REDIS_FILL_BUDGET_MS (1-1000). Compose app uses redis:6379, while hosts use the published port. Redis is disposable cache data with no persistence guarantee; outage/eviction/refill all fall back to PostgreSQL. Phase 12 reconciliation/retry behavior is described below. Stop before Phase 13.


## Phase 12 retries and reconciliation

V5 adds durable failure/replay metadata and consumer_failure. OutboxPublisher retains its one-row FOR UPDATE SKIP LOCKED transaction and dedicated pool. A savepoint after the row lock lets a failed broker send or PUBLISHED update roll back before recording a jittered schedule or FAILED state; the caller sees the failure only after that ledger commit. A database outage/crash can roll back bookkeeping and leave a send uncounted. Success still requires broker acknowledgment. Event IDs/payloads are never replaced and arrival order remains unconstrained.

ConsumerFailures records failures only after the audit transaction ends, scoped by consumer/topic/partition/offset. A length-prefixed SHA-256 of key/value rejects changed record identity; event_id is nullable for malformed envelopes. RETRY_WAIT gates redelivery using database time. FAILED prevents automatic restart; the single-partition listener stops without skipping/acknowledging the failed record. ConsumerRecoveryScheduling restarts an eligible stopped listener on the Kafka poll schedule. Marker/effect commit, ledger resolution, then Kafka acknowledgment is the success order. Failure bookkeeping is independent of deduplication. Consumer counts are recorded failures; publisher counts include successful publication. All instances must use compatible retry settings and the documented single consumer group.

The SECURITY INVOKER atlas_replay_outbox / atlas_replay_consumer functions require FAILED plus the inspected expected replay_count and a documented reason code. They atomically reset the cycle's count/schedule and advance replay metadata, preserving original identity and payload. A repeated expected count returns false. The operator must stop the affected consumers, correct the cause, replay, then restart; immutable poison cannot be repaired by changing its payload. Latest reason/time plus cumulative replay count are retained indefinitely, alongside externally retained operator evidence.

ReconciliationService.inspect runs REQUIRES_NEW, read-only REPEATABLE READ with a 10s transaction bound. Counts/details share a committed snapshot and include all HELD/CONFIRMED quantities. The read-only scripts/reconcile.sql uses identical predicates. Old/stuck/failed work, missing or inconsistent event history, missing audit effects and capacity drift are visible. Returned details and overdue candidates are bounded; complete counts still scan the dataset. ReconciliationWorker ends inspection before independently calling ExpirationProcessor.expireVerified. That path locks reservation then resource and checks conservation/history using a fresh statement after lock acquisition before applying the existing atomic expiration. It refuses uncertainty, never assigns capacity from a sum and never fabricates events or resets failed budgets. Ordinary expiration polling and Kafka/Redis authority boundaries are unchanged.

Configuration, exact inspection/replay/unpause commands and stuck-worker handling are in [README](../README.md#recovery-procedures). No public administration endpoint, dead-letter skipping, production deployment or Phase 13 work is added. Verification is recorded in [Phase 12 evidence](VERIFICATION.md#phase-12). Reviewed official behavior: [PostgreSQL snapshots](https://www.postgresql.org/docs/17/transaction-iso.html), [savepoint rollback](https://www.postgresql.org/docs/17/sql-rollback-to.html), and [Spring Kafka container stopping](https://docs.spring.io/spring-kafka/reference/kafka/annotation-error-handling.html).

## Phase 13 failure verification

FailureRecoveryIT exercises the existing commit, acknowledgment and recovery contracts through real HTTP requests and disposable PostgreSQL/Kafka/Redis infrastructure. Its test-only SQL gates observe the exact pre-commit or post-send boundary before terminating the verified waiting PostgreSQL backend. A test listener callback fails acknowledgment only after an independent connection observes the committed audit/marker. Kafka pause/resume and Redis stop/start are actual container controls. Redis uses a selected stable loopback port across restart; Docker can remap an automatically published port, which changes the endpoint rather than testing client recovery.

Fresh Spring application contexts recreate servers, pools, workers and clients while retaining database/broker state. These are deterministic application restarts and connection failures; they do not claim exhaustive JVM/process/host crash coverage. Every case checks legal persisted state, capacity conservation and event-history shape; publication/consumption scenarios check one audit effect per stable event ID. All fault code and SQL fixtures are test-scoped, with no production hook, migration or API change. Exact deadlines and results are retained in [Phase 13 evidence](VERIFICATION.md#phase-13). Stop before Phase 14.


## Phase 14 load measurement contract

Decision [D]: use the official k6 **2.3.0** image, pinned to `sha256:9c2dee7f8ed74d317e4027c06a10f169b625638189de8d4555d0b3486a5aeb34`. The pulled binary reports commit `e088784614`, Go `1.27.1`, Linux amd64. Release and API references: [official release](https://github.com/grafana/k6/releases/tag/v2.3.0), [scenario executors](https://grafana.com/docs/k6/latest/using-k6/scenarios/executors/ramping-vus/), [response classification](https://grafana.com/docs/k6/latest/javascript-api/k6-http/set-response-callback/) and [custom summaries](https://grafana.com/docs/k6/latest/results-output/end-of-test/custom-summary/). No new application dependency, endpoint, migration or tuning change is introduced.

The [load runner and workloads](../scripts/load/README.md) use isolated session-labelled infrastructure and manifest-scoped data. Normal/spike workloads recycle holds; a four-unit hot resource produces expected capacity conflicts; concurrent identical hold keys exercise claim/replay; mixed traffic retains HELD and CONFIRMED allocations. Warmup is excluded from custom request/latency/category metrics. Throughput includes measured in-flight drain; it counts API requests, including replay requests and expected conflicts, rather than unique reservations. Successful/conflict latency is recorded separately. Only the documented hot-resource capacity conflict is accepted as expected; other business or infrastructure failures fail the run.

Connection/backlog instrumentation is external read-only PostgreSQL sampling, with timestamps and sampling gaps retained. Application connections aggregate the API/consumer and dedicated publisher pools; observer connections are recorded separately. Publication delay uses existing `created_at` and `published_at` for events created within the measured scenario, after bounded convergence. It includes queueing and the mark after broker acknowledgment; it does not measure consumer delivery latency. Sampling can miss peaks. CPU utilization, GC/heap, exact broker transport latency and saturation bottlenecks remain unmeasured.

Post-run invariant checks use one repeatable-read database snapshot. Cleanup stops application workers, checks zero **committed** Kafka consumer lag, matches manifest IDs to run-prefixed resources, then deletes only dependent run data and keys in one transaction. An unrelated resource/claim canary must remain byte-for-byte unchanged. Project/volume labels gate removal of the private stack. Failed runs retain logs and private volumes for diagnosis. Local authentication overhead, shared Windows/WSL hardware, short duration, closed VUs and one measured run per scenario limit interpretation; these results do not establish a production SLO, supported user count or production readiness. Actual measurements and complete commands are recorded in [Phase 14 evidence](VERIFICATION.md#phase-14). Stop before Phase 15.
