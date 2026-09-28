# Atlas - Learning Guide and Evidence Ledger

## Manual demo follow-up - 2026-09-29

The user's first manual invocation used Windows PowerShell instead of the required PowerShell 7 and failed on `RandomNumberGenerator.GetBytes`. The PowerShell 7 run then passed host startup, normal operations and Kafka recovery, but did not observe a Redis hit in exactly two reads. The demo now waits up to 10 seconds for an actual hit and checks the committed capacity on each read; production cache behavior and timeouts are unchanged. An explicit PowerShell 7 prerequisite now gives a clear startup error.

Targeted container demo `p15-20260928195141` passed through Redis failure/recovery, expiration/restart, reconciliation and final invariants. Host startup was not repeated, having passed in the preceding manual run; no Java code changed and no full regression rerun was needed. Original failed evidence is retained. [Repair explanation and actual evidence](VERIFICATION.md#phase-15).

Learning point: a healthy optional service does not guarantee that the first two cache reads are hits. A demo should use a bounded readiness assertion while still failing if the behavior is never demonstrated. User-run startup and partial demo execution are observed; interview understanding and the repaired demo's personal execution remain unverified.

This guide separates plans from observed implementation evidence. Phases 1-15 are **COMPLETE** for the local v1 handoff. Final verification passed 345 integration + 92 unit tests, fresh host/container startup and normal/failure demonstrations; full raw evidence and unresolved/deferred work are in the [Phase 15 ledger](VERIFICATION.md#phase-15). Earlier sections record each phase's historical boundary. Expected outcomes are not observed outcomes.

Codex creating a file does not establish that the user has reviewed, tested, or understood it. Personal review status for every phase: **NOT RECORDED**. The specification's sample first-person testing/resume language is not evidence and must not be reused as an accomplished claim.

Read [PRD.md](PRD.md) for sources and invariants, [TRD.md](TRD.md) for decisions, [APP_API_FLOW.md](APP_API_FLOW.md) for behavior, [DEVELOPER_INTERFACE_BRIEF.md](DEVELOPER_INTERFACE_BRIEF.md) for tools, [BACKEND_SCHEMA.md](BACKEND_SCHEMA.md) for data, and [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) for gates.

## Prompt 0 - Work actually performed

On 2026-09-27, inspected S:/Atlas and its applicable ancestor instruction locations. The workspace was empty and no applicable AGENTS.md was found. Read the current sequential prompt attachment, recovered and read the original Atlas specification from the Build With Codex conversation, and extracted all 14 pages of the six-document PDF guide. An initial extraction hit Windows console encoding for a glyph; rerunning Python with UTF-8 mode succeeded. No source content was invented to fill a missing attachment.

Created exactly these seven Markdown planning files: PRD.md, TRD.md, APP_API_FLOW.md, DEVELOPER_INTERFACE_BRIEF.md, BACKEND_SCHEMA.md, IMPLEMENTATION_PLAN.md, LEARNING_GUIDE.md. Adapted the guide to developer/API interfaces, recorded requirement-versus-choice provenance, and reviewed contracts for consistency. No application code, build configuration, migrations, dependencies, services, tests, or benchmark scripts were created or executed.

Documentation checks and their actual outcomes are recorded at the end of this file after verification. Application test results: **NOT RUN - implementation has not begun**. Benchmark results: **NOT RUN**. These are not environment-blocked implementation phases; no implementation phase has been requested.

## How to maintain this ledger

After each requested phase, replace its actual-work fields with changed paths, the important code path and decision rationale, commands, expected/observed results, and evidence links. Record failures, diagnosis, fixes, and reruns. Keep failed and blocked checks visible. Only change status to COMPLETE when the implementation-plan gate passes. Stop after updating it.

InfrastructureIT, SchemaIT, ResourceApiIT, ReservationHoldIT, AllocationConcurrencyIT, ReservationLifecycleIT and ExpirationIT and IdempotencyIT and OutboxIT and MessagingIT and CacheIT now exist; Phase 12 onward test names remain planned. The Phase 1 harness makes `.\mvnw.cmd verify` run the full suite and validate selectors. Use `.\mvnw.cmd '-Dit.test=<Name>IT' verify` for a targeted integration class, followed by full verify. Test infrastructure checks must not silently skip when Docker is unavailable.

## Phase 1 - Project setup

Status: **COMPLETE**. All required checks passed on 2026-09-27 after Docker became available and the issues below were fixed. No business logic was added.

**WHAT:** Java 21 Spring Boot 4.1.1/Maven application, all eight package boundaries, shared/local/test profiles, official checksum-pinned wrapper, PostgreSQL Compose service, optional Redis/Kafka/app containers, environment template, non-root application Dockerfile, README, and one infrastructure integration-test class.

**WHY:** A reproducible toolchain and actual database tests establish the foundation before allocation logic. A packaged JAR is necessary but does not prove database connectivity, migration execution, or runtime health.

**HOW:** The Boot parent fixes library/plugin versions. Enforcer accepts only Java 21; the wrapper selects Maven 3.9.11. PostgreSQL is the only default Compose service. The application uses Hikari, JPA validation and Flyway; it has no production SQL yet. InfrastructureIT starts real PostgreSQL, supplies dynamic credentials, executes a test-only repeatable migration, initializes JPA, and checks the HTTP health endpoint and hidden metrics. Testcontainers is not allowed to skip without Docker. Test resources stay out of the packaged application.

**Common failure modes:** JAVA_HOME differs from PATH; missing Docker engine; unavailable image/dependency; missing local DB password; old volume credentials; treating `package -DskipTests` as test success; adding production business schema before Phase 2.

**Actual Codex changes:** Added pom.xml, mvnw/mvnw.cmd and wrapper properties, .gitignore/.gitattributes/.dockerignore, .env.example, compose.yml, Dockerfile, README.md, AtlasApplication, eight package-info files, application.yml/application-local.yml, production migration-location README, InfrastructureIT, test profile and R__infrastructure_probe.sql. Updated planning status, configuration/version notes and this guide. Retained exact logs, dependency tree, image digests and plugin versions in [Phase 1 evidence](VERIFICATION.md#phase-1). No resource/reservation entities, controllers, workers, business migrations, Redis client, or Kafka client were added.

**Initial implementation checks (2026-09-27, historical):**

- PASS: official wrapper generation; Maven 3.9.11 on Microsoft Java 21.0.12.1; download checksum validation.
- PASS: application and integration-test sources compiled, and the executable JAR packaged. Explicit `-DskipTests package` exited 0 in 4.333 seconds; no tests executed by that command.
- PASS: Compose 2.39.4 parsed all profiles; image release tags/digests resolved from the registry. This is not container startup evidence.
- PASS: JAR inspection found Java 21 bytecode, production migration location, and no test profile/test SQL/Testcontainers library. All effective build plugins have fixed versions.
- PASS negative check: Java 17 `validate` exited 1 because Enforcer rejected the toolchain.
- FAIL then fixed: initial `verify` hit a Testcontainers tag-plus-digest parsing incompatibility. Changed the test reference to the same PostgreSQL digest without the tag. The next run passed that point; no digest, assertion, or Docker requirement was weakened.
- Initially BLOCKED (subsequently resolved below): rerun of full `verify` exited 1 after 20.022 seconds: one class setup error, zero assertion failures, zero skipped tests, `Could not find a valid Docker environment`. Its three test methods did not execute. No positive database, migration, or health result is claimed.
- Observed negative startup: packaged local application against deliberately unreachable 127.0.0.1:1 exited 1 with PostgreSQL connection refused through Flyway initialization. It did not report successful startup. This does not substitute for the blocked healthy-startup check.

**Failure investigation and fix:** The inherited JAVA_HOME pointed at JDK 17 while PATH used JDK 21; commands explicitly selected JDK 21 and README explains the discrepancy. A draft Temurin image tag returned 404, so the verified published 21.0.12.1_1 tag/digest replaced it. The Testcontainers image-parser failure was repaired without changing the selected image. Docker was absent from PATH, its standard Windows installation paths, services, and named pipes; Testcontainers independently confirmed no engine. No system-wide Docker installation was attempted. Network downloads required sandbox escalation and succeeded; approval review did not reject them.

**Final verification and observed results (2026-09-27):**

- Docker Desktop 4.88.1 supplied Linux engine 29.7.2 and Compose 5.4.0. The running app's old PATH and sandbox context did not see the new CLI; the verified per-user CLI path and escalated local-engine access worked.
- First Docker-enabled run connected to PostgreSQL 17.11 and executed the test migration, but Tomcat hit a Windows Java NIO Unix-domain loopback error. A tiny standalone selector/pipe probe reproduced it. IPv4 preference did not help; a project-local `jdk.net.unixdomain.tmpdir` did. This is a process-local environment workaround, not a test relaxation or global OS change.
- The next run exposed two configuration failures: JDBC sessions were in Asia/Calcutta, and health included liveness/readiness groups. Fixed application.yml by using Hikari connection-init-sql to set UTC after JDBC startup and explicitly disabling Boot's default probe groups. The test assertions were unchanged.
- **PASS:** final full `verify` exited 0 in 12.640 seconds. **3 tests, 0 failures, 0 errors, 0 skipped.** Real PostgreSQL connectivity, UTC sessions, JPA initialization, Flyway execution/validation and actual HTTP behavior passed. This was the complete Phase 1 regression suite; Phase 2 extends it below.
- **PASS:** the isolated Compose PostgreSQL service became healthy. The production-profile database contains only flyway_schema_history with zero applied migrations; the test-only probe did not leak into it and no Phase 2 tables exist.
- **PASS:** packaged application on the host started and returned HTTP 200 with exactly `{"status":"UP"}`; metrics returned 404. The PowerShell check initially compared a byte-array response with text; decoding UTF-8 fixed the verification harness without changing application behavior.
- **PASS:** Redis and Kafka optional profiles became healthy. The app Dockerfile built and ran as uid=10001/gid=10001, and its HTTP health/metrics checks matched the host results. This proves local startup only, not later-phase messaging/cache behavior.
- Verification services and the host JVM were stopped afterward. Existing user resources were preserved. The isolated PostgreSQL volume and ignored local verification credentials remain for reproducibility. No external deployment occurred.

**Additional Codex changes in this verification turn:** corrected two settings in src/main/resources/application.yml; documented the Windows socket-directory setup; updated README, TRD, PRD, developer brief, implementation status and this learning guide. Added final raw logs, JUnit report, HTTP responses, database inspection, container identity and cleanup evidence under [docker-verification](VERIFICATION.md#phase-1). Test source and business scope were unchanged.

**Verify it yourself:** Follow README's Java 21, Docker PATH and project-local Java socket-directory setup, then run `.\mvnw.cmd verify`. The targeted `-Dit.test=InfrastructureIT` selector is available but a second identical successful test run was not needed after the full suite passed. Follow README for credentials and Compose startup; expect health 200 with status UP and metrics 404. The [final evidence README](VERIFICATION.md#phase-1) records the exact isolated project, ports, environment, commands and outcomes. No required Phase 1 check remains blocked.

**Interview explanation:** The foundation is now verified on a pinned Java/Maven/Spring stack with real PostgreSQL: its tests prove connection setup, UTC sessions, Flyway execution, JPA initialization and minimal HTTP health. Host and container startup both passed. This is infrastructure evidence only; allocation, idempotency, lifecycle and messaging correctness are not implemented yet.

**Questions:** Why can JAVA_HOME select a different Java than PATH? What does the wrapper checksum protect? Why is `test` insufficient here? Which files enter the production JAR? Which Phase 1 claims are verified versus blocked?

## Phase 2 - Database foundation

Status: **COMPLETE** on 2026-09-27. Required schema/upgrade checks and the full regression suite passed. These are historical Phase 2 results; Phase 3 was completed subsequently below.

**WHAT:** Flyway V1 creates resource/reservation with UUID keys, required fields, TIMESTAMPTZ timestamps, a restrictive resource FK, positive quantity/total capacity, bounded availability, nonnegative version, name validation, four allowed status values, terminal timestamp shape/deadline checks and three reservation indexes. Internal immutable Resource/Reservation records use UUID and Instant; ReservationStatus describes the allowed state graph. No Redis/Kafka code, API endpoints, repositories or lifecycle services were introduced.

**WHY:** Database constraints defend each row even when a caller bypasses validation. A resource-leading index supports the FK and invariant queries; the owner/time/ID index supports future pagination; the partial HELD expiry index supports later worker discovery. No extra name index is justified yet. Models stay internal so future APIs can validate inputs and expose deliberate DTOs instead of persistence state.

**HOW:** PostgreSQL generates default IDs and insertion timestamps. Flyway owns schema; Hibernate stays in validate mode. Defaults do not update fields on later mutations. Row CHECKs enforce values and timestamp shape, but cannot compare old/new history or total quantities across other rows. Future transactions must enforce immutable fields, authenticated ownership, post-lock deadlines, legal transitions and conservation. Only HELD -> CONFIRMED/CANCELLED/EXPIRED is permitted. CONFIRMED retains capacity; only cancellation/expiry of a HELD row will release it. Same-state command retries are not state transitions.

The records are persistence-independent snapshots, not JPA entities or API DTOs. They do not perform allocation, validate ownership or promise that arbitrary in-memory construction represents a valid persisted row. Persistence adapters and dedicated DTOs will be added when their API phases require them. JPA initialization is regressed, but this phase makes no ORM entity-mapping claim.

**Common failure modes:** Treating a row CHECK as a transition guard, relying on timestamp defaults to update existing rows, assuming an FK creates a child index automatically, releasing confirmed capacity, creating a users FK without a users table, or claiming one rollback test proves concurrent allocation. The migration intentionally has no universal updated_at >= created_at check because wall clocks can move backward.

**Actual Codex changes:** Added `src/main/resources/db/migration/V1__resource_and_reservation.sql`; `inventory/Resource.java`, `reservation/Reservation.java`, `reservation/ReservationStatus.java`; `src/test/java/com/atlas/SchemaIT.java`; and `src/test/java/com/atlas/reservation/ReservationStatusTest.java`. Updated inventory/reservation package descriptions and migration README, README.md, BACKEND_SCHEMA, TRD, PRD, developer brief, implementation status and this guide. Added [Phase 2 evidence](VERIFICATION.md#phase-2), raw Maven/Docker output, JUnit reports and a source manifest. Dependencies, images, Compose and InfrastructureIT were unchanged.

**Tests and observed results:**

- Initial targeted run: unit test passed; SchemaIT ran 50 cases with 1 failure, 0 errors/skips. The upgrade fixture asked Flyway to create a schema, so Flyway correctly added a schema-creation history marker; the test incorrectly expected empty history. The database migration and remaining checks passed. [Failure log](VERIFICATION.md#phase-2) retained.
- Fixed the fixture by pre-creating its isolated schema, matching Phase 1's existing public schema. No database constraint or expected invariant was weakened. Added a dedicated reservation default-ID/timestamp check.
- **PASS targeted:** `-Dit.test=SchemaIT verify` exited 0 in 12.123 seconds:51 schema cases and the 1 unit test passed, no failures/errors/skips.
- **PASS full regression:** `verify` exited 0 in 16.505 seconds: **54 integration tests (51 schema + 3 infrastructure) and 1 unit test**, all passed with zero failures/errors/skips. Finished at 2026-09-27 21:25:36 +05:30.
- Verified fresh V1 and upgrade from Phase 1 empty Flyway history without baseline/clean; checksum validation/no-op rerun; only the two planned business tables; keys, NOT NULL, FK/orphan/delete protection; valid/invalid names and capacity/quantity bounds including INTEGER overflow; all 32 status/terminal-timestamp combinations; microsecond deadline boundaries; defaults/types/indexes; rollback of a capacity change after invalid reservation insert.
- An explicit boundary test confirms that raw SQL can rewrite CONFIRMED to HELD or exceed resource capacity while satisfying row CHECKs. Another assertion shows confirmation alone leaves allocated capacity unchanged. These are SQL-level demonstrations, not implemented lifecycle operations. All fixtures roll back in disposable Testcontainers databases.
- Full InfrastructureIT proves the application starts with V1 and its test-only probe, initializes JPA/UTC connectivity and returns minimal HTTP health with metrics hidden. No H2, Redis or Kafka was used. Existing Compose volumes/databases were not migrated or reset.

**Verify it yourself:** Follow README's Java 21, Docker PATH and process-local Windows `jdk.net.unixdomain.tmpdir` setup. Run `.\mvnw.cmd '-Dit.test=SchemaIT' verify`, then `.\mvnw.cmd verify`. The [evidence README](VERIFICATION.md#phase-2) records the exact cache flags, environment, pinned PostgreSQL digest and expected results. Compare JUnit reports with 51 SchemaIT / 3 InfrastructureIT / 1 ReservationStatusTest. Missing Docker must fail, not skip.

**Interview explanation:** PostgreSQL checks facts inside each row and protects resource references. It cannot infer a legal transition history or enforce the sum of allocations with these CHECKs. Those guarantees will require conditional updates and reservation/resource changes in one transaction. Confirming a reservation changes its state while its quantity continues to count as allocated.

**Questions:** Why can a row pass every CHECK yet violate conservation? Why did Flyway's schema-creation marker invalidate the original upgrade fixture? Why are timestamp defaults insufficient for updates? What does ON DELETE RESTRICT protect, and why does it not prohibit direct deletion of a reservation? Which tests are schema evidence rather than concurrency evidence?

## Phase 3 - Resource API and caller identity

Status: **COMPLETE** on 2026-09-27. These are historical Phase 3 HTTP/identity results; Phase 4 was completed subsequently below.

**WHAT:** Resource POST/create, GET/lookup and GET/availability; explicit request/response DTOs; JdbcTemplate repository and transaction service; strict JSON and name/capacity validation; shared structured errors with server request IDs; development-only HTTP Basic with stable Alice/Bob/admin UUIDs and USER/ADMIN permissions. No reservation endpoint, ownership workflow, holds, workers, idempotency, Redis or Kafka integration was added.

**WHY:** Authentication must establish identity before authorization. A client-controlled userId or role cannot grant admin permission or later reservation ownership. Resource responses should expose a deliberate contract instead of binding protected internal fields. Availability can change after the read; clients must not use it as allocation proof.

**HOW:** The local profile reads all three private environment passwords and fails startup when missing/blank or beyond BCrypt's 72-byte limit. BCrypt strength 10 hashes are retained in the configured UserDetailsService; each request gets a fresh principal to avoid credential erasure destroying the hash. Verified LocalPrincipal contains an immutable UUID; admin has USER and ADMIN. Without local, Basic authentication is disabled, resource access is denied, and an explicit rejecting UserDetailsService suppresses Boot's generated user. Security is stateless with no form login, session or browser workflow; local CLI Basic over localhost is the documented limit.

The request permits only name/totalCapacity. Java strip() normalizes edge whitespace, bean validation enforces nonblank/max 120 UTF-16 code units/no NUL characters and positive Integer, and Jackson 3 rejects unknown/duplicate fields, trailing tokens, fractional/out-of-range/coerced numbers and non-string names. PostgreSQL INSERT RETURNING generates UUID/timestamps and initializes full availability/version 0 in a transaction. JDBC rows map to internal records and then dedicated DTOs. Availability reads clock_timestamp() with the counter in one database query and sends no-store/cached=false.

RequestIdFilter runs before security, ignores any incoming request ID, and creates a new UUID header. Controller advice and security error writer return code/message/requestId/details with field/reason-only validation details. Error bodies omit rejected values, SQL, stacks and credentials. Health is public/minimal; other Actuator paths require admin, and metrics remain disabled.

**Common failure modes:** Trusting identity headers; adding a fallback password; enabling development Basic implicitly; storing raw credentials in principal/logs; reusing a credential-erased UserDetails instance; accepting float-to-int or numeric-to-string conversion; returning generic HTML for security errors; treating an availability snapshot or unused Idempotency-Key as protection.

**Actual Codex changes:** Added the Boot-managed security starter to pom.xml (Spring Security 7.1.1); local principal/identity/security and JSON configuration; shared ApiError/ApiException/advice/writer/RequestIdFilter; CreateResourceRequest, ResourceResponse, AvailabilityResponse, ResourceController/Service/Repository; local YAML environment mappings, .env.example fields and optional app Compose credential wiring. Added ResourceApiIT and LocalIdentityConfigurationTest. Updated InfrastructureIT's anonymous metrics expectation from404 to 401 for the intentional permission gate and added a nonlocal identity-denial case. V1 and existing schema tests were unchanged. Updated all affected API/setup/schema/status/learning documents and [Phase 3 evidence](VERIFICATION.md#phase-3).

**Tests and observed results:**

- Initial compile failed on quotation escaping in the Basic realm string. Fixed the source quoting; [compile log](VERIFICATION.md#phase-3) retained. No tests ran in that attempt.
- First HTTP run:49 ResourceApiIT checks, 3 failures, 0 errors/skips; all 5 unit tests passed. Numeric name 123 was incorrectly accepted as text, so added explicit Jackson textual coercion Fail rules. Two expected results were incorrect: GET on create-only /api/resources returns 405, and Spring Security's authenticated authorities include FACTOR_PASSWORD. Corrected those expectations without changing USER/ADMIN rules. [Failure log](VERIFICATION.md#phase-3) retained.
- Final review added explicit NUL-name validation because PostgreSQL cannot store NUL text; two HTTP cases now return 400 before SQL. The earlier successful 49 resource / 104 integration suite is preserved as historical logs; final results below supersede it.
- **PASS targeted:** ResourceApiIT selector exited 0 in 20.967 seconds:51 HTTP/database integration checks plus 5 unit checks passed, zero failures/errors/skips.
- **PASS full regression:** verify exited 0 in 25.996 seconds, finished 2026-09-27T21:50:05+05:30: **106 integration tests (51 resource + 51 schema + 4 infrastructure) and 5 unit tests**, zero failures/errors/skips.
- Valid admin creation returns 201/Location and normalized name, server UUID/times, initial full capacity/version 0. Alice, Bob and admin read lookup/availability. Positive capacity boundaries, duplicate names, request IDs and validation details passed. Test-provided counter change showed an earlier availability response can become stale; it was not an allocation implementation.
- Invalid/null/empty/malformed bodies, protected fields, duplicate properties/trailing tokens, bad scalar types/overflow, names/capacities/UUIDs, missing resources and missing-ID collection method passed. Missing/bad/unknown/malformed Basic credentials returned 401, Alice/Bob creation stayed403 despite forged identity headers, and authenticated principals had stable UUIDs/roles. Credential validation startup-negative checks passed.
- Public health remained200 minimal; anonymous metrics401, USER403 and admin404; no Set-Cookie was emitted. Nonlocal-profile resource request stayed denied. Actual responses are recorded under [HTTP evidence](VERIFICATION.md#phase-3).
- **PASS:** dependency tree resolves Boot security4.1.1/Spring Security 7.1.1; Compose app configuration parses with credential wiring. This syntax check did not start or migrate existing Compose services. Testcontainers cleaned all disposable test containers; existing user volumes were untouched.

**Verify it yourself:** Follow README's JDK 21/Docker/Windows socket-directory setup; `local` startup requires all three caller passwords in the JVM environment (Compose .env is not automatically read by a host JVM). Run `.\mvnw.cmd '-Dit.test=ResourceApiIT' verify`, then `.\mvnw.cmd verify`. Use [README's CLI requests](../README.md#local-identity-and-resource-requests); curl's --user username prompts privately. Substitute the returned ID in lookup/availability. Retained outputs came from the actual local-profile Testcontainers HTTP app; these test UUIDs no longer refer to live data. No real password is in the docs.

**Interview explanation:** Credentials establish a stable principal; role checks decide whether it may create or read. A claimed userId has no authority. The resource counter is observed from PostgreSQL, but a GET cannot reserve it. Transactional holds and ownership enforcement arrive in Phase 4; creation retries still produce duplicates until Phase 8.

**Limitations:** Local Basic is CLI/localhost only: no TLS/production auth, account provisioning/recovery, persistent accounts or rate limiting. CSRF protection is disabled for this local API and browser authentication is not supported. Password changes require restart; UUIDs must remain stable. Name length uses the API's stricter UTF-16 bound. Metrics are protected but unexposed. No fault-injected503/500 or performance claim; no separate host/container deployment smoke run in Phase 3. Reservation lifecycle/idempotency/cache/messaging remain NOT STARTED. User personal review/understanding is NOT RECORDED.

**Questions:** Why does a body userId not authenticate anyone? Why must hashes survive credential erasure? What caused numeric name coercion? Why does GET /api/resources return 405? What does the availability timestamp promise? What happens if the same create request is retried with an Idempotency-Key now?

## Phase 4 - Atomic temporary holds

Status: **COMPLETE** on 2026-09-27. Required transaction/ownership checks and the full regression gate passed. These are historical Phase 4 results; the API remains intermediate.

**WHAT:** POST /api/reservations creates HELD; GET /api/reservations/{id} reads only the caller's row; GET /api/users/{userId}/reservations lists only the caller's rows with ascending keyset pagination. Added configurable hold duration, dedicated DTOs, JDBC repository and transactional service. No lifecycle transitions, worker, idempotency, outbox, Redis or Kafka logic.

**WHY:** An availability pre-read can become stale. The conditional decrement evaluates allocation in PostgreSQL and a transaction prevents capacity being consumed without its reservation. Authentication provides immutable ownership; neither headers/body userId nor admin status authorize another user's reservation. A server-persisted deadline avoids client-controlled duration and survives later configuration changes.

**HOW:** READ COMMITTED ReservationService.create first issues UPDATE resource with `available_capacity >= quantity`, decrementing available, incrementing version and updating the resource timestamp. It does not SELECT first. On zero rows only, an existence lookup distinguishes unknown resource 404 from insufficient capacity409. After success, INSERT samples one database clock_timestamp() in a materialized CTE and uses it for createdAt/updatedAt and expiresAt+configured seconds. V1 still generates the UUID. The Spring proxy commits before controller success; errors escape with rollbackFor=Exception.class so resource changes and reservation share one transaction.

HoldProperties defaults to 300 seconds and validates integer 1-3600 at startup; application.yml maps ATLAS_HOLD_TTL_SECONDS, and Compose forwards it. The actual hold suite sets17 seconds, proving the configured value rather than assuming the default. Request quantity is a required positive JSON Integer; strict parsing rejects malformed/coerced/out-of-range values and extra protected fields. Owned GET filters both ID and user_id; foreign/missing404 are identical. Listing compares path userId with authenticated caller (foreign403), then uses owner-filtered `(created_at,id)` SQL, limit+1 and a canonical opaque cursor. Limit defaults20, range 1-100, invalid/empty query values400. Admin can allocate for itself but has no foreign-user bypass.

**Common failure modes:** Read-check-write allocation; separate decrement/insert commits; swallowing an insertion exception; JVM-time or transaction-start-time deadlines; trusting body owner/deadline; treating copied cursors as authorization; missing the ID pagination tie-breaker; assuming a passed deadline releases capacity; blindly retrying an ambiguous POST before Phase 8.

**Actual Codex changes:** Added HoldProperties/HoldConfiguration; CreateReservationRequest, ReservationResponse, ReservationPage, ReservationCursor, ReservationRepository, ReservationService and ReservationController. Added hold duration configuration to shared YAML/.env.example/Compose app, and updated reservation package description. Added ReservationHoldIT (57 real HTTP/PostgreSQL cases) and HoldConfigurationTest (8 startup cases). ResourceApiIT's previous unknown-reservation route assertion now expects RESERVATION_NOT_FOUND because owned GET exists; no assertion was relaxed. V1, dependency pins, identity/security, schema tests and resource allocation-independent code remain unchanged. Updated README, API/schema/TRD/PRD/developer brief, implementation status and this guide. [Phase 4 evidence](VERIFICATION.md#phase-4) contains raw reports, response transcripts and final source/artifact checks.

**Tests and observed results:**

- **PASS first targeted run:** ReservationHoldIT verify exited 0 in 35.887 seconds:57 integration + 13 unit tests, zero failures/errors/skips. No Phase 4 failed attempt or blocker occurred.
- **PASS full regression:** verify exited 0 in 41.065 seconds, finished 2026-09-27T22:10:20+05:30: **163 integration tests (57 hold + 51 resource + 51 schema + 4 infrastructure) and 13 unit tests (8 hold config + 4 identity config + 1 state graph)**. All failures/errors/skips zero.
- Successful hold persisted authenticated Alice UUID, server ID, quantity 3/HELD/null terminal timestamps, full configured 17-second deadline derived from DB time, Location and request ID. Resource available 10->7/version 0->1 and conservation passed; owned GET matched the POST DTO.
- Insufficient capacity/quantity above available or total returned 409 with no mutation; missing resource 404, invalid quantities/UUIDs/protected fields400, missing authentication401, foreign GET404 and foreign list403 passed. Forged identity headers did not change owner or grant access. Admin could hold/read/list only its own data.
- Pagination passed empty-list/null-cursor, owner filtering, repeated-createdAt ID tie-breaker, default 20/max100, cursor continuation and invalid limit/cursor/date/precision cases. A copied cursor could not leak another owner's rows. This is not a frozen snapshot claim.
- **Actual insertion-failure rollback:** A trigger installed only in the test database observed available 7/version 1 after allocation and advanced a test sequence, then raised SQLState23514. Sequence advancement survives rollback and proves the fault saw the decrement. HTTP returned a sanitized503; resource capacity, version and updatedAt exactly matched their pre-request snapshot, no reservation existed, conservation passed. Trigger/function/sequence were removed, and the next valid hold succeeded. No production fault hook was added.
- An update-time BIGINT version overflow separately returned 503 with no insert or partial resource change. These are expected injected failures handled within passing tests; neither is a failed test run.
- Repeating an identical POST with the same Idempotency-Key created two IDs and decremented twice, confirming the explicit intermediate retry risk. A deliberately overdue HELD fixture remained visible/allocated; no expiration worker ran. Confirm/cancel routes remained unavailable. No concurrent workload was run in Phase 4; Phase 5 results are recorded separately below.
- **PASS:** Compose config parses with hold TTL wiring; retained Docker listing shows no Testcontainers containers. Tests used their own disposable PostgreSQL databases; no existing Compose database was modified/reset. Packaging and JPA/UTC/health regressions passed in the full suite. No separate host/container launch was repeated in this phase.

**Failure investigation and fix:** No required check failed. During implementation review, explicit limit parsing distinguishes omitted (default 20) from empty (400), and cursor timestamp bounds/microsecond precision prevent invalid SQL timestamp inputs. The fault tests intentionally generate database errors; the passing assertions verify rollback and secret-free structured errors, not a production incident.

**Verify it yourself:** Follow README's JDK21/Docker/process-local Windows socket-directory setup, then `.\mvnw.cmd '-Dit.test=ReservationHoldIT' verify` and full `.\mvnw.cmd verify`. These tests supply private random caller credentials and 17-second TTL dynamically. For a host app, export all local caller passwords plus an optional ATLAS_HOLD_TTL_SECONDS; .env is not automatically loaded into the JVM. [README hold requests](../README.md#api-examples) show creation/read/list with returned IDs. Historical test UUIDs in [HTTP evidence](VERIFICATION.md#phase-4) no longer point to live rows. Default startup must use300 seconds; invalid TTL fails.

**Interview explanation:** The conditional UPDATE decides whether allocation can occur; the encompassing transaction prevents a reservation insertion failure from leaving consumed capacity behind. Ownership is an authenticated UUID, not a user claim in a request. A held deadline is durable data, but automatic release and retry deduplication are future work.

**Limitations:** API is not complete. No confirmation/cancellation, automatic expiration/release, idempotency or events/caching/messaging. Overdue HELD still counts as allocated; ambiguous POST retries can allocate twice. Local Basic retains its documented CLI/localhost limitations. Sequential conservation/rollback evidence does not prove the Phase 5 concurrency scenarios. No performance/production claim or user personal-understanding inference.

**Questions:** Why is the resource existence SELECT after the conditional UPDATE safe for allocation? What proves the inserted-row fault happened after decrement? Why does a test sequence survive rollback while business rows do not? How does DB time avoid shortening a hold before allocation succeeds? Why is a cursor not identity proof? What happens to an overdue HELD row now? What happens if a POST times out after commit?

## Phase 5 - Allocation concurrency proof

Status: **COMPLETE** on 2026-09-27. Seven concurrency cases and the full regression gate passed. These are historical Phase 5 results; Phase 6 is recorded below.

**WHAT:** Added [AllocationConcurrencyIT](../src/test/java/com/atlas/AllocationConcurrencyIT.java), which sends real HTTP requests against a disposable pinned PostgreSQL 17.11 database. It covers capacity 10 (three independent rounds of 100 requests), capacity 1, mixed quantities, independent resources, and rollback while competing allocations wait.

**WHY:** A mock can return a programmed success or conflict without executing any SQL. It cannot demonstrate PostgreSQL row locks, UPDATE predicate rechecks after another transaction commits, separate MVCC snapshots, or rollback visibility. A sequential real-database test also cannot exercise waiting updaters. These tests therefore use actual application transactions on multiple PostgreSQL connections and explicitly observe overlapping backend sessions.

**HOW:** Each batch uses a CyclicBarrier with one party per client plus the coordinator. All clients reach it before sending HTTP or acquiring a database connection. A separate fixture connection holds a row lock until pg_stat_activity reports at least two distinct active allocation UPDATE sessions waiting on locks, then releases it. The normal Spring service transaction performs the conditional decrement and insertion. There is no enclosing test transaction, repository mock, in-memory allocation lock, sleep-based synchronization or automatic retry.

During traffic, an independent autocommit observer repeatedly uses one SQL statement snapshot to check `0 <= allocated <= total`, capacity bounds, and `available + SUM(HELD/CONFIRMED quantity) = total`. Using two separate reads could mix commits and report false drift. After all bounded futures complete, successful response UUIDs and quantities must exactly match database rows; resource version must equal successful allocations. Every unexpected HTTP status, wrong conflict code, transport exception or timeout fails, rather than being converted into a capacity conflict.

The test-only pool has 32 connections; barrier/overlap budgets are 15s, lock/statement timeouts 20s/30s, HTTP timeout 60s, batch timeout 75s and test timeout 120s. A 3600-second TTL and absent expiration worker isolate allocation. Observers and gates use separate connections. Production defaults are unchanged. The fixed mixed dataset contains 25 requests each for quantities 1, 2, 3 and 4 against capacity 17; ordering is scheduler-dependent, so accepted quantity and exact row matching matter more than a fixed winner count.

**Rollback under contention:** A BEFORE INSERT trigger exists only in the disposable test database. For the designated Bob fault request, it verifies its transaction sees available=7/version=1 after decrementing three units from ten, then advances a sequence. It waits on an advisory lock while at least two healthy updaters contend on the resource. Releasing the gate permits a second sequence increment immediately before the injected 23514 exception. The two markers survive rollback and prove both the decrement and intentional failure site were reached; a timeout cannot impersonate that path. The fault returns one expected 503, creates no Bob row, and healthy requests still allocate all ten units exactly once each. The final resource version is ten. Trigger/function/sequence are cleaned up; none are packaged in the app.

**Actual changes:** One new integration-test class; README, plan, learning guide and affected status/verification references in PRD/TRD/schema/API/developer documents; [Phase 5 evidence](VERIFICATION.md#phase-5). All production source, V1 migration, existing tests, dependency pins and container configuration remain unchanged.

**Observed results:**

| Final full-run scenario | Requests | 201 / 409 | Rows | Allocated / available |
| --- | --- | --- | --- | --- |
| Capacity 10, each of three rounds | 100 each | 10 / 90 each | 10 each | 10 / 0 each |
| Capacity 1 | 100 | 1 / 99 | 1 | 1 / 0 |
| Mixed quantities, capacity 17 | 100 | 6 / 94 | 6 | 17 / 0 |
| Independent B, while A stays locked | 20 | 7 / 13 | 7 | 7 / 0 |
| A, after releasing its lock | 8 | 3 / 5 | 3 | 3 / 0 |
| Healthy requests behind injected rollback | 100 | 10 / 90 | 10 | 10 / 0 |

The rollback scenario also has exactly one deliberate fault request returning 503, tracked separately. Zero unexpected infrastructure errors occurred. Every sampled/final conservation check passed. Distinct overlapping allocation sessions observed in the full run ranged from 3 to 19 by scenario; these are sampled maxima, not a claim of 100 simultaneous database connections. Mixed winners differed in the targeted run (8 successes/92 conflicts), while its allocated total remained 17.

- Initial targeted run: PASS, 7 integration + 13 unit tests, 28.057s, finished 22:35:52+05:30.
- Final targeted run after strengthening the intentional-fault marker: PASS, 7 integration + 13 unit tests, 24.138s, finished 22:37:05+05:30.
- Full verify: PASS, **170 integration + 13 unit tests**, 50.145s, finished 22:38:13+05:30. All failures/errors/skips zero.

**Repeat policy and investigation:** Three capacity-10 rounds are built into every class run. The final source was exercised by the targeted run and full regression (six capacity-10 rounds); the earlier passing draft is retained separately. No failed run was discarded, no retries-until-green occurred, and no production fix was needed. Review after the initial pass added the second sequence marker so an advisory wait timeout could not be mistaken for the intentional insert failure. No required check was blocked.

**Verify it yourself:** Follow the exact Java/Docker/Windows socket-directory setup in [Phase 5 evidence](VERIFICATION.md#phase-5), run `.\mvnw.cmd '-Dit.test=AllocationConcurrencyIT' verify`, then `.\mvnw.cmd verify`. Inspect the PHASE5 lines in logs and JUnit XML; do not accept skips or turn 503/timeouts into 409s. Credentials are generated privately in memory; resource fixtures and fault metadata belong only to Testcontainers.

**Interview explanation:** PostgreSQL serializes updates to the same resource row. A waiting UPDATE rechecks `available_capacity >= quantity` after a competing commit. Successful decrement and reservation insertion commit together; a failed insertion rolls them both back before another allocator proceeds. Independent resource rows can progress while a different row is locked. The observed HTTP outcomes and committed quantity totals support that reasoning for the tested interleavings.

**Limitations:** Finite schedules and sampled committed snapshots cannot prove every possible interleaving. This is correctness evidence with deliberately controlled contention, not a latency/throughput benchmark, fairness guarantee, multi-instance deployment proof or production readiness claim. Local Basic limitations, duplicate retry risk, overdue HELD allocation and missing lifecycle/expiration/idempotency remain. Redis/Kafka were not introduced. No claim of the user's personal review or understanding is made.

**Questions:** Why is a client barrier insufficient without observing database overlap? Why must it precede pool acquisition? Why does mixed allocation require quantities as well as counts? How can two separate snapshot reads falsely imply drift? What distinguishes the expected injected 503 from an infrastructure failure? Why do sequence markers survive while the decrement rolls back?

## Phase 6 - Confirmation and cancellation

Status: **COMPLETE** on 2026-09-27. Historical Phase 6 evidence; Phase 7 follows below.

**WHAT:** Owner-only POST `/api/reservations/{id}/confirm` and `/cancel`, both requiring an empty body and returning 200 Reservation. Confirmation retains allocation. Cancellation releases the held quantity exactly once with the state change. Same-terminal repeats return the existing row unchanged; incompatible terminal actions return 409 INVALID_STATE_TRANSITION. HELD at/past its deadline and persisted EXPIRED return 409 RESERVATION_EXPIRED. Overdue HELD is not automatically changed or released in this phase.

**WHY:** Two independent requests may both initially believe they can change a hold. The reservation row lock serializes those decisions. A transaction couples cancellation state and capacity release so neither can commit alone. Confirmation must never consume the same capacity twice. Server-authenticated ownership and a post-lock database deadline prevent identity claims and lock waits from bypassing eligibility.

**HOW:** The service starts a READ COMMITTED transaction, SELECTs by reservation ID and owner UUID FOR UPDATE, then issues a separate SELECT clock_timestamp(). ReservationLifecycle checks the locked state: matching terminal state means no write; EXPIRED or overdue HELD means expired conflict; another terminal state means invalid transition. For a valid HELD, the repository updates the terminal state and corresponding timestamp/updatedAt using that one decision instant. Cancellation then increments resource availability by quantity and version by one, using the same timestamp. Exceptions escape the proxy to roll back both writes. Confirmation never updates the resource.

**One shared lock order:** future idempotency claim -> one existing reservation -> its resource when releasing -> future outbox. Phase 6 uses only the reservation/resource portions. New holds update a resource and insert a previously invisible reservation; they never lock an existing reservation. Future workers must lock one reservation per transaction, then its resource, with state/time rechecks after the reservation lock. No worker was added. Deadline validity is decided after the reservation lock, not at HTTP arrival or eventual commit; a valid decision may commit later than expiresAt. Repeating CONFIRMED or CANCELLED remains a no-op success after the original hold deadline.

**Changed files:** ReservationController/Service/Repository now implement lifecycle commands; new ReservationLifecycle holds the production state/deadline policy; reservation package documentation updated. New [ReservationLifecycleIT](../src/test/java/com/atlas/ReservationLifecycleIT.java) and [ReservationLifecycleTest](../src/test/java/com/atlas/reservation/ReservationLifecycleTest.java) cover real transactions and exact boundary comparisons. The old ReservationHoldIT assertion that lifecycle routes are unavailable now asserts their nonempty-body rejection with no hold mutation. Existing allocation tests, V1 schema, dependencies, identity/security and configuration are unchanged. API flow, README, TRD/schema/PRD/developer references, this guide and plan now describe Phase 6.

**Observed checks:**

| Check | Actual outcome |
| --- | --- |
| Fresh confirm; repeated confirm | 200, same CONFIRMED row on repeat; resource capacity/version/timestamp unchanged |
| Fresh cancel; repeated cancel | 200, same CANCELLED row on repeat; quantity 3 released once (available 7 -> 10, version 1 -> 2) |
| Same action after original deadline | 200 terminal result with no new write |
| Opposite terminal action | 409 INVALID_STATE_TRANSITION, no mutation |
| Foreign Bob/admin, forged identity, anonymous, missing/invalid ID | 404/401/400 as documented; hold remains HELD and allocated |
| Nonempty bodies including {}, null, whitespace, userId | 400 INVALID_REQUEST; no mutation |
| Overdue HELD and persisted EXPIRED fixtures | 409 RESERVATION_EXPIRED; no mutation |
| Confirm versus cancel, three rounds per run | Exactly one 200 and one 409 each; state/capacity conserved |
| Sixteen simultaneous confirms | Sixteen identical 200 bodies, one terminal mutation, allocation retained |
| Sixteen simultaneous cancellations | Sixteen identical 200 bodies, one release, capacity restored exactly once |
| Waiting confirm/cancel across deadline | Observed blocked before deadline; after unlock both returned 409 with HELD/available 7 unchanged |
| One microsecond before, equal to, one microsecond after deadline | Production-policy unit tests accept only before, for both actions |
| Injected resource-release failure | 503; full reservation/resource snapshots restored; subsequent cancellation succeeds |

**Actual verification:** Targeted `.\mvnw.cmd -B -ntp '-Dmaven.repo.local=S:\Atlas\.tools\repository' '-Dit.test=ReservationLifecycleIT' verify` passed 16 integration + 19 unit tests, zero failures/errors/skips, 31.937 seconds; finished 2026-09-27T22:51:23+05:30. Full verify passed **186 integration + 19 unit tests**, zero failures/errors/skips, Maven-reported 01:11 min; finished 22:53:09+05:30. Phase 5's three capacity-10 regressions still each returned 10 successes/90 conflicts/10 rows/zero availability. No required check failed or was blocked. [Evidence](VERIFICATION.md#phase-6) preserves exact commands, logs, reports and HTTP responses.

The targeted confirm/cancel rounds were all won by cancellation; the full-run rounds were won by confirmation, confirmation, then cancellation. Winner order is intentionally nondeterministic. Both runs asserted one winner and checked state, capacity, version and allocated sum together in one SQL snapshot after every race. Concurrent same-operation tests also require every response body to be identical. Three rounds per invocation are fixed; no retry-until-green or conversion of infrastructure errors into conflicts exists.

**How the failure and deadline tests work:** The release-failure trigger exists only in the test database. It verifies the same transaction already sees CANCELLED, advances a nontransactional sequence, then raises SQLState23514 before updating resource capacity. The surviving marker proves execution reached that failure point. Both business rows, including timestamps/version, must exactly match the pre-request snapshots after 503. Removing the trigger permits a successful retry. Deadline tests use a dedicated connection holding the reservation lock, set a short deadline using database time, observe the HTTP transaction waiting before it, poll that database clock until expiry, then commit/unlock. They use bounded database-state waits and no synchronization sleeps. Precise equality is tested directly against the production policy rather than claiming a real HTTP request landed on an exact microsecond.

**Verify it yourself:** Follow [Phase 6 environment and commands](VERIFICATION.md#phase-6), run ReservationLifecycleIT then full verify. [API flow](APP_API_FLOW.md#phase-6-confirmation-and-cancellation-implemented) documents empty-body requests, response fields, the state matrix, errors and lock order. Use different live holds to demonstrate confirm and cancel; a confirmed hold cannot be cancelled. Historical evidence UUIDs are disposed. No user credentials appear in transcripts.

**Interview explanation:** Locking the reservation chooses the legal next state once. Confirmation changes state while retaining the original allocation. Cancellation changes state and restores capacity in the same transaction. A competing cancellation sees CANCELLED and does nothing, while a competing confirm sees a conflicting terminal state. Sampling database time after locking prevents an early-arriving request from confirming a hold after waiting beyond its deadline.

**Limitations:** No expiration worker or automatic recovery, durable idempotency, outbox, Redis/Kafka integration, deployment or benchmark. Create retries remain unprotected, and overdue HELD still consumes capacity. Same-terminal no-op behavior is not stored HTTP response replay. Local Basic remains a localhost CLI identity model. Finite races provide evidence for tested schedules, not all possible executions. No claim of the user's personal understanding is made.

**Questions:** Why sample time after the locking statement returns? Why can a valid decision commit after its deadline? Why does cancellation require one transaction for state and capacity? Why is repeating a terminal action safe without an idempotency key? Why can confirmation never be followed by cancellation in this version? What remains allocated when a HELD deadline passes today?

## Phase 7 - Expiration worker

Status: **COMPLETE** on 2026-09-27. Stop before Phase 8.

**WHAT:** Automatic PostgreSQL-backed expiration of overdue HELD reservations. A bounded poll discovers due candidate IDs; separate transactions lock each with SKIP LOCKED, recheck state and database time, transition to EXPIRED and restore capacity atomically. Multiple workers can discover the same candidate without duplicate release. No Redis notification, Kafka dependency, new HTTP endpoint or migration was introduced.

**WHY:** Deadline passage is not a database transaction. A crashed application or missed in-memory timer must not lose pending expiration work. HELD rows and their persisted deadlines are the durable work list. PostgreSQL locks coordinate workers with each other and with confirm/cancel, while one transaction keeps reservation state and resource capacity consistent.

**HOW:** ExpirationScheduling defaults to enabled, with 1000ms initial/fixed delay and at most 100 candidate IDs per poll. ExpirationWorker discovers HELD rows with expires_at <= database statement_timestamp(), ordered by expires_at/id using the existing partial-index predicate. Discovery acquires no mutation locks and is only a hint. For each ID, a separate ExpirationProcessor proxy uses REQUIRES_NEW/READ_COMMITTED, selects the reservation FOR UPDATE SKIP LOCKED and then samples clock_timestamp() in another statement. It checks HELD and decisionTime >= expiresAt. The shared repository writes EXPIRED/expiredAt/updatedAt and restores quantity with one resource version increment. Both writes commit or roll back together.

The lock order remains one existing reservation -> its resource. Holds update a resource then insert a previously invisible reservation and never lock an existing one. Workers skip locked reservations; stale terminal/missing/not-due candidates do nothing. Per-item failures escape the transaction proxy before the batch catches/logs them; other items keep their independent commits. Failed/skipped work remains discoverable next poll. Discovery failures are logged/counted and the scheduler retries on its next invocation. No retry-until-success loop hides a failure within one batch.

**Expiration lag:** expiresAt determines eligibility; expiredAt records the later database decision that materializes expiration. A deadline can pass while GET still returns HELD and available capacity remains reduced. That is expected: capacity is restored only at expiration commit, and every persisted HELD still counts in conservation. Poll delay, processing time, backlog, skipped locks, resource waits, failures, disabled scheduling, downtime and restart all add lag. A one-second interval is not a one-second completion guarantee. An oldest batch filled by persistently locked/failing rows can delay later rows. There is no hard lag or fairness guarantee. A valid pre-deadline confirm/cancel can commit after expiry while a worker skips its reservation lock; terminal states then prevent expiration or duplicate release.

**Actual changed files:** Added ExpirationProperties/ExpirationConfiguration/ExpirationScheduling and ExpirationProcessor/ExpirationWorker. ReservationRepository adds candidate discovery and SKIP LOCKED lookup and extends the existing timestamp update for EXPIRED; ReservationService/package comments describe the shared order. application.yml, .env.example and Compose wire ATLAS_EXPIRATION_ENABLED (true), ATLAS_EXPIRATION_POLL_MS (1000, bounds 100-60000), ATLAS_EXPIRATION_BATCH_SIZE (100, bounds 1-100). Added ExpirationIT, ExpirationConfigurationTest and ExpirationBoundaryTest. test-only application-test.yml disables automatic scheduling to preserve earlier overdue fixtures; the new suite drives actual worker proxies and separately tests enabled scheduling. Previous Java test sources, V1, dependency pins and identity/security are unchanged. Current API/schema/TRD/PRD/developer docs, README, plan and evidence were updated.

**Diagnostics:** atlas.expiration.attempts counts committed expired, skipped and failed outcomes; atlas.expiration.discovery.failures counts poll discovery failure. atlas.expiration.decision.lag records expiresAt-to-database-decision for successful worker commits; it excludes later resource-wait/commit delay and is not a completion-lag SLO. Metrics remain in-process with existing public Actuator exposure unchanged. Failure logs contain reservation ID and exception category, not SQL or credentials.

**Observed checks:**

| Scenario | Actual result |
| --- | --- |
| Due hold, repeat worker calls | One EXPIRED transition/release, available 7 -> 10, version 1 -> 2; repeats do nothing |
| Bounded batch of 2, five due and one future hold | Successive polls expire 2/2/1; future hold stays HELD |
| Locked candidate plus free candidate | Locked row skipped, free row expires; later poll reclaims skipped row |
| Two workers on the same hold | First owns reservation while waiting for resource; second skips; one total release |
| Expiration versus confirm/cancel, worker owns lock first | EXPIRED wins; request returns RESERVATION_EXPIRED; one release |
| Confirm/cancel owns lock with valid pre-deadline decision | Worker skips after deadline; request commits terminal state; no later expiration/release |
| Stale candidate changed to future deadline or terminal state | Post-lock recheck skips; no early or repeated release |
| Injected resource-release failure | Failed row fully rolls back; another candidate commits; next poll succeeds after fault removal |
| Discovery SQL failure | Counted/logged; no business mutation; next healthy poll recovers |
| Application-context restart against same PostgreSQL | Prior context/pool closed; new scheduler automatically discovers and expires durable overdue row |
| Exact deadline boundary | Production predicate rejects 1 microsecond before, accepts equality/after; terminal states excluded |
| Configuration | Defaults/bounds/disable accepted; malformed/out-of-range settings fail startup |

All worker/lifecycle races assert status, available capacity, version, allocated quantity and conservation using committed SQL snapshots. Deterministic fixture locks and observed PostgreSQL wait states establish order; no sleeps or mocked clocks establish correctness. Test triggers gate pre-deadline lifecycle updates and inject expiration-release failure only in the disposable database. A nontransactional sequence proves the failing release had already seen EXPIRED before rollback. Test gates/faults are absent from the production JAR.

**Failures, fixes and actual verification:** Initial targeted verification failed with one test error: queryForMap returned java.sql.Timestamp and the assertion cast it to OffsetDateTime. Production had expired the hold; the test failed while inspecting the result. The fix uses a typed queryForObject(..., OffsetDateTime.class) for the same database-time bounds assertion; no requirement was relaxed. The failed log/XML are retained. Corrected targeted and full runs passed. Review then removed a small restart-test timing assumption by waiting for both committed EXPIRED and the subsequent success metric before asserting recovery; final targeted/full runs passed again. This was not an observed production defect or retry-until-green policy.

Final targeted run: **12 integration + 38 unit tests**, zero failures/errors/skips, 26.753s, finished 2026-09-27T23:14:02+05:30. Final full run: **198 integration + 38 unit tests**, zero failures/errors/skips, 01:13 min, finished 2026-09-27T23:15:44+05:30. Compose syntax verification passed with the new environment variables. Exact commands, all earlier outcomes and reports are retained in [Phase 7 evidence](VERIFICATION.md#phase-7). No required check remains blocked.

**Verify it yourself:** Follow the Java/Docker/Windows socket setup in the evidence README, run `.\mvnw.cmd '-Dit.test=ExpirationIT' verify`, then `.\mvnw.cmd verify`. For the running application use an ordinary short-lived hold, then GET its status and availability after the worker commits. Keep expiresAt and expiredAt distinct. Disable/re-enable scheduling only deliberately; disabled workers do not reclaim overdue capacity. No claim is made that README reproduction commands were separately run against user Compose data.

**Interview explanation:** PostgreSQL is both the source of truth and the durable expiration work list. Each worker locks one reservation, rechecks its deadline/state using database time, and commits EXPIRED with the capacity release. Another worker skips or observes the terminal state. A failure rolls back both changes, so the same persisted HELD row can be retried after a later poll or restart.

**Limits:** Restart evidence closes/reopens application contexts, pools and schedulers in the same JVM against persistent PostgreSQL; it is not an OS kill or host crash test. Races cover finite controlled schedules, not every interleaving or a throughput benchmark. No durable idempotency, outbox, broker/cache integration, reconciliation or repair ledger was added. Creation retries can still duplicate effects. No claim about the user's personal review or understanding is made.

**Questions:** Why is a passed deadline different from persisted EXPIRED? Why is candidate discovery insufficient without a post-lock recheck? Why does SKIP LOCKED prevent waiting on another worker but not resource lock waits? Why does a failed item not roll back another item's successful expiration? Which lag is measured, and which delay is excluded? What survives an application restart?

## Phase 8 - Durable API idempotency

Status: **COMPLETE** on 2026-09-27. Targeted and full verification passed. Stop before Phase 9.

**WHAT:** Mandatory caller/operation-scoped keys, canonical SHA-256 request hashes and durable original HTTP responses for resource creation, hold creation, confirmation and cancellation. PostgreSQL is the only claim arbiter; no JVM lock, Redis or Kafka was added.

**WHY:** A lost response cannot tell a client whether commit happened. A durable retry contract lets the client recover that result without making another reservation or decrementing capacity twice. Returning the present reservation is different: replay may still say HELD after current state becomes EXPIRED.

**HOW:** Authentication and typed validation happen first. A nonlocking ownership check precedes the unique key claim. The composite primary key `(caller_id, operation, idempotency_key)` arbitrates `INSERT ... ON CONFLICT DO NOTHING RETURNING` inside a READ COMMITTED transaction. The winner mutates and serializes the response before committing. The loser waits for the winner's transaction, then reads its result in a separate SQL statement with a fresh snapshot. Same hash replays; different hash conflicts. If the winner rolls back, a waiter can claim and execute instead. A bounded lock wait returns sanitized retryable 503.

Canonical input is a fixed-order JSON array of operation plus validated name/capacity, resource UUID/quantity, or reservation path UUID. Normalized names and UUID case compare consistently; transport whitespace/property order and request IDs are excluded. Never change this encoding casually while old keys are retained. There is no initial lookup that authorizes a second mutation.

The exact JSON is serialized once and persisted as UTF-8 TEXT with status, content type and Location, then returned as bytes only after commit. Domain 404/409 rejections use a JDBC savepoint: roll back any partial business work, keep the claim, store the rejection. Business ApiException rules avoid marking that shared transaction rollback-only for expected rejections; database/unexpected failures escape and roll back everything. Creation, response storage, and terminal-state/capacity release rollback are covered by real database tests. Spring's transaction-bound connection is shared with JdbcTemplate; there are no managed persistence entities in this implementation.

**Actual changes:**

- `src/main/resources/db/migration/V2__durable_idempotency.sql`: scoped primary key, key/hash/operation/response-shape checks, RESTRICT foreign keys and lookup/corruption indexes. V1 remains unchanged.
- `src/main/java/com/atlas/idempotency/`: key validation, typed operation facade, transaction executor, serialized response envelope and incomplete-claim health indicator.
- Resource/reservation controllers now require keys and return the stored response envelope. Reservation services retain their atomic allocation/state rules while allowing the executor to handle expected domain rejections at its savepoint. Worker code is unchanged.
- `IdempotencyIT` adds real HTTP/PostgreSQL races and fault injection. `SchemaIT` adds V1-to-V2 upgrade, scope, shape and FK tests. Existing mutation tests use fresh keys for new intentions, delete dependent idempotency fixtures first, and replace the two earlier intentional duplicate-creation assertions with replay assertions.
- API flow, schema, TRD, README, developer brief, plan and evidence document the current contract. Existing dependency/container pins remain unchanged.

**Observed verification:** Initial IdempotencyIT passed 25 integration + 38 unit tests in 32.096s, finished 2026-09-27T23:45:46+05:30. Expanded targeted `IdempotencyIT,SchemaIT` passed **92 integration (28 + 64) + 38 unit tests**, zero failures/errors/skips, in 35.727s, finished 2026-09-27T23:48:09+05:30. Full regression passed **239 integration + 38 unit tests**, zero failures/errors/skips, in 01:31 min, finished 2026-09-27T23:52:54+05:30. No required check is blocked. Exact commands, logs and reports are in [Phase 8 evidence](VERIFICATION.md#phase-8).

| Experiment | Observed outcome |
| --- | --- |
| 13 overlapping requests with one key, each of four mutation endpoints | Exactly 1 original and 12 byte-identical replays per endpoint; at least 2 independent DB sessions observed waiting on the unique claim |
| Hold duplicate effect | 1 reservation, quantity 2, available capacity 8 of 10, resource version 1; conservation holds |
| Resource duplicate effect | 1 newly created resource; Unicode JSON, original 201 and Location preserved |
| Confirm/cancel duplicate effects | 1 terminal transition; confirmation keeps allocation, cancellation releases once |
| Concurrent mismatch, changed body/path, normalized JSON/UUID/name | Changed request conflicts without overwrite; canonical equivalents replay |
| Caller/operation isolation and ownership | Separate callers and operations have separate records; foreign owners/admin cannot replay private results |
| Stored capacity/missing/expired/conflicting results | Original error JSON retained after later state/capacity changes; body requestId remains original, header is fresh |
| Response-storage failure after reservation/decrement | Claim, reservation and decrement roll back; retry succeeds once |
| Winner insertion failure while duplicate waits | First gets 503; waiter claims after rollback and gets 201; one reservation/decrement |
| Business write followed by expected rejection | Savepoint undoes the write; completed rejection persists and replays |
| Unique claim timeout | Existing 5s lock timeout yields 503 within test's 4-12s window; retry after rollback succeeds |
| Application-context restart | Close/reopen contexts and pools against same PostgreSQL; original response replays with one allocation |
| Lost response after commit | Raw socket request; DB commit observed independently; response never read; retry returns saved 201/body/Location |
| Raw SQL incomplete committed claim | Replay fails closed with 503; health is DOWN; no allocation or takeover |
| Migration/constraints | Fresh V1+V2 and existing V1 hold upgrade pass; constraints reject malformed/partial data and invalid references |

**Failure/retention rules:** Store 200/201 and eligible authenticated/validated domain 404/409 results. Do not store malformed requests, authentication/role/ownership denials, mismatches, infrastructure failures or timeouts. A stored capacity conflict stays a conflict even after units become free; a new intention needs a new key. Retention is indefinite; deleting records could permit duplicate effects. No cleanup worker exists. Database access must protect stored user-linked responses. A CHECK permits an empty in-transaction claim, so application completion and operational detection are still required.

**Why mocks cannot prove this:** A mock cannot reproduce PostgreSQL's unique-index wait, READ COMMITTED statement snapshots, transactional rollback/savepoints, foreign-key behavior or commit visibility across independent connections. The tests use real requests, transactions, barriers, test-only advisory gates and bounded observation of pg_stat_activity. Unexpected infrastructure failures fail assertions; they are not counted as business conflicts.

**Verify it yourself:** Follow the environment setup in the evidence README, run `.\mvnw.cmd '-Dit.test=IdempotencyIT,SchemaIT' verify`, then `.\mvnw.cmd verify`. README curl examples create a key once per intention and reuse it on retry. No user Compose database was reset or used for these tests.

**Limits:** Context restart uses the same JVM and durable PostgreSQL, not an OS/host crash. Lost-response testing proves commit plus unread response and successful retry, not every network fault. Finite schedules are not performance or exhaustive-interleaving claims. Basic authentication remains for localhost development. Earlier requests without stored keys cannot be recovered retroactively. Full-response retention grows storage. Outbox/events, Redis/Kafka integration and later reconciliation remain unimplemented. No assertion about the user's personal review or understanding is made.

**Interview explanation:** PostgreSQL's scoped uniqueness elects the mutation owner; its transaction commits the effect and original response together. A duplicate waits, compares a canonical hash, and recovers that stored response. Transaction rollback leaves the key available to a waiting or later retry.

**Questions:** Why is the replay SELECT a separate statement? Why must a rejected business mutation roll back to a savepoint before storing an error? Why does a replayed error have two different request IDs? Why must caller, operation and path participate in the contract? What could go wrong if retention or canonicalization changed?

## Phase 9 - Transactional outbox

Status: **COMPLETE** on 2026-09-28. Stop before Phase 10.

**WHAT:** A durable, versioned event accompanies every committed HELD creation and terminal reservation transition. Sequence 1 is ReservationHeld; sequence 2 is ReservationConfirmed, ReservationCancelled or ReservationExpired. Each committed envelope has a stable server-generated event UUID. All events remain PENDING; no publisher, consumer or direct Kafka call was added.

**WHY:** Writing an event after the business commit leaves a crash window. Writing it before business work in a separate transaction can announce a change that never commits. The existing PostgreSQL transaction now includes reservation, capacity, transition sequence, event and (for HTTP) stored idempotency response. Failure of any part rolls back the whole operation.

**HOW:** ReservationService appends the event after hold insertion or after the locked terminal transition and any release. ExpirationProcessor appends after EXPIRED and capacity release inside its existing REQUIRES_NEW per-reservation transaction. OutboxWriter uses MANDATORY propagation: it must join a caller transaction, and a detached invocation fails. Existing reservation locks serialize terminal sequence advancement. Replays and same-terminal no-ops return before append. Lock order remains API claim -> existing reservation -> resource if releasing -> outbox; new holds allocate before inserting their previously invisible reservation.

The typed envelope contains eventId, schemaVersion=1, eventType, aggregateType=RESERVATION, aggregateId, aggregateSequence, occurredAt, correlationId and data (resourceId, userId, quantity, status, expiresAt, releasedQuantity). Decision time comes from the existing database-clock sample; it is not broker/commit time. HTTP correlation is the server X-Request-Id, while workers generate their own correlation UUID. JSONB stores semantic JSON, not exact original key/byte ordering. Existing API response DTOs are unchanged.

**Release/order semantics:** No CapacityReleased event exists. Cancellation/expiration alone carry releasedQuantity equal to quantity; HELD/CONFIRMED carry zero. This avoids two event types claiming the same release. Future consumers must deduplicate eventId before applying effects. Sequence describes logical order within one reservation, not resource-wide/global order or eventual arrival order. There is no exactly-once delivery claim. A rolled-back attempt has no committed event identity; a retry can generate a new UUID. A committed event keeps its ID across replay/restart and future transport retries.

**Activation policy and actual environment:** The workspace matched the verified Phase 8 manifest before editing. Docker initially reported its Linux-engine pipe unavailable; starting Docker Desktop succeeded. Read-only Compose inventory then found no Atlas project containers, so no existing local reservation dataset was activated or reset. Testcontainers created disposable databases. V3 takes an exclusive reservation-table lock and rejects any nonempty reservation dataset before schema changes; tests prove HELD and all terminal rows survive refusal with V2 intact. Empty V2 reservation datasets may retain resources/idempotency rows. Stop all old writers before migration and run only Phase 9 code afterward. Historical data needs a separately designed import or explicitly authorized reset of an identified disposable database; this phase does neither.

**Actual changes:**

- `V3__transactional_outbox.sql` adds the activation gate, reservation.transition_sequence, outbox table, aggregate FK, unique aggregate sequence, type/sequence/publication/JSON-shape checks and a partial PENDING index. V1/V2 remain unchanged.
- `outbox/ReservationEvent.java` defines the envelope; `OutboxWriter.java` appends inside a mandatory transaction. Reservation snapshots/repository now carry and increment sequence; the service and expiration processor append events.
- `OutboxIT` adds 12 real PostgreSQL cases. SchemaIT adds 19 cases (83 total) for upgrade/refusal and constraints. IdempotencyIT now checks total event count against committed transition sequences after all 28 cases; ExpirationIT checks per-reservation counts after worker races/recovery. Existing SQL fixtures now maintain state/sequence shape and delete outbox children before reservations.
- README, architecture/TRD, schema, API flow, developer brief, product/phase status and this guide describe the implemented contract. Dependency/container pins, API DTOs, credentials and public routes are unchanged.

**Verification actually observed:**

| Command (with repository-local Maven cache and documented Java/Docker setup) | Result | Finished |
| --- | --- | --- |
| `.\mvnw.cmd -B -ntp '-Dmaven.repo.local=S:\Atlas\.tools\repository' '-Dit.test=OutboxIT,SchemaIT,IdempotencyIT' verify` | PASS: 123 integration (12 + 83 + 28) + 38 unit, zero failures/errors/skips; 52.612s | 2026-09-28T14:30:50+05:30 |
| `.\mvnw.cmd -B -ntp '-Dmaven.repo.local=S:\Atlas\.tools\repository' verify` | PASS: 270 integration + 38 unit, zero failures/errors/skips; 01:41 min | 2026-09-28T14:33:33+05:30 |

All required checks passed; no test run failed or was skipped. Between the targeted/full runs, an event-count assertion was added to the existing expiration race suite and unused OutboxIT imports were removed. No production changes followed the passing full run. See [Phase 9 evidence](VERIFICATION.md#phase-9) for raw logs, XML reports, counts and source fingerprints.

| Experiment | Expected and observed |
| --- | --- |
| HELD then confirm/cancel/expire | Exactly two events, sequences 1/2, correct types/quantities/timestamps/correlation and all PENDING |
| Same key replay, later creation replay and new-key same-terminal repeat | No extra event, release or changed committed event identity |
| Outbox insert fails after create/confirm/cancel/expire writes | Test-only trigger observed matching business state/capacity first; failure rolled back full rows, sequence, event and API key; retry succeeded |
| Idempotency completion fails after outbox insert | Event observed inside transaction, then event/reservation/decrement/key all rolled back |
| Capacity/missing rejection or reservation insertion failure | No event; persisted domain errors retain their existing idempotency contract |
| Concurrent confirm versus cancel | One 200, one 409, one terminal event, state/capacity agree |
| Detached OutboxWriter invocation | MANDATORY transaction guard rejects it, no separate event commit |
| Application-context restart and replay | Event ID/payload/metadata unchanged, one reservation/decrement/event |
| Concurrent duplicates in Phase 8 suite and worker races in Phase 7 suite | Event counts equal committed transition counts; all prior assertions pass |
| Empty V2 upgrade | Resource/key preserved, V3 applied once, empty outbox |
| Nonempty V2 in each of four statuses | Migration refuses, rows/capacity/V2 schema preserved, no outbox or sequence column created |
| Invalid outbox rows | Duplicate event/aggregate sequence, missing FK, wrong state/sequence, bad type/version/status/payload shape rejected |

**Database versus application:** Constraints enforce shape and uniqueness, not complete historical truth or envelope/table equality. SchemaIT intentionally permits a structurally valid fixture envelope with placeholder values to show that boundary. Production mapping and OutboxIT verify real envelopes against committed rows. Direct privileged SQL can still break history/conservation; outbox data is immutable through this application, not protected by a universal history trigger. Indefinite retention grows storage and includes user-linked payloads.

**Why real PostgreSQL:** Unique constraints, row locks, transactional DDL, foreign keys, savepoints and visibility across commits are the properties under test. Mocks cannot demonstrate them. Finite barrier-controlled races are correctness evidence, not throughput or exhaustive-interleaving proof. Restart closes/reopens application contexts and pools in the same JVM, not an OS/host crash. No publication or downstream-delivery experiment ran.

**Verify it yourself:** Follow the evidence README environment commands and run the targeted selector plus full verify. Before starting a local app on existing V2 data, stop old writers and inspect reservation counts; do not bypass a refused migration. With an eligible database, use ordinary keyed hold/confirm/cancel requests and inspect `outbox_event ORDER BY aggregate_id,aggregate_sequence`; events remain PENDING. Replaying the same key must not increase event count. These SQL/curl examples are reproduction instructions; actual verification used isolated tests.

**Interview explanation:** The outbox stores the intent to publish in the same database commit as the reservation change. It prevents a committed transition from losing that intent, but does not by itself deliver a message. Stable IDs and logical sequence prepare future consumers for duplicates and reordering.

**Questions:** Why must the outbox writer join the existing transaction? Why can old reservation history not be fabricated at upgrade? Which failures roll back an already inserted event? Why is release quantity carried only on the terminal event? What does sequence prove, and what does it not prove about delivery?

## Phase 10 - Kafka and duplicate-safe consumption

Status: **COMPLETE** on 2026-09-28. This phase ended before Phase 11, subsequently implemented below.

**WHAT:** Optional Kafka publication of committed outbox events and one transactional reservation-audit consumer. Local Kafka is enabled; existing HTTP behavior and reservation allocation remain unchanged.

**WHY:** PostgreSQL commit and Kafka acknowledgment cannot form one atomic transaction here. Repeating an uncertain send is necessary for recovery, so stable IDs and a transactional consumer claim prevent repeating the database business effect. Kafka producer idempotence alone does not solve the send/mark or consumer commit/ack gaps.

**HOW:** OutboxPublisher locks one due PENDING row using FOR UPDATE SKIP LOCKED in a dedicated two-connection pool, sends the persisted envelope, waits for broker acknowledgment, then marks PUBLISHED. Rollback leaves the original row eligible with its stable eventId. Fixed-delay bounded batches run on a separate scheduler from expiration. The listener validates the v1 envelope and calls ReservationAuditService through a transaction proxy. A unique `(event_id,consumer_name)` INSERT ON CONFLICT claim and audit insert commit together. Only afterward does MANUAL_IMMEDIATE acknowledge synchronously. Failures stop the listener without skipping the record; correction plus app restart replays from the committed offset.

**Common failure modes:** Marking before acknowledgment; assuming producer idempotence means cross-database exactly-once delivery; committing a marker without its effect; acknowledging before database commit; silently skipping poison records; using unbounded broker waits on the API pool; assuming a Kafka key enforces database sequence order. Retain markers as long as replay is possible; this implementation retains events/markers/audit indefinitely. Single-broker local persistence is not replicated durability.

**Actual Codex changes:**

- Added BOM-pinned Spring Kafka 4.1.1 / kafka-clients 4.2.1 and Testcontainers Kafka 2.0.5 in pom.xml; Apache Kafka 4.3.1 and PostgreSQL 17.11 immutable images remain unchanged.
- Added V4 processed_event/reservation_audit tables with composite keys, marker FK, shape/type/sequence constraints and inspection index. Existing V3 history is preserved; V1-V3 were not changed.
- Added configuration/KafkaProperties, KafkaMessagingConfiguration and OutboxScheduling; outbox/PublisherTransactions and OutboxPublisher; messaging/ReservationEventDecoder, ReservationAuditService and ReservationAuditListener. Disabled messaging creates no Kafka clients/workers. No reservation transaction publishes directly to Kafka.
- Added application-kafka.yml, opt-in environment settings, Compose persistent kafka-data and explicit topic setup. README/TRD/schema/API/developer documents describe acknowledgment, locks, pool isolation, timeouts, crash recovery, retention, ordering and manual recovery.
- Added 11 real PostgreSQL/Kafka MessagingIT scenarios, 29 decoder unit cases, two SchemaIT cases plus fresh/upgrade expectations, and InfrastructureIT opt-in isolation. Test-only SQL fault triggers and advisory gates remain confined to disposable Testcontainers databases.

**Tests and observed results:** Targeted MessagingIT/SchemaIT passed **96 integration + 67 unit**, zero failures/errors/skips, at 15:17:27 +05:30. Full verify passed **284 integration + 67 unit**, zero failures/errors/skips, at 15:23:15 +05:30 on 2026-09-28 (03:28 min). [Evidence and run ledger](VERIFICATION.md#phase-10), [full log](VERIFICATION.md#phase-10), [counts](VERIFICATION.md#phase-10), and [MessagingIT report](VERIFICATION.md#phase-10).

Observed normal send/mark/consume/offset commit; failed mark after acknowledged send produced two identical broker records with one marker/audit effect; two real consumer containers with separate groups contended on the same stable consumer marker, with PostgreSQL advisory/transactionid waits observed before release, and committed one effect. A failure injected at acknowledgment after visible DB commit left the offset uncommitted; a new container with the same group redelivered and retained one effect. Audit insertion failure rolled back its marker and prevented acknowledgment, then recovered after removing the test fault and restarting the listener. Locked HELD event caused terminal-before-held delivery `[2,1]`, yet audit retained both logical sequences and capacity conservation held. Concurrent publishers skipped a locked row, uncommitted business events were invisible, an unavailable broker failed within the test's 10s bound and preserved PENDING, and a new app context automatically drained pending work using the actual scheduler and annotated listener. Poison input stopped before a later record; changed content for the same event ID was rejected.

V4 fresh/upgrade/constraints passed with existing reservation/event/idempotency history preserved. Full regression includes earlier real HTTP, ownership, idempotency, lifecycle, expiration and allocation contention checks. Mocks cannot prove PostgreSQL unique-index arbitration/rollback/visibility or Kafka broker acknowledgment/group-offset behavior. These tests use real infrastructure and bounded condition waits/latches/futures, not synchronization sleeps.

Three initial targeted attempts failed in the test harness: ambiguous generic AssertJ overload (no tests ran), tag-plus-digest parsing by Testcontainers (messaging initialization failed), and a test Admin API timeout smaller than its default request timeout (11 setup errors). Fixes used a typed local variable, the same immutable image in digest-only syntax, and explicit 3s request/5s API timeouts. Their logs remain linked in the evidence ledger; no failure was relabeled as a conflict or skipped. A documentation command also failed PowerShell parsing before writing any files; the corrected command completed.

Local Compose Kafka started healthy using the existing private verification environment at **localhost:19092**. Broker CLI returned **4.3.1**. Topic atlas.reservation-events.v1 has one partition/RF1 and indefinite retention; its same topic ID/configuration survived container recreation on the named volume. Kafka remains running. No existing local PostgreSQL data was migrated or reset; application/business tests used disposable containers.

**Verify it yourself:** Follow [README Phase 10](../README.md#architecture), run `.\mvnw.cmd '-Dit.test=MessagingIT,SchemaIT' verify` then `.\mvnw.cmd verify`, and inspect outbox status, audit/marker counts and group offsets. Use localhost:19092 for this session's existing Compose broker (the template default is 9092). Exact cache/JDK/Docker setup and actual commands are in the evidence ledger. Consumer failure needs correction and app restart; health alone does not prove progress. No required check remains blocked.

**Interview explanation:** At-least-once delivery plus a unique marker committed with the audit gives one database effect per event/consumer while markers are retained. Duplicate records remain possible across independent commits. The append-only audit tolerates reordered events; a future latest-state projection would need sequence/gap handling. External email/payment effects would require another reliability boundary.

**Questions:** Why can send success coexist with a PENDING row? What proves the marker and effect committed before acknowledgment? Why did the concurrent consumer test wait on the unique claim? Why can arrival be 2,1,1? Which failures require an operator restart? Why is deleting markers unsafe? Which outcomes were tested versus inferred?

**Limits and phase boundary:** Failure injection uses SQL triggers and an acknowledgment adapter with real containers, plus fresh listener/app contexts in one JVM; it does not simulate host power loss or every network partition. The audit is the only consumer effect. No exactly-once transport, benchmark, production readiness, Redis cache, durable retry/exhaustion/replay, or reconciliation claim is made. Those later phases remain NOT STARTED.

## Phase 11 - Redis caching

Status: **COMPLETE** on 2026-09-28. Stop before Phase 12.

**WHAT:** Optional cache-aside availability and individual owned reservation reads, with observation metadata, finite TTLs, post-commit invalidation and safe PostgreSQL fallback.

**WHY:** An informational cached view can reduce repeated data reads, but cached capacity/identity/state must never authorize a mutation. Redis and PostgreSQL cannot provide perfect coherence through best-effort invalidation; the implementation bounds accepted refill age and cache lifetime instead.

**HOW:** Cache availability under atlas:v1:availability:{resourceUUID} for 2s, reservation snapshots under atlas:v1:reservation:{reservationUUID} for 5s (HELD additionally capped by remaining database-measured lifetime). Verify reservation ownership against PostgreSQL before every Redis lookup. Hits preserve original observedAt, set cached=true and do not extend TTL. Miss/outage/corrupt data falls back to a PostgreSQL snapshot. Do not cache domain errors or overdue HELD. Read metadata is separate from unchanged mutation/replay DTOs; listings and full resource lookup remain database reads.

A miss records Redis TIME before fetching the DB snapshot. Both a monotonic client check and the atomic Redis fill script reject a refill older than 1s. The script checks time at SET execution, so delayed commands cannot install stale snapshots after a client timeout. Best-effort invalidation follows commit for resource creation and hold/confirm/cancel/expire; rollback does not invalidate. The one DEL clears both affected keys where applicable. Cache errors cannot reverse commit or fail its response. Allocation, transitions, ownership, idempotency and expiration decisions remain PostgreSQL-only.

**Common failure modes:** Allocating from cached availability; authorizing from cached userId; using stale HELD to approve cancellation; invalidating before commit; treating successful DEL as perfect coherence; refreshing TTL on every hit; admitting unbounded late fills; allowing a Redis timeout to fail an already committed response. Cache expiry, normal clock progress and bounded fill age are assumptions behind the storage bound.

**Actual Codex changes:** Added BOM-managed Lettuce 7.5.2.RELEASE (Netty 4.2.17.Final); no database migration or mutation/replay DTO changes. Added configuration/RedisCacheProperties and RedisCacheConfiguration; common/RedisCacheStore, ReadCache and CacheInvalidation; reservation/ReservationReadResponse and the ownership/snapshot queries. Wired only documented read paths and after-commit hooks into resource/reservation services and expiration. Added redis Spring profile and host/Compose environment settings, retaining the pinned Redis 8.4.7 image. Default-disabled startup creates no Redis client/health dependency. Added CacheIT (16 cases), RedisCacheConfigurationTest (11 cases), and a test listener on five core suites that proves the enabled/unavailable regression actually encountered invalidation failures. Updated one hold-read assertion for additive metadata and InfrastructureIT for opt-in isolation. Updated README, TRD, API/schema/developer contracts and phase status.

**Tests and observed results:** [Cache selector](VERIFICATION.md#phase-11) passed 16 integration + 78 unit tests at 15:52:35 +05:30 (44.077s). [Redis-unavailable core regression](VERIFICATION.md#phase-11) passed 120 integration + 78 unit at 15:57:18 (01:23 min), with actual invalidation fallback counts in allocation 62, expiration 35, idempotency 33, holds 77 and lifecycle 26. Full [verify](VERIFICATION.md#phase-11) passed **300 integration + 78 unit**, zero failures/errors/skips, at **16:02:27 +05:30**, 2026-09-28 (02:21 min). [Evidence ledger](VERIFICATION.md#phase-11), [counts](VERIFICATION.md#phase-11), [full CacheIT report](VERIFICATION.md#phase-11).

Real Redis/PostgreSQL tests verified hits/misses/TTL/metadata, explicit key eviction, DB ownership even with a cached snapshot and changed owner, malformed/missing fields/wrong Redis types, invalidation only after commit, no invalidation on rollback, Redis pause/outage with committed hold/exact replay/DB fallback and health UP, recovery, stale availability that cannot authorize quantity 10 against actual capacity 9, stale HELD that cannot cancel a CONFIRMED row, HELD TTL cap/DB expiration, and two controlled refill-after-invalidation races. The races deliberately restored stale availability/state while capacity/state in PostgreSQL remained correct; TTL then removed the stale keys. Fills older than 1s were rejected for both kinds. A paused server caused a fill timeout; the resumed server and direct old-token script test installed no stale key. Full verification reran all prior PostgreSQL/Kafka correctness suites and the final incomplete-payload check.

The first compile used nonexistent CompletableFuture convenience methods on Lettuce's ConnectionFuture interface; corrected through toCompletableFuture. Two cache-test compiles corrected the ACL method name/typed arguments. The first outage selector's unit-default check inherited the deliberate enabled=true integration override; isolating that unit environment fixed it, with one intermediate constant-name compile correction. Failed logs are retained; no failure was counted as a conflict or skipped. No required check remains blocked.

Local Compose Redis started healthy, PING returned PONG, server CLI reported 8.4.7, and the existing private verification settings publish it at **localhost:16379**. Kafka remains optional and unchanged. No existing local PostgreSQL data was migrated/reset; all business/fault tests used disposable containers. [Changed files](VERIFICATION.md#phase-11) and source hashes document the phase boundary.

**Verify it yourself:** Follow [README Phase 11](../README.md#recovery-procedures). Run `.\mvnw.cmd '-Dit.test=CacheIT' verify`; run the five-suite selector with `-Datlas.redis.enabled=true -Datlas.redis.host=127.0.0.1 -Datlas.redis.port=1`; run `.\mvnw.cmd verify`. Exact selectors, JDK/cache/socket settings and commands are in the evidence ledger. Inspect observedAt/cached and PTTL on your own keys. Host clients for the current local Redis use port 16379; template default is 6379. Redis failures fall back; PostgreSQL failure cannot be bypassed with cached identity.

**Interview explanation:** PostgreSQL decides every allocation, transition, owner check and deadline. Redis supplies a finite-lived informational snapshot. Commit invalidation helps freshness but a racing reader can refill old data; a server-checked fill deadline plus TTL bounds storage under stated assumptions. An outage reduces cache effectiveness without undoing committed work.

**Questions:** Why does an owner query precede a cache hit? Why can stale data reappear after DEL? Why check fill age on both client and Redis? How is HELD lifetime capped without making Redis authoritative? Which operations never use the cache? What did the outage listener prove? What does the bound exclude?

**Limits:** Accepted stale entries can remain up to **3s availability / 6s reservation** after mutation with the 1s maximum fill budget, plus a bounded read-command wait (100ms default). This is a storage/lookup bound under working clocks/expiry, not perfect coherence or an HTTP delivery SLA. Process/network delays and DB expiration-worker lag are separate. Outage tests use paused Redis/closed endpoints; eviction is controlled DEL, not an LRU stress benchmark. No throughput/latency benchmark, production security, durable invalidation retry or next-phase recovery implementation is claimed. Stop before Phase 12.

## Phase 12 - Retry and reconciliation

Status: **COMPLETE** on 2026-09-28. Required full verification passed; Phase 13 is not started.

**WHAT:** Durable publisher/consumer retries, attempt limits, fixed failure categories, guarded replay and consistent-snapshot reconciliation with safe expiration repair.

**WHY:** A retry must retain its original event identity and stop after a bounded recorded budget. An integrity finding does not establish a safe replacement value for capacity or missing history.

**HOW:** RetryPolicy uses exponential equal jitter, default base 1s/max 60s and five recorded attempts. OutboxPublisher retains its row lock across a savepoint rollback and commits a retry schedule or FAILED state after a failed send/mark. Consumer failures persist separately after effect rollback, gate early redelivery and stop on exhaustion without acknowledging/skipping. Controlled database functions require FAILED, expected replay_count and a reason code, preserving event/record identity. Inspection uses read-only REPEATABLE READ; guarded expiration locks reservation then resource and checks a fresh committed invariant/history snapshot before the existing atomic transition/release/event write.

**Common failure modes:** Counting a failed ledger commit as a persisted attempt; restarting before next_attempt_at; replacing event IDs; skipping poison offsets; comparing separately committed snapshots; overwriting capacity; fabricating events; forgetting that a lost broker acknowledgment can duplicate delivery. Database/network failure can leave an attempted send uncounted.

**Actual Codex changes:** Added V5 retry/replay metadata, consumer_failure, indexes and replay functions; RecoveryProperties/Configuration, RetryPolicy, ConsumerFailures, ConsumerRecoveryScheduling, ReconciliationService/Worker/Scheduling, guarded expiration and scripts/reconcile.sql. Updated publisher/listener behavior and configuration/environment settings. Added RecoveryIT (21 cases), 14 configuration/jitter unit cases and 16 schema cases; adjusted affected messaging/schema fixtures. No new dependency, public admin endpoint or Phase 13 feature was added. Updated the affected technical/schema/API/operator sections and [README runbook](../README.md#recovery-procedures).

**Tests and observed results:** After the user restored Docker, the required full verify finished on 2026-09-28 at **22:44:39 +05:30**, exit 0, in **5m56s**: **337 integration + 92 unit tests**, zero failures/errors/skips. This includes **21 RecoveryIT**, **11 MessagingIT**, **101 SchemaIT**, all existing allocation/lifecycle/cache/API regressions, and the corrected V4-to-V5 preservation assertion. Existing state, envelopes and audit effects survived upgrade; rerunning Flyway was a no-op. Compose configuration validation also passed. No successful suite was rerun after final acceptance.

The earlier targeted run had 133 integration cases with one schema fixture assertion failure (timestamp JSON from differently configured connection time zones). Its observer now explicitly uses UTC; the final run verified the correction. The earlier full attempt passed 92 unit cases but failed all 12 integration-class initializers because Docker was unavailable; no integration cases executed then. Those unsuccessful logs remain evidence. No infrastructure error was counted as a business conflict or skipped.

Passing recovery scenarios include jitter bounds/exhaustion, retry gates, restart/replay with stable identity, concurrent replay with one winner and four broker records producing one audit effect, consumer poison exhaustion with no offset skip across restart, rollback when failure bookkeeping fails, repeated/concurrent reconciliation, corruption refusal and the read-only operator script. Eight independent traffic workers committed 80 holds and 80 terminal transitions while **32** snapshot inspections reported zero false drift in the final run; final available capacity was 60 of 100. A gated uncommitted allocation and a repair waiting for a resource writer also passed. These are actual PostgreSQL/Kafka checks, not mock proofs.

Earlier fixture corrections addressed restart-before-due, JDBC Timestamp typing, duplicate restart flags and an observation transaction holding a table lock across migration. Full logs and concise reports are retained in [Phase 12 evidence](VERIFICATION.md#phase-12) and [test summary](VERIFICATION.md#phase-12). Docker restart diagnosed an inaccessible sailor-ingest.sock runtime file. A scoped stop/socket-removal attempt failed with Windows error 1920; no container/volume/database reset or deletion occurred. The user then restored Docker, which responded with engine 29.7.2 before the successful full run. The environment blocker is resolved.

**Verify it yourself:** With Docker running, use the exact full Maven verify command in the evidence ledger to reproduce all required checks, including V4-to-V5 preservation. The targeted selector is `.\mvnw.cmd '-Dit.test=RecoveryIT,MessagingIT,SchemaIT' verify`. Follow README for read-only inspection, controlled replay and restart; do not reset offsets or erase dedupe markers. The recorded full verification passed; stop before Phase 13.

**Interview explanation:** Retry scheduling and failure state are durable, but database/Kafka delivery remains at least once. Reconciliation observes one committed snapshot and only repairs through a locked, idempotent transition. Uncertain drift is reported for investigation. The completed local acceptance checks establish the recorded scenarios, with broader Phase 13 failure testing still deferred.

**Questions:** Why can a crash leave a send uncounted? How does an expected replay count prevent repeated operator resets? Why does a read-only snapshot avoid false drift? Why must a repair acquire locks and recheck after inspection?

## Phase 13 - Failure and recovery suite

Status: **COMPLETE** on 2026-09-28. All eight requested scenarios and the complete applicable test suite passed. At Phase 13 completion, Phases 14-15 remained NOT STARTED.

**WHAT:** End-to-end failure injection over real PostgreSQL, Kafka and Redis, actual authenticated HTTP requests and fresh application contexts.

**WHY:** A committed mutation can survive a lost response or downstream outage, while a pre-commit database failure must leave no partial state. Recovery must retain stable identity, apply effects once and restore capacity only through a legal transition.

**HOW:** Test-only PostgreSQL trigger gates observe writes before commit or a publisher mark reached after broker acknowledgment. A control connection rechecks and terminates only the exact waiting backend. A raw HTTP socket reads zero response bytes after an independent SQL observer proves commit. A real consumer's acknowledgment callback fails only after SQL observes its committed marker/audit. Actual broker pause/resume and Redis stop/start test infrastructure outages. Fresh contexts recreate HTTP servers, pools, workers and clients against retained infrastructure. Competing workers overlap through an observed lock, and pending expiration/publication drains through normal scheduled workers. Latches, lock observations and bounded awaits replace synchronization sleeps.

**Common failure modes:** Injecting before the intended boundary; confusing uncommitted writes with committed state; reading the supposedly lost response; counting broker duplicates as duplicate business effects; losing the Kafka offset; letting a container restart change the endpoint; converting infrastructure errors to capacity conflicts; or claiming whole-machine crash coverage from controlled connection/context failures.

**Actual Codex changes:** Added [FailureRecoveryIT](../src/test/java/com/atlas/FailureRecoveryIT.java) with exactly eight scenarios, suite-owned containers, test-only SQL gates/sequence, rechecked backend termination, consumer acknowledgment callback and stable Redis loopback binding. Every case asserts conservation, legal status/sequence and event-history shape; delivery cases assert marker/audit counts and committed offsets. Production code, V1-V5 migrations, API behavior, dependency/container pins and existing tests are unchanged. Updated only affected status, test-instruction, technical-boundary and evidence sections. No Phase 14 work was added.

**Tests and observed results:** Full verify finished at **23:21:30 +05:30**, 2026-09-28, exit 0, in **6m36s**: **345 integration + 92 unit tests**, zero failures/errors/skips. FailureRecoveryIT ran **8/8** successfully (76.68s), alongside every existing allocation, lifecycle, expiration, idempotency, outbox, messaging, reconciliation, cache, API/permission and infrastructure check. Required checks: executed/passed eight scenarios plus full regression; blocked **none**; unexecuted **none**. No successful full suite was rerun afterward.

Observed boundaries/results: PostgreSQL termination before commit rolled back reservation, capacity, event and idempotency writes; retry/replay created one hold. Lost HTTP response replayed exact original status/body/Location after application restart. Five mutations committed during actual Kafka pause and five audit effects recovered. Publisher recovery produced two identical broker records for one stable event and one audit effect. Consumer restart retained one unchanged committed audit row and advanced the previously unacknowledged offset. Redis outage preserved cancellation/hold/confirmation commits, then the existing client refilled current cache values after the same container restarted. Two independent expiration workers produced one expiry/release/event. Pending-work restart expired one due hold, retained the future hold and delivered three events; second restart/replay preserved counts and current state.

The first focused run passed seven scenarios and 92 unit cases, with one Redis refill timeout. A disposable diagnostic probe confirmed Docker remapped the randomly published port (49677 to 59982) on stop/start. The fixture now selects and verifies a stable loopback port; production code and the 10s refill deadline were unchanged. Only that failed integration scenario was rerun and passed (one integration + 92 unit, 1m17s) before the required full run. Initial failure reports and all logs are retained in [Phase 13 evidence](VERIFICATION.md#phase-13); [test summary](VERIFICATION.md#phase-13) and [scenario results](VERIFICATION.md#phase-13) separate executed, blocked and unexecuted checks.

**Verify it yourself:** With Docker running, use the exact focused and full Maven commands in the evidence ledger. The README links FailureRecoveryIT and full verify. Fault fixtures and container controls target only disposable suite infrastructure; normal startup exposes no fault flag or route. Each scenario has a 120s outer timeout, 5-15s boundary/HTTP checks and up to 35s delivery/recovery waits. See the evidence matrix for exact bounds. Stop before Phase 14.

**Interview explanation:** The suite distinguishes rollback before commit from uncertainty after commit, then proves recovery with stable IDs, durable idempotency and duplicate-safe database effects. PostgreSQL remains authoritative while Kafka/Redis fail. Fresh application contexts and terminated database connections are the tested restart/failure mechanisms; these results do not claim exhaustive OS/process/host crashes or performance. Generated implementation and test results do not establish personal mastery.

**Questions:** Which observation proves the fault reached its intended boundary? Why can two broker records produce one business effect? Why does a replayed HELD response differ from the current EXPIRED state? Why must the Redis endpoint stay stable during a client-recovery test? What additional uncertainty would a whole-process kill introduce?

## Phase 14 - Reproducible load tests

Status: **COMPLETE** on 2026-09-29. Stop before Phase 15.

**WHAT:** Five native k6 workloads: normal, spike, hot-resource contention, concurrent idempotent retries and mixed hold/confirm/cancel/retained-HELD traffic. Real PostgreSQL, Kafka, Redis and the unchanged application run in an isolated labelled benchmark stack.

**WHY:** Latency/throughput numbers need a stated workload, configuration, error policy and raw evidence. Correctness and eventual delivery remain acceptance conditions when latency or backlog grows.

**HOW:** Generate manifest-scoped resources and keys, record seed 42 and server-generated IDs, warm up separately, run fixed closed VU scenarios, classify errors, collect custom measured latency/request metrics and external SQL samples, then validate one consistent snapshot. Drain delivery/audit within 60 seconds, stop workers, verify committed Kafka lag zero, delete only manifest-matched run rows, preserve an unrelated canary, and remove only owned infrastructure.

**Common failure modes:** Reporting fast conflicts as successful allocations; counting infrastructure failures as contention; mixing warmup/setup with measurements; combining percentiles from different workloads; treating sampled peaks as continuous maxima; calling publication marks consumer delivery; broad cleanup; claiming a bottleneck without utilization data.

**Actual Codex changes:** Added pinned k6 toolchain, isolated resource-limited Compose, PowerShell setup/run/observation/cleanup controller, common helpers and five workload entry points, four PostgreSQL observation/validation/publication/cleanup scripts, a raw-evidence summarizer and reproduction instructions under `scripts/load`. Updated only affected documentation and Phase 14 evidence. No Java source, API, migration, dependency or existing test change.

**Tests and observed results:** Five smoke checks and five measured workloads passed once each. Measured scenarios ran 30 seconds after 5-second warmup plus 5-second drain/gap, with 15-second graceful in-flight deadlines. There were 3,769 measured API requests: 3,212 successes, 557 expected hot-resource conflicts, zero unexpected failures and 100% measured contract checks. Every run passed capacity bounds/conservation, legal history/state, one reservation per successful hold claim, marker/effect consistency, eventual delivery and scoped cleanup. Mixed final state including warmup was 73 HELD, 73 CONFIRMED, 144 CANCELLED, allocated quantity 288; recycled scenarios ended with zero allocation. Full regression passed **345 integration + 92 unit tests**, zero failures/errors/skips, in **03:40 min**. No blocked/unexecuted check; successful runs were not repeated.

| Workload | Requests | req/s | p50 / p95 / p99 ms | Expected conflicts | Unexpected | DB connections max* | Backlog max* | Publication p95 ms |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: |
| normal | 780 | 25.76 | 295.52 / 385.46 / 406.95 | 0 | 0 | 9 | 8 | 486.62 |
| spike | 711 | 23.42 | 748.44 / 1095.19 / 1247.26 | 0 | 0 | 11 | 272 | 16901.01 |
| hot-resource | 769 | 25.16 | 898.55 / 1301.04 / 1495.71 | 557 | 0 | 11 | 25 | 3305.02 |
| idempotent-retry | 772 | 24.69 | 809.00 / 1248.82 / 1463.57 | 0 | 0 | 11 | 215 | 17015.80 |
| mixed | 737 | 24.12 | 300.00 / 400.37 / 457.18 | 0 | 0 | 8 | 4 | 303.24 |

*Database connection/backlog maxima are sampled. Each case had 21 measured observations; maximum gaps were 1.43-1.57 seconds. Percentiles above cover all measured API responses, including conflicts; success/conflict latency and publication p50/p95/p99 are separate in raw result JSON. Publication p95 near 17 seconds in spike/retry is an observed limitation; both queues drained within 60 seconds. [Phase 14 evidence](VERIFICATION.md#phase-14) records exact CPU/OS/versions/container limits, dataset/concurrency/durations, commands, logs and results. One short shared Windows/WSL run per workload does not establish saturation, a production SLO or hardware-isolated stability; CPU/GC/consumer latency and bottlenecks were not measured. No personal understanding is inferred.

**Verify it yourself:** Follow the [load README](../scripts/load/README.md), use a new session ID and reproduce setup -> smoke -> measurements -> invariant checks -> offset/canary-guarded cleanup. Inspect raw category points, custom measured summary, DB snapshots and publication timestamps; recalculate throughput using measured elapsed including drain. Exact executed commands and full regression results are in the evidence ledger. If infrastructure cannot run, record BLOCKED and leave performance unclaimed. Stop before Phase 15.

**Interview explanation:** Correct allocation and replay can survive contention while response and publication latency increase. PostgreSQL post-run invariants establish what was committed; request rates and error categories show what clients observed. Stable IDs and transactional markers prevent duplicate audit effects. These are local measurements under recorded limits, not maximum production capacity.

**Questions:** Why are 769 hot-resource requests only 212 successes? Why do replay calls increase HTTP throughput without creating extra reservations? What does p99 include? Why do snapshots include warmup while request metrics exclude it? Which sampling peaks might be missed? Why must the consumer offset be committed before deleting its markers? What evidence would identify the actual bottleneck?

## Phase 15 - Final review and interview handoff

Status: **COMPLETE**, 2026-09-29. Fresh host/container startup, corrected normal/recovery demo, affected isolation check, complete regression and final documentation/scope checks passed. Required checks have no outstanding failures or blocks; unresolved/deferred work is disclosed.

**WHAT:** A current README with startup/configuration, keyed API examples, architecture/state diagrams, schema relationships, recovery runbook and evidence links; a seven-document implementation review; an executable isolated normal/failure demo and honest learning/handoff material.

**WHY:** A handoff must match the implementation and distinguish measured behavior from aspirations, finite tests from production guarantees, and Codex-authored work from personal understanding.

**HOW:** Compare each planning contract with production SQL/transaction/security/cache/messaging code and retained tests. Fix stale current statements and broken examples, document unresolved/deferred items, run fresh host/container startup and deterministic HTTP/SQL recovery demonstrations, retain failure details, and run the full applicable regression once after the repair. Do not deploy externally.

**Common failure modes:** Calling all configured technology authoritative, replaying with a fresh key, trusting availability or path userId, releasing confirmed capacity, deleting dedupe/history to clear an error, presenting samples as continuous peaks, equating generated explanations with personal mastery, or forgetting that Compose process variables override an env file.

**Actual Codex changes:** Consolidated the README and updated affected current contracts in all seven planning documents. Added `scripts/handoff/demo.ps1`, an isolation-only Compose override and read-only inspection SQL. Added schema relationships and final learning/review/evidence material. No production Java, dependency, migration, normal Compose or existing test behavior changed.

**Tests and observed results:** Clean host JAR startup passed with V1-V5 migrations, PostgreSQL connectivity, public health and authenticated HTTP. The first container segment failed because restoring absent process variables through the .NET API left empty credentials overriding the private env file. The runner now preserves variable existence and explicitly isolates its CLI child's session values. The affected complete startup/demo flow then passed, including normal permissions/validation, atomic holds and lifecycle, zero-body-read response replay, Kafka pause with durable retries and pending-work application restart, Redis outage/fallback/refill, overdue HELD with workers disabled and EXPIRED after restart, read-only reconciliation, conservation, transition history and one audit effect per event. Final available capacity was 7 from total 10, with 3 confirmed units retained. Review also found SQL CLI calls bypassing the isolation helper; they now use it. A container/demo-only check with empty/conflicting inherited values passed (31 HTTP observations); its host segment was intentionally NOT RUN because the default full check already passed (33 observations). The full applicable regression ran once: 345 integration + 92 unit tests, zero failures/errors/skips, Maven time 3:24. No application/test source changed in the subsequent helper-only repair. The first documentation check found one broken historical Phase 13 anchor; a legacy alias repaired it, with the finding retained. First failure, corrected/targeted logs and documentation/scope checks are retained in [Phase 15 evidence](VERIFICATION.md#phase-15).

**Verify it yourself:** Apply README prerequisites, build with full `verify`, then run `.\scripts\handoff\demo.ps1 -SessionId p15-reproduce-001` with a new ID. Inspect real HTTP responses and final snapshots before reading the explanations below. The script uses private ports/credentials/volumes, bounded waits and ownership-checked cleanup; failed data/logs remain until guarded cleanup. Reproduce specific regression suites and Phase 14 loads using their exact evidence commands. Do not reset ordinary Atlas data.

**Interview explanation:** Use the one-minute explanation and deeper mechanisms below as study material, then explain them without looking at generated code. Finite local tests support the listed project properties; they do not establish production HA/security, universal schedules, perfect caching or exactly-once transport.

**Questions:** Which operation establishes each atomic boundary? What survives a committed response loss or app restart? Why is deadline passage not a release? Which mismatches are repaired, refused or operator-replayed? Which part did you personally modify, run or explain independently? Your answers/reproductions remain NOT RECORDED until you supply them.

## One-minute architecture explanation

Atlas is a local Java 21 and Spring Boot service for reserving fixed capacity. PostgreSQL is the authority. A hold uses a conditional capacity update and inserts the reservation in one transaction, so concurrent callers cannot allocate the same units. The caller comes from authentication. A scoped idempotency key, the business change, its event and the original HTTP response commit together; retries replay that result. Confirmation retains capacity. Cancellation or a database-backed expiration worker releases a HELD reservation exactly once. An outbox publisher sends committed events to optional Kafka and marks them after acknowledgment. Delivery can duplicate, so the consumer commits a unique processed marker with its audit effect before acknowledging. Optional Redis caches informational reads; allocation and ownership still use PostgreSQL. Reconciliation inspects a consistent snapshot, safely expires verified overdue holds and reports uncertain corruption. Real-infrastructure tests and local load results support these behaviors, with explicitly limited production and personal-understanding claims.

## Mechanisms you should be able to explain

### Allocation and lifecycle

The decisive statement is `UPDATE resource SET available_capacity=available_capacity-quantity WHERE id=resourceId AND available_capacity>=quantity`. PostgreSQL serializes updates to the same row and rechecks the predicate after a concurrent updater commits. A client-side SELECT followed by a decrement would let two callers act on the same old capacity; this statement makes admission part of the write. Under READ COMMITTED, a zero-row result means no admission; a subsequent existence probe distinguishes missing resource from capacity conflict. Different resources can allocate independently.

The reservation insertion, outbox event and completed idempotency result join the same transaction. An insertion/serialization/event failure rolls all of them back, including the decrement. The database bounds CHECK prevents negative or above-total counters, but only the transaction maintains the cross-row equation `available + HELD quantities + CONFIRMED quantities = total`. Every persisted HELD counts even after its deadline. Confirmation changes HELD to CONFIRMED without another decrement; cancellation locks the owned reservation, checks state/time, changes it and releases its quantity in the same transaction. The common existing-row order is key claim -> reservation -> resource -> event. A new hold locks the resource then inserts a previously invisible reservation; it never locks an existing reservation. Same-target repeats do no further business write; incompatible terminal commands conflict. Phase 5's independent-transaction PostgreSQL test observed exactly 10 successes and 90 conflicts for 100 one-unit attempts at capacity 10. Mocks cannot demonstrate PostgreSQL predicate rechecks, isolation, locks or rollback under real contention.

### Durable idempotency

Scope is authenticated caller UUID, stable operation and key. The canonical validated request hash includes typed fields and relevant path UUIDs, with fixed array ordering and name/UUID normalization. Transport formatting, request IDs and passwords are excluded. A unique PostgreSQL claim arbitrates concurrent duplicates; there is no lookup-only decision, JVM mutex or Redis claim. A losing insert waits for the winner's commit/rollback, then a separate READ COMMITTED statement sees the committed record. Matching hash replays; mismatching hash returns 409. If the winner rolls back, a contender can become the next winner.

The claim is not committed separately from mutation/result. A savepoint rolls back eligible domain rejection work before storing that deterministic 404/409; unexpected failures abort the entire transaction. Original status, UTF-8 JSON body, content type and Location survive restart. A lost response after commit can therefore replay without another reservation/decrement; a replayed HELD body can differ from present state. Stored error requestId stays in the original body while the new header identifies the current attempt. Validation, auth/ownership, key mismatch and infrastructure failures are not newly retained. A stored capacity conflict stays a conflict even after capacity becomes free. A new key means a new intention, so arbitrary new keys cannot deduplicate retries. Bounded duplicate waits return retryable errors; full records are retained indefinitely, with storage/privacy/retention work deferred.

### Expiration and authoritative time

Eligibility uses PostgreSQL `clock_timestamp()` sampled after the reservation lock, not JVM time, HTTP arrival or transaction-start CURRENT_TIMESTAMP. A request can wait across the deadline; the post-lock sample must reject it. Equality is expired. Conversely, a decision sampled just before the deadline may commit afterward and remain valid under the documented policy. Configuration affects only new deadlines.

The worker discovers bounded due HELD IDs through a partial index, then starts one independent transaction per candidate. `FOR UPDATE SKIP LOCKED` allows competing workers to skip an occupied row; discovery is only a hint. It rechecks state/deadline, changes HELD to EXPIRED, increments availability and appends the event atomically. One winner releases; terminal or failed candidates cannot double-release, and failed transactions leave the row eligible for a later poll/restart. Deadline passage only removes eligibility; capacity remains allocated until this transaction commits. Fixed-delay polls, old locked rows, backlog, resource waits and outages can increase lag; bounded candidate counts are not a fairness or end-to-end completion guarantee. The demo deliberately disables both workers to show overdue HELD, then restores them on app restart.

### Transactional outbox and publication

Each successful reservation transition writes one version-1 envelope with a stable event UUID and aggregate sequence: HELD=1, terminal=2. It commits with business state and the HTTP result. Replays and same-state no-ops append nothing. There is no separate CapacityReleased event: cancellation/expiration carry releasedQuantity, so consumers must not count a second release.

A publisher transaction locks one due PENDING row with SKIP LOCKED using a separate max-two connection pool. It waits for a bounded Kafka acknowledgment, then marks PUBLISHED and commits. If the process fails after send but before that commit, the broker may have the record while PostgreSQL still has PENDING. Retrying sends the same ID/payload; producer idempotence does not make the database and Kafka one atomic system. Stable logical sequence explains history, but independent locks/retries can deliver sequence 2 before 1; the shared Kafka key does not establish strict aggregate publication order. The audit effect is order independent. Any future state projection needs explicit sequence/gap handling. Holding a DB connection while sending has a throughput cost; Phase 14 measured substantial publication delay in spike/retry traffic rather than claiming unlimited throughput.

### Consumer deduplication and retries

A unique `(eventId,consumerName)` marker and its audit row commit in one PostgreSQL transaction. Concurrent duplicates contend on the marker: a winner applies once; a loser verifies the existing effect's semantic JSON content and produces no second effect. A failed effect rolls its marker back. Different payload for an already processed ID is an error, not a harmless duplicate. Consumer tables have no producer outbox/reservation FK, so delivery/arrival is independent of producer inspection.

Acknowledgment happens after the effect transaction commits and failure bookkeeping resolves. A crash before acknowledgment redelivers a record whose marker already exists; the effect remains once. The original failing record stays unacknowledged while a separate durable failure transaction records its topic/partition/offset and key/value hash. RETRY_WAIT gates early redelivery; the single-partition container stops and restarts when due. FAILED stops automatic progress until controlled replay, and later records wait behind it. No poison record is silently skipped. Equal-jitter exponential delays have a positive minimum and a configured recorded-attempt limit. Network ambiguity or database bookkeeping rollback can leave a send uncounted; limits are not a bound on every unknowable physical send.

### Optional caching

Cache-aside serves availability and individual owned reservation snapshots. PostgreSQL rechecks reservation ownership before any cached status can be returned; authentication and mutation admission never come from Redis. A hit preserves observedAt and sets cached=true. TTLs are 2 seconds for availability and 5 seconds for reservation state, capped by HELD lifetime. Cache failures fall back to SQL; post-commit invalidation is best effort and cannot reverse a committed business result.

An old read can finish after invalidation and refill stale content. The implementation bounds refill age to at most the configured 1-second maximum and does not extend an already loaded stale key beyond its remaining lifetime. With healthy Redis expiry and the documented time assumptions, possible post-mutation staleness is bounded by fill budget plus TTL (up to 3/6 seconds). This is not perfect coherence, a response latency SLO or permission to act on cached capacity. Deadline/ownership/lifecycle/idempotency always use PostgreSQL; cached HELD status is informational. Corrupt cache shape is ignored. Direct Redis tampering, clock faults or privileged database mutation are outside the local guarantees.

### Reconciliation and controlled repair

Counts and bounded details use one read-only repeatable-read snapshot. Reading availability and reservation sums from different transactions could report legitimate in-flight commits as false drift; the consistent snapshot compares committed state at one boundary. All HELD/CONFIRMED quantities count, including overdue HELD. Findings cover overdue holds, aged/due/failed events, consumer failures, missing/inconsistent history or effects and capacity drift. Full counts can scan the dataset despite bounded displayed details, so large-dataset scalability remains unmeasured.

Inspection ends before repairs. Each overdue candidate is rechecked under reservation -> resource locks; a fresh statement verifies conservation and history after locks before using the normal atomic expiration operation. Other allocation/release writers serialize on that resource; confirmation preserves allocated quantity. Uncertain integrity is refused/logged. Reconciliation never sets availability from a snapshot sum, fabricates missing history, deletes markers or silently resets failed budgets. Ordinary expiry is not a capacity-drift correction.

FAILED outbox/consumer work needs an operator to record identity/cause, stop affected workers, correct the cause and call the SECURITY INVOKER replay function with the inspected expected replay count and reason. Its compare-and-set check preserves event ID/payload or original offset/hash, increments replay metadata and resets only that cycle's schedule. A repeated expected count returns false. Resume and inspect stable IDs, one audit effect and committed offsets. An immutable poison payload cannot be fixed by editing it or resetting offsets. Latest replay metadata is not a complete operator audit trail.

## Questions to answer without generated code

Practice aloud or write answers before checking the mechanisms/tests. This checklist is not a record that you have learned them.

1. Why can two SELECT/check/decrement clients over-allocate, and which PostgreSQL predicate recheck prevents it here?
2. What happens to capacity when reservation insertion or outbox insertion fails?
3. Which invariant is cross-row, and why is a column CHECK insufficient?
4. Why does confirmation retain capacity without decrementing again? What locks prevent a double cancellation release?
5. Why is an overdue HELD still allocated, and why is transaction-start time unsafe after lock waits?
6. How do two expiration workers choose one winner? What lag/fairness is not guaranteed?
7. What exactly scopes an idempotency key? Which path/body fields affect its hash?
8. How can the losing duplicate see the committed response, and what if the winner rolls back?
9. Which errors are stored, which are retryable, and why can a stored conflict replay after availability improves?
10. Why can replay return HELD after current state is EXPIRED, and why are error-body/header request IDs different on replay?
11. Which crash gap duplicates publication, and why does a Kafka producer setting not close that gap?
12. How can sequence 2 arrive before 1? Why can this audit consumer tolerate it?
13. Why must marker and effect commit together and acknowledgment occur later?
14. What prevents the same event ID with changed content from being accepted?
15. What does a poison record do to later records in the local partition? Which recovery actions preserve its identity?
16. How can a stale cache refill race invalidation? What is the tested conditional staleness bound?
17. Why must PostgreSQL ownership be checked even on a cache hit?
18. How would different capacity/sum snapshots produce false drift? Why is uncertain corruption refused?
19. What does the replay expected-count check prevent? Which attempts can be uncounted?
20. Which measured latency population includes conflicts and warmup? What can sampled backlog miss?
21. Which experiment would establish a bottleneck, and why do current results not establish production capacity?
22. Which part have you personally reviewed, reproduced and modified, and which claim can you defend independently?

## Normal behavior and failure-recovery demo

Run the [documented handoff demo](../README.md#verified-normal-and-failure-demo) with a new session ID. Watch total 10 -> confirmed 3/available 7; Bob's hold/cancel temporarily consumes/releases 2. Discard a committed hold response body and replay the same key; verify no additional reservation. Pause Kafka, commit another hold and inspect PENDING plus recorded retry; restart the app with persisted work and restore the broker, then verify one audit effect per event. Stop Redis, cancel that hold and read correct SQL fallback, then restart Redis and observe a fresh cache hit. Create a 3-second hold with both workers disabled, observe due HELD and rejected confirmation, then restore workers on app restart and verify EXPIRED plus available 7. Inspect the read-only reconciliation report.

These are executed local demonstrations, with raw HTTP/SQL observations and bounded deadlines in [Phase 15 evidence](VERIFICATION.md#phase-15). The lost-body demonstration reads headers but zero response body bytes; broader deterministic pre/post-commit/ack failures are in [Phase 13 evidence](VERIFICATION.md#phase-13). The full demo is Codex-executed; your own reproduction remains NOT RECORDED. It uses a separate project and labelled volumes, so cleanup never requires resetting ordinary Atlas data.

## Ownership of work and claim gates

| Activity | Codex record | Your personal record |
| --- | --- | --- |
| Implementation authoring | Generated implementation across Phases 1-14; final scripts/docs in Phase 15 | Independent authoring/modification NOT RECORDED |
| Implementation review | Compared all seven planning contracts with source and evidence; gaps listed in final review | Personal review NOT RECORDED |
| Testing | Executed retained real-infrastructure suites, load workloads and final demos | Personal execution/reproduction NOT RECORDED |
| Understanding | Explanations/questions supplied as study material | Ability to explain/modify without generated code NOT RECORDED |

Use project facts in a portfolio with Codex assistance disclosed. Do not claim independent design, personal mastery, user counts, coverage percentages, deployment or production readiness from this record. The table records missing evidence, not a judgment about what you may have done outside this chat.

| Evidence-backed project claim | Evidence / boundary |
| --- | --- |
| Java 21/Spring Boot backend uses PostgreSQL atomic capacity allocation | [Phases 4](VERIFICATION.md#phase-4)/[5](VERIFICATION.md#phase-5); exact capacity-10 test and rollback |
| Durable idempotent mutations recover concurrent retries and lost responses | [Phase 8](VERIFICATION.md#phase-8); original response, one reservation/decrement |
| Atomic events and duplicate-safe audit effects recover acknowledgment gaps | [Phases 9](VERIFICATION.md#phase-9)/[10](VERIFICATION.md#phase-10)/[13](VERIFICATION.md#phase-13); no exactly-once transport |
| Database-backed expiry and guarded reconciliation handle due/failed work | [Phases 7](VERIFICATION.md#phase-7)/[12](VERIFICATION.md#phase-12); corruption reported/refused |
| Optional Redis outage does not revoke committed reservation behavior | [Phase 11](VERIFICATION.md#phase-11) and final demo; conditional bounded staleness |
| Reproducible local k6 measurements distinguish conflicts and failures | [Phase 14](VERIFICATION.md#phase-14); one short run/scenario, recorded hardware/config, no SLO |
| Final local startup/normal/recovery instructions are verified | [Phase 15](VERIFICATION.md#phase-15); failed fixture attempt retained and repaired |

A factual portfolio line: "Codex-assisted Java/Spring Boot reservation demo using atomic PostgreSQL allocation, durable idempotency and outbox events, with duplicate-safe audit consumption and reproducible real-infrastructure evidence." Before turning it into an individual resume claim, personally reproduce the checks, explain tradeoffs and identify your own contribution. Record those activities explicitly; no existing claim gate assumes them.

Honest limits: fixed single-resource capacity; confirmed units remain allocated; local Basic identity/PLAINTEXT broker rather than production security; single local partition; no exactly-once transport/strict arrival order; finite schedules and conditional cache bounds; indefinite records/retention growth; no supported unsafe drift/history repair; no backup/restore, host power-loss, production HA/multi-region or saturation proof. Uniform success/JSON logging, exported request/operations metrics, tracing and dashboards are unresolved/deferred planning aspirations. Phase 14 publication p95 near 17 seconds is preserved, not optimized away or described as a production target. Completed, resolved failures, blocked checks and deferred work are separately listed in the final ledger. No external deployment occurred.

## Documentation verification record

Executed on 2026-09-27 using the bundled Python runtime in UTF-8 mode through PowerShell, exit code 0. Assertions checked the exact seven-file set, headings, balanced Markdown fences, absence of Unicode replacement characters, all relative document links, phase numbering 1-15, NOT STARTED status in both phase documents, all nine requested learning fields in every phase, and absence of application files. All structural assertions passed.

Manual consistency review checked the five-minute default, retained confirmed capacity, persisted-HELD conservation, post-lock database deadline boundary, reservation-before-resource lock order for existing rows, original-response idempotency replay, indefinite retention, event sequence versus arrival order, single transition/release event, cache fallback, and safe reconciliation boundaries across the documents. No conflicting contract was found in this review; this is a documentation review, not proof of runtime correctness.

Historical Prompt 0 structural check (it expects the original all-NOT-STARTED snapshot and is not the current acceptance check after Phase 1; retained to document what was checked then):

```powershell
@'
from pathlib import Path
import re
d = Path(r'S:\Atlas\docs')
expected = {'PRD.md', 'TRD.md', 'APP_API_FLOW.md',
            'DEVELOPER_INTERFACE_BRIEF.md', 'BACKEND_SCHEMA.md',
            'IMPLEMENTATION_PLAN.md', 'LEARNING_GUIDE.md'}
assert {p.name for p in d.iterdir() if p.is_file()} == expected
for name in expected:
    p = d / name
    text = p.read_text(encoding='utf-8')
    assert text.count(chr(96) * 3) % 2 == 0, name
    for target in re.findall(r'\]\(([^)]+)\)', text):
        if re.match(r'^[A-Za-z][A-Za-z0-9+.-]*:', target):
            continue
        assert (d / target.split('#')[0]).exists(), (name, target)
for name in ('IMPLEMENTATION_PLAN.md', 'LEARNING_GUIDE.md'):
    text = (d / name).read_text(encoding='utf-8')
    assert re.findall(r'^## Phase (\d+) - ', text, re.M) == [str(i) for i in range(1, 16)]
    assert len(re.findall(r'^Status: \*\*NOT STARTED\*\*\.', text, re.M)) == 15
print('PASS: planning file set, links, fences, phase numbering and statuses')
'@ | python -X utf8 -
```

At Prompt 0, application checks were not run and version pinning was deferred. That historical statement is superseded by the Phase 1 evidence above: build/pins are recorded, PostgreSQL integration initially blocked but now passed, and Phase 2 evidence now records the database foundation. This is a historical Prompt 0/Phase 11 record; current phase status and evidence are recorded above, not inferred from the original check. No blocker prevented completion of Prompt 0.

Phase 11 current documentation checks passed on 2026-09-28: relative links/fences, Phases 1-11 COMPLETE and 12-15 NOT STARTED, unchanged V1-V4 migrations and idempotency/messaging/outbox sources, and saved source/changed-file inventory. See [current check record](VERIFICATION.md#phase-11).
