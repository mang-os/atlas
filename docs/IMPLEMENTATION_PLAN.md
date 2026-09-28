# Atlas - Implementation Plan

Phases 1-15 are **COMPLETE** for the local v1 handoff. Phase evidence below records actual changes, failures/repairs and acceptance results. Phase 15 verified fresh host/container startup, normal/failure demonstrations and one full regression (345 integration + 92 unit tests, zero failures/errors/skips), reviewed all seven planning documents and disclosed unresolved/deferred items. [Final execution ledger](VERIFICATION.md#phase-15), [requirement review](VERIFICATION.md#phase-15). Personal review/understanding remains NOT RECORDED. No external deployment or production-readiness claim. Stop with the final handoff.

## Working contract

- Implement only the phase explicitly requested.
- Read the relevant documents before editing code.
- PostgreSQL is the source of truth.
- Correctness comes before performance.
- Redis must never decide whether allocation is allowed.
- Kafka delivery is at least once.
- Never invent test results, benchmark numbers, or completed work.
- Run the requested phase checks and applicable regression tests.
- If a required check cannot run, mark it BLOCKED, explain why, and provide the exact verification steps. Do not mark the phase complete.
- Update LEARNING_GUIDE.md after every implementation phase.
- Report changed files, test results, limitations, and verification steps.
- Stop after every phase. Never start the next phase automatically.

## Document ownership and change control

Read [PRD.md](PRD.md) for scope/invariants, [TRD.md](TRD.md) for decisions, [APP_API_FLOW.md](APP_API_FLOW.md) for the public contract, [DEVELOPER_INTERFACE_BRIEF.md](DEVELOPER_INTERFACE_BRIEF.md) for developer experience, and [BACKEND_SCHEMA.md](BACKEND_SCHEMA.md) before related changes. Update all affected contracts together. [LEARNING_GUIDE.md](LEARNING_GUIDE.md) separates planned explanations from actual changes and observed evidence.

The current user's sequential prompts determine phase order. The specification's embedded sample code, suggested replay statuses, and retrospective interview statements do not override this contract. No frontend, production deployment, or proof of production readiness is implied.

Before beginning a requested phase, inspect current files, applicable AGENTS.md, and prior phase evidence. Do not overwrite user work. A phase can become IN PROGRESS, COMPLETE, FAILED, or BLOCKED, but COMPLETE requires every required check plus applicable regression tests to pass. A blocked predecessor must be repaired before claiming dependent capabilities work. If the user explicitly changes a design choice, update the documents and explain the impact before implementing it.

## Evidence protocol and planned verification interface

Phase 1 establishes Maven Surefire/Failsafe so `./mvnw.cmd verify` runs unit and integration tests and fails on test failure. Integration class names below now exist with exact actual selectors/commands recorded in their evidence. Phase 14 k6 workloads and Phase 15 startup/demo checks are separately executable scripts. Do not configure missing-test selectors to silently succeed.

For every check record: date, command, versions/environment, dataset/seed, expected result, observed result, PASS/FAIL/BLOCKED/NOT RUN, report path, and limitations. Proposed evidence location is `docs/evidence/phase-N/`; create it only when work is performed. Keep raw test reports and summaries linked. Testcontainers requires a reachable Docker daemon and downloadable pinned images. If blocked, give the exact failing command and steps such as starting Docker, checking `docker version`, and rerunning the same selector and full `verify`; do not invent an environment diagnosis without inspection.

The planned phase selector is `.\mvnw.cmd '-Dit.test=<ClassName>IT' verify` after Phase 1 configures integration-test execution. Full `.\mvnw.cmd verify` is the regression gate. Phase 1 reran this command successfully: three infrastructure tests passed with zero failures, errors, or skips. Earlier blocked/failed logs are preserved alongside the final evidence. Phase 2 also verified the SchemaIT selector and full suite; Phase 3 verified ResourceApiIT and full verify; Phase 4 verified ReservationHoldIT and full verify; Phase 5 verified AllocationConcurrencyIT and full verify; Phase 6 verified ReservationLifecycleIT, boundary policy unit tests and full verify; Phase 7 verified ExpirationIT and full verify; Phase 8 verified IdempotencyIT/SchemaIT and full verify; Phase 9 targeted checks and full regression passed. Phase 10 MessagingIT/SchemaIT and full verification also passed; Phase 11 CacheIT, enabled/unavailable core checks and full verification passed; RecoveryIT, FailureRecoveryIT, the five k6 workloads and final startup/demo checks are now recorded in their phase evidence.

## Phase 1 - Project setup

Status: **COMPLETE**. Full verify passed (3 tests, 0 failures/errors/skips); PostgreSQL Compose startup, Flyway execution/validation, JPA, UTC sessions, host/container startup and HTTP health are verified. Optional Redis/Kafka profiles also passed smoke checks. The Windows Java socket-directory setting is documented in README. See [Phase 1 evidence](VERIFICATION.md#phase-1). Read PRD, TRD, developer interface brief, this contract.

Deliver Java 21/Maven/Spring Boot under com.atlas; verify and pin a supported compatible Boot version and dependencies/containers. Add Web, Validation, Data JPA, Flyway, Actuator, JUnit, Testcontainers; required packages; local/test configuration; Maven wrapper; .gitignore; README; environment reference; Compose PostgreSQL plus optional Redis/Kafka profiles. No business logic or business tables.

Required checks: wrapper build, real PostgreSQL connectivity, application startup, Flyway configuration, and minimal health endpoint. Proposed `InfrastructureIT` plus full verify, PostgreSQL Compose startup, local application startup, and HTTP health check. Record effective versions and dated official compatibility references in TRD. Add real startup evidence and any port/Docker restrictions to the learning guide. Stop.

## Phase 2 - Database foundation

Status: **COMPLETE** on 2026-09-27. SchemaIT passed 51 cases; full verify passed 54 integration tests plus 1 unit test, zero failures/errors/skips. Fresh schema, Phase 1 upgrade, constraints, relationships, indexes and rollback passed. See [Phase 2 evidence](VERIFICATION.md#phase-2). Read BACKEND_SCHEMA and lifecycle/invariants in PRD/TRD.

Add resource/reservation Flyway migrations, UUID keys/FK, timestamps, indexes, positive quantity/capacity bounds, allowed states and timestamp shape checks. Domain models and API DTO boundaries remain separate. Confirmed capacity stays allocated. Explain what constraints enforce and what transaction logic must enforce.

Required checks: `SchemaIT` against real PostgreSQL for fresh migrations, invalid/valid rows, relationships, constraints, and upgrade from Phase 1; full verify. No Redis/Kafka integration. Update schema and learning evidence. Stop.

## Phase 3 - Resource API and caller identity

Status: **COMPLETE** on 2026-09-27. ResourceApiIT passed 51 tests; full verify passed 106 integration + 5 unit tests, zero failures/errors/skips. Local startup/credentials, identity permissions, input validation, resource reads, structured errors and Actuator restrictions passed. See [Phase 3 evidence](VERIFICATION.md#phase-3). Read API flow, TRD D2, developer brief, schema.

Add create/get/availability endpoints, names/capacity validation, server IDs, structured errors and request IDs. Implement local authenticated principal UUIDs, USER/ADMIN permissions, and environment-provided local credentials. Document setup and limitations; availability is informational. Idempotency is not yet implemented.

Required checks: `ResourceApiIT` for valid/invalid inputs, missing IDs/resources, missing/bad credentials, admin-only creation, user reads, and restricted metrics. Verify errors are consistent and secret-free. Run full verify; publish actual local request examples and update learning evidence. Stop.

## Phase 4 - Atomic temporary holds

Status: **COMPLETE** on 2026-09-27. ReservationHoldIT passed 57 cases; full verify passed 163 integration + 13 unit tests, zero failures/errors/skips. Atomic hold/DB deadline, conflicts, ownership/listing/pagination and injected rollback passed. See [Phase 4 evidence](VERIFICATION.md#phase-4). API is intermediate with no idempotency or expiration worker. Read allocation/lock/time rules, API ownership contract, schema.

Add hold creation, owned reservation read and user listing. Atomically decrement capacity and insert HELD in one PostgreSQL transaction. Use server database time and configured hold duration. Derive ownership from authentication. Explicitly document duplicate-retry risk and lack of an expiration worker at this intermediate phase.

Required checks: `ReservationHoldIT` for successful allocation, insufficient capacity, unknown resource, invalid quantity, foreign reads/listing, and injected insertion failure rolling back capacity. Check persisted expiry and invariant. Full verify. Update learning guide; do not claim a complete API. Stop.

## Phase 5 - Prove concurrency correctness

Status: **COMPLETE** on 2026-09-27. AllocationConcurrencyIT passed 7 cases, including three capacity-10 rounds. Full verify passed 170 integration + 13 unit tests, zero failures/errors/skips. Exact 10/90 outcomes, capacity 1, mixed quantities, independent resources and rollback under observed contention passed. See [Phase 5 evidence](VERIFICATION.md#phase-5). Read capacity invariant and transaction design.

Add synchronized concurrent tests with independent real PostgreSQL transactions. Use barriers and bounded futures/timeouts, not sleeps. Do not put a 100-party barrier inside an already acquired undersized connection pool; release request attempts before connection acquisition and measure real overlap. Use sufficiently long deterministic hold deadlines and no expiration worker to isolate allocation.

Required checks: `AllocationConcurrencyIT`, capacity 10 with 100 one-unit requests -> exactly 10 successes, 90 capacity conflicts, 10 rows, available zero; capacity 1; mixed quantities; independent resources; rollback under contention. Check quantity sums and conservation after each scenario. Unexpected infrastructure errors fail rather than count as conflicts. Run full verify and record actual counts, repeated-run policy, and why mocks cannot prove locking. Stop.

## Phase 6 - Confirm and cancel

Status: **COMPLETE** on 2026-09-27. ReservationLifecycleIT passed 16 cases and the new boundary policy unit suite passed 6. Full verify passed 186 integration + 19 unit tests, zero failures/errors/skips. Same/opposite terminal operations, owner checks, deadline boundaries/waits, simultaneous requests and release rollback passed. See [Phase 6 evidence](VERIFICATION.md#phase-6). Read state matrix, post-lock deadline rule, common lock order.

Lock owned reservations, check state/time, confirm without decrement, cancel HELD before deadline with one atomic release. Repeated same terminal action returns existing state; incompatible actions conflict. Overdue HELD returns expired error and remains allocated until Phase 7 worker exists.

Required checks: `ReservationLifecycleIT` for repeated confirm/cancel, confirm versus cancel, simultaneous cancels, foreign user access, deadline equality/just-before/after, and a transaction waiting across the deadline. Assert state and capacity together after every race; rollback injected release failure. Full verify. Update API and learning guide. Stop.

## Phase 7 - Expiration worker

Status: **COMPLETE** on 2026-09-27. ExpirationIT passed 12 cases; expiration configuration/boundary tests add 19 unit cases. Full verify passed 198 integration + 38 unit tests, zero failures/errors/skips. Competing workers, lifecycle races, isolated rollback/retry, stale candidates, bounded batches and automatic recovery after application-context restart passed. Initial test timestamp-conversion failure and successful repair/reruns are retained in [Phase 7 evidence](VERIFICATION.md#phase-7). Read worker design and expiry lag explanation.

Discover bounded due candidates, lock using SKIP LOCKED, process one reservation per transaction, recheck state and database deadline, mark EXPIRED and restore capacity atomically. Support multiple workers and restart recovery; no Redis notification dependence.

Required checks: `ExpirationIT` for two workers on one hold, worker versus confirm/cancel, rollback, restart with overdue holds, and lock-wait deadline behavior. Use controlled persisted deadlines and deterministic synchronization. Assert exact one-time release and conservation. Full verify. Explain passed deadline versus persisted status in learning guide. Stop.

## Phase 8 - Durable API idempotency

Status: **COMPLETE** on 2026-09-27. Targeted IdempotencyIT/SchemaIT passed 92 integration + 38 unit cases; full verify passed 239 integration + 38 unit, zero failures/errors/skips. All four mutation replay races, mismatch/isolation, persisted rejections, rollback/savepoints, restart, lost response, timeout, health detection and V1-to-V2 upgrade passed. See [Phase 8 evidence](VERIFICATION.md#phase-8). Requires Phase 7. Read TRD idempotency contract, API replay rules, schema.

Add durable scoped keys for resource creation and all reservation mutations. Canonical hash includes relevant path params. Use PostgreSQL uniqueness and transaction-safe claim/replay; store original response with mutation. Define stored domain failures, rollback/retryable failures, indefinite retention, new requestId header on replay, and authorization checks. No in-memory lock or lookup-only deduplication.

Required checks: `IdempotencyIT` for concurrent identical keys, payload/path mismatch, caller/operation isolation, rollback, restart, lost response after commit, original status/body/Location replay, persisted conflict replay, and timeout behavior. Prove one reservation/decrement and one resource for resource-create retries; no incomplete committed claim. Full verify. Update all affected docs and learning guide. Stop.

## Phase 9 - Transactional outbox

Status: **COMPLETE** on 2026-09-28. Targeted OutboxIT/SchemaIT/IdempotencyIT passed 123 integration + 38 unit; full verify passed 270 integration + 38 unit, zero failures/errors/skips. Atomic transition/event/response rollback, replay/no-op counts, envelopes, ordering, restart, constraints and guarded activation passed. See [Phase 9 evidence](VERIFICATION.md#phase-9). Requires Phase 8. Read event design, migration activation policy, schema.

Add stable event IDs, versioned envelope, sequence numbers and unique transition event rows. Every successful HELD creation/terminal transition writes its event with business changes. No separate CapacityReleased and no Kafka publisher yet. Check existing data before enabling event-history guarantees; follow the explicit empty-dataset or approved migration gate.

Required checks: `OutboxIT` for each transition, failure on event insert rolling back state/capacity/idempotency response, business failure leaving no event, replay/repeated action producing no extra event, envelope validation, and sequence uniqueness. Full verify. Document logical versus transport ordering and update learning guide. Stop.

## Phase 10 - Kafka publication and duplicate-safe consumption

Status: **COMPLETE** on 2026-09-28. Targeted checks passed 96 integration + 67 unit; full verification passed 284 integration + 67 unit, zero failures/errors/skips. Real delivery, duplicate sends/consumption, both acknowledgment gaps, rollback, restart, ordering, opt-in isolation, V4 upgrade and persistent local Kafka checks passed. See [Phase 10 evidence](VERIFICATION.md#phase-10). Requires Phase 9. Read publisher locking/timeouts, consumer transaction, ordering contract.

Enable pinned local Kafka, publish committed rows with bounded sends and mark after acknowledgment. Use per-row transaction locking; document crash unlock and ambiguous sends. Add reservation audit consumer with transactional marker and effect; acknowledge only after commit. Demonstrate out-of-order event tolerance as well as duplicates. No exactly-once transport claim.

Required checks: `MessagingIT` with real PostgreSQL/Kafka for normal delivery, duplicate sends, failure after send before mark, concurrent duplicate consumers, failure after consumer commit before acknowledgment, consumer effect rollback, and reordered sequences. Prove one audit effect per event/consumer. Full verify. Update operational instructions and learning guide. Stop.

## Phase 11 - Redis caching

Status: **COMPLETE** on 2026-09-28. Cache selector passed 16 integration + 78 unit; enabled/unavailable core regression passed 120 integration + 78 unit; full verify passed 300 integration + 78 unit, zero failures/errors/skips. Ownership, post-commit/rollback behavior, outage/eviction, stale snapshots, bounded fill races and local Redis passed. See [Phase 11 evidence](VERIFICATION.md#phase-11). Requires Phase 10. Read cache scope, TTL/fill budget, API ownership rules.

Add optional cache-aside availability and status reads, key naming, TTLs, observedAt/cached metadata, post-commit invalidation, bounded timeouts, and fallback. PostgreSQL retains all authoritative decisions; cached status never authorizes access or transitions. No cached idempotency authority.

Required checks: `CacheIT` for outage, eviction, stale availability/status, stale refill after invalidation, timeout/fallback, ownership under cache hits, and failure after commit. Rerun allocation/lifecycle/idempotency tests with Redis unavailable plus full verify. State the tested staleness bound and limitations. Update learning guide. Stop.

## Phase 12 - Retries and reconciliation

Status: **COMPLETE**, 2026-09-28. Full verification passed: 337 integration and 92 unit tests, zero failures/errors/skips, including all 21 RecoveryIT cases and the V4-to-V5 preservation check. See [Phase 12 evidence](VERIFICATION.md#phase-12). Requires Phase 11. Read retry ledger, event ordering, snapshot invariant, repair boundaries.

Add exponential jitter, bounded attempts, durable scheduling/failed state, sanitized diagnostics, consumer pause/recovery, and controlled replay preserving event ID. Detect overdue holds, old/stuck work, missing expected events, failed events, and capacity drift. Use a consistent snapshot; reuse safe expiry/retry paths; report uncertain corruption instead of overwriting capacity or inventing events.

Required checks: `RecoveryIT` for scheduling/jitter bounds, exhausted attempts, restart recovery, replay identity, repeated reconciliation, consumer poison records, and checks during active traffic without false drift. Test actual corruption detection and that unsafe auto-repair is refused. Full verify. Record exact inspection/replay/unpause procedures and learning evidence. Stop.

## Phase 13 - Failure and recovery suite

Status: **COMPLETE**, 2026-09-28. All eight FailureRecoveryIT scenarios passed; full verify passed 345 integration + 92 unit tests, zero failures/errors/skips. No required check is blocked or unexecuted. See [Phase 13 evidence](VERIFICATION.md#phase-13). Requires Phase 12. Read all failure contracts and existing evidence.

Create deterministic fault hooks or infrastructure controls. Test PostgreSQL failure before commit; lost HTTP response after commit; Kafka outage while reservations continue; publisher restart after send before mark; consumer restart after DB commit before acknowledgment; Redis outage/restart; competing expiration workers; and application restart with pending work.

Required checks: `FailureRecoveryIT` with real infrastructure and complete `.\mvnw.cmd verify`. Assert conservation, legal states, no duplicate business effects, and eventual recovery within recorded bounded deadlines. Separate expected business conflicts, infrastructure failures, and blocked/unexecuted checks. Fault hooks must be test-scoped and unavailable accidentally in normal runs. Update learning guide. Stop.

## Phase 14 - Reproducible load tests

Status: **COMPLETE**, 2026-09-29. Five smoke and five measured k6 workloads passed, with post-run conservation/history/claim/effect and scoped cleanup checks after every run. Full regression passed 345 integration + 92 unit tests, zero failures/errors/skips. No required check is blocked or unexecuted. See [Phase 14 evidence](VERIFICATION.md#phase-14). Requires Phase 13. Read PRD acceptance, observability plan, developer evidence conventions.

Add k6 normal, spike, hot-resource, idempotent retry, and mixed lifecycle scenarios; deterministic dataset setup, run IDs, and scoped cleanup. Record hardware, OS, exact versions, application/pool/broker/cache settings, dataset, seed, duration, concurrency, commands, and raw outputs. Never clean non-test data.

Required checks: smoke each script, run planned measured workloads, validate DB invariants after each run, and run applicable regression tests. Separate expected 409s from unexpected errors; report throughput, p50/p95/p99, error categories, database connection usage, outbox backlog, and event delay where instrumented. Missing instrumentation is a limitation, not an invented measurement. Exact k6 commands depend on scripts created in this phase and must be recorded then. If benchmarks cannot run, deliver scripts/commands, mark measurement and phase BLOCKED, and claim no results. Update learning guide. Stop.

## Phase 15 - Final documentation and interview preparation

Status: **COMPLETE**, 2026-09-29. Fresh host/container startup and normal/recovery demo passed; targeted CLI/SQL environment-isolation check passed after a runner repair. One full verify passed 345 integration + 92 unit tests, zero failures/errors/skips. All seven contracts reviewed; required checks have no outstanding failures, blocks or unexecuted work. Initial fixture failure, repairs and deferred gaps remain explicit in [Phase 15 evidence](VERIFICATION.md#phase-15). Read all seven documents and implementation.

Complete README startup, configuration, executable API examples, architecture/state diagrams, schema, recovery runbooks, and evidence links. Review implementation against every requirement and decision; fix in-scope discrepancies or identify unresolved items. Verify startup and demonstrations from a clean disposable environment. Record full applicable regression results after fixes.

Complete learning guide with a truthful one-minute explanation, deep mechanism explanations, questions answered without generated code, normal/failure demo, limitations, and evidence-backed resume claims. Distinguish Codex-written work from user-reviewed/tested/understood work; do not infer personal understanding. Report completed, failed, blocked, and deferred work separately. No external deployment or production-readiness claim. Stop with handoff.

## Repair procedure

Stay in the requested phase. Explain the failure, inspect evidence, diagnose root cause, apply the smallest appropriate fix without weakening tests/invariants, rerun the failed checks and relevant regression suite, and update LEARNING_GUIDE with actual failure/cause/fix/results. If environment prevents verification, mark BLOCKED and give exact unblocking/rerun steps. Stop. Do not automatically proceed to another phase.

## Prompt 0 consistency review

The planning set aligns on: five-minute configurable holds; database post-lock deadline decisions; retained confirmed capacity; local authenticated UUID ownership; original-response replay and indefinite retention; one event per transition; logical sequence without strict arrival guarantees; one-reservation worker transactions; optional bounded-stale cache; snapshot-based reconciliation; and all phases NOT STARTED. Version pinning is intentionally assigned to Phase 1, not claimed as performed. No application tests or benchmarks were run during planning.
