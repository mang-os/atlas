# Atlas - Developer Interface Brief

Adaptation of the guide's UI/UX brief to a backend project. This specifies the experience of using the API, local tools, examples, and diagnostic output. It does not request screens, fonts, themes, a website, or an admin dashboard.

## Interface principles

Decisions [D]: predictable JSON and HTTP conventions; explicit identity; informative errors; repeatable setup; no misleading success. [APP_API_FLOW.md](APP_API_FLOW.md) owns endpoint and status contracts. [TRD.md](TRD.md) owns transaction semantics and proposed defaults. Implementation evidence belongs in [LEARNING_GUIDE.md](LEARNING_GUIDE.md).

API naming uses plural resource nouns, camelCase fields, uppercase error codes and state names, UUIDs, UTC timestamps, and consistent nulls. Lifecycle actions use `/confirm` and `/cancel`. Explanations must distinguish a persisted HELD state from eligibility after its deadline. Availability examples always explain that a successful read cannot guarantee a later hold.

## Local developer journey

The [current README](../README.md) owns startup/configuration, complete keyed API examples, diagrams and recovery commands. Configure Java 21 and the documented Windows socket workaround first. Docker is required for the full acceptance suite; a package-only build is not acceptance. Container profiles and application enable flags are separate; a host JVM does not read `.env` automatically.

```powershell
Set-Location S:\Atlas
.\mvnw.cmd -B -ntp verify
.\scripts\handoff\demo.ps1 -SessionId p15-reproduce-001
```

Use a fresh session ID. The verified demo creates a separate private project/ports/volumes, checks host/container startup and migrations, then records normal and failure-recovery HTTP/SQL observations. It removes only its own labelled infrastructure; it never resets the ordinary Atlas database. Logs, snapshots and commands are in [Phase 15 evidence](VERIFICATION.md#phase-15). All mutations need stable scoped keys; owner identity comes from local authentication, with no arbitrary userId/admin ownership bypass. [Full API contract](APP_API_FLOW.md).

## Required examples and evidence

| Example | What the developer must understand |
| --- | --- |
| Create a resource, read it, inspect availability | Admin versus user role and informational availability |
| Hold units, read/list owned holds | Server-derived ownership, deadline, positive quantity |
| Confirm and cancel | Terminal state behavior and one-time release |
| Attempt at the deadline | Logical expiry can precede persisted EXPIRED |
| Repeat the same key; reuse with changed input | Original replay versus conflict, including original 201 |
| Stop Kafka, hold capacity, restart it | Durable reservation and eventual event delivery |
| Repeat an event delivery | One database audit effect despite duplicate transport |
| Stop Redis | Reads fall back; writes remain correct |
| Run reconciliation | Safe repairs versus corruption requiring investigation |

Each example must give prerequisites, concrete commands, expected status/body shape, cleanup scope, and links to actual test evidence when available. Generated output is labeled illustrative until executed. Use readable variable names for returned UUIDs; do not paste fake output as a test log.

## Error, empty, and waiting states

Empty lists return a 200 page with an empty items array. Invalid requests identify the invalid field without exposing internals. Ownership denial reveals no foreign reservation detail. Infrastructure failures get a request ID and retry guidance; capacity conflicts are ordinary domain outcomes, not infrastructure outages.

Bounded waits must terminate with an explicit retryable response or operational failure. Idempotency claims do not spin indefinitely. Record expiry lag and publication backlog so a user can distinguish pending work from successful completion. A health endpoint must not expose credentials or full configuration. Kafka/Redis degradation is observable without implying loss of PostgreSQL correctness.

## Diagnostics and evidence presentation

Desired observability: requestId, reservationId/eventId, operation/outcome/duration and sanitized category. Actual implementation uses templated sanitized failure logs and process-local Micrometer meters, not uniform JSON/success request logs or exported request timers. This gap is explicitly UNRESOLVED and deferred in the final review; no metric/dashboard endpoint is promised. Keep secrets and full request/response bodies out of logs. Show expected 409 conflicts separately from unexpected 5xx/timeouts in dashboards or command output.

Verification reports list changed files, exact commands, environment, expected versus observed outcomes, elapsed run time where useful, and limitations. Use PASS only for an executed successful check, FAIL for an executed failure, BLOCKED for a required check prevented by environment, and NOT RUN for unexecuted checks. Phase status remains NOT STARTED until work is explicitly requested; later required blocked checks prohibit COMPLETE.

No color-only meaning is permitted in terminal output or documentation; use text labels. Markdown tables and fenced commands must remain readable without styling. Operations documentation must warn next to destructive database-reset commands and scope them to disposable test data; normal startup must never erase a local database.

## Deferred interface work

An OpenAPI description or collection may be added during API phases if it improves reproducibility, but is not a separate frontend deliverable. Production auth, user registration, browser sessions, payment checkout, web UI, public hosting, and interactive operations dashboards are outside this plan.

## Phase 6 caller workflow

Use owner credentials, POST /api/reservations/{id}/confirm or /cancel, and no request body.
Both return 200 Reservation. Same-terminal repeats return the current terminal row without
another effect; opposite terminal actions conflict. Overdue HELD returns an expired error
but remains allocated until the Phase 7 worker commits. Foreign IDs return 404 even for admin.
[API flow](APP_API_FLOW.md#phase-6-confirmation-and-cancellation-implemented) documents exact
errors and [evidence](VERIFICATION.md#phase-6) records actual HTTP/race verification.
Run ReservationLifecycleIT then full verify.

## Phase 7 expiration behavior

Automatic expiration defaults to a one-second fixed-delay poll with at most 100 candidates.
[README](../README.md#reservation-state-machine) documents enable/poll/batch variables,
metrics and failure investigation. A passed deadline prevents confirm/cancel immediately by
database decision time, while GET may still report HELD until the worker commits EXPIRED.
Availability increases only with that commit. This lag is expected and has no fixed maximum
under contention, backlog, disabled workers or outages. No public expire endpoint was added.
[Phase 7 evidence](VERIFICATION.md#phase-7) records races, isolated rollback, later retries
and automatic processing after application-context restart. Run ExpirationIT then full verify.

## Phase 8 retry experience

Every mutation requires a key; README examples generate it once per new intention and preserve it for retries. The API replays original status/JSON/Location, including stored 404/409 rejections. The replay header distinguishes recovery from a new mutation. Use GET for present state. Key mismatch is 409; missing/invalid/repeated headers are 400. Auth/ownership is evaluated on every attempt. A replayed error body keeps its original requestId while the response header traces the current attempt.

Run IdempotencyIT and SchemaIT then full verify. No new environment variable or credential is needed. Records are indefinite; do not force retries by deleting them. Health detects committed incomplete claims without exposing details. [Phase 8 evidence](VERIFICATION.md#phase-8) covers PostgreSQL duplicate races, all four endpoints, rollback, restart and lost responses.

## Phase 9 event inspection

No API request or response fields change. Successful reservation transitions now commit an internal outbox event with business state. SQL inspection is the developer interface, with no public event/admin route. Phase 9 initially created PENDING events; Phase 10 now optionally publishes and Phase 12 adds bounded retries/replay. Replay/no-op does not create another event. V3 refuses nonempty reservation history; follow README's inventory and stopped-writer procedure before local activation. Run OutboxIT/SchemaIT/IdempotencyIT and full verify; [Phase 9 evidence](VERIFICATION.md#phase-9) records observed results.

## Phase 10 delivery and audit inspection

Enable the Kafka Compose profile and opt the app in with ATLAS_KAFKA_ENABLED=true or the kafka Spring profile. README documents host/container commands, settings, SQL inspection and group offsets. Kafka stays optional by default; HTTP credentials/contracts do not change.

Inspect PENDING/PUBLISHED counts, audit/processed marker joins and Kafka group offsets. Audit is an internal database effect with no new public route/dashboard. Micrometer counters `atlas.outbox.publication` (published/failed) and `atlas.audit.events` (applied/duplicate) are process-local; Actuator remains health-only, so no public metrics endpoint is promised. Publisher failures log sanitized categories. Consumer errors stop that listener without acknowledgment. Phase 12 durable RETRY_WAIT gates automatic restart; FAILED requires controlled replay after the cause is corrected. Database health alone does not prove delivery. Run MessagingIT/SchemaIT and full verify; see [Phase 10 evidence](VERIFICATION.md#phase-10). Optional caching and Phase 12 recovery are described below.

## Phase 11 cache observations

Enable Redis optionally with the Compose redis profile plus ATLAS_REDIS_ENABLED=true (or the app redis profile). Inspect observedAt/cached on availability and individual reservation reads; a hit preserves the original observation time. CLI requests and owner credentials are unchanged. A stale HELD/availability view remains informational. Redis outage/eviction causes PostgreSQL fallback; database ownership is checked even on a hit.

README documents keys, TTL, environment budgets and inspection commands. Micrometer counters atlas.cache.read (hit/invalid), atlas.cache.failure (read/fill/invalidation), and atlas.cache.invalidation are process-local; health/metrics endpoint exposure is unchanged. No cached userId can authenticate a caller. Run CacheIT, the enabled-but-unavailable core regression selector, then full verify. [Phase 11 evidence](VERIFICATION.md#phase-11) records races/limits and actual results.


## Phase 12 operational interface

Use [README's recovery runbook](../README.md#recovery-procedures), the read-only scripts/reconcile.sql, durable outbox/consumer_failure rows, and Kafka group offsets. There is no new HTTP admin route. Replay functions require an exact failed identity, expected replay count and fixed reason code; stop affected consumers, correct the cause, replay, restart and verify progress without resetting offsets or deleting markers.

Reconciliation logs finding counts and refused repair IDs/categories. atlas.audit.failures (category) and atlas.reconciliation.repairs (expired/skipped/refused) join existing process-local publication counters; public metrics exposure remains unchanged. Health is not a delivery guarantee. Run RecoveryIT/MessagingIT/SchemaIT and full verify; [Phase 12 evidence](VERIFICATION.md#phase-12) retains full logs and concise results.
