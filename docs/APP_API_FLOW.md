# Atlas - Application and API Flow

Current local v1 endpoints, idempotency, lifecycle/expiration, outbox, optional delivery/cache and guarded recovery are implemented. All four mutations require a key. Phase-qualified headings identify where a feature originated; older evidence is historical. [Verified startup and demo](../README.md#verified-normal-and-failure-demo), [final review](VERIFICATION.md#phase-15), [source authority](PRD.md), [transaction decisions](TRD.md).

## Phase 3 resource API (implemented)

Use the local setup and chosen environment passwords in [README](../README.md#local-identity-and-resource-requests). Alice/Bob have USER; admin has USER and ADMIN. Their stable UUIDs are in that setup and TRD. Authentication is HTTP Basic only with explicitly selected `local`; all three credentials are required at startup. No client-supplied userId/header/role can authenticate or grant permissions. Outside `local`, resource access is denied. This CLI/localhost identity has no production TLS, account management, recovery or rate-limiting guarantee; it is not configured for browser authentication.

| Method/path | Permission | Request | Response |
| --- | --- | --- | --- |
| POST /api/resources | ADMIN | Exactly name and totalCapacity | 201 Resource, Location: /api/resources/{generated-id} |
| GET /api/resources/{id} | USER/ADMIN | Canonical 8-4-4-4-12 UUID | 200 Resource |
| GET /api/resources/{id}/availability | USER/ADMIN | Canonical UUID | 200 Availability, Cache-Control: no-store |
| GET /actuator/health | Public | none | Minimal 200 status UP when healthy |
| GET /actuator/metrics | ADMIN gate; endpoint disabled | none | Anonymous 401; USER 403; admin 404 |

Create name is normalized with Java strip(), then checked nonblank, <=120 UTF-16 code units, and without NUL characters. totalCapacity is a JSON integer 1-2147483647; fractional values (including 1.0), quoted numbers, booleans and overflow are rejected. Name must be a JSON string. Reject unknown/protected fields, duplicate JSON properties, trailing tokens, malformed/empty/null/array bodies. `quantity` is not a resource-create field; reservation quantity validation is documented in the implemented Phase 4 section below. Duplicate resource names are allowed. Initial availableCapacity=totalCapacity and version=0; PostgreSQL generates IDs and UTC timestamps. Dedicated request/response DTOs are separate from internal records and JDBC persistence.

Resource JSON fields: id, name, totalCapacity, availableCapacity, version, createdAt, updatedAt. Availability: resourceId, availableCapacity, observedAt, cached. A fresh DB read has cached=false; an optional cache hit has cached=true and preserves the original observation time. Availability is informational and cannot promise a later hold; Phase 3 introduced no hold API; Phase 4 now adds the atomic hold endpoint below.

| HTTP/code | Implemented condition |
| --- | --- |
| 400 INVALID_REQUEST | Invalid name/capacity/body/UUID or protected field |
| 401 AUTHENTICATION_REQUIRED | Missing, malformed, unknown or invalid credentials; Basic challenge returned |
| 403 FORBIDDEN | Authenticated user tries admin creation/Actuator access |
| 404 RESOURCE_NOT_FOUND | Valid UUID resource does not exist |
| 404 NOT_FOUND | Unknown endpoint |
| 405 METHOD_NOT_ALLOWED | Unsupported method, including GET /api/resources without an ID |
| 406 NOT_ACCEPTABLE | Response media type not supported |
| 415 UNSUPPORTED_MEDIA_TYPE | Unsupported request Content-Type |
| 503 TEMPORARILY_UNAVAILABLE | Resource JDBC operation raises a data-access error |
| 500 INTERNAL_ERROR | Unexpected failure, with sanitized message |

Every application response has server-generated X-Request-Id; supplied IDs are ignored. Errors use `{code,message,requestId,details}`. details is an array of `{field,reason}` for bean-validation errors and an empty array for other errors. Header and error-body requestId match on the first result; stored error replay retains its original body requestId and uses a new attempt header. Messages do not include rejected values, SQL, stack traces or credentials. Authentication/authorization failures use the same contract as controller failures.

**Current Phase 8 behavior:** resource creation requires a key and replays the stored 201, body and Location for matching retries. Phase 3 evidence records the earlier unprotected behavior; it no longer describes current retries. Metrics are permission-gated but not enabled.

[Phase 3 evidence](VERIFICATION.md#phase-3) contains recorded POST 201, GET 200 and validation 400 responses from the actual local-profile HTTP tests. UUIDs in these transcripts belonged to disposable test data and no longer identify live resources. Follow README commands to obtain fresh IDs. No actual credential is published.

## Phase 4 temporary holds and owned reads (implemented)

| Method/path | Permission | Request | Success |
| --- | --- | --- | --- |
| POST /api/reservations | USER/ADMIN | Exactly resourceId canonical UUID, quantity positive integer | 201 Reservation + Location /api/reservations/{generated-id} |
| GET /api/reservations/{id} | Authenticated owner only | Canonical reservation UUID | 200 Reservation |
| GET /api/users/{userId}/reservations | Path UUID must equal authenticated principal UUID | Optional limit/cursor | 200 {items,nextCursor} |

Owner comes from authenticated LocalPrincipal; no input userId/header is trusted. Admin can create its own holds but cannot read/list other users' reservations. Foreign and missing reservation GETs have the same404 RESERVATION_NOT_FOUND. Foreign user-list paths return 403 FORBIDDEN without an admin bypass. All routes require credentials (missing/invalid401).

Quantity must be a JSON integer 1-2147483647, never zero/negative/fractional/quoted/boolean/overflow. Missing/null/malformed resourceId or quantity returns 400 INVALID_REQUEST. Reject extra fields including userId, expiresAt, status and id, duplicate properties and trailing JSON. Resource unknown returns 404 RESOURCE_NOT_FOUND; insufficient available capacity (including quantity above total) returns 409 INSUFFICIENT_CAPACITY. SQL insertion/update errors return sanitized503 TEMPORARILY_UNAVAILABLE with the operation rolled back. Existing error fields/request IDs apply.

Creation executes one READ COMMITTED PostgreSQL transaction. First UPDATE resource with `available_capacity >= quantity` atomically decrements, increments version and updates its timestamp. There is no pre-read allocation check. Only a zero-row UPDATE is followed by an existence lookup to distinguish404/409. After successful allocation, one materialized clock_timestamp() sample sets reservation createdAt/updatedAt and expiresAt=createdAt+configuredTTL. The reservation is inserted HELD with database UUID and null terminal timestamps. The Spring service proxy commits before the controller returns 201/Location; errors escape and roll back both statements.

Default holdTTL is300 seconds (`ATLAS_HOLD_TTL_SECONDS`, integer 1-3600). Invalid configuration fails startup. Existing deadlines do not change when configuration changes. Reservation responses contain id, userId, resourceId, quantity, status, expiresAt, confirmedAt, cancelledAt, expiredAt, createdAt and updatedAt. Null terminal timestamps are explicit; no persistence entity is exposed. Owned reads show persisted state, including overdue HELD.

Listings use owner-filtered keyset SQL ascending `(created_at,id)`, fetching limit+1 to detect a next page. Omitted limit defaults20, range 1-100; malformed/empty limits and cursors return 400. nextCursor is null on an exhausted/empty result. Cursor is unpadded canonical Base64URL of a UTC Instant and UUID tuple; clients should treat it as opaque and return it verbatim. It is unsigned and never authorization evidence. Timestamp precision is bounded to database microseconds and supported cursor years1-9999 to reject invalid SQL timestamps. A copied/tampered position can only change which of the authenticated user's rows are shown. Concurrent inserts may appear on later pages; there is no frozen listing snapshot.

**Current retry contract:** all mutations require a key; matching retries replay the original stored result without another allocation. Confirmation/cancellation are implemented in Phase 6 and automatic expiration in Phase 7. A passed deadline does not change persisted HELD or release capacity; all HELD rows still count in conservation until the expiration worker commits. This API is not complete; [Phase 5 concurrency correctness evidence](VERIFICATION.md#phase-5) is available; it adds no endpoints. Phases 10/11 add optional asynchronous Kafka publication/audit and informational caching.

[README hold requests](../README.md#api-examples) and [Phase 4 evidence](VERIFICATION.md#phase-4) include actual hold/read/list/conflict/rollback HTTP transcripts. Test UUIDs are historical, not live resources.

## Phase 6 confirmation and cancellation (implemented)

Send `POST /api/reservations/{id}/confirm` or `POST /api/reservations/{id}/cancel` with authenticated owner credentials and **no request body**. Nonempty bodies (including `{}`, `null`, whitespace or a userId) return 400 INVALID_REQUEST. A malformed path UUID returns 400; absent credentials 401; missing/foreign reservation IDs 404 RESERVATION_NOT_FOUND, including admin access to another owner's reservation. Supplied identity/role headers never authorize the operation.

Success is 200 with the same Reservation DTO fields documented above. A fresh confirmation sets status=CONFIRMED and confirmedAt=updatedAt to the decision time; cancelledAt/expiredAt remain null. A fresh cancellation sets status=CANCELLED and cancelledAt=updatedAt; confirmedAt/expiredAt remain null. IDs, owner, quantity, createdAt and expiresAt are retained. No new Location or replay header is promised. Structured errors include code, message, requestId and details; the response X-Request-Id is fresh for every attempt.

Both operations run in READ COMMITTED transactions: SELECT the row by reservation ID **and authenticated owner** FOR UPDATE; after the lock returns, issue a separate SELECT clock_timestamp(). Apply the matrix below using that one instant. A matching terminal operation returns the existing row unchanged. HELD is eligible only when decisionTime < expiresAt; equality/past returns 409 RESERVATION_EXPIRED with no state or capacity change. CONFIRMED-to-cancel or CANCELLED-to-confirm returns 409 INVALID_STATE_TRANSITION. Persisted EXPIRED returns 409 RESERVATION_EXPIRED for either operation.

Confirmation changes only the reservation: no second decrement and no resource version/timestamp change. Cancellation changes HELD to CANCELLED, then atomically adds its quantity to available capacity and increments resource version in the same transaction. Resource updatedAt uses the same decision time. If release fails, both the state transition and resource changes roll back and the API returns sanitized 503 TEMPORARILY_UNAVAILABLE for database errors. The resource capacity CHECK is allowed to fail on corruption; values are never clamped.

**Shared lock order:** idempotency claim -> one existing reservation -> its resource when releasing -> outbox insertion. API requests now claim the key first; workers lock reservation then resource and append outbox. Confirmation takes only the reservation lock; cancellation locks reservation then resource. New holds lock the resource then insert a fresh, previously invisible reservation, and never lock an existing reservation. Phase 7 expiration workers lock one reservation per transaction, then that resource, and recheck state/deadline after the reservation lock. Ordinary reads use snapshots without mutation locks.

Eligibility is decided after the reservation lock, not at HTTP arrival or commit time. A valid operation may commit after the deadline, including a cancellation that later waits for the resource lock. Expiration workers obey the same reservation lock, so they cannot undo a committed terminal result. The original deadline does not expire CONFIRMED or CANCELLED states. Overdue HELD rows still consume capacity until their expiration transaction commits.

```powershell
# Use an ID returned by a hold owned by Alice; curl prompts for her local password.
# Generate once for each new intention; retain the same key on retries.
$confirmKey=[guid]::NewGuid().ToString()
$cancelKey=[guid]::NewGuid().ToString()
curl.exe --user alice -X POST --header "Idempotency-Key: $confirmKey" "http://localhost:8080/api/reservations/$reservationId/confirm"
# Cancel a DIFFERENT still-HELD reservation before its deadline:
curl.exe --user alice -X POST --header "Idempotency-Key: $cancelKey" "http://localhost:8080/api/reservations/$otherReservationId/cancel"
```

With a new key, repeated terminal operations return existing state through the locked lifecycle policy. With the original key, Phase 8 returns the original stored response. Creation retries with a new key represent a new intention and can create another row. The API is local v1 with the limitations in README. See [Phase 6 tests and actual HTTP transcripts](VERIFICATION.md#phase-6).

## Phase 7 automatic expiration (implemented)

No new public endpoint or request identity is added. The internal worker runs automatically by default after startup, then with a fixed delay between completed batches. It discovers at most the configured number of persisted HELD IDs whose expiresAt is due by PostgreSQL statement time, ordered by expiresAt/id. Discovery does not mutate or lock reservations. Each candidate then runs through a separate READ COMMITTED transaction: SELECT that reservation FOR UPDATE SKIP LOCKED, sample clock_timestamp() after acquisition, recheck HELD and decisionTime >= expiresAt, set EXPIRED/expiredAt/updatedAt, and add quantity back to its resource with one version increment. Both writes commit together; failure rolls both back.

The reservation -> resource lock order matches cancellation. A locked reservation is skipped rather than waited on. A stale candidate may now be terminal, missing or no longer due and is skipped after rechecking. Concurrent workers may discover the same ID; only one can perform the HELD-to-EXPIRED transition and release. A failed candidate is logged/counted after rollback, and the remaining candidates continue in their own transactions. Failed/skipped rows stay discoverable on later polls. If discovery itself fails, the scheduled invocation reports the failure and the next poll tries again. PostgreSQL persists all pending work; no Redis notification, in-memory timer or broker is authoritative.

Existing GET/list DTOs now eventually show status=EXPIRED and a nonnull expiredAt equal to updatedAt; confirmedAt/cancelledAt remain null. expiresAt retains the original eligibility deadline; expiredAt records the later database decision that materialized expiration. GET does not itself expire anything. Confirm/cancel on overdue HELD or persisted EXPIRED still return 409 RESERVATION_EXPIRED. CONFIRMED retains capacity permanently in this version; CANCELLED/EXPIRED are never released again.

**Expiration lag:** crossing expiresAt makes a hold ineligible for confirmation/cancellation, but does not itself commit a state transition. Until the worker transaction commits, reads can legitimately return HELD and availability remains reduced; all HELD rows count in conservation. Poll delay, backlog, skipped reservation locks, resource waits, failed transactions, disabled scheduling, outages and restart time can increase lag. A valid pre-deadline confirm/cancel may commit after the deadline while the worker skips its lock. No maximum lag or strict fairness guarantee is claimed; repeated locks/failures filling the oldest candidate batch can delay later rows. Recovery requires a running worker and a healthy database.

Defaults: ATLAS_EXPIRATION_ENABLED=true, ATLAS_EXPIRATION_POLL_MS=1000 (100-60000), ATLAS_EXPIRATION_BATCH_SIZE=100 (1-100). The enable flag controls scheduling; it is not a client permission. Disabling it stops automatic reclamation while hold/confirm/cancel still work. Configure values before startup; host JVMs do not read .env automatically. Multiple application instances can use the same database without a leader lease. See [Phase 7 evidence](VERIFICATION.md#phase-7) for verified schedules, failure/restart recovery and limits.

## Common contract [D unless marked]

Use JSON, server-generated UUID identifiers, camelCase fields, UTC RFC3339 timestamps, and `/api` routes from S. Integer quantities/capacities range from 1 to 2,147,483,647. Resource names are trimmed, 1-120 UTF-16 code units in the implemented API; names need not be unique. Resource capacity is immutable in v1. Reject malformed UUIDs, non-integral/out-of-range numbers, unknown fields, and client-supplied owner/status/deadline fields.

Authentication uses the local scheme in TRD. All four mutation requests now require exactly one `Idempotency-Key`: 1-128 ASCII characters matching `[A-Za-z0-9._:-]+`. Do not trim keys. Missing, invalid or repeated headers return 400 INVALID_IDEMPOTENCY_KEY without consuming a key. Earlier-phase evidence remains historical.

| Method and path | Permission | Request | Success |
| --- | --- | --- | --- |
| POST /api/resources | ADMIN | name, totalCapacity | 201 Resource + Location |
| GET /api/resources/{id} | USER/ADMIN | none | 200 Resource |
| GET /api/resources/{id}/availability | USER/ADMIN | none | 200 Availability |
| POST /api/reservations | USER/ADMIN | resourceId, quantity | 201 Reservation + Location |
| GET /api/reservations/{id} | Owner | none | 200 Reservation |
| GET /api/users/{userId}/reservations | Same authenticated user | limit, cursor | 200 page, even empty |
| POST /api/reservations/{id}/confirm | Owner | empty body | 200 Reservation |
| POST /api/reservations/{id}/cancel | Owner | empty body | 200 Reservation |
| GET /actuator/health | Minimal local health | none | 200 UP or 503 unavailable |
| GET /actuator/metrics | ADMIN | currently disabled | 404; diagnostics exposure remains deferred |

Resource: id, name, totalCapacity, availableCapacity, version, createdAt, updatedAt. Availability: resourceId, availableCapacity, observedAt, cached. Reservation: id, userId, resourceId, quantity, status, expiresAt, confirmedAt, cancelledAt, expiredAt, createdAt, updatedAt; reads may add observedAt and cached. Null lifecycle timestamps remain explicit. IDs in these descriptions are types; examples below use actual UUID syntax.

Listing uses ascending `(createdAt,id)` keyset pagination, default limit 20, maximum 100, and opaque cursor encoding that tuple. Invalid cursor returns 400. Empty list is `{ "items": [], "nextCursor": null }`; no hidden admin bypass. Concurrent inserts may appear on later pages; pagination is not a frozen snapshot.

## Developer journey

1. Start PostgreSQL and the local API; establish a configured authenticated identity.
2. Admin creates a resource. User reads availability; another client may allocate immediately afterward.
3. User submits a hold. Only the conditional PostgreSQL decrement decides acceptance. On commit return 201 and Location; on conflict return 409.
4. GET the owned reservation or list the caller's reservations. A passed deadline can coexist with persisted HELD until the worker commits; it is no longer confirmable.
5. Confirm before deadline to retain capacity, or cancel before deadline to release it once. The Phase 7 worker eventually expires abandoned holds and restores capacity.
6. From Phase 8, retry any lost mutation response with the exact same key and canonical request. Use GET for present state; original replay may describe a prior state.

## State and response matrix

| Persisted state | Confirm | Cancel | Expiration worker |
| --- | --- | --- | --- |
| HELD, post-lock DB time < expiresAt | CONFIRMED, 200; capacity unchanged | CANCELLED, 200; restore quantity | No change |
| HELD, time >= expiresAt | 409 RESERVATION_EXPIRED; no state/capacity change | 409 RESERVATION_EXPIRED; no state/capacity change | EXPIRED; restore quantity |
| CONFIRMED | 200 existing state; no new effect | 409 INVALID_STATE_TRANSITION | No change |
| CANCELLED | 409 INVALID_STATE_TRANSITION | 200 existing state; no new effect | No change |
| EXPIRED | 409 RESERVATION_EXPIRED | 409 RESERVATION_EXPIRED | No change |

Confirm/cancel and worker columns are implemented in Phases 6 and 7 respectively. Same-operation repetition returns the existing terminal state without a write, even after the original deadline. At the exact deadline, a HELD reservation is no longer eligible. Phase 8 adds stored-response replay before re-evaluating business state. All existing-reservation mutations use the same reservation-before-resource order, after the API key claim when applicable.

## Mutation flow at final scope

Validate authenticated identity, role, syntax, body, and ownership. Claim the scoped idempotency key transactionally. On replay return its stored result. Otherwise allocate or lock an existing reservation; decide using authoritative database time. Mutate resource/reservation and append one event for a successful transition. Store the response in the same transaction. Commit before returning success. Outbox creation is implemented in Phase 9. Optional publication/consumption and post-commit cache invalidation are implemented in Phases 10/11.

A capacity rejection is a committed idempotency result without a reservation or outbox event. An unexpected insertion or event-write failure rolls back the entire business transaction and its claim. Response loss after commit is recoverable via replay; broker/cache failure after commit cannot revoke the reservation.

## Errors and retry guidance

```json
{
  "code": "INSUFFICIENT_CAPACITY",
  "message": "The requested quantity is no longer available",
  "requestId": "9ee9a4b9-7b91-4493-98b3-6077d770155a",
  "details": []
}
```

| HTTP | Code / condition | Client action |
| --- | --- | --- |
| 400 | INVALID_REQUEST, INVALID_IDEMPOTENCY_KEY | Correct input; never blindly retry |
| 401 | AUTHENTICATION_REQUIRED | Supply valid credentials |
| 403 | FORBIDDEN | Correct role or user-list scope |
| 404 | RESOURCE_NOT_FOUND, RESERVATION_NOT_FOUND (including foreign owner) | Check permitted identifier |
| 409 | INSUFFICIENT_CAPACITY | New key only for a deliberate new attempt |
| 409 | RESERVATION_EXPIRED, INVALID_STATE_TRANSITION | GET current state; cannot revive a terminal reservation |
| 409 | IDEMPOTENCY_KEY_MISMATCH | Restore original payload or use a new key for new intent |
| 503 | TEMPORARILY_UNAVAILABLE | Bounded backoff and same key after Phase 8 |
| 500 | INTERNAL_ERROR | No details leaked; investigate request ID and use same key for ambiguous commit |

Every response gets a server-generated `X-Request-Id`. Validation errors may add `details` with field and reason; never include credentials, stack traces, SQL, or another user's data. Replay returns the stored status/body/content type/Location and `Idempotency-Replayed: true`; its HTTP request ID identifies the current attempt. No downstream Kafka/Redis error alone becomes an error for a committed mutation.

## Phase 8 durable replay (implemented)

Scope is authenticated caller UUID + stable operation + key. The four operations are `v1.resource.create`, `v1.reservation.create`, `v1.reservation.confirm`, `v1.reservation.cancel`. Reservation IDs participate in the canonical request hash, not the operation name: reusing one confirm key on another owned reservation returns 409 IDEMPOTENCY_KEY_MISMATCH. Canonicalization is specified in TRD; normalized name, quantity/capacity and path/body UUIDs all participate.

| Outcome | Persisted/retry behavior |
| --- | --- |
| 201 resource/hold or 200 confirm/cancel | Persist exact original JSON UTF-8 bytes, status, content type and Location; same request/key replays |
| Valid authenticated missing resource/reservation 404 or deterministic business 409 | Persist and replay, including insufficient capacity, expired deadline and invalid transition |
| Validation 400, authentication 401, role/ownership denial (403 or masked 404) | Do not persist; correct input/access first |
| Same scope but changed canonical request | 409 IDEMPOTENCY_KEY_MISMATCH; leave original record unchanged |
| Lock/pool/query timeout or database/infrastructure failure | 503 TEMPORARILY_UNAVAILABLE, no new record/effect; back off then retry the original key |
| Unexpected server/serialization failure | Roll back claim and business writes; no stored 500 |
| Response lost after commit | Retry same key/request; replay the committed original result |

Replay sets `Idempotency-Replayed: true`; first results omit it. Every attempt has a fresh `X-Request-Id`. A stored error body preserves its original requestId, so on replay it differs from the header. Authentication, role and ownership are checked again; access failures are not replayed from old authorization. A stored HELD response can describe a reservation now CONFIRMED, CANCELLED or EXPIRED; use owned GET for current state. Stored capacity conflicts remain conflicts even after capacity becomes free. A deliberate new attempt needs a new key.

Responses are retained indefinitely. There is no expiration, deletion API or cleanup worker; do not manually delete keys to force a retry. This grows storage and retains user-linked responses. Requests made before Phase 8 have no durable replay record. `GET /actuator/health` reports 503/DOWN if a committed incomplete claim is found; investigate rather than take over the key. See [Phase 8 verification](VERIFICATION.md#phase-8).

## Idempotent HTTP examples (implemented; illustrative IDs)

```http
POST /api/resources
Authorization: Basic <local-admin-credentials>
Content-Type: application/json
Idempotency-Key: demo-resource-001

{"name":"Demo capacity","totalCapacity":10}
```

```http
POST /api/reservations
Authorization: Basic <local-user-credentials>
Content-Type: application/json
Idempotency-Key: demo-hold-001

{"resourceId":"ca59f9e3-b25a-4e72-b457-40e589b9539e","quantity":1}
```

The resource UUID above is illustrative; replace it with the returned resource ID. Replaying `demo-hold-001` with identical input returns the original 201 and body even if that reservation has subsequently expired. Changing quantity with the same key returns 409. A confirm key reused on a different reservation also conflicts because the path ID participates in the hash. The same key string in a different operation or by a different caller has a separate scope.

## Worker and operator journeys

Expiration discovers a bounded candidate set, locks one due HELD reservation per transaction, rechecks state/time, releases and transitions atomically, and later invalidates cache. Polling delay, lock contention, and outages create expiry lag. Capacity stays conservatively allocated until the transaction commits.

Publisher sends committed outbox rows, marks only acknowledged sends, and may resend stable event IDs after failure. The audit consumer atomically inserts its dedupe marker and audit row, then acknowledges. Reconciliation reports missing history and drift and reuses safe worker/retry paths. Phase 12 documents controlled replay; no public replay endpoint or admin dashboard is implied.

## Phase 9 event creation (implemented, internal only)

HTTP routes, permissions, required idempotency keys, statuses and response DTOs are unchanged. An accepted hold/terminal transition now also writes one durable outbox event before the same transaction commits. Event serialization/insertion failure returns sanitized 503 for HTTP and rolls back reservation, capacity, sequence, event and idempotency claim/result. Worker failures similarly roll back state/release/event and remain eligible for a later expiration attempt.

| Successful change | Event type | Aggregate sequence | releasedQuantity |
| --- | --- | --- | --- |
| New HELD | ReservationHeld | 1 | 0 |
| HELD -> CONFIRMED | ReservationConfirmed | 2 | 0 |
| HELD -> CANCELLED | ReservationCancelled | 2 | reservation quantity |
| HELD -> EXPIRED | ReservationExpired | 2 | reservation quantity |

No separate CapacityReleased event exists. Same-terminal repeats, rejected operations and idempotent replay add no event. Replay may retain a previous HTTP state as before. Event IDs remain stable after commit; logical sequence is per reservation, not a delivery order guarantee. Events begin PENDING and have no new HTTP route; Phase 10 can publish them. Activation requires an empty reservation dataset; see [schema upgrade policy](BACKEND_SCHEMA.md#phase-9-migration-and-enforcement-boundary). Phase 10 delivery is documented below.

## Phase 10 asynchronous delivery

No endpoint, request/response field, status code or credential changes. Mutations still commit PostgreSQL business state, outbox and idempotent response together. Optional Kafka publication happens afterward; HTTP 2xx/replay does not guarantee publication or consumption. A Kafka outage does not make a committed mutation fail.

PENDING -> publisher row lock -> broker acknowledgment -> PUBLISHED database commit. A send/mark failure can publish the same eventId again. Consumer validation -> marker and audit transaction -> commit -> Kafka acknowledgment. A commit/ack failure replays with one audit effect. Arrival may be out of sequence; logical audit ordering never changes reservation capacity.

Failed consumers stop without skipping the record; correct the cause then restart the app. No public replay route is added. [README](../README.md#architecture) describes operations. Optional Redis caching is implemented below; automated recovery is described in the Phase 12 section below. Stop before Phase 13.

## Phase 11 cached reads

GET /api/resources/{id}/availability may return cached=true with the original database observedAt. Individual GET /api/reservations/{id} adds observedAt and cached to its existing state fields. These headers remain no-store to prevent an additional HTTP cache. Resource lookup, reservation listing, all mutation/replay responses and errors keep their existing shape and use PostgreSQL.

Authenticate, check reservation ownership in PostgreSQL, then look up its cached snapshot. Missing/foreign IDs still return the same 404, including for admin. A new owner cannot receive an old cached owner's payload. Missing/malformed/expired cache or Redis outage falls back to a new PostgreSQL observation. Database failure still fails the authoritative check rather than trusting a cached userId. Availability never guarantees a hold; stale HELD cannot authorize confirm/cancel, and passed deadlines still use database decision time.

Default TTLs are 2s availability and 5s reservation (HELD capped by remaining lifetime), with a maximum 1s fill budget. Invalidation follows successful commit and is best effort. A refill after invalidation can briefly restore stale state: stored staleness can reach 3s/6s under working clocks/expiry, plus a bounded Redis read wait; HTTP delivery and expiration-worker lag are separate. observedAt is not rewritten on a hit. [README](../README.md#recovery-procedures) and [TRD](TRD.md#phase-11-optional-cache-implementation) define timeouts, guard and recovery limits. Stop before Phase 13.


## Phase 12 recovery behavior

Public request/response formats, caller ownership and HTTP idempotency behavior are unchanged. Successful HTTP mutations can have pending/failed asynchronous delivery; replaying their API key does not reset an event's retry budget. Publisher and consumer use durable jittered retry times and FAILED states. Consumer effect/marker commit precedes failure-ledger resolution and Kafka acknowledgment. FAILED records stop delivery without skipping offsets; controlled operator replay preserves the original event identity/content. No public replay route is added.

Reconciliation detects overdue holds, old/stuck/failed delivery, missing or inconsistent event history, missing audit effects and capacity drift in a consistent committed snapshot. It only expires a due hold after locked integrity checks, using the established state/capacity/event transaction and post-commit cache invalidation. Uncertain corruption is reported without overwriting capacity. Exact operator inspection/replay/restart steps are in [README](../README.md#recovery-procedures); [Phase 12 evidence](VERIFICATION.md#phase-12) records acceptance checks. Stop before Phase 13.
