# Verification and reproducibility

## Recorded final regression

The final local verification dated 2026-09-29 passed **345 integration + 92 unit tests**, zero failures/errors/skips. [Machine-readable suite totals](verification/test-summary.json) preserve counts and suite durations. These results precede the repository-presentation changes. Hosted CI is independent evidence: the [Verify run for commit 58c9020](https://github.com/mang-os/atlas/actions/runs/36491079222) completed successfully. This does not turn the recorded local load observations into production performance guarantees.

Run from the repository with JDK 21 and Docker's Linux engine:

```powershell
.\mvnw.cmd -B -ntp verify
```

On Linux/macOS: `./mvnw -B -ntp verify`. Docker is required; missing infrastructure is an acceptance failure. Unit-only `test` does not execute the integration suite. Reports are generated under `target/surefire-reports` and `target/failsafe-reports`.

## Capacity and lifecycle

Three recorded 100-request concurrency rounds against capacity 10 each produced 10 successful holds, 90 conflicts, 10 rows and zero availability. Independent-resource, mixed-quantity and rollback cases also passed. Lifecycle/expiration tests cover competing workers and confirm/cancel/deadline races. See [AllocationConcurrencyIT](../src/test/java/com/atlas/AllocationConcurrencyIT.java), [ReservationLifecycleIT](../src/test/java/com/atlas/ReservationLifecycleIT.java) and [ExpirationIT](../src/test/java/com/atlas/ExpirationIT.java).

## Failure recovery

All eight recorded failure/recovery scenarios passed. [Scenario results](verification/failure-recovery.json) identify each test and its measured duration; [FailureRecoveryIT](../src/test/java/com/atlas/FailureRecoveryIT.java) contains the executable checks. Commit/ack failures, concurrent retry claims and cache/recovery boundaries have additional dedicated suites listed in the totals.

## Load observations

Five measured workloads recorded 3,769 requests, 557 expected capacity conflicts and zero unexpected failures. Each had a 30-second measurement window after 5-second warmup and a drain interval, seed 42.

| Workload | Requests | req/s | All-request p95 ms | Expected conflicts | Publication p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| normal | 780 | 25.76 | 385.46 | 0 | 486.62 |
| spike | 711 | 23.42 | 1095.19 | 0 | 16901.01 |
| hot-resource | 769 | 25.16 | 1301.04 | 557 | 3305.02 |
| idempotent-retry | 772 | 24.69 | 1248.82 | 0 | 17015.80 |
| mixed | 737 | 24.12 | 400.37 | 0 | 303.24 |

These are one short shared-host run per workload on Windows/WSL2, Intel i7-13620H, approximately 16 GB physical RAM / 8 GB Docker RAM. App limits: 2 CPUs / 768 MiB; PostgreSQL 2 CPUs / 512 MiB; Kafka 1 CPU / 1,024 MiB; Redis 0.5 CPU / 128 MiB. Pools: API/consumer 10, publisher 2. The spike/retry outbox backlogs drained within the 60-second bound. The values do not establish saturation, a production SLO or supported user count. See the [pinned workload setup and commands](../scripts/load/README.md).

## Startup and operational reproduction

After building, use [Operations](OPERATIONS.md) for normal startup. PowerShell 7 is required for the isolated recovery demonstration:

```powershell
.\scripts\handoff\demo.ps1 -SessionId atlas-reproduce-001
```

The script generates local private credentials and tests startup, normal mutations, response replay, Kafka outage/restart, Redis fallback and expiration recovery. It generates observations under `docs/evidence/phase-15/runs` locally. Preserve failure evidence; guarded cleanup is described in Operations. Load scripts likewise generate local run evidence.

## Historical phase index

The seven planning documents retain the incremental build history. Their phase-evidence links lead here in the public repository. Original command logs, raw test XML, environment snapshots and dated phase artifacts remain local and are deliberately excluded from publication; some include host paths or generated credentials. Public summaries contain no runtime credential dumps. Reproduce checks to obtain fresh raw evidence.

<a id="phase-1"></a>
<a id="phase-2"></a>
<a id="phase-3"></a>
<a id="phase-4"></a>
<a id="phase-5"></a>
<a id="phase-6"></a>
<a id="phase-7"></a>
<a id="phase-8"></a>
<a id="phase-9"></a>
<a id="phase-10"></a>
<a id="phase-11"></a>
<a id="phase-12"></a>
<a id="phase-13"></a>
<a id="phase-14"></a>
<a id="phase-15"></a>

| Phase | Verified scope |
| --- | --- |
| 1–3 | Infrastructure, schema, resource API and local identities |
| 4–5 | Transactional holds and real PostgreSQL concurrency |
| 6–7 | Lifecycle locking and expiration workers |
| 8–9 | Atomic idempotency and transactional outbox |
| 10–11 | Acknowledged Kafka publishing, deduplicated audit effects and optional Redis |
| 12–13 | Bounded retries, guarded replay/reconciliation and failure recovery |
| 14–15 | Measured local workloads, final contract review and startup/recovery demonstration |

Production security/HA/retention and broader observability remain unverified/deferred. Personal understanding is not inferred from generated tests or explanations.
