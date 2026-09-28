# Phase 14 load workloads

Run from the repository root in PowerShell 7, with Docker available and the tested Java 21 application JAR built. This runner creates a separate `atlas-load-<session>` Compose project. It never connects to your ordinary Atlas database. The pinned k6 2.3.0 image is in [toolchain.json](toolchain.json); all infrastructure pins and benchmark settings are in [compose.load.yml](compose.load.yml).

Apply the [root README prerequisites](../../README.md#prerequisites) first. On this Windows host, that includes the Java 21 path and process-local sockets workaround; the [evidence ledger](../../docs/VERIFICATION.md#phase-14) records the exact Maven environment and command used.

```powershell
# Build/test the application first if target/atlas-0.1.0-SNAPSHOT.jar is absent or stale.
.\mvnw.cmd -B -ntp verify
# Use a fresh session ID; existing evidence is never overwritten.
.\scripts\load\run.ps1 -Mode All -SessionId p14-reproduce-001 -Seed 42 -DurationSeconds 30
python scripts/load/summarize.py docs/evidence/phase-14/runs/p14-reproduce-001
```

`-Mode Smoke` runs only the five 3-second smoke cases; `-Mode Measured` runs only measurements. `-Scenario normal|spike|hot-resource|idempotent-retry|mixed` selects one case. Measured durations are 15-600 seconds. Defaults are 30 seconds, seed 42 and all scenarios. Increase duration deliberately; these defaults are short local comparisons, not a saturation study. If Docker needs its installed path on this Windows machine, prepend `C:\Users\Tarun K\AppData\Local\Programs\DockerDesktop\resources\bin` to the current process PATH, as recorded in the evidence commands.

| Scenario | Measured concurrency | Dataset | Journey |
| --- | --- | --- | --- |
| normal | 8 constant VUs | 16 resources, capacity 100 each | Hold one, read availability, cancel; 50 ms pacing |
| spike | 2 -> 20 -> 2 VUs | 16 resources, capacity 100 each | Normal journey; 5 s ramp up, 20 s plateau, 5 s ramp down at the default duration |
| hot-resource | 24 constant VUs | One resource, capacity 4 | Hold one; successful holders wait 50 ms then cancel, conflicts wait 10 ms |
| idempotent-retry | 16 constant VUs | 16 resources, capacity 100 each | Two simultaneous identical holds, a third replay, then one cancellation; quantity 2 |
| mixed | 8 constant VUs | 16 resources, capacity 2,000,000 each | Hold quantity 1-3; deterministic 25% confirm, 25% retained HELD, 50% cancel; owned read |

Smoke uses two VUs (hot-resource uses three; spike ramps 1 -> 3 -> 1). Every case gets fresh server-generated resource IDs, a run-prefixed name and recorded manifest. Seed/VU/iteration determine selection, quantities and actions; concurrent execution timing remains nondeterministic. The large mixed capacity deliberately avoids exhaustion so lifecycle throughput is distinct from hot-resource contention. Confirmed capacity remains allocated. The benchmark hold duration is 3,600 seconds; expiration and reconciliation remain enabled, but ordinary mixed holds are not due during a run.

Each measured case uses a 5-second one-VU warmup, a 5-second drain/gap, then the recorded scenario; smoke uses 1 + 1 seconds. In-flight measurement iterations have a 15-second graceful deadline. Throughput divides **measured API requests** by measured elapsed time including this final drain. Custom latency and error metrics include only the measurement scenario, excluding setup/warmup. k6 built-in metrics in the raw file include warmup and must not be substituted silently. API request throughput is different from successful hold or journey throughput.

Only `409 INSUFFICIENT_CAPACITY` in the hot-resource case is expected. All transport failures, other errors, unexpected conflicts and replay/DTO contract violations fail acceptance; no infrastructure failure is reclassified as contention. Successful and expected-conflict latency are also stored separately. Retries assert identical original status/body/Location, replay headers, one reservation per successful durable claim and capacity conservation; request IDs are deliberately not body equality inputs.

The runner samples PostgreSQL connections/backlog with read-only SQL approximately once per second; Docker execution overhead means the actual interval is longer and recorded. Application connections include the API/consumer pool and the separate publisher pool; total connections also include the observer. Peaks between samples can be missed. After bounded delivery/audit convergence, SQL computes publication p50/p95/p99 over events **created in the measured window**, using `published_at - created_at`. This includes queueing, send and the mark after broker acknowledgment; it is not consumer latency or a Kafka-only transport metric. No CPU utilization, distributed tracing, heap/GC percentile, or saturation bottleneck is instrumented here.

After every smoke and measured run, a consistent PostgreSQL snapshot checks bounds/conservation, legal states/history, event envelope identity, completed claims, one reservation per successful hold claim, and marker/effect consistency. Final delivery checks require all events published and exactly one audit effect/marker per event. Recycling cases must have no remaining HELD reservations. Started/completed iteration counts must match. Full snapshots, metrics, categories, offsets and cleanup evidence are retained beside the manifest.

Cleanup first stops the app, verifies the audit consumer's committed offset equals the Kafka topic end, and matches manifest resource IDs **exactly** to run-prefixed database rows. SQL deletes only that run's dependent rows and caller-scoped keys in one transaction. An unrelated resource/claim canary must remain unchanged. Cache entries have existing 2/5-second TTLs; the entire private Redis container is removed at session completion. The runner then verifies session labels on its project containers and volumes before removing its own stack/volumes. It does not reset the topic mid-session or discard consumer markers before acknowledged delivery.

On failure, logs and private database/broker volumes remain, with the app stopped. Diagnose before discarding them. Guarded removal of only that session is available:

```powershell
.\scripts\load\run.ps1 -SessionId p14-reproduce-001 -CleanupOnly
```

Generated passwords live only in ignored `.tools/load/<session>.env`, never in result manifests. Basic authentication is the documented development identity mechanism; load results include its password-verification cost. Raw command argument arrays are in each session's `commands.jsonl`; raw k6 JSON, summary, stdout/stderr, database samples, publication percentiles, validation and cleanup are in each case folder. A failed measurement is not a benchmark result. Preserve failed evidence and rerun only failed checks under a new session ID. Actual executed results are in [Phase 14 evidence](../../docs/VERIFICATION.md#phase-14).
