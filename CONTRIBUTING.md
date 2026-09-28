# Contributing to Atlas

Keep changes focused and describe the behavior they alter. For bugs, include the endpoint or worker, expected/actual results, versions and a minimal reproduction with credentials removed.

## Development

Use JDK 21 and a running Docker Linux engine. Run `./mvnw -B -ntp verify` (Windows: `.\mvnw.cmd -B -ntp verify`). The integration suite requires real infrastructure; a unit-only pass is not full verification. See [Operations](docs/OPERATIONS.md).

## Review boundaries

- Keep allocation and reservation insertion atomic. Preserve capacity conservation.
- Serialize lifecycle transitions; check authoritative deadlines after locking.
- Commit idempotency responses and outbox events with their business mutations.
- Deduplicate consumer effects transactionally. Never claim exactly-once transport.
- Keep Redis informational and preserve PostgreSQL fallback.
- Add meaningful tests for changed concurrency or recovery behavior and update the affected contracts.

Never include local credentials, generated runtime logs or video assets. Do not change or remove assertions merely to obtain a green build. Schema changes require a migration and an explicit compatibility assessment.
