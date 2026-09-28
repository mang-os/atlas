# Flyway migrations

V1 creates the resource and reservation tables, row constraints and lookup indexes.
It upgrades Phase 1's empty production schema (optionally containing Flyway history).
Flyway validates naming/checksums; applied migrations are immutable. Hibernate DDL
generation stays disabled. No lifecycle trigger or cross-table allocation logic is
implemented here. See docs/BACKEND_SCHEMA.md for the transaction boundary.

InfrastructureIT also loads a separate test-only repeatable probe migration.
SchemaIT uses only production migrations for fresh/upgrade and constraint checks.

V2 adds durable scoped idempotency claims and original responses.
V3 adds reservation transition sequences and transactional outbox events. It locks
reservation exclusively and refuses a nonempty reservation dataset without changing
or deleting historical data. Stop old API/worker writers before activation; never run
mixed pre-outbox/Phase 9 writers. Use a fresh database, or follow the explicit historical
import/disposable-reset decision process in docs/BACKEND_SCHEMA.md. No automatic backfill
or reset exists. Outbox events remain PENDING; publication is Phase 10 work.
