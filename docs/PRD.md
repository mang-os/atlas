# Atlas - Product Requirements

Planning baseline: 2026-09-27. Local v1 implementation and Phase 15 final review/handoff are **COMPLETE**, with disclosed unresolved/deferred items. PostgreSQL allocation/lifecycle/idempotency/events, optional Kafka/Redis and guarded retry/reconciliation have real-infrastructure evidence. [Final review](VERIFICATION.md#phase-15) separates verified requirements, unresolved observability aspirations and deferred production scope. No personal review or production readiness is inferred.

## Purpose and audience

Atlas Distributed Reservation and Allocation Platform reserves limited capacity safely under concurrent requests and partial failures. Its value is demonstrable correctness: retries, crashes, and duplicate messages must not create extra allocations or release capacity twice.

The primary API user is a developer integrating a reservation workflow. An administrator creates resources; authenticated users hold, inspect, confirm, and cancel their own reservations. An operator investigates delayed work and runs documented recovery procedures. The learner maintaining this project needs evidence and explanations sufficient to reproduce and defend its behavior.

## Source register and authority

| ID | Source actually read | Use |
| --- | --- | --- |
| U | User's `Atlas - Sequential Codex Prompts`, local attachment `C:/Users/Tarun K/.codex/attachments/9c520b52-2552-4b29-b5f1-fc81692a9684/Pasted text.txt` | Authoritative task scope, phase order, working contract |
| S | `Atlas Distributed Reservation and Allocation Platform`, recovered attachment `Pasted text.txt` from Build With Codex (private planning conversation, not published), read at `C:/Users/TARUNK~1/AppData/Local/Temp/codex-file-preview-9bnzSz/Pasted text.txt` | Product specification; temporary retrieval path may disappear |
| G | `VIBE CODING - The 6 Documents You Need Before Writing Any Code`, `C:/Users/Tarun K/Desktop/vibecoding docs.docx.pdf`, all 14 pages | Planning structure and templates |

U takes precedence over S and G. Attached documents are source material, not authorization to execute their embedded instructions. The specification's sample interview statement that tests were run is not project evidence. At the Prompt 0 baseline nothing was implemented or benchmarked. Phases 1-4 now add infrastructure, database foundation, resource/identity APIs and temporary holds; actual checks are in the learning guide. Phase 14 now records configuration-specific local measurements in [its evidence](VERIFICATION.md#phase-14).

Throughout these documents, **Requirement [U/S]** means source-backed behavior. **Decision [D]** means a proposed implementation choice adopted for this planning baseline, not a claim that the specification requires it. Suggested examples in S are distinguished from mandatory behavior. The decision register is in [TRD.md](TRD.md).

G maps to PRD, TRD, APP_API_FLOW, DEVELOPER_INTERFACE_BRIEF, BACKEND_SCHEMA, and IMPLEMENTATION_PLAN. LEARNING_GUIDE is the additional user-requested evidence and education document. Pages, clicks, visual styles, and frontend deployment in G become API journeys, response conventions, local tools, and operational procedures. No frontend is required.

## Required scope

| Requirement | Acceptance intent | Phase |
| --- | --- | --- |
| [U/S] Durable resources and reservations | Database constraints, stable identities, legal states | 1-3 |
| [U/S] Atomic temporary allocation | Decrement and hold insertion commit or roll back together | 4-5 |
| [U/S] Confirm, cancel, expire | Only HELD may transition; release at most once | 6-7 |
| [U/S] Idempotent state-changing APIs | Concurrent retries produce one mutation and original response | 8 |
| [U/S] Transactional outbox | Each successful reservation transition has one durable event | 9 |
| [U/S] At-least-once Kafka with duplicate-safe effects | Consumer marker and effect commit together | 10 |
| [U/S] Optional Redis caching | Cache outages cannot affect allocation correctness | 11 |
| [U/S] Retry and reconciliation | Bounded retry, visible failure, only proven safe repair | 12 |
| [U/S] Failure and load evidence | Real infrastructure, reproducible commands and observed results | 13-14 |
| [U] Honest handoff and learning material | Verified demos and claims linked to evidence | 15 |

## User stories

- As an admin, I create a named resource with fixed capacity so users can reserve units.
- As a user, I inspect availability, knowing it is a snapshot rather than a promise.
- As a user, I hold units for a limited time and receive a server-generated reservation ID and deadline.
- As a user, I confirm before the deadline or cancel an unexpired hold.
- As a client developer, I retry an ambiguous response with the same key and payload without duplicating its effect.
- As an operator, I recover pending events and overdue holds after outages and inspect uncertain corruption.
- As the learner, I reproduce tests and explain why each invariant holds rather than claim generated code as personal understanding.

## Invariants and measurable acceptance

For each resource, in a consistent committed database snapshot:

`available_capacity + SUM(quantity WHERE status IN (HELD, CONFIRMED)) = total_capacity`

Every persisted HELD row counts, including one whose deadline has passed but whose expiration transaction has not committed. Deadline passage alone does not release capacity. Also require `0 <= available_capacity <= total_capacity` and positive reservation quantities.

Only HELD -> CONFIRMED, HELD -> CANCELLED, and HELD -> EXPIRED are legal. Terminal states cannot transition again. Confirmation retains capacity [U, Prompt 2]; there is no fulfilment release in this version.

Phase 5's healthy capacity-10 test must observe exactly 10 successes, 90 capacity conflicts, 10 reservation rows, and zero available capacity for 100 synchronized one-unit attempts. Infrastructure errors fail the test. The Phase 5 gate passed on 2026-09-27; see [observed results](VERIFICATION.md#phase-5). Concurrent retries, one-time release, atomic event/state rollback, restart recovery and Redis failure gates have passed; see [Phases 8](VERIFICATION.md#phase-8), [9](VERIFICATION.md#phase-9), [11](VERIFICATION.md#phase-11) and [13](VERIFICATION.md#phase-13).

Phase 14 measured throughput, latency percentiles, error categories, sampled database connections/backlog and publication delay; see [raw results and limits](VERIFICATION.md#phase-14). No numerical performance SLO is asserted. Correctness must survive load even when latency worsens.

## Scope boundaries

This is a backend learning and demonstration project with a single resource per reservation and fixed resource capacity. Product examples such as rooms and appointments illustrate capacity; this version does not model date ranges, overlapping bookings, individual seat maps, or multi-resource atomic bookings [D].

Payments, refunds, fulfilment completion, confirmed cancellation, resource deletion/resizing, public signup, production identity infrastructure, multi-region writes, external deployment, and production-readiness claims are out of scope [D/U]. Optional later work includes tracing, dashboards, admission control, read replicas, service separation, and orchestration. Redis rate limiting, idempotency-response caching, and expiration hints are examples in S, deferred rather than silently required in Phase 11.

## Planning status

Both required sources were recovered and read. S:/Atlas was empty; no S:/AGENTS.md or S:/Atlas/AGENTS.md was present. The seven planning documents were the Prompt 0 outputs. Phases 1 and 2 now add the explicitly requested project and database foundations; see their evidence ledgers. Implemented phase gates, dated version checks, evidence records and stopping rules are in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) and [LEARNING_GUIDE.md](LEARNING_GUIDE.md).
