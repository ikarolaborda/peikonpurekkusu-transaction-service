# transaction-service

The query-side record of what happened: consumes payment facts, writes
**immutable** transaction rows, serves history. .NET 10 · EF Core 10 ·
Confluent.Kafka 2.15.

## Behavior

- Consumes `payments.payment.captured.v1` (→ `purchase`) and
  `payments.payment.reversed.v1` (→ `refund`). Idempotent via
  `processed_events` written in the same EF transaction as the record; offsets
  stored only after the durable write; 3 attempts → `transaction-service.<topic>.dlq`.
- Transactions are append-only — a BEFORE UPDATE/DELETE trigger enforces it at
  the database. Corrections are new reversing rows, never edits.
- Emits `transactions.transaction.recorded.v1` through the transactional
  outbox (polling relay, Confluent wire format via Apicurio ccompat).
- REST (behind ForwardAuth `X-User-Id`): `GET /transactions` (cursor-paginated,
  optional `account_id`), `GET /transactions/{id}`. Health at `/health/live`+`/ready`.

## Patterns map

- **Mediator** — plain-DI `ICommandHandler<,>` + `Mediator` resolver (no
  MediatR: v13 is commercial/RPL)
- **Transactional Outbox** — shared relay pattern (`FOR UPDATE SKIP LOCKED`)
- Immutability enforced in the database (trigger), not just the mapping

## Deviations

Schema via `EnsureCreated` + trigger DDL at startup (single-replica dev
compose); production path is EF migration bundles in a one-shot container.
