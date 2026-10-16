# CBL-10.1. Payout state machine and DB schema

| Field | Value |
| :-- | :-- |
| Epic | E10, Payout/Refund Service (Saga) |
| Repository | `cbl-payout` |
| Size | M |
| Wave and parallel group | W2-PAYOUT |
| Depends on | CF-1 (`billing/payout/v1`), CF-2 (`billing/events/v1`), 1.2 (cbl-kit/outbox), 1.3 (cbl-kit/inbox) |
| Contract version | `cbl-protos@cf-2` |
| Status | done, review closed |

## 1. Context and goal

Payout owns the payout state machine (`CBL-000`, section 2) and runs the saga
`requested -> approved -> reserved -> sent_to_gateway -> completed/failed` with
hold compensation (`compensated`) and operator rejection (`rejected`).
The task creates the epic foundation: the DB schema (`payouts`, `payout_transitions`,
outbox/inbox), the domain transition matrix and `state_version` with optimistic
locking - for 10.2 (saga), 10.4 (unknown status), 10.5 (approvals),
10.6 (refunds), 10.7 (chaos tests).

States and flows are fixed by `CBL-000` (section 5, payout), `CBL-003`
(section 6: `payout.events` - `requested/approved/completed/failed/compensated`) and
CF-1 (`billing/payout/v1/PayoutState`).

## 2. Boundaries

In scope: migrations 00001 - 00003, the state machine domain, the storage layer
of transitions with CAS, main wiring (migrations, DB pool), aligning the
repository skeleton with the standard set (go.mod, linter v2, Makefile, Dockerfile,
compose), unit/integration tests, e2e schema checks on compose.

Out of scope: PayoutService gRPC handlers (10.2, 10.5), the reserve-command-commit
saga (10.2), `ledger.events`/`gateway.payout.events` consumers (10.2, 10.4), limits and
approvals (10.5), refunds (10.6), publication of `payout.commands`/`payout.events` (10.2),
status re-check (10.4).

## 3. Functional requirements

FR-1. `payouts` schema (migration 00001): `id` uuid PK (UUIDv7), `merchant_id`
(1 - 128), `idempotency_key` (1 - 255) + `request_hash` (1 - 128) with the
uniqueness of `(merchant_id, idempotency_key)`, `state` (CHECK against the canonical
states), `state_version bigint >= 1`, `amount_minor > 0`, `amount_asset`
(`^[A-Z0-9]{2,16}$`), `amount_decimals` (0 - 18), the `fee_minor`/`fee_asset`/
`fee_decimals` triple (nullable, all-or-nothing, `>= 0` when set), `network`
(1 - 64), `destination_address` (1 - 256), `merchant_ref` (not null, up to 255),
`reserve_id` (the Ledger hold identifier, nullable until the `reserved` step),
`gateway_payout_id`, `provider`, `txid`, `failure_reason` (nullable),
`created_at`, `updated_at`, `completed_at` (CHECK: only for `completed` and
mandatory for it). Indexes: `(merchant_id, created_at DESC, id DESC)` for
history; the partial `(updated_at) WHERE state IN ('requested','approved',
'reserved','sent_to_gateway','failed')` for saga recovery and background jobs
(10.4, 10.7): `failed` is included so that an uncompensated hold does not hang.

FR-2. Transition history `payout_transitions` (append-only, UPDATE/DELETE/
TRUNCATE trigger): `payout_id` (FK), `from_state` (empty for creation; CHECK against the
dictionary), `to_state` (CHECK against the dictionary), `state_version`, `reason`,
`occurred_at`; the uniqueness `(payout_id, state_version)`. The journal is the state
audit and the timeline source for 10.4/10.6.

FR-3. Transition matrix (domain, the single source of rules):

```
requested       -> approved | reserved | rejected | failed
approved        -> reserved | rejected | failed
reserved        -> sent_to_gateway | failed
sent_to_gateway -> completed | failed
failed          -> compensated
terminal: completed, compensated, rejected
```

Semantics: `requested` - the request is created (10.2: a small one passes the limits
and goes to the reserve, a large one waits for approval 10.5); `approved` - the operator
approval is passed (10.5); `reserved` - the hold in Ledger is created (2.4);
`sent_to_gateway` - the command is sent to Connector (`payout.commands`);
`completed`/`failed` - the result of the external step; `failed -> compensated` -
compensation: release of the hold (or closing the saga without release if there was
no reserve) - performed by 10.2; `rejected` - operator rejection before the reserve
(terminal). Backward transitions and transitions from terminal states are forbidden;
the predicates `allowed(from, to)` and `terminal(state)`. A self-transition (updating
data without changing the state: `gateway_payout_id`, `fee`, a repeated confirmation)
is allowed only in non-terminal states, increments `state_version` by 1 and is written
to the journal.

FR-4. `state_version`: 1 at creation, every accepted transition (including a
self-transition) increments it by exactly 1. A transition is performed optimistically:
reading under a row lock and `UPDATE ... WHERE id = ? AND state_version = ?`; a version
conflict - `domain.ErrStateConflict` for caller retries (saga 10.2, consumer 10.4);
the `payout_transitions` row is written in the same transaction.

FR-5. Second DB barrier: the `payouts` BEFORE INSERT trigger requires
`state='requested' AND state_version=1` and empty saga data (`reserve_id`,
`gateway_payout_id`, `provider`, `txid`, `failure_reason`, `completed_at`,
`fee_*`): substituting the context before transitions is impossible; the BEFORE UPDATE
trigger checks the `(OLD.state, NEW.state)` pairs against the matrix,
`NEW.state_version = OLD.state_version + 1`, the immutability of fields (`id`,
`merchant_id`, `idempotency_key`, `request_hash`, `created_at`, `amount_*`,
`network`, `destination_address`, `merchant_ref` - the recipient details cannot be
substituted after approval/sending) and of `reserve_id` after it is fixed;
self-transitions in terminal states are forbidden. The Go and SQL matrices are checked
by exhaustive enumeration of all state pairs in an integration test.

FR-6. Migrations 00002 (outbox) and 00003 (inbox) - reference DDL from `cbl-kit`
(`CBL-003`, section 6), as in the ledger (2.1) and payment (4.1).

FR-7. Storage: `Create` (inserting `requested` version 1 - the state, version and
saga data are forcibly reset to the initial ones, the version 1 history row in the
same transaction; a taken idempotency key - `ErrDuplicateKey`),
`Transition`/`TransitionTx` (CAS on `state_version` + journal row;
the tx variant - for writing the transition and the outbox row in one transaction of
saga 10.2 and consumer 10.4), `PayoutTx` (reading the payout in the passed Guard
transaction), `Get` (the payout and history in one read-only repeatable read
transaction).

FR-8. Wiring: main applies migrations (`MIGRATIONS_DIR`, default
`./migrations`) before the servers start, configures the DB pool (`DB_MAX_OPEN_CONNS`
default 20, ceiling 100; `DB_MAX_IDLE_CONNS` default 10; lifetime
30 minutes) and requires `PG_DSN`. The skeleton is aligned with the standard set:
go.mod (GORM + pgx driver, UUID, testify, testcontainers, cbl-kit/cbl-protos with
replace), `.golangci.yml` v2 (gosec), Makefile (test-integration, e2e, test-e2e,
lint with the `services/` context), Dockerfile (shared `services/` context),
local compose (postgres `payout`, `app` profile).

## 4. Contracts

External contracts do not change (CF-1/CF-2 are frozen): the domain `State` corresponds
to `billing.payout.v1.PayoutState` (a 1:1 mapping by names, checked by a test).
The correspondence of states to `payout.events` events (10.2): `requested`, `approved`,
`completed`, `failed`, `compensated`; the states `reserved`, `sent_to_gateway`,
`rejected` have no events in CF-2 (internal saga steps).

## 5. Data model and migrations

```sql
payouts(id, merchant_id, idempotency_key, request_hash, state, state_version,
        amount_minor, amount_asset, amount_decimals,
        fee_minor, fee_asset, fee_decimals,
        network, destination_address, merchant_ref, reserve_id,
        gateway_payout_id, provider, txid, failure_reason,
        created_at, updated_at, completed_at)
payout_transitions(id bigserial, payout_id, from_state, to_state, state_version,
                   reason, occurred_at, UNIQUE (payout_id, state_version))
outbox, inbox - as in CBL-003 (section 6)
```

Migrations are forward only; 00001 creates the payout tables and triggers, 00002/00003 -
the infrastructure event tables.

## 6. State machines and rules

Matrix FR-3; the states and transitions are duplicated by the DB trigger (FR-5) and
checked by exhaustive enumeration of pairs in an integration test. The difference from the
payment state machine (4.1): `failed` is not terminal - only compensation is possible
from it (10.2), while the states `reserved`/`sent_to_gateway` reflect saga steps and
are not published to `payout.events`.

## 7. Edge cases

Creation with version 1 (the passed state, version and saga data are ignored;
a direct INSERT with another state or a pre-set `gateway_payout_id` is rejected by the
BEFORE INSERT trigger); concurrent transitions from one version (exactly one
wins); a stale transition after a newer one (rejection, the version and journal do not
grow); a backward transition (`sent_to_gateway -> reserved`), a transition from a
terminal state (`completed -> failed`), an unknown pair (domain and trigger); a
self-transition in a non-terminal state (allowed, writes a journal row) and in a
terminal one (forbidden); a direct UPDATE bypassing CAS; UPDATE of immutable fields
(including `amount_*`, `network`, `destination_address`, `merchant_ref`); changing
`reserve_id` after it is fixed; UPDATE/DELETE/TRUNCATE of the journal; writing a
journal row with an unknown `from_state`/`to_state`; a duplicate
`(merchant_id, idempotency_key)`; numeric boundaries: zero/negative amount, an empty
asset, decimals out of range, empty `network`/`destination_address`, strings above the
length limits; a partial `fee` triple; `completed` without `completed_at` and
`completed_at` outside `completed`; compensation without failed (10.2 calls strictly
`failed -> compensated`); a repeat `failed -> compensated` (terminal rejection or a
version conflict); rollback of an external transaction with `TransitionTx` (the
transition and journal are not committed).

## 8. Non-functional requirements

Transactionality: a transition and its history - one transaction; concurrency -
optimistic retries (T-PAY-02-like, as in the ledger and payment); the journal is
append-only - audit; the CreatePayout/GetPayout deadline of 2 s (CF-1) fits within
a read/write by PK; the partial saga index limits background job scans (10.4)
to non-terminal states.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit (domain) | the whole matrix (every allowed pair), forbidden pairs (backward, from terminal, unknown states), `terminal()`, the 1:1 `PayoutState` mapping, version increment, self-transitions |
| Integration (storage) | Create with v1 history and forced `requested` (saga data is cleared); a CAS transition and increment; concurrent transitions (exactly one); a stale version (the version and journal do not grow); exhaustive enumeration of all pairs against the trigger (a mirror of the matrix); a direct INSERT/UPDATE bypassing the state machine; immutable fields (including the recipient details) and `amount_*`; `reserve_id` is fixed on reserve and immutable afterwards; a self-transition writes a journal row; `completed_at` is mandatory for `completed` and forbidden outside it; a repeat compensation - terminal rejection; `TransitionTx` rolls back with the external transaction; the journal `from_state`/`to_state` dictionary; append-only; CHECKs of amounts/assets/scales/address/lengths/partial fee; idempotency key uniqueness |
| E2E | compose stand (run instruction `test/e2e/E2E.md`, scenario E2E_Payout_Schema): migrations applied (version 3), the chain `requested -> reserved -> sent_to_gateway -> completed` and the compensation chain `requested -> approved -> reserved -> failed -> compensated` with every transition recorded, a forbidden transition and a journal overwrite are rejected by the second barrier |
| Load | 14.1: payout is not loaded with a separate profile |

Target unit+integration coverage: at least 80%.

## 10. Work plan and commit

Tests first (domain, storage integration), then migrations, domain/state.go,
storage/repository.go, main and skeleton wiring, the stand e2e script. Commit
`CBL-10.1: payout state machine and DB schema`; the spec is in the same commit.

## 11. Open questions and decisions

- `reserve_id` (the Ledger hold identifier) is stored in `payouts` since 10.1:
  `CommitReserve`/`ReleaseReserve` accept only `reserve_id` (`ledger.proto`),
  there is no reserve lookup by `ref_id`; without the stored identifier, compensation
  after a saga restart (10.2/10.7) is impossible. The field is fixed on the transition
  to `reserved` and is immutable via the second barrier.
- `failed` is not terminal: the saga must close the hold (`failed ->
  compensated`); for refusals before the reserve, 10.2 moves `failed -> compensated`
  without calling Ledger (release is not required), so that a payout has a single
  final compensation status. Operator rejection before the reserve is a separate
  terminal `rejected`.
- Refund-to-original-payout/invoice link (10.6): CF-1 `CreatePayoutRequest`
  has no `invoice_id`; the `merchant_ref` field is stored for correlation (Billing API
  7.4). If 10.6 requires an explicit reference, the contract is extended by its owner
  (`billing/payout/v1` - CF-1, `billing/events/v1` - CF-2) in a separate task.
- Approvals (10.5) add no states: `requested -> approved` covers the
  two-step flow; operator/approval-time columns and the idempotency of a repeated
  `ApprovePayout` (CF-1 has no `idempotency_key`) are 10.5, added by a migration
  if needed.
- The too-early policy for `gateway.payout.events` (a result may arrive before
  `sent_to_gateway` due to a race between the saga and the consumer): an early
  `submitted/confirmed` must park/retry rather than go to the DLQ; it is formulated
  in 10.2/10.4 (pattern - KI-payment-05).
- `reserved -> failed` (the command did not go out) and `reserved -> sent_to_gateway`:
  the branch choice is 10.2; the matrix allows both.
- Relapses of the Unknown status (10.4): `sent_to_gateway` remains non-terminal,
  the status re-check does not change the state until the verdict; the matrix pairs
  already cover both outcomes.
- Immutability of `destination_address`/`network`/`merchant_ref`: there are no
  legitimate change scenarios; an address change is a new payout (checked in 10.2).

## 12. Review (CBL-001, step 6)

Four independent passes (15.10.2026). The security BLOCKER (substitution of
`destination_address`/`network`/`merchant_ref` by a self-transition bypassing the second
barrier) is closed by extending the immutable set of the trigger and by tests for every
field; the architecture BLOCKER (the schema does not store `reserve_id` - compensation
by 10.2 after a restart is impossible) is closed by the `reserve_id` column fixed on
reserve and immutable. MAJORs are closed: `failed` is included in the partial saga index
(the hold does not hang without background review); the tx variants
`TransitionTx`/`PayoutTx` are added for the Guard transaction of 10.2/10.4; the INSERT
guard requires empty saga data, `Create` clears it; CHECKs of string lengths and
all-or-nothing `fee` are added; `completed` requires `completed_at`; tests are extended
(all immutable fields, the `from_state` dictionary, boundaries, repeat compensation,
external transaction rollback, the self-transition journal). AGENTS.md is updated in
this commit (T-PAYOUT-01..05, KI, runbook, task journal). Accepted without changes:
the journal is written by the application (design 4.1, direct DML is outside the threat
model - KI about roles), the `ErrDuplicateKey` message with identifiers (payment
pattern, input validation 10.2), `reflection.Register` and the runtime/migrations DSN -
existing platform KIs.

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | MAJOR: `make test-e2e` is a no-op (no Go package); MINOR: repeat compensation, the self-transition journal, immutable fields other than amount_minor, and boundaries are not covered; NIT: the FR-5 wording, a typo, a comment, a dead branch, the main exit code, `$version`, the Dockerfile cache, UUIDv7 | the target is removed before 10.2; tests added (repeat, journal, fields, boundaries); FR-5 reworded; typos fixed; the branch is left with a comment; the exit code as in payment (outside 10.1); `$version` initialized; COPY following the payment pattern; UUIDv7 documented |
| Architecture | BLOCKER: no `reserve_id`; MAJOR: `failed` outside the saga index, no tx hook for the outbox, the recipient details are not immutable; MINOR: go.mod version drift, `test-e2e`, nullable `completed_at`, partial fee, 10.5 fields, normalization of the state-event mapping, too-early 10.2/10.4, tests, AGENTS; NIT: typos, the network format, `readyz`, the e2e SQL | `reserve_id` added; the index extended; `TransitionTx`/`PayoutTx`; immutable recipient details; go.mod aligned with the unified set; the target removed; `completed_at` mandatory; fee all-or-nothing; 10.5/10.6 and too-early recorded in §11; tests extended; AGENTS updated; typos fixed; the network non-emptiness CHECK; `readyz` without the DB until 0.7; the e2e SQL is deliberate |
| With context | MAJOR: AGENTS is not updated, `reserve_id`, `test-e2e`, the spec is not closed; MINOR: version drift, immutable recipient details, tests of immutable fields, migrations of 10.5/10.6 in §11, CF-2 vs CF-3, KI-01/KI-ledger-08; NIT: a tautological assert, a main comment, `$version`, the from_state dictionary, the smoke line, README, an extra kafka in compose, fee all-or-nothing | AGENTS (T-PAYOUT-*, KI, runbook, task journal); `reserve_id`; the target removed; the spec closed; go.mod; immutable; tests; §11 extended (10.5, 10.6, CF-2); KI; the assert removed; the comment; `$version`; the dictionary; the smoke reflected; README - NIT outside 10.1; kafka left (10.2 uses it right away); the CHECK added |
| Security | BLOCKER: substitution of `destination_address`/`network`/`merchant_ref`; MAJOR: the audit is not mandatory at the DB level, `failed` outside the index, no CHECKs of lengths/formats, saga fields on INSERT; MINOR: the error text, `completed_at`, UUIDv7, e2e interpolation, `test-e2e`, compose passwords, reflection; NIT: digest, .dockerignore | immutable recipient details + tests; accepted (design 4.1, outside the threat model - T-PAYOUT-01); the index; length CHECKs and checks in tests; the INSERT guard + `Create` clearing; the message as in payment; `completed_at` mandatory; documented; the e2e is safe (GUID); the target removed; passwords as in payment (dev); reflection - KI (0.7); digest/.dockerignore - 0.2 |
