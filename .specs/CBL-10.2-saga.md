# CBL-10.2. Payout saga: reserve, command, result, compensation

| Field | Value |
| :-- | :-- |
| Epic | E10, Payout/Refund Service (Saga) |
| Repository | `cbl-payout` |
| Size | L |
| Wave and parallel group | W2-PAYOUT |
| Depends on | 10.1, 10.5 (approval policy), 2.4 (Ledger reserves), CF-2 |
| Contract version | `cbl-protos@cf-2` (`billing/payout/v1`, `billing/events/v1`) |
| Status | done, review closed |

## 1. Context and goal

The payout saga (`CBL-000` section 5): `requested (Ledger Reserve) -> reserved ->
sent_to_gateway (payout.commands -> Connector) -> completed | failed`, and on
`failed` - compensation: release of the hold (`compensated`); already posted
operations are corrected by reversal, a reserve is not a posting (T-LED-06). The external
step (the gateway) is separated from the internal one: the command goes out via
`payout.commands` (outbox), the result comes in `gateway.payout.events` (inbox). An
unknown status after a timeout is task 10.4; here - the happy path and compensation.

The task implements the saga orchestrator on top of the 10.1/10.5 contracts:
`CreatePayout`/`GetPayout` (CF-1), the reserve step (gRPC Ledger 2.4),
emission of `payout.commands`/`payout.events` via the outbox (10.1),
the `gateway.payout.events` consumer and compensation.

## 2. Boundaries

In scope: `CreatePayout`/`GetPayout` (usecase + transport), `Advance` (the reserve
and command step), the Ledger client (Reserve/Commit/Release), outbox emission of
payout events (requested/approved/completed/failed/compensated) and of the command,
the `gateway.payout.events` consumer (submitted/confirmed/failed), compensation,
continuation after approval (10.5), main wiring (relay, consumer, LEDGER_ADDR),
unit/integration/e2e tests (Kafka).

Out of scope: status re-check after a timeout (10.4 - status request via
Connector), background recovery of stuck sagas (10.4/10.7), a real provider in
Connector (10.3, KI-01), refunds (10.6), operator RBAC (12.x), mTLS (0.5/0.6).

## 3. Functional requirements

FR-1. `CreatePayout` (CF-1): input `idempotency_key`, `merchant_id`, `amount`
(Money), `network`, `destination_address`, `merchant_ref`. Validation: string
limits (merchant_id `[A-Za-z0-9_-]{1,128}` - the Ledger chart of accounts form 2.6,
key 1 - 255, network 1 - 64, address 1 - 256, merchant_ref up to 255 without
control characters), amount `> 0`, the asset uppercase 2 - 16 characters,
decimals 0 - 18; `request_hash` - sha256 of the canonical body representation with
field lengths (length-prefix: concatenation via a separator would allow collisions of
different bodies). Creation - 10.1 `Create` (`requested`, version 1): a taken
`(merchant_id, idempotency_key)` with the same hash - an idempotent return of the
existing payout (the repeat resumes the saga: if approval is not required and the
payout is in `requested|approved`, `Advance` is performed - a transport failure of
Reserve does not leave the request forever); with a different hash -
`ErrIdempotencyConflict` (to the transport - `AlreadyExists`). After creation: if the
10.5 policy requires approval - the payout waits for votes (gate), otherwise `Advance`
immediately.

FR-2. `Advance` (the "reserve + command" step) for `requested` (approval not
required) and `approved`: the hold account is a deterministic UUIDv5 of the Ledger
chart of accounts (`merchant/<asset>/<decimals>/<merchant_id>`, the algorithm and
golden vectors `CBL-003` §9, KI-ledger-08); `Reserve` (gRPC, deadline 1 s, key
`payout:{id}:reserve`, `ref_id` = the payout id). Success: one DB transaction - a
transition to `reserved` (patching `reserve_id`), a transition to `sent_to_gateway`,
an outbox record of `payout.commands` (PayoutCommand: payout_id, amount, network,
address, idempotency_key `payout:{id}:command`); `payout.events` events for
`reserved`/`sent_to_gateway` are not published (CF-2 has none). A business refusal
by Ledger (no account, insufficient funds - `InvalidArgument`/`FailedPrecondition`/
`UnknownAccount`): a transition `requested|approved -> failed` with a reason and
immediate compensation without release (there is no reserve) - `failed -> compensated`.
A transport error (timeout/unavailable): the error goes up, the saga does not move -
a repeated `Advance` (client 7.4 or 10.4) is safe: Reserve is idempotent by key.
A repeated `Advance` in a state outside `requested|approved` - an idempotent return of
the current payout.

FR-3. Emission of `payout.events` (outbox, following the payment pattern 4.7): a
sink on `Repository.TransitionTx` and `Create` (the `payout.requested` event); types
`requested`, `approved`, `completed`, `failed`, `compensated`; `reserved`,
`sent_to_gateway`, `rejected` have no type and are skipped. Envelope: topic
`payout.events`, key `payout_id`, headers `event_type` `payout.<state>`,
`producer` `cbl-payout`, `schema_version` `v1`, `occurred_at` (mirrors the body),
`traceparent` from the context. Body `PayoutEvent`: payout_id, merchant_id, type,
amount (Money), fee (when the triple is complete), txid, provider, failure_reason,
state_version, occurred_at.

FR-4. Consumer `gateway.payout.events` (group `cbl-payout-gateway`, the topic +
retry 5s/1m/10m + DLQ via the kit mechanics; processing in the Guard transaction, the
payout is read under a row lock - verdicts of one payout are serialized and cannot
mix up commit/release):
- `submitted`: from `sent_to_gateway` - a self-transition with `gateway_payout_id`
  and `txid`; from `requested`/`approved`/`reserved` - too early (retry via the
  retry topics); from terminal - stale (WARN, ack);
- `confirmed`: requires a non-empty `gateway_payout_id` matching the fixed
  `submitted` (binding of the verdict to the actual sending; an event before
  `submitted` or with a foreign identifier - DLQ), a confirmation without
  `reserve_id` - DLQ without calling Ledger; then `CommitReserve` (key
  `payout:{id}:commit`), then a CAS transition `sent_to_gateway -> completed`
  (patching txid, fee, completed_at); a Ledger error/conflict - event retry
  (commit is idempotent);
- `failed`: the same binding to `gateway_payout_id`; `sent_to_gateway -> failed`
  (reason) and compensation: release (when `reserve_id` is set) before the transition
  `failed -> compensated`; for the state `failed` (an event repeat after a partial
  success) - only release + `compensated`; from terminal - stale.
An invalid event (an invalid payout_id, a negative/mismatched fee,
decimals outside 0 - 18, control characters, overlong strings) - DLQ.
An unknown non-empty enum type is acked with a WARN (BACKWARD_TRANSITIVE
may add types, as in 4.3); an unknown payment - retry.

FR-5. Compensation: release is called only when `reserve_id != NULL`;
`failed -> compensated` without a reserve (refusals before the reserve) does not call
Ledger. The order "external idempotent operation, then CAS transition": a failure
between the steps is safe (an event/saga repeat completes it), transitions do not
roll back already performed Ledger operations. A version conflict in the consumer -
plan retry (up to 3, as 4.3).

FR-6. Transport: `CreatePayout` (response payout_id + state), `GetPayout` (the full
`Payout`); error mapping: `storage.ErrNotFound` -> `NotFound`,
validation/`ErrIdempotencyConflict` -> `InvalidArgument`/`AlreadyExists`,
`ErrSagaUnavailable`/`ErrLedgerUnavailable` -> `FailedPrecondition`
(the client repeats the call, the operations are idempotent), conflicts -> `Aborted`;
`ApprovePayout` - from 10.5.

FR-7. Wiring: `LEDGER_ADDR` - the Ledger gRPC client (deadline 1 s per call);
with `KAFKA_BROKERS` - the publisher, the outbox relay (commands and events) and the
`gateway.payout.events` consumer; without Ledger/Kafka the service starts with a WARN,
the saga step answers `ErrSagaUnavailable`, gateway verdicts are retried until Ledger
is configured, the 10.5 approve flow works. After a successful approval (and on an
idempotent repeat against an approved payout) `ApprovalService` calls `Advance`
best-effort with its own timeout (client cancellation does not kill the step): the
payout continues the saga without manual intervention; errors are logged, not returned
to the client; stuck `approved` (an Advance failure) - recovery 10.4/10.7.

## 4. Contracts

- gRPC: `billing.payout.v1.PayoutService` CreatePayout/GetPayout (CF-1);
  the `billing.ledger.v1.LedgerService` client Reserve/CommitReserve/ReleaseReserve
  (deadline 1 s; idempotency_key is mandatory, `account_id` - the plan UUIDv5).
- Kafka: producer `payout.commands` (key payout_id; PayoutCommand) and
  `payout.events` (PayoutEvent); consumer `gateway.payout.events`
  (GatewayPayoutEvent; group `cbl-payout-gateway`), kit retry/DLQ.
- Storage: `payouts.reserve_id`, `gateway_payout_id`, `provider`, `txid`,
  `fee_*` (10.1); outbox (10.1); no new tables.

## 5. Data model and migrations

No migrations: the 10.1 schema contains all saga fields. The creation `request_hash`
is filled per FR-1 and is used for the idempotent comparison of repeats.

## 6. State machines and rules

Saga transitions follow the 10.1 matrix: `requested -> reserved | failed`,
`approved -> reserved | failed`, `reserved -> sent_to_gateway | failed`,
`sent_to_gateway -> completed | failed`, `failed -> compensated`. All transitions -
`TransitionTx` (CAS + journal), `payout.events` events - from FR-3. Lock order:
`payouts` -> `outbox` (the 10.5 order is preserved).

## 7. Edge cases

A repeated `CreatePayout` with the same key and body (an idempotent return with
saga resumption), with a different body (`AlreadyExists`; the hash separates field
boundaries even with a separator inside values); a repeated `Advance`
(idempotent); Reserve succeeded but the write transaction failed (a repeated Reserve by
key returns the same reserve_id, a duplicate hold is impossible); a business refusal
by Ledger (failed + compensated without release, a canonical reason without internal
details); Ledger unavailable (the error goes up, the state does not change); the
command is published, the response is lost (10.4); `submitted` after `confirmed` (stale);
a verdict before `submitted` or with a foreign `gateway_payout_id` (DLQ, Ledger is not
called); `confirmed` without a reserve (DLQ); `failed` after `completed` (stale,
WARN); a repeat `failed` after `compensated` (release by key - an idempotent
no-op, the transition is forbidden - stale); an event for an unknown payout (retry,
then DLQ); an invalid event, including control characters and multibyte
strings above the limit (DLQ; reason truncation - at a rune boundary, without invalid
UTF-8); an unknown non-empty enum type (WARN, ack); approval is not required (a small
payout goes straight to the saga); approval is required (waits for votes, after the
approval - continuation); a saga without Kafka (the command is in the outbox,
publication later).

## 8. Non-functional requirements

The CreatePayout deadline of 2 s (CF-1): reserve - 1 s, then the DB transaction;
idempotency of all external steps (Ledger keys, outbox, inbox); at-least-once
delivery; the consumer - in the Guard transaction (atomicity of inbox and transitions);
logs without amounts and addresses (payout_id, state, version, operation key);
traceparent from channels/context.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit (domain) | the hold account UUIDv5 against the golden vectors (`merchant/USDT/2/m-1` -> `58686062-...`), invalid keys |
| Unit (usecase) | CreatePayout: validation, an idempotent repeat, a body conflict, small -> Advance, large -> gate; Advance: reserve + two transactions + command, a business refusal -> failed+compensated, a transport error; consumer: the event plan by state (submitted/confirmed/failed), stale/too-early, commit/release before transitions, a missing reserve |
| Integration | real PostgreSQL + a fake Ledger: the full happy path (requested -> reserved -> sent_to_gateway -> completed) with the command outbox row and events; compensation (failed -> release -> compensated); a business reserve refusal; idempotent repeats; `payout.events` events from transitions (requested/approved/completed/failed/compensated); the journal and versions |
| E2E | Kafka + PostgreSQL (testcontainers): `CreatePayout` (usecase) -> outbox -> relay -> `payout.commands`; an external "connector" reads the command and publishes `gateway.payout.events submitted/confirmed` -> payout completes the saga (commit + completed), `payout.events` contains requested/completed; the failed -> compensated branch |
| Load | 14.1 |

Target unit+integration coverage: at least 80%.

## 10. Work plan and commit

Tests first (domain, usecase, integration, e2e Kafka), then the domain account
derivation, the storage event sink, usecase/PayoutService + consumer + Ledger client,
transport CreatePayout/GetPayout, main wiring (relay, consumer, LEDGER_ADDR,
approval continuation), AGENTS.md. Commit
`CBL-10.2: payout saga: reserve, command, result, compensation`; the spec is in the
same commit.

## 11. Open questions and decisions

- The order "external idempotent operation -> CAS transition" (not the other way
  around): terminal states are not fixed before the external step, otherwise
  compensation or completion is impossible; an event repeat completes the saga.
- Ledger calls in the consumer are performed inside the Guard transaction
  (T-PAYOUT-08): the payout row lock serializes verdicts (concurrent confirmed/failed
  cannot mix up commit and release), the external operation is idempotent, an event
  repeat is safe; the cost - the transaction is held up to 1 s per call, the consumer
  throughput is limited by Ledger.
- `payout.commands` is published in-tx together with the transition to
  `sent_to_gateway` (outbox): the state reflects "the command is accepted for sending",
  the actual sending is relay/Connector; the difference from the "gateway response" is
  documented in 10.4.
- Verdict binding: `confirmed`/`failed` must carry a `gateway_payout_id` matching the
  fixed `submitted`; an event before `submitted` or with a foreign identifier goes to
  the DLQ. This protects against a premature commit/release on a forged event
  (KI-payout-02); if `submitted` is lost (a lost webhook), recovery is 10.4/9.x.
- `provider` is absent in the CF-2 `GatewayPayoutEvent`: `submitted` fixes
  `gateway_payout_id`/`txid`, `provider` stays NULL until 10.4 (status re-check/route)
  or a CF-2 extension (KI-payout-11).
- The hold account is the merchant account by `merchant_id` (as the ledger credits it
  in 2.7, `MerchantAccountKey(merchant_id, asset, decimals)`); the payout `merchant_ref`
  is the merchant's external reference and is not part of the chart of accounts; the
  `merchant_id` formats in CreatePayout and the chart of accounts match.
- Without Ledger (the 10.1/10.5 compose stands), the saga step is unavailable:
  `ErrSagaUnavailable` (FailedPrecondition); the saga e2e - on testcontainers with a
  fake Ledger.
- Background recovery of stuck sagas (requested/approved without a reserve, failed
  without compensation, an "orphan" active reserve after a failure between Reserve and
  the transaction, a lost `submitted`) - 10.4/10.7 (KI-payout-10); here - resumption on
  a Create/Approve repeat and best-effort continuation.
- fee and txid come from `gateway.payout.events` (CF-2); a partial fee triple is
  rejected (DLQ).

## 12. Review (CBL-001, step 6)

Four independent passes (15.10.2026). BLOCKERs are closed: verdict binding -
`confirmed`/`failed` must carry a `gateway_payout_id` matching the fixed `submitted`,
a verdict before `submitted` - DLQ without calling Ledger (a forged event neither
completes nor compensates the payout); the payout row lock in the consumer serializes
verdicts (commit and release do not mix).
MAJORs are closed: `request_hash` switched to length-prefix (collisions at field
boundaries are impossible); the `merchant_id` format is aligned with the Ledger chart
of accounts (the payout does not hang on account derivation); a repeated `CreatePayout`
resumes the saga (a transport failure of Reserve does not leave the request); the
approval continuation is called on an idempotent repeat as well, with its own
timeout (client cancellation does not kill the step); `ErrLedgerUnavailable` ->
FailedPrecondition; amounts are removed from logs, `failure_reason` for Ledger refusals
is canonicalized; reason truncation is at a rune boundary; fee decimals and control
characters are validated; `confirmed` without a reserve - DLQ without calling Ledger;
an unknown non-empty enum - WARN/ack (BACKWARD_TRANSITIVE
compatibility); AGENTS.md is updated (T-PAYOUT-08, KI-payout-10/11, runbook).
MINOR/NIT are closed: a test classifying Ledger codes, tests of continuation and hash
collisions, idempotency keys checked in the fakes, the e2e `event_type` from the event
type, ConsumerOption instead of a fake service, the transition log only on an actual
fact. Accepted without changes: duplication of small helpers (the platform pattern),
`provider` until 10.4 (KI-payout-11), configuration without an env override of the
consumer group (0.7), testcontainers 0.34.0 (the unified set, govulncheck - 0.2).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | MAJOR: provider is not filled (CF-2 without the field), amounts in logs, Internal for ErrLedgerUnavailable, merchant_id incompatible with the plan; MINOR: continuation/approved events/Reserve-after-crash without tests, confirmed without a reserve, fee decimals, byte truncation, a tautological assert, the e2e event_type, AGENTS, traceparent; NIT: a fake service for options, the command clock, the transition log, duplicated helpers | KI-payout-11 + the spec; the log cleared; FailedPrecondition; the format aligned; tests added; the guard added; validation; truncation by runes; fixed; event_type fixed; AGENTS; recorded; ConsumerOption; accepted; fixed; accepted |
| Architecture | BLOCKER: request_hash collisions; MAJOR: hanging requested/approved (continuation/replay), Ledger in the Guard transaction without a recorded decision, merchant_id vs the plan, provider, AGENTS/KI-02; MINOR: continuation in the request path, byte truncation, amounts in a log, unknown enum -> DLQ, the Redis tier, a testcontainers downgrade; NIT: ConsumerOption, readyz, NotFound masking, the outbox order | length-prefix + a test; replay resumption + continuation on a repeat + WithoutCancel/timeout; T-PAYOUT-08; the format aligned; KI-payout-11; AGENTS + KI-02; its own timeout; by runes; the log cleared; WARN/ack as 4.3; recorded (the Redis fast path - 7.4); accepted; ConsumerOption; KI until 0.7; as 4.3; T-PAYOUT-08 |
| With context | BLOCKER: AGENTS.md/the spec is not closed; MAJOR: merchant_id, request_hash, amounts in a log, replay without Advance, provider; MINOR: classify 0%, continuation without a test, concurrent branches without tests, the e2e submitted, gRPC codes, the consumer without Ledger, compose without LEDGER_ADDR, duplications, asserts, dead code, the KI reference, the log, timeout; NIT: ConsumerOption, reason sanitization, go.mod | AGENTS/the spec closed; fixed; tests; continuation tests; fake conflicts; the e2e submitted; Unavailable; WARN; recorded; accepted; fixed; `_ =` removed; the reference; the log; 12m; ConsumerOption; canonicalization; aligned with the set |
| Security | BLOCKER: forging gateway.payout.events moves money without binding to the command; MAJOR: amounts/Ledger texts in logs and events, control characters/UTF-8, a commit/release race without a lock; MINOR: traceparent in a log, hash collisions, merchant_id, confirmed without a reserve, a business refusal as retry, the continuation ctx, provider, versions, codes; NIT: the outbox payload (KI-04), UNSPECIFIED, fee without an upper bound, reflection, fakes | gateway_payout_id binding + the verdict-before-submitted DLQ + the row lock; canonical reasons, amounts removed; control/UTF-8 validation, truncation by runes; locking read; recorded; length-prefix; aligned; the guard; recorded in the runbook; WithoutCancel/timeout; KI-payout-11; the unified set, govulncheck in 0.2; FailedPrecondition; KI-04; accepted (4.3); accepted; KI-08; tests added |
