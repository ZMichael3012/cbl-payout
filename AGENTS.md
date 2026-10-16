# AGENTS.md - cbl-payout

Payout and refund saga with compensations and two-step confirmation.

Mandatory global rules: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, contracts `../../.specs/CBL-003-contracts.md`.
The em dash (U+2014) is forbidden, only the hyphen is used.

## 1. Boundaries

In scope: the payout state machine, reserves, payout.commands, compensations, refunds.

Out of scope: other platform services; interaction only via `cbl-protos` contracts
(gRPC), Kafka and the perimeter REST (see `CBL-000`, section 2).

## 2. Contracts

- REST: administrative /healthz, /readyz
- gRPC: PayoutService server (CreatePayout/GetPayout - 10.2, ApprovePayout - 10.5;
  CreatePayout accepts refund_invoice_id - refund, 10.6)
- Kafka produces: payout.commands, payout.events (via outbox, 10.2;
  refund events carry refund_invoice_id, 10.6; operator veto - rejected, cf-3)
- Kafka consumes: gateway.payout.events (group `cbl-payout-gateway`; ledger.events - 10.4/10.7)
- Storage: PostgreSQL payout (payouts, payout_transitions, payout_approvals, outbox, inbox)

Contracts are frozen in `cbl-protos`: CF-1 gRPC, CF-2 events + additive refund
fields `cbl-protos@cf-2.1` (CBL-10.6); see `CBL-004`.

## 3. Architecture

Layers: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Libraries: the standard `CBL-002` set (Gin, GORM + pgx driver, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` is forbidden, migrations are goose only, forward only.
Schema (10.1): `payouts` (saga state, `state_version`, amounts, recipient details,
hold `reserve_id`), `payout_transitions` (append-only journal), `outbox`, `inbox`.
Approvals (10.5): `payout_approvals` (append-only operator votes, unique
`(payout_id, operator_id)`), threshold policy - `internal/domain/approval.go`,
service - `internal/usecase/approval.go` (locking read + CAS + rollback of a
conflicting attempt), transport - `internal/transport/payout_grpc.go`.
Saga (10.2): `internal/usecase/payout.go` (Create/Get/Advance: Reserve Ledger ->
reserved -> outbox payout.commands -> sent_to_gateway; failed compensation ->
compensated), `internal/usecase/payout_events.go` (payout.events and
payout.commands envelopes), `internal/usecase/gateway_payout_consumer.go` (submitted/
confirmed/failed, row lock and binding of the verdict to gateway_payout_id),
`internal/ledgerclient/ledger.go` (gRPC Ledger, 1 s deadline), hold account -
`internal/domain/account_plan.go` (UUIDv5 of the account plan, golden vectors).
Refunds (10.6): `payouts.refund_invoice_id` (migration 00005: canonical UUID CHECK,
partial index, immutability by the second barrier), canonicalization and inclusion
in `request_hash` only when the value is non-empty, the marker in every `payout.events`.
The transition matrix - `internal/domain/state.go`, duplicated by the DB trigger
(`payouts_transition_guard`); transitions only via `storage.Repository.Transition`/
`TransitionTx` with CAS on `state_version`. `Create` forcibly sets `requested`/1 and
clears saga data (the second barrier - `payouts_insert_guard`).
main applies migrations (`MIGRATIONS_DIR`) and configures the DB pool (default 20,
ceiling 100, lifetime 30 minutes).

## 4. Commands

```powershell
make build        # go build ./...
make test         # go test ./...
make test-integration  # integration (testcontainers: PostgreSQL)
make test-e2e     # end-to-end saga on testcontainers: Kafka+PostgreSQL, fake Ledger
make vet          # go vet ./...
make lint         # golangci-lint in Docker (context services/)
make up           # docker compose -f deploy/docker-compose.yml --profile app up -d --build
make down         # docker compose -f deploy/docker-compose.yml down -v
```

`make test-e2e` is the end-to-end saga (Kafka+PostgreSQL on testcontainers, fake
Ledger); compose stands do not run the saga: they have no Ledger (KI-payout-10).
Compose stand e2e runs follow the `test/e2e/E2E.md` instruction (scenarios
E2E_Payout_Schema and E2E_Payout_Approval; artifacts in `test/e2e/.runs/<mark>/`);
there are no PowerShell run scripts.

## 5. Ports and environment

HTTP `8080`, gRPC `9080`, metrics `9280`.
Variables (applicable to the service): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`
(mandatory), `MIGRATIONS_DIR` (default `./migrations`), `DB_MAX_OPEN_CONNS`
(default 20, ceiling 100), `DB_MAX_IDLE_CONNS` (default 10),
`PAYOUT_APPROVAL_POLICY` (approval tier JSON, see 10.5: fail-safe default 2
operators, an invalid value refuses startup; changed only by a restart),
`LEDGER_ADDR` (gRPC Ledger for the saga; empty - the saga step is unavailable, WARN),
`KAFKA_BROKERS` (outbox relay and consumer gateway.payout.events; empty - WARN);
`SCHEMA_REGISTRY_URL`, `REDIS_ADDR` - 10.4/0.5.

## 6. Development rules

- Pipeline: SDD spec in `.specs/` -> architecture -> contracts -> tests first -> code ->
  review by 4 agents (no-context, architecture, with-context, security - blocking) ->
  AGENTS.md -> final runs -> linters -> commit.
- Unit+integration coverage is at least 80%; money scenarios only on real PostgreSQL and
  Kafka (testcontainers); local e2e in docker-compose is mandatory.
- Money is only int64 in minor units plus decimals; float is forbidden.
- Godoc docstrings are mandatory in English: the first line starts with the entity name and
  ends with a period; comments explain why; errors are wrapped with `%w`; panics in business
  code are forbidden.
- Commit: `CBL-<epic>.<number>: <short message>`, the spec with the same prefix in the same
  commit.

## 7. SLO and metrics

The SLO is refined in task 14.4. Base metrics: latency p95/p99, error rate, outbox lag,
consumer lag; dashboards after task 0.7.

## 8. Trade-offs

| ID | Decision | Pros | Cons and compensation |
| :-- | :-- | :-- | :-- |
| T-PAYOUT-01 | Transition matrix in the domain plus DB triggers (the second barrier): BEFORE INSERT fixes `requested`/version 1 and empty saga data, BEFORE UPDATE checks pairs, increments `state_version`, immutable fields (amounts, recipient details, `reserve_id` after fixation); the journal is append-only | bypassing the state machine, substituting the address/amount and double compensation are impossible; the audit is complete | the matrix is duplicated in Go and SQL; a direct DML role can append to the journal bypassing the application (modification and deletion are forbidden) | the matrix is mirrored by exhaustive enumeration of pairs in an integration test; direct DML is outside the dev stand threat model (prod roles - KI-payout-03) |
| T-PAYOUT-02 | Optimistic transitions: a read under a row lock and CAS on `state_version`; the tx variant `TransitionTx` writes the transition and the outbox row in one transaction | a correct history, no global locks; saga 10.2 and consumer 10.4 are atomic with the events | under contention one of the transitions gets `ErrStateConflict` and is retried | retries in the saga/consumer; a metric after 0.7 |
| T-PAYOUT-03 | `failed` is not terminal: the only outgoing transition is `failed -> compensated`; compensation is a release of the hold in Ledger (without postings, T-LED-06) or closing the saga without release if there was no reserve | the payout has a single final compensation status, the hold does not hang; the release does not create a fictitious movement | the saga must complete the compensation after a restart; the compensation queue is the partial saga index (includes `failed`) | background saga recovery - 10.4/10.7; retention/KI |
| T-PAYOUT-04 | Internal saga steps (`reserved`, `sent_to_gateway`) are not published in `payout.events`: events carry business milestones (`requested/approved/completed/failed/compensated/rejected`); an operator veto (10.5) emits `rejected` in the transition transaction (the type was added at CF-3, 15.10.2026) | the contract is stable, consumers (Notification 8.x, ClickHouse 11.x) see business milestones rather than internal steps; an operator rejection is delivered to the merchant as `payout.failed` with the status `rejected` | two events fewer per lifecycle (reserved/sent_to_gateway are absent) - reconciliation waits for the actual transitions in the journal | the `rejected` type is frozen by the tag `cbl-protos@cf-3`; an integration test for the emission |
| T-PAYOUT-05 | Saga data is filled only by transitions (`reserve_id`, `gateway_payout_id`, `provider`, `txid`, `failure_reason`, `completed_at`, `fee_*`); `Create` clears the passed values, the INSERT guard forbids them | a silent substitution of the saga context before approval is impossible | the guardian logic is duplicated in the repository and the trigger | tests: an INSERT with ready fields is rejected; `Create` with pre-filled fields clears them |
| T-PAYOUT-06 | Two-step approval = two different operators (uniqueness of `(payout_id, operator_id)`), the operator's decision is immutable (append-only), a veto by one is enough; `required` in a vote is a policy snapshot, the decision follows the current policy | the two-person rule is structural (a repeat is idempotent, a bypass by changing a decision is impossible); the vote audit is complete | an operator cannot change their mind after voting (append-only); approval progress is not visible in CF-1 `Payout` (12.2); a policy downgrade between votes completes the payout on a repeat | the operator registry and RBAC - 12.1/12.2; the read contour of votes - 12.2; a downward drift must account for the 10.2 gate (re-gate `requested`) |
| T-PAYOUT-07 | Approval threshold policy (10.5) with fail-safe: without configuration - two operators, a zero value of the struct does not "switch off" approval; strict JSON parsing (mandatory fields, `null`/unknown fields forbidden) and refusal to start on a typo | a typo in the configuration does not disable manual payout confirmation; configurability without a release | rigidity: any format change is through a restart and the `Describe` log; `default:0` (legal) requires a deliberate operator decision | the startup log `payout approval policy configured`; an alert on `default=0` on duty (runbook); thresholds by USD equivalent are manual, as in 4.5 |
| T-PAYOUT-08 | Saga (10.2): the order "external idempotent Ledger operation -> CAS transition"; in the consumer the external call is performed inside the Guard transaction, the payout is read under a row lock; the gateway verdict must carry a `gateway_payout_id` matching the fixed `submitted` | terminalization does not outrun the external step; concurrent confirmed/failed are serialized (commit and release do not interleave); repeats are safe (reserve/commit/release keys) | the Guard transaction is held for up to 1 s per Ledger call (up to ~3 s with retries), the consumer throughput is limited by Ledger; a verdict before `submitted` goes to the DLQ (a lost submitted - manual investigation, KI-payout-10) | the command emission and the transition to `sent_to_gateway` in one transaction; plan retries up to 3; a DLQ alert in the runbook; under growing load - a two-phase scheme as in kit T-KK5 |
| T-PAYOUT-09 | A refund (10.6) is the `refund_invoice_id` field, not a separate type: the state machine and saga are shared (CBL-000: "a special case of a payout"); Notification distinguishes `refund.*` by the marker | no new states/branches, shared guarantees of idempotency and compensation; part of the contract (additive fields, `cf-2.1`) | Payout does not check the existence/owner of the account or the balance (no data - database-per-service): balance aggregation and ownership are mandatory in Billing API 7.x; an arbitrary marker with an unauthenticated CreatePayout - KI-payout-02 | the registry/balance check - 7.x (spec 7.4); `refund.*` types - 8.x; the marker is immutable by the second barrier; UUID canonicalization and the link to request_hash |

## 9. Known issues

| ID | Problem | Impact | Plan | Status |
| :-- | :-- | :-- | :-- | :-- |
| KI-payout-01 | CI and pre-commit are not configured | linters are run manually | task 0.2 | open |
| KI-payout-02 | No mTLS and authN on the gRPC PayoutService (9080) and topics: since 10.2 `CreatePayout` of any `merchant_id` creates a payout and reserves another merchant's funds (Reserve in Ledger), `GetPayout` returns the full Payout; since 10.6 `CreatePayout` with an arbitrary `refund_invoice_id` initiates `refund.*` notifications to a merchant with someone else's/fabricated account, and `GetPayout` discloses someone else's marker (Payout does not check the account's existence and owner - an obligation of 7.x); `ApprovePayout` (10.5) with arbitrary `operator_id` gathers a quorum or irreversibly rejects; producing to `gateway.payout.events` allows confirming/compensating someone else's `sent_to_gateway` payout without a real send (binding to `gateway_payout_id` is only the second barrier; a forged `submitted` overrides it); `payout.commands` allows forging commands to the gateway (cross-references KI-ledger-02, KI-payment-04) | prod deployment is forbidden; do not expose gRPC and topics beyond the trusted network | mTLS/authorization 0.5/0.6; ACL: produce to `gateway.payout.events` only Connector, `payout.commands` only cbl-payout; operator RBAC - 12.1/12.2; ownership of `refund_invoice_id` and the account balance - 7.x; incident investigation - the runbook | open |
| KI-payout-09 | CF-1 `ApprovePayoutRequest` has no `idempotency_key`: idempotency relies on the vote row `(payout_id, operator_id)`; when CAS retries are exhausted the client (12.2) must repeat the call, otherwise the vote is silently lost | redelivery is safe for the same operator; network failures require a client retry policy | extending the contract with a key is a question for the CF owner at 12.2; in the runbook: `Aborted` = repeat the call | open |
| KI-payout-10 | No background recovery of stuck sagas: `requested`/`approved` with no movement (a Reserve failure), `failed` without compensation, an "orphan" active reserve after a failure between Reserve and the write transaction, a verdict before `submitted` (DLQ) | the payout or the hold hangs until manual intervention; compose stands have no Ledger - the saga is unavailable | 10.4 (status re-check) and 10.7 (chaos tests/recovery); until then - resuming by repeating Create/Approve and the runbook | open |
| KI-payout-11 | The payout `provider` is not filled: CF-2 `GatewayPayoutEvent` does not carry the provider, `submitted` fixes only `gateway_payout_id`/`txid` | `payouts.provider` and the provider field in `payout.events` are empty (affects Reconciler 9.x/reporting) | filling in 10.4 (status re-check/route) or extending CF-2 by the contract owner | open |
| KI-payout-03 | Runtime and migrations under one DSN: the schema owner can drop the second barrier triggers | in the dev contour the barrier is dropped by the DB owner (like KI-payment-03) | separating the migrator/runtime roles when going to prod (0.4/0.6) | open |
| KI-payout-04 | No retention and partitioning of `payouts`, `payout_transitions`, `outbox` | growth of the DB and backups | task 14.2 (like KI-ledger-09, KI-payment-02) | open |
| KI-payout-05 | The payouts account `account_id` (UUIDv5 of the account plan) in 10.2 is reproduced from golden vectors (KI-ledger-08) | risk of drift with the ledger | moving the derivation to cbl-kit before 10.2/7.4; the vectors are mandatory in tests | open |
| KI-payout-06 | The gateway provider is not chosen (systemic KI-01): `CreatePayout` in Connector (10.3) is not implemented | blocks the end-to-end flow 10.2/10.4/10.7 with a real gateway | mock-gateway/stubs until the provider is chosen | open |
| KI-payout-07 | The race detector is unavailable locally (no CGO on Windows) | concurrency tests without `-race` | CI (0.2) | open |
| KI-payout-08 | gRPC reflection is enabled unconditionally | excessive API exposure in prod | disable in the prod profile at observability 0.7 | open |

## 10. Task journal

| CBL | Date | Status | Spec | Commit |
| :-- | :-- | :-- | :-- | :-- |
| CBL-10.1 | 2026-10-15 | done, review closed (payouts/payout_transitions schema, the state machine with the double barrier and CAS, `reserve_id`, e2e both chains, coverage 91.6%) | `.specs/CBL-10.1-payout-schema.md` | fcd3b8a |
| CBL-10.5 | 2026-10-15 | done, review closed (approval threshold policy with fail-safe, append-only vote journal, two different operators and a veto, gRPC ApprovePayout, e2e grpcurl, coverage 90.5%) | `.specs/CBL-10.5-approvals.md` | 8a6e49a |
| CBL-10.2 | 2026-10-15 | done, review closed (saga: Ledger reserve from golden vectors, payout.commands via outbox, gateway.payout.events consumer with verdict binding, commit/release, compensation, approval continuation, e2e Kafka) | `.specs/CBL-10.2-saga.md` | d5788fc |
| CBL-10.6 | 2026-10-15 | done, review closed (refunds: refund_invoice_id in the cf-2.1 contracts and payout.events, CHECK/index/immutability, canonicalization and hash, refund e2e) | `.specs/CBL-10.6-refunds.md` | 58e7b4d |
| CBL-10.5 (fix) | 2026-10-15 | done (payout.rejected type under the cf-3 contract: an operator veto emits the event in the transition transaction; integration test) | `.specs/CBL-10.5-approvals.md` | d77611b |
| CBL-16.7 | 2026-10-16 | done (English comments and documentation across the repository: code, tests, migrations, configs, all four specs, AGENTS.md, README; e2e runs via `test/e2e/E2E.md` with scenarios E2E_Payout_Schema and E2E_Payout_Approval, `schema.ps1`/`approval.ps1` and `make e2e` removed; validation run PASS; coverage unchanged) | `.specs/CBL-16.7-conventions.md` | this commit |

## 11. Runbook

Payout state machine (10.1):

1. State and history: `SELECT state, state_version, reserve_id, gateway_payout_id
   FROM payouts WHERE id = '...'`; the transition path - `SELECT from_state, to_state,
   state_version, reason, occurred_at FROM payout_transitions WHERE payout_id = '...'
   ORDER BY state_version`.
2. `ErrStateConflict` in the logs: a concurrent transition (saga 10.2 versus consumer
   10.4); the caller repeats the loop with a fresh version read (`Get`/`PayoutTx`).
3. Editing the state directly in the DB is impossible (trigger): the fix is only a
   valid transition through the application; `payout_transitions` is append-only.
4. Stuck payouts (no movement): `SELECT id, state, updated_at FROM payouts
   WHERE state IN ('requested','approved','reserved','sent_to_gateway','failed')
   AND updated_at <= now() - interval '15 minutes' ORDER BY updated_at` - saga
   investigation in 10.4/10.7; an uncompensated `failed` is a priority (the hold).
5. `reserve_id` is fixed at `reserved` and is immutable; the compensation
   (`failed -> compensated`) must use exactly it (release).

Approvals (10.5):

1. The policy is printed at startup (`payout approval policy configured`);
   changing it is only by restart. `default=0` (approval not required) requires
   a deliberate decision: verify against the log and alert on duty.
2. Operator votes: `SELECT operator_id, approve, required, reason, created_at
   FROM payout_approvals WHERE payout_id = '...' ORDER BY id` - the quorum audit;
   `required` - the policy requirements at the moment of the vote. A direct UPDATE/DELETE
   is impossible (append-only); the operator's decision is immutable.
3. The operator gets `FailedPrecondition approval not required` - the amount is below
   the threshold; `FailedPrecondition operator vote is immutable` - an attempt to change
   the decision; `Aborted` - a state conflict, repeat the call (KI-payout-09).
4. A `requested` payout without votes for longer than the SLA - check the policy in the
   startup log and the number of votes; finish with a vote via `ApprovePayout` (Back-office
   12.2); after mTLS (0.5/0.6) verify the operators against the registry (KI-payout-02).

Saga (10.2):

1. Payout state: `SELECT state, state_version, reserve_id, gateway_payout_id,
   provider, failure_reason FROM payouts WHERE id = '...'`; the journal -
   `payout_transitions ORDER BY state_version`. Outbox: unpublished rows
   `SELECT id, topic, headers->>'event_type' FROM outbox WHERE published_at IS
   NULL ORDER BY id` (they accumulate without Kafka, KI-payout-04/02).
2. A verdict went to the DLQ `gateway.payout.events.dlq`: the reason is in `x-error`;
   typical ones - a verdict before `submitted` or someone else's `gateway_payout_id`
   (binding), an invalid fee/rows. Investigation: compare `gateway_payout_id`/`txid`
   with Connector; a replay is possible only after the cause is removed (manual replay,
   10.4).
3. A stuck `requested`/`approved` (no movement): check `LEDGER_ADDR`, the log
   `payout saga continuation failed`; repeating `CreatePayout`/`ApprovePayout`
   with the same key resumes the saga; background recovery - KI-payout-10
   (10.4/10.7). `ErrLedgerUnavailable` - Ledger is unavailable, repeat.
4. `failed` without `compensated` (the hold is not released): a priority investigation;
   the release is idempotent (`payout:{id}:release`), it is completed by repeating the
   event or with the operator procedure 10.7; an "orphan" active reserve in Ledger by
   the payout `ref_id` - KI-payout-10.
5. A suspicious completion/compensation (KI-payout-02): compare
   `gateway_payout_id`/`txid` with Connector; with a confirmed forgery - incident
   investigation (access to the topic/gRPC), money corrections - only a reversal
   (10.x); prod is forbidden until ACL/mTLS (0.5/0.6).

Refunds (10.6):

1. Finding refunds: `SELECT id, state, refund_invoice_id, merchant_ref FROM
   payouts WHERE refund_invoice_id = '<invoice_id>' ORDER BY created_at`
   (partial index `payouts_refund_idx`); aggregation of partial refunds and
   the account balance - Billing API 7.x.
2. A refund to an unknown/someone else's account (KI-payout-02): compare
   `refund_invoice_id` with Billing API and payment records; `refund.*`
   events to the merchant may have been sent - notify 8.x/7.x; money corrections -
   only a reversal after the investigation.
3. The refund marker is immutable; the error "immutable fields cannot change" on
   an edit is expected (direct SQL is forbidden).
