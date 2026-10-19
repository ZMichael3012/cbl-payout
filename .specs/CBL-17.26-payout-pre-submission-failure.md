# CBL-17.26 (cbl-payout part). Pre-submission payout failure compensation

## 1. Context and goal

The 17.26 increment D global E2E run (umbrella `test/e2e/.runs/20261019t130356z/`,
scenario GE-3) found an integration gap between the connector and the saga: the
connector's `PayoutProcessor` publishes a `failed` `GatewayPayoutEvent` WITHOUT a
`gateway_payout_id` when the provider definitively rejects a payout before any
provider payout exists (`publishFailed`: "no mapping is recorded: there is no
submission to poll"). The saga consumer rejected every verdict without a gateway
identifier (`gateway_payout_id is required for verdict`) and every verdict against a
payout without a recorded identifier (`verdict before submitted`), so the event went
through the retry topics into `gateway.payout.events.dlq` and the payout stayed in
`sent_to_gateway` with its hold locked. Stand evidence: the GE-3 run with an injected
400 rejection produced the connector logs `payout rejected by provider` /
`payout failed before submission` and the saga DLQ
`usecase: invalid gateway payout event: gateway_payout_id is required for verdict`.

Goal: accept exactly the connector pre-submission rejection shape (a `failed` verdict
without a gateway identifier against a payout that never recorded one) and compensate
it (release the hold), while keeping every other verdict-before-submitted shape a DLQ
error.

## 2. Scope

In scope: `cbl-payout` consumer validation and compensation
(`internal/usecase/gateway_payout_consumer.go`), the provider backfill on the
compensation path, the triage log field, unit/integration tests, the spec amendments.

Out of scope: the connector producer (its classification is the source of truth and is
unchanged); the connector's `payout.commands.dlq` "no event" class (a persistent
temporary failure leaves the saga in `sent_to_gateway` - KI-payout-10, the status
re-check is 10.4/17.16); an explicit rejection-stage marker in the event contract
(the recorded follow-up if the stand ever leaves the trusted network).

## 3. Architecture of the solution

- `validateGatewayEvent`: the mandatory `gateway_payout_id` applies to a confirmation
  only (`gateway_payout_id is required for confirmation`); a failed verdict may omit
  it (the binding decides).
- `validateVerdictBinding(payout, event, eventType)`: the recorded identifier is
  trimmed; when nothing is recorded the only accepted shape is
  `failed` + empty event identifier; every other shape returns
  `verdict before submitted`; when something is recorded the event identifier must
  match (`gateway_payout_id mismatch`). The provider check is unchanged.
- `compensate`: the provider of a provider-bearing verdict is backfilled on the
  failed transition (mirroring the completion backfill of T-PAYOUT-10); the log line
  gains `pre_submission` (true when the payout had no recorded identifier), so
  incident triage can distinguish the accepted no-id shape from a real gateway
  failure.
- The money path is unchanged: release only when `reserve_id` is set, the fixed key
  `payout:{id}:release`, release before the CAS transition, `failed -> compensated`
  in one plan.

### 3.1. Verdict matrix (payout state `sent_to_gateway`)

| Type | Event id | Recorded id | Outcome |
| :-- | :-- | :-- | :-- |
| failed | empty | empty | compensate + release (the accepted pre-submission shape) |
| failed | empty | set | DLQ `gateway_payout_id mismatch` |
| failed | set | empty | DLQ `verdict before submitted` |
| failed | set | set, equal | compensate |
| failed | set | set, different | DLQ `gateway_payout_id mismatch` |
| confirmed | empty | any | DLQ `gateway_payout_id is required for confirmation` |
| confirmed | set | empty | DLQ `verdict before submitted` |
| confirmed | set | set, equal | complete (commit) |
| confirmed | set | set, different | DLQ `gateway_payout_id mismatch` |
| submitted | any | any | self-transition patch (unchanged, no binding) |

## 4. Functional requirements

- FR-1: a failed verdict without a gateway identifier against a payout without a
  recorded identifier compensates the payout (release when a reserve exists).
- FR-2: every other verdict shape before a recorded submission stays a DLQ error and
  never calls Ledger.
- FR-3: a confirmation still requires a matching non-empty identifier.
- FR-4: the compensation backfills the provider of a provider-bearing verdict when
  the payout has none.
- FR-5: the compensation log carries `pre_submission`.

## 5. Contracts

No contract changes: `gateway.payout.events` (CF-2) already carries the failed type
without a mandatory identifier; the connector producer is unchanged. The cross-service
semantics are recorded in AGENTS.md T-PAYOUT-08 and KI-payout-10.

## 6. Data model and migrations

None: the change touches validation and the existing transition patch fields
(`provider`).

## 7. State machines and rules

The transition matrix is unchanged (`sent_to_gateway -> failed -> compensated`); the
verdict-binding rule is relaxed by exactly one cell (section 3.1).

## 8. Edge cases

- A failed verdict with a fabricated identifier against a never-submitted payout: DLQ
  (the second half of the exception).
- A failed verdict without an identifier against a submitted payout: DLQ (a forged or
  duplicated pre-submission failure cannot kill a submitted payout).
- A pre-submission failed verdict with an empty provider (the connector's "no provider
  supports the payout" and refund-mapping failures): accepted and compensated.
- An early state (`requested`/`approved`/`reserved`) with the no-id failed shape: too
  early (retry) - unreachable with a real connector (the command emission and the
  `sent_to_gateway` transition share one transaction).
- Residual ambiguity (recorded, not mitigated): between the optimistic
  `sent_to_gateway` and the recorded submission the payout may be in flight at the
  provider (a connector crash window, T-CONNECTOR-19); the no-id shape is a
  trusted-connector statement of a definitive rejection, and the connector must never
  emit it for an ambiguous outcome. The forged-producer exposure is the pre-existing
  KI-payout-02 (the barrier is one message weaker, not a new capability).

## 9. Non-functional requirements

Coverage at least 80% (the repo metric with `-coverpkg`, baseline 83.2%); the fix
must not regress it; money scenarios run on real PostgreSQL (integration).

## 10. Test plan

### 10.1. Unit matrix

| ID | Case | Expected |
| :-- | :-- | :-- |
| U-1 | failed + empty id + empty recorded (provider set) | compensated, one release with the fixed key, failed+compensated transitions, reason stored, provider backfilled |
| U-2 | failed + empty id + empty recorded + empty provider | compensated (the connector's no-provider shape) |
| U-3 | failed + id + empty recorded | `verdict before submitted`, no release |
| U-4 | failed + empty id + recorded | `gateway_payout_id mismatch`, no release |
| U-5 | confirmed + empty id | `gateway_payout_id is required for confirmation` |
| U-6 | confirmed + id + empty recorded (pre-existing) | `verdict before submitted`, no commit |

### 10.2. Integration matrix (PostgreSQL)

| ID | Case | Expected |
| :-- | :-- | :-- |
| I-1 | create -> no-id failed verdict | compensated on real PostgreSQL, release recorded, 5 transitions, outbox `payout.requested`/`payout.failed`/`payout.compensated`, provider backfilled |
| I-2 | create -> submitted -> id failed verdict (pre-existing) | compensated, 6 transitions (regression) |

### 10.3. Smoke set

The existing saga e2e (`make test-e2e`, testcontainers Kafka+PostgreSQL) stays green.

### 10.4. E2E scenario description

Global stand (umbrella GE-3): a payout command rejected by the provider (the mock
injected 400) produces the connector `failed` event without a gateway identifier; the
saga compensates (release) and the ledger shows the hold released; the stand
continuation is recorded in the umbrella run directory.

### 10.5. Test-first evidence

Red run before the implementation (parent `9be477f`): the new unit test
`TestGatewayConsumerPreSubmissionFailedCompensates` fails with
`usecase: invalid gateway payout event: gateway_payout_id is required for verdict`
(the exact production error from the GE-3 stand run); the transcript is kept in the
task session.

### 10.6. Review closure (19.10.2026)

Four review passes on the staged diff; findings closed before the commit:

- the spec/AGENTS contradiction (BLOCKING): this spec plus the CBL-10.2 FR-4/review
  amendments and the AGENTS T-PAYOUT-08/KI-payout-10/runbook/journal updates;
- the security review accepted the residual in-flight ambiguity for the trusted stand
  with the recorded trade-off (T-PAYOUT-08) and the connector-side guarantee;
- the missing matrix cell (failed + id + no recorded id) and the empty-provider case
  added; the integration test on real PostgreSQL added;
- the compensation backfills the provider (mirroring completion) and the log carries
  `pre_submission`; the recorded identifier is trimmed; the error wording and the
  godoc/comment updates.

## 11. Work plan and commit

1. This spec and the CBL-10.2/AGENTS amendments.
2. Red tests (10.1 U-1/U-2/U-3 + 10.2 I-1) on the parent commit; red evidence
   recorded.
3. The implementation (section 3).
4. Green runs: unit, integration (PostgreSQL), the saga e2e, coverage, lint, the em
   dash check.
5. Four review passes; findings closed.
6. Commit `CBL-17.26: ...` with this spec; the umbrella run continuation (GE-3) is
   recorded in the umbrella repository.

## 12. Open questions and decisions

- An explicit rejection-stage marker in `GatewayPayoutEvent` (or the command/attempt
  id) would remove the inference from a missing field; recorded as the follow-up if
  the stand leaves the trusted network (the forgery barrier relies on topic ACLs,
  KI-payout-02). Not required for the stand.
