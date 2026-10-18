# CBL-17.11. Payout provider propagation

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-payout` |
| Size | S |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 10.2 (saga and gateway consumer), 17.11 connector increment (submitted events carry the provider), CF-4 |
| Contract version | none (`GatewayPayoutEvent.provider` and `PayoutEvent.provider` already exist; no `.proto` change) |
| Status | in progress |

## 1. Context and goal

The connector payout pipeline (17.11, `cbl-connector@c4408eb`) publishes
`gateway.payout.events` with the provider field (CF-4). The payout consumer
ignores it, so `payouts.provider` and the provider in `payout.events` stay empty
(KI-payout-11). This increment closes the KI: the submitted event records the
provider, the verdict provider is bound to it, and the business `payout.events`
carry the provider for the Reconciler and reporting.

## 2. Scope

In scope: provider validation and persistence in the gateway consumer, verdict
provider binding and legacy backfill, the provider immutability DB barrier
(migration 00006), unit/integration/e2e tests, docs.

Out of scope: provider-specific payout confirmations (17.16); provider routing
(17.17); the connector pipeline (done).

## 3. Architecture sketch

`internal/usecase/gateway_payout_consumer.go`:

- `submittedPatch` validates the event provider (lowercase `^[a-z0-9][a-z0-9_-]{1,31}$`,
  empty tolerated for legacy producers with a WARN) and sets
  `storage.TransitionPatch.Provider`.
- `validateVerdictBinding` rejects a verdict whose provider contradicts the
  recorded `payouts.provider`.
- The completion patch backfills the provider for a legacy payout (recorded
  value empty) so the completed `payout.events` event carries it.
- `payout_events.go` already renders `payout.Provider` into the payload.

Migration 00006 replaces the transition guard with the added rule: once
`provider` is set it cannot change (the second barrier).

## 4. Functional requirements

FR-1. A submitted event with a valid provider stores it; a legacy submitted
without the provider is accepted with a WARN and keeps the column NULL.

FR-2. A verdict (confirmed/failed) with a provider different from the recorded
one is an invalid event (DLQ); an empty verdict provider is tolerated.

FR-3. A legacy payout completed with a provider-bearing verdict records the
provider, and the completed `payout.events` payload carries it.

FR-4. The DB guard rejects changing `provider` once set.

## 5. Contracts

No `.proto` change: CF-2 `GatewayPayoutEvent.provider = 7` and
`PayoutEvent.provider = 7` are consumed and populated.

## 6. Data model and migrations

Migration 00006 recreates `payouts_transition_guard` with the provider
immutability rule; forward only, Down restores the 10.1 body.

## 7. State machines and rules

The transition matrix is unchanged; the provider is saga data fixed at
`sent_to_gateway` (submitted) and never changes afterwards.

## 8. Edge cases

E-1. Invalid provider text (uppercase is normalized, control characters or an
    over-long value) -> invalid event.
E-2. Verdict before submitted -> existing DLQ path (no provider binding).
E-3. Provider mismatch on a stale verdict -> invalid event, no transition.
E-4. A legacy payout without a provider stays valid; the next completed verdict
    backfills it.

## 9. Non-functional requirements

- NFR-1. Coverage stays at least 80% unit+integration.
- NFR-2. No new dependencies.
- NFR-3. English only, no em dash, godoc on exported entities.

## 10. Test plan

### 10.1. Unit matrix

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| P-01 | submitted provider | provider `coingate` | stored on the payout |
| P-02 | legacy submitted | no provider | accepted, column NULL |
| P-03 | invalid provider | `Bad Provider!` | invalid event |
| P-04 | verdict mismatch | recorded coingate, verdict bitpay | invalid event, state unchanged |
| P-05 | legacy backfill | recorded NULL, confirmed coingate | completed with provider |

### 10.2. Integration matrix (PostgreSQL)

| ID | Case | Input | Expected |
| :-- | :-- | :-- | :-- |
| I-01 | happy path provider | submitted + confirmed with provider | persisted; `payout.completed` payload carries it |
| I-02 | mismatch | verdict provider differs | error, no transition, no event |
| I-03 | DB barrier | UPDATE provider after set | rejected by the trigger |

### 10.3. Smoke set

`make test`, `make test-integration`, `make test-e2e`, `make vet`.

### 10.4. E2E scenario descriptions

| Scenario | Description | Tag or stand |
| :-- | :-- | :-- |
| E2E_Payout_ProviderPropagation (extended saga e2e) | testcontainers Kafka/PostgreSQL: the submitted event carries the provider, the completed payout stores it, `payout.events` completed carries it, and a verdict with another provider is rejected (DLQ) | `go test -tags e2e ./test/e2e/` |

### 10.5. Load plan

Not applicable.

### 10.6. Test-first evidence

Red run recorded before implementation (18.10.2026):
`go test ./internal/usecase/ -run
'TestGatewayConsumerSubmittedRecordsProvider|TestGatewayConsumerVerdictProviderMismatch|TestGatewayConsumerVerdictProviderBackfillsLegacy'`
failed on HEAD with three failures (the provider was ignored, a cross-provider
verdict was accepted and no backfill happened); the integration provider tests
were red as well until migration 00006 landed. After implementation: unit
green; the full battery with integration (`go test -tags integration
-coverpkg=./internal/... ./...`) green, total coverage 83.2%; the saga e2e
(`go test -tags e2e ./test/e2e/`) green with the provider assertions (submitted
records the provider, the completed payout and `payout.events` carry it);
`go vet`, lint and the em dash check pass. Review: the four passes (no-context,
architecture, with-context, security) were performed by the orchestrator in
this run under the KI-06 procedure (the subagent rounds were skipped due to the
session budget); the checks found no findings: FR-1..FR-4 and P-01..P-05 /
I-01..I-03 are realized and tested, the provider is recorded under the same row
lock and Guard transaction, a cross-provider verdict fails closed before any
transition, the DB guard blocks a post-hoc substitution, old tests are
untouched, and the documentation/KI/journal/e2e are consistent.

## 11. Work plan and commits

Single commit: spec, consumer validation and persistence, migration 00006,
tests, e2e extension, AGENTS journal and KI closure.
