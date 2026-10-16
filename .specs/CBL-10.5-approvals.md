# CBL-10.5. Limits and two-step confirmation of large payouts

| Field | Value |
| :-- | :-- |
| Epic | E10, Payout/Refund Service (Saga) |
| Repository | `cbl-payout` |
| Size | M |
| Wave and parallel group | W2-PAYOUT |
| Depends on | 10.1 |
| Contract version | `cbl-protos@cf-2` (`billing/payout/v1` ApprovePayout, CF-1) |
| Status | done, review closed |

## 1. Context and goal

Large payouts require manual operator confirmation by amount threshold
(`CBL-000` section 5; source: the architecture document, the admin route
`POST /admin/v1/payouts/{id}/approve` and `/reject`). The task introduces:

- a threshold policy of approval by asset and amount (`PAYOUT_APPROVAL_POLICY`) - below
  the threshold a payout is small and does not require manual confirmation, above it requires
  **two operators** (two-step: the two-person rule, not two saga stages);
- gRPC `ApprovePayout` (CF-1): recording an operator vote and, when the threshold is
  reached, the transition `requested -> approved`; a rejection - `requested -> rejected`;
- the vote journal `payout_approvals` (append-only, audit and idempotency of
  repeats by operator).

The gate in `CreatePayout` (a small payout goes into the saga without approval) is connected in
10.2, where `CreatePayout` itself appears; 10.5 delivers the policy, the approval
service and the RPC.

## 2. Boundaries

In scope: the domain threshold policy (`ApprovalPolicy`), the migration
`payout_approvals`, storage methods (locking read, votes, tx runner), the usecase
`ApprovalService`, the gRPC handler `ApprovePayout` and the `main` wiring, tests
unit/integration/e2e (grpcurl on compose).

Out of scope: `CreatePayout`/`GetPayout` (10.2), starting the saga after approval
(the reserve) - 10.2, the operator button itself in Back-office (12.2), RBAC/audit of
the admin contour (12.1), GetPayout for the operator (10.2).

## 3. Functional requirements

FR-1. The `PAYOUT_APPROVAL_POLICY` policy (JSON): requirement tiers by asset,
`{"USDT":[{"below_minor":1000000,"approvals":0}],"default":2}`. The first tier where
`amount < below_minor` sets the required number of operators; otherwise `default`
(2 by default - fail-safe: without configuration, large payouts do not pass without
two operators). Parsing is strict: `below_minor` and `approvals` are mandatory
(absence, `null`, unknown fields and typos - an error), assets in upper
case 2 - 16 characters, duplicates are rejected, `approvals` 0 - 10, `default`
0 - 10; an empty string - default values. The zero value of the policy
struct ("not configured") is treated as fail-safe 2, an explicit
`"default":0` - a legal operator decision. At a tier boundary
(`amount == below_minor`) the next tier applies. An invalid configuration -
the service does not start (exit 1); the effective policy is logged
(`ApprovalPolicy.Describe`).

FR-2. Migration `payout_approvals` (append-only): `id bigserial`, `payout_id`
(FK), `operator_id` (format `^[A-Za-z0-9._:@-]{1,128}$` - the operator identity
contract 12.1), `approve boolean`, `reason` (up to 255, without control characters),
`required` (1 - 10, audit of the requirements at the moment of the vote; 0 - "approval not required" -
the vote is not written), `created_at`; uniqueness of `(payout_id, operator_id)` -
a repeated vote by the same operator is idempotent and is not counted twice, the prefix
of the unique index covers queries by `payout_id`; append-only triggers
(UPDATE/DELETE/TRUNCATE).

FR-3. `ApprovePayout` (usecase `ApprovalService.Approve`): input - payout_id,
operator_id, approve, reason. Conditions (the order of checks is fixed):
- payout not found - `storage.ErrNotFound`;
- the operator is outside the format/limits, a reason with control characters or
  a rejection without a reason - `ErrInvalidApprovalInput`;
- a repeat by the operator with a different decision - `ErrVoteLocked` (the decision is immutable,
  append-only; a retroactive veto is impossible);
- the state is not `requested`: a repeat by an operator who has already voted - an idempotent
  success (the current state), otherwise `ErrPayoutDecided` (the state check
  is more important than the threshold: a decided payout responds "already decided");
- state `requested`, the required number 0 - `ErrApprovalNotRequired` (the vote is
  not written);
- `approve=false` - a vote and the transition `requested -> rejected` (reason in
  `failure_reason` and the transition journal); a single rejection is enough;
- `approve=true` - a vote; when `required` distinct operators are gathered, the payout
  transitions `requested -> approved`; a repeat with the same decision recalculates
  the threshold under the current policy (a decrease of `required` between votes completes
  the payout), but does not change the state by itself.

FR-4. Transactionality and concurrency: the vote and the transition (when the threshold is reached) - one
transaction; before counting votes the payout is read under a row lock
(`PayoutForUpdate`); the transition - `TransitionTx` with CAS; a version conflict rolls back
the attempt entirely (the vote is not committed) and is retried up to 3 times, then an error
appears (to the transport - `Aborted`). Two simultaneous votes by different operators produce
exactly one transition to `approved` at `required=2`. The lock order
(`payouts` -> `payout_approvals`) is fixed for the future outbox emission 10.2.

FR-5. gRPC `ApprovePayout` (transport): error mapping - `ErrNotFound` ->
`NotFound`, `ErrInvalidApprovalInput` -> `InvalidArgument`, `ErrPayoutDecided`/
`ErrApprovalNotRequired`/`ErrVoteLocked` -> `FailedPrecondition`,
`context.Canceled`/`DeadlineExceeded` -> the codes of the same names, conflicts after
retries -> `Aborted`, anything else -> `Internal`; success returns `Payout` (CF-1).
`CreatePayout`/`GetPayout` respond with `Unimplemented` until 10.2.

FR-6. `main` wiring: the repository, `ApprovalService` and registration of
`PayoutService` on the gRPC server; parsing and logging of the policy at startup; fatal
startup errors (no DSN, migrations, configuration, port) terminate the process with
code 1 (`fatal`) - the orchestrator sees the failure.

## 4. Contracts

- gRPC: `billing.payout.v1.PayoutService.ApprovePayout`
  (`ApprovePayoutRequest{payout_id, operator_id, approve, reason}`,
  CF-1, `cbl-protos@cf-2`); `Payout.state`, `state_version`, `failure_reason` -
  as in 10.1.
- HTTP perimeter (`/admin/v1/payouts/{id}/approve`) - Back-office 12.2 via
  this RPC; 10.5 does not bring up the perimeter.
- The `payout.events` event `payout.approved` from the transition - 10.2 (the outbox emission
  will appear together with the saga; the current task writes transitions to the journal).
- Environment variable: `PAYOUT_APPROVAL_POLICY` (JSON, see FR-1).

## 5. Data model and migrations

Migration `00004_payout_approvals.sql` (forward only):

```sql
payout_approvals(id bigserial, payout_id uuid REFERENCES payouts(id),
                 operator_id text, approve boolean, reason text,
                 required int, created_at timestamptz,
                 UNIQUE (payout_id, operator_id))
```

Plus append-only triggers (the 10.1 pattern). Existing data is not
rewritten: `requested` payouts without votes remain waiting for approval.

## 6. State machines and rules

There are no new states: the `requested -> approved | rejected` pair from the
10.1 matrix is used. `rejected` is terminal (a single rejection); `approved` is the entry of the 10.2 saga.
Two-step is the two-operator rule (`required >= 2`), not an intermediate
state: while the number of votes is less than required, the payout remains `requested`.

## 7. Edge cases

A vote without a preceding creation (not found); a repeated vote by the same
operator with the same decision (idempotent, one row; the threshold is recalculated
under the current policy); a repeat with a different decision (the decision is immutable -
`ErrVoteLocked`, a retroactive veto is impossible); a vote after the decision
(idempotent for the voter, `ErrPayoutDecided` otherwise, including at the
current threshold 0); a rejection after one approval (by different operators -
rejected; a repeated rejection by the same one - idempotent); gathering `required` from
one operator repeatedly (not counted); `required=0` (the vote is rejected,
the state does not change); an empty/overlong/non-printable `operator_id`,
a reason longer than 255 or with control characters, a rejection without a reason;
concurrent votes (row lock: exactly one transition at the threshold); a race of two
calls by one operator (unique + decision comparison); a race with the saga after
`approved` (a repeated vote - an idempotent success by the vote row); the policy
changes between votes (in `required` the value at the moment of the vote is fixed,
the decision - by the current policy); the payout is terminal (completed/compensated) before
the vote - `ErrPayoutDecided`; a CAS conflict rolls back the attempt entirely (the vote is
not committed); a conflict after 3 attempts - `Aborted` (client retry).

## 8. Non-functional requirements

The vote transaction: locking read + vote insert + a possible transition; there are no external
calls; the number of writes per call - 6 - 8; the ApprovePayout deadline (CF-1, 2 s)
is met; logs: Debug on a recorded vote (payout_id, operator_id,
approve, required), Info on a transition (state, version), without amounts and addresses.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit (domain) | `ParseApprovalPolicy`: default values, tiers and `default`, tier boundaries, duplicates, `null` (including `default`/`approvals`), missing fields, typos and unknown fields, invalid JSON, approvals boundaries 0/10, asset case; `RequiredApprovals`; the zero value of the policy (fail-safe 2) versus an explicit `default:0`; `Describe` |
| Unit (usecase/transport) | input validation (operator format, reason, rejection without a reason); idempotent repeat; decision change - `ErrVoteLocked`; threshold recalculation on a repeat; the state is more important than the threshold; error mapping (including Canceled/DeadlineExceeded/VoteLocked) and argument passing; the `Payout` response; `Unimplemented` CreatePayout/GetPayout |
| Integration | two votes by different operators -> approved (2 rows, the journal); a repeated vote -> one row; a decision change - `ErrVoteLocked` (the decision is immutable); a vote after the decision (an idempotent success / `ErrPayoutDecided`), including after a saga step; a rejection -> rejected with the reason; `required=0` -> ErrApprovalNotRequired without rows; not found; concurrent votes -> exactly one transition; the append-only journal; CHECKs on operator_id/reason/required on a real DB; vote rollback on a CAS conflict in the transaction (storage) |
| Local E2E | compose + grpcurl (`test/e2e/E2E.md`, scenario E2E_Payout_Approval, compose stand): a vote by op-1 on a large payout -> remains requested, op-2 -> approved; an idempotent repeat does not change the state; a rejection -> rejected; a small payout below the threshold -> FailedPrecondition `approval not required`; UPDATE/TRUNCATE of the vote journal are rejected |
| Load | 14.1: approvals are outside the hot profile |

Target unit+integration coverage: at least 80%.

## 10. Work plan and commit

Tests first (domain, usecase, integration, the grpcurl scenario), then migration
00004, `internal/domain/approval.go`, storage methods, `internal/usecase/approval.go`,
`internal/transport/payout_grpc.go`, the main wiring, AGENTS.md. Commit
`CBL-10.5: limits and two-step confirmation of large payouts`; the spec in the same
commit.

## 11. Open questions and decisions

- Two-step = two different operators (source: the architecture document,
  the admin approval route): the uniqueness of `(payout_id, operator_id)` makes the rule
  structural, a repeated vote is idempotent. A rejection by one operator
  is enough (a veto). The operator's decision is immutable (append-only): a change
  of the decision is rejected with `ErrVoteLocked`, a retroactive veto via a repeat
  is impossible.
- Idempotency of `ApprovePayout` without an `idempotency_key` in CF-1: redelivery
  of the call by the same operator - a success by the vote row; the absence of the key
  is recorded in AGENTS (KI-payout-09), extending the contract - a question for
  the CF owner.
- The policy changes between votes: `required` in the vote row - an audit at
  the moment, the transition decision is made under the current policy and the number of rows;
  a repeated vote recalculates the threshold, therefore a decrease of `required` completes
  the payout without a new operator.
- A downward threshold drift after creation (a large payout became small): a vote returns
  `ErrApprovalNotRequired`, the saga does not pick up such a `requested` payout -
  it is the obligation of 10.2 to re-gate `requested` under the current policy at creation
  and/or in background recovery (10.2/10.7).
- The gate in `CreatePayout` (a small amount without approval, a large one - into waiting) and
  the resumption of the saga after `approved` - 10.2; 10.5 does not bring up the saga.
- `rejected` has no event in CF-2 (`payout.events` types without rejected):
  a Notification about an operator rejection - a question for 12.2/8.x (recorded in 10.1,
  T-PAYOUT-04). Closed at CF-3 (15.10.2026): the type `PAYOUT_EVENT_TYPE_REJECTED`
  has been added to `payout.events`, a veto emits `payout.rejected` in the transition
  transaction (the integration test
  `TestIntegrationRejectionEmitsPayoutEvent`), the webhook - `payout.failed` with the
  status `rejected` (8.x).
- Operator RBAC (who is entitled to approve) - 12.1/12.2; here only
  the format and uniqueness of the operator identifier are checked. Before mTLS/authN (0.5/0.6)
  any member of the trusted network can gather a quorum with arbitrary
  operator_id values (KI-payout-02).
- Back-office (12.2) will not see approval progress from CF-1 `Payout` (there is no
  vote count): the read contour of votes - a contract extension or a separate RPC
  by the CF owner at 12.2.

## 12. Review (CBL-001, step 6)

Four independent passes (15.10.2026). BLOCKERs closed: the fail-open parsing of the
policy (`null` and a skipped `approvals` silently gave 0 - manual approval
was switched off) is closed by strict parsing on pointers with unknown
fields and `null` forbidden, a fail-safe zero value and regression tests; the veto bypass
(a repeat with a changed decision silently "succeeded") is closed by comparison with the
recorded vote and `ErrVoteLocked`. MAJORs closed: the order of checks
(the state is more important than the threshold, `required=0` only for `requested`); a repeat with the same
decision recalculates the threshold (a policy downgrade completes the payout);
KI-payout-02 is extended (ApprovePayout without authN: a quorum with arbitrary
operator_id values and an irreversible veto - a live barrier from 10.5), KI-payout-09 added
(no idempotency_key in CF-1), AGENTS.md and the spec updated; fatal startup errors
terminate the process with code 1; rows are validated in Go and the DB (operator_id
regex, reason without control characters, required 1 - 10, a rejection requires
a reason); a conflicting attempt rolls back the vote (a test on a real DB);
the transport codes are refined (Canceled/DeadlineExceeded/VoteLocked), tests for
argument passing and Unimplemented; e2e: grpcurl pinning, `-max-time`,
port waiting, an idempotent repeat, TRUNCATE. Accepted without changes:
duplication of e2e script helpers (a platform pattern), the read RPC of vote
progress (12.2, §11), the absence of a separate index (the prefix of the unique index).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No-context | MAJOR: the order of checks (required before state); a repeat does not recalculate the threshold; MINOR: gaps in integration tests, a `test-e2e`-like wait, grpcurl without a pin, AGENTS, the exit code; NIT: a redundant index, a duplicate package comment, dead clamps, a fail-open zero value, the Debug log text, weak transport assertions | the order fixed; recalculation added; tests added (decision change, saga steps, CHECKs, rollback); sleep and pinning; AGENTS; `fatal`; the index removed; the comment removed; the zero-value guard; the text/assertions |
| Architecture | BLOCKER: silent ignoring of a decision change (a veto bypass); MAJOR: a downward policy drift hangs `requested` (the 10.2 obligation is recorded), KI-02 does not describe approval; MINOR: a fail-open zero value, counting without WHERE approve (the invariant is recorded), the lock order, no approval progress in CF-1 (12.2), rejected without an event, transport codes, the migration (index/required/case), test gaps; NIT: the exit code, the Debug log, e2e duplication, per-service health | `ErrVoteLocked`; §11: the re-gate requirement for 10.2; KI-02 updated; the zero-value guard; the invariant "in requested all votes are approve" + a comment; the lock order in FR-4/AGENTS; the read RPC - 12.2 (§11); rejected - §11/10.2; the codes; the index/required/regex; tests; `fatal`; distinguishing a write from a repeat; accepted; health - 0.7 |
| With-context | MAJOR: AGENTS (step 7), the spec was not closed, a fail-open skipped `approvals`; MINOR: a repeat without recalculation, the zero value, bytes vs characters, the organization of integration tests/vote rollback, the read contour 12.2, weak transport tests, e2e duplication/a version pin; NIT: a comment, Debug text, the index, helper copies, the policy parsing order | AGENTS updated; the spec closed; strict parsing; a repeat recalculates; the guard; synchronized (Go is stricter than the DB, recorded); storage tests; §11; assertions; pinning/max-time/waiting; removed; the index removed; accepted; `fatal` |
| Security | BLOCKER: fail-open of the policy (`null`/a skipped field); MAJOR: the scope of KI-02 (a quorum/a veto/leakage of details without authN), mandatory KI/AGENTS; MINOR: control characters and whitespace operator_id, the GORM log and UUID validation, a decision change, the exit code, `Aborted` without a retry contract, a veto without a reason; NIT: the index, TRUNCATE in e2e, the container name, bytes/characters, `default:0` in the runbook | strict parsing + the zero guard; KI-02 extended + the runbook; regex validation + CHECKs, control forbidden, the reason mandatory; `fatal` + §11 (UUID is validated by 10.2; GORM logging - 0.7); `ErrVoteLocked` + WARN; `Aborted` in the runbook; reject reason required; the index removed; TRUNCATE added; the compose name constant is acknowledged; recorded; the runbook alert `default=0` |
