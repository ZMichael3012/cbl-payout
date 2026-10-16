# CBL-10.6. Refunds as a special case of a payout

| Field | Value |
| :-- | :-- |
| Epic | E10, Payout/Refund Service (Saga) |
| Repository | `cbl-payout` (contract input - `cbl-protos` 5b3d1e0) |
| Size | M |
| Wave and parallel group | W2-PAYOUT |
| Depends on | 10.2, 10.5 |
| Contract version | `cbl-protos@cf-2.1` (additive refund fields, 5295373) |
| Status | done, review closed |

## 1. Context and goal

A refund is a special case of a payout (`CBL-000`, section 5; `CBL-003`, section 2:
`POST /v1/refunds` with `invoice_id`, `amount`, `destination_address`): the same
saga path (reserve -> command -> result -> commit or compensation), the difference
is the link to the original invoice and the event marking (`refund.completed` in the
merchant notification types, the architecture document, the notifications section).

The payout contracts had no refund field: `CreatePayoutRequest`,
`Payout` and `PayoutEvent` have been extended backward-compatibly (`refund_invoice_id`,
`cbl-protos` 5b3d1e0, buf breaking against CF-2 is green). Payout stores the marker,
emits it in `payout.events`; the amount check (not more than the refundable remainder
of the invoice) and REST `/v1/refunds` - Billing API 7.x (Payout does not own invoices,
database-per-service); the `refund.*` types at the merchant - Notification 8.x.

## 2. Boundaries

In scope: the `refund_invoice_id` migration (CHECK format, immutability by the second
barrier), input validation and canonicalization, inclusion of the marker in `request_hash`,
passing it through to `Payout`/`GetPayout` and `payout.events`, unit/integration/e2e tests.

Out of scope: REST `/v1/refunds` and the invoice balance check (7.x), the `refund.*`
types in Notification (8.x), partial refunds against one invoice by aggregation (7.x),
`invoice_id` in other services (Payment/Quote), reversal postings (10.x).

## 3. Functional requirements

FR-1. Contract (additive, CF-2-compatible): `CreatePayoutRequest.
refund_invoice_id`, `Payout.refund_invoice_id`, `PayoutEvent.refund_invoice_id`
(`cbl-protos` 5b3d1e0). An empty value - a regular payout; a non-empty one - a refund.

FR-2. Validation: a non-empty `refund_invoice_id` is canonicalized as a UUID
(the case and form are normalized to the lowercase canonical form), an invalid value
or a nil UUID and a whitespace string - `ErrInvalidPayoutInput`
(InvalidArgument) before the payout is created; a non-empty marker is included in `request_hash`
(a repeat of the key with a different refund invoice - a body conflict), an empty marker does not
change the hash: an idempotent repeat of payouts created before 10.6 remains valid.

FR-3. Storage: migration 00005 adds `refund_invoice_id text` with a UUID
format CHECK (NULL - a regular payout) and a partial index on the value;
`payouts_transition_guard` is updated (CREATE OR REPLACE): the refund
marker is immutable after creation (on a par with the amounts and the recipient address).

FR-4. Saga: a refund goes through the existing 10.2 path without changes
(`requested -> reserved -> sent_to_gateway -> completed/failed ->
compensated`, idempotency keys, verdict binding); the 10.5 approval
applies under the common threshold policy.

FR-5. Events: every refund event carries `refund_invoice_id`
(requested/approved/completed/failed/compensated); Notification 8.x, by a
non-empty marker, publishes the `refund.*` types instead of `payout.*` (implementation -
8.x); `GetPayout` returns the marker in `Payout` (`CreatePayoutResponse` under
CF-1 carries only the identifier and the state - the marker is known to the caller from
the request).

FR-6. Observability: the creation log distinguishes a refund (without amounts and addresses:
payout_id, refund_invoice_id); the refund maintenance runbook.

## 4. Contracts

- gRPC: `CreatePayoutRequest.refund_invoice_id` (input), `Payout.
  refund_invoice_id` (Create/Get responses), CF-1 + additive fields.
- Kafka: `PayoutEvent.refund_invoice_id` in `payout.events` (CF-2 +
  an additive field); `payout.commands` does not change (the command does not know about a refund).
- Storage: `payouts.refund_invoice_id` (migration 00005).

## 5. Data model and migrations

```sql
ALTER TABLE payouts ADD COLUMN refund_invoice_id text
    CHECK (refund_invoice_id IS NULL OR refund_invoice_id ~ '^[0-9a-f]{8}-...');
CREATE INDEX payouts_refund_idx ON payouts (refund_invoice_id)
    WHERE refund_invoice_id IS NOT NULL;
CREATE OR REPLACE FUNCTION payouts_transition_guard() ... -- + immutability of the marker
```

Forward only; existing payouts get NULL (regular ones).

## 6. State machines and rules

There are no new states or transitions: a refund uses the 10.1 state machine and the 10.2 saga
entirely. The difference is the marker `refund_invoice_id`, immutable by the second barrier.

## 7. Edge cases

An empty marker (a regular payout); an invalid UUID (InvalidArgument before the insert);
the UUID case/form (canonicalized); a repeat of the key with the same body (idempotent),
with a different refund invoice (a conflict); an invalid UUID on a direct INSERT (CHECK);
a marker change by UPDATE (immutable); a refund with approval (a large one - two
operators); a refund failure/compensation (`refund.*` events with the marker);
several partial refunds against one invoice (several payouts with the same
marker - allowed, balance aggregation - 7.x); a refund of an unknown invoice
(Payout does not check the existence of the invoice - the 7.x area, documented).

## 8. Non-functional requirements

The changes are additive and do not affect the performance of the saga; the CHECK and the index
are cheap; canonicalization and validation - before the creation transaction; logs without amounts.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit (usecase) | UUID validation and canonicalization; the marker is included in `request_hash`; a refund goes through Advance; an invalid one - without creation |
| Unit (transport) | passing `refund_invoice_id` from the request and into the `Payout` of the response (Create/Get) |
| Integration | a refund: creation -> the saga up to completed with the marker in the requested/completed events; immutability of the marker (the trigger); the CHECK format on INSERT |
| E2E | Kafka+PostgreSQL: a refund is created, the command is sent, submitted+confirmed -> completed; the completion events carry `refund_invoice_id` |
| Load | 14.1 |

Target unit+integration coverage: at least 80%.

## 10. Work plan and commit

Contracts (`cbl-protos` 5b3d1e0, the spec
`.specs/CBL-10.6-refund-fields.md`), then tests first (usecase, transport,
integration, e2e), migration 00005, storage/usecase/transport, updating
the compose scripts (version 5), AGENTS.md. Commit
`CBL-10.6: refunds as a special case of a payout`; the spec in the same commit.

## 11. Open questions and decisions

- A refund is represented by the `refund_invoice_id` field, not by a separate payout type:
  the state machine and the saga are shared (`CBL-000`: "a special case of a payout"), a separate kind
  would add states and branches to no benefit; Notification distinguishes the types by the
  marker.
- The refund amount check (the invoice balance) - Billing API 7.x: Payout has no
  data about invoices (database-per-service); Payout trusts the amount of the caller within
  the common limits.
- Several partial refunds against one invoice are aggregated on the 7.x side;
  Payout stores the marker and allows several payouts with the same invoice.
- A refund to an arbitrary address - as with a payout (the address is set by the merchant);
  compliance checks - 13.x.
- `refund.*` webhooks to the merchant - 8.x (in CBL-003 section 4
  `refund.completed` and `refund.failed` are recorded; the decision on `refund.compensated` -
  the contract owner at CF-3).
- Payout does not check the existence and the owner of `refund_invoice_id` and the
  invoice balance (no data, database-per-service): with an unauthenticated
  `CreatePayout` a member of the trusted network can initiate `refund.*` with
  an arbitrary invoice (KI-payout-02); the balance and ownership check is mandatory
  in Billing API 7.x, before that - the general prod ban (0.5/0.6).

## 12. Review (CBL-001, step 6)

Four independent passes (15.10.2026). There are no security blockers: the second
barrier body of the trigger is not weakened (immutable fields, reserve_id, the version,
the matrix, the terminal states are identical to 00001, only the immutability of the
marker is added), UUID canonicalization is consistent with the CHECK. BLOCKERs closed:
determinism of the e2e refund step (waiting for the event by payout_id and type
instead of reading the first records of the topic); the `request_hash` regression (an empty marker
no longer changes the hash - a repeat of payouts before 10.6 is idempotent; a golden test of the format).
MAJORs closed: the Down of migration 00005 restores the previous trigger body
(verified on PostgreSQL); the additive contracts are frozen by the tag
`cbl-protos@cf-2.1` and the cbl-protos journal; the specs are aligned (the marker in
`GetPayout`; `CreatePayoutResponse` without `Payout`); the logs distinguish a refund
(`refund_invoice_id`, without amounts); `uuid.Nil` and whitespace strings are rejected;
the transport test checks the passing of the marker into the saga input; the integration test
asserts the CHECK constraint; the e2e scripts use `-ge` against the migration version.
AGENTS.md and CBL-003 are updated in this commit (T-PAYOUT-09, KI-payout-02,
the refund runbook). Accepted without changes: the partial index `payouts_refund_idx`
as groundwork for 7.x/12.x (cold), `CREATE INDEX` without CONCURRENTLY (the stand),
UUIDv7 is not required (any valid UUID is accepted - invoices come from 7.x).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No-context | BLOCKER: the e2e refund step reads the first records of the topic; MAJOR: the Down of 00005 breaks the trigger, request_hash across the upgrade boundary, the marker in CreatePayoutResponse, the FR-6 log, AGENTS, the transport test; MINOR: script comments, the CHECK assert, approval/repeat edge cases, uuid.Nil; NIT: the index, the exact number of records | waitForPayoutEvent by payout_id+type; Down restored; the hash conditional + a golden test; the specs aligned; the log added; AGENTS; a test with input capture; the comments; the constraint assert; nil/whitespace + repeats covered; the groundwork recorded; the predicate |
| Architecture | MAJOR: the contract version tag, CBL-003 not synchronized, request_hash, Down, the e2e race, the invoice balance has nowhere to be checked (7.x); MINOR: the test plan §9, FR-6, refund.failed in CBL-003, Reconciler/Reporting, AGENTS, uuid.Nil/whitespace; NIT: the FR-5 wording, UUID versions, CONCURRENTLY | the cf-2.1 tag released; CBL-003 updated; the hash; Down; waitForPayoutEvent; recorded in §11 (7.x); tests; the log; recorded; notes for 9.x/11.x; AGENTS; fixed; aligned; accepted |
| With-context | BLOCKER: the request_hash regression; MAJOR: the contract tag, AGENTS cbl-payout and cbl-protos, CBL-003, FR-6; MINOR: Down, edge cases, a weakened transport assert, e2e fragility, hardcoded versions; NIT: wording, urn/nil, a duplicate helper | the conditional hash + golden; the cf-2.1 tag + the journal; both AGENTS; CBL-003; the log; Down; tests; input capture; waitForPayoutEvent; `-ge`; aligned; nil rejected; accepted |
| Security | no BLOCKER; MAJOR: the Down of 00005 with a broken trigger, request_hash across the upgrade, KI/runbook for the new risk; MINOR: uuid.Nil, FR-6, the CreatePayout wording, the marker conflict test, form canonicalization; NIT: the script comment, format asymmetry | Down restored; the conditional hash; KI-payout-02 extended + the runbook; nil rejected; the log; aligned; the conflict test (plain vs refund); canonicalization covered (uppercase); the comment; noted in the spec |
