# CBL-12.2. Additive Payout.approved_by for the administrative contour

| Field | Value |
| :-- | :-- |
| Epic | E12 (task CBL-12.2, the `cbl-payout` part) |
| Repository | `cbl-payout` |
| Size | S |
| Depends on | CBL-10.5 (the append-only vote journal), `cbl-protos@cf-3.9` |
| Status | done, review closed |

## 1. Context and goal

The frozen admin API (`openapi/admin.v1.yaml`, `PayoutAdmin.approved_by`) shows the
approval progress of a large payout. CF-1 `Payout` has no such field, so CBL-12.2
adds it additively (`cbl-protos@cf-3.9`, field 13) and fills it in `cbl-payout`:
`ListApprovals` reads the append-only vote journal in vote order, positive
(approving) votes only - a vetoing operator is not an approval; `GetPayout` and
`ApprovePayout` fill the contract field.

## 2. Requirements

FR-1. `storage.Repository.ListApprovals(payoutID)` returns the operators with
`approve = true`, ordered by `(created_at, id)`; a rejecting vote stays out of
the list (the review follow-up of 12.2).

FR-2. `usecase.ApprovalService.Approvals` delegates to the store; the transport
fills `Payout.approved_by` in `GetPayout` and `ApprovePayout`; an approval read
failure maps through the existing `payoutStatusError` (Internal, no details).

FR-3. Forward only: no migration, no behavior change for the vote flow; the
journal stays the source of truth.

## 3. Test plan

Unit: the fake store's `ListApprovals`; transport assertions for the filled field.
Integration: `TestIntegrationListApprovals` inserts two approvals and one veto
and expects exactly the approvers in vote order.

## 4. Work plan

Commit `CBL-12.2: approved_by in the Payout responses` (ec98c25) and the review
fix commit (`approve = true` + the veto test case).
