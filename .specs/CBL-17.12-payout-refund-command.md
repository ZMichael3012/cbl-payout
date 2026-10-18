# CBL-17.12. Payout refund command marker

| Field | Value |
| :-- | :-- |
| Epic | E17 (real provider integration) |
| Repository | `cbl-payout` |
| Size | S |
| Wave and parallel group | W5-PROVIDER |
| Depends on | 10.6 (refund as a payout), cf-4.2 (refund marker in `PayoutCommand`) |
| Contract version | `cbl-protos@cf-4.2` (additive) |
| Status | done |

## 1. Context and goal

A refund is a payout with `refund_invoice_id` (10.6). Provider refunds must go
back through the provider that accepted the original payment and use its
refunds API, so the Connector needs to distinguish a refund in
`payout.commands`. The contract addition landed in `cbl-protos@cf-4.2`
(`PayoutCommand.refund_invoice_id = 6`); this increment emits it.

## 2. Scope

In scope: `BuildPayoutCommandEnvelope` fills `refund_invoice_id` for a refund
payout; unit and integration assertions; docs.

Out of scope: the Connector refund branch (its 17.12 increment).

## 3. Architecture sketch

`internal/usecase/payout_events.go`: the command payload gains
`RefundInvoiceId = *payout.RefundInvoiceID` when the marker is set; a normal
payout keeps the field empty. `payout.events` already carry the marker (10.6).

## 4. Test plan

| ID | Case | Expected |
| :-- | :-- | :-- |
| P-01 | refund payout command | `refund_invoice_id` equals the source invoice |
| P-02 | normal payout command | the field is empty |
| I-01 | refund saga integration | the emitted command carries the marker |

## 5. Evidence

Red run recorded before implementation: `BuildPayoutCommandEnvelopeRefundMarker`
failed on HEAD (the marker was absent). After implementation: unit green, the
refund integration flow green (the command carries the marker), `go build`,
`go vet` and lint clean; the full battery and the saga e2e run with the final
commit.
