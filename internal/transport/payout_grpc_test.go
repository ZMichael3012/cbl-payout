package transport_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	payoutv1 "github.com/cbl/cbl-protos/gen/go/billing/payout/v1"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/transport"
	"github.com/cbl/cbl-payout/internal/usecase"
)

// fakeDecider substitutes the approval service and records the arguments.
type fakeDecider struct {
	payout       storage.Payout
	err          error
	approvals    []string
	approvalsErr error
	gotID        string
	gotOp        string
	gotApprov    bool
	gotReason    string
}

// Approve returns the configured result.
func (f *fakeDecider) Approve(_ context.Context, payoutID, operatorID string, approve bool, reason string) (storage.Payout, error) {
	f.gotID = payoutID
	f.gotOp = operatorID
	f.gotApprov = approve
	f.gotReason = reason
	return f.payout, f.err
}

// Approvals returns the configured vote list (CBL-12.2).
func (f *fakeDecider) Approvals(context.Context, string) ([]string, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.approvals, f.approvalsErr
}

// fakeSaga substitutes the saga operations and records the creation input.
type fakeSaga struct {
	payout   storage.Payout
	err      error
	gotInput *usecase.CreatePayoutInput
}

// Create returns the configured result.
func (f *fakeSaga) Create(_ context.Context, in usecase.CreatePayoutInput) (storage.Payout, error) {
	f.gotInput = &in
	return f.payout, f.err
}

// Get returns the configured result.
func (f *fakeSaga) Get(context.Context, string) (storage.Payout, error) {
	return f.payout, f.err
}

func TestApprovePayoutSuccessMapping(t *testing.T) {
	t.Parallel()

	txid := "tx-1"
	reason := ""
	feeMinor := int64(50)
	feeAsset := "USDT"
	feeDecimals := 2
	decider := &fakeDecider{payout: storage.Payout{
		ID:                 "p-1",
		MerchantID:         "m-1",
		State:              string(domain.StateApproved),
		StateVersion:       2,
		AmountMinor:        500_000,
		AmountAsset:        "USDT",
		AmountDecimals:     2,
		Network:            "trc20",
		DestinationAddress: "Taddr",
		TxID:               &txid,
		FailureReason:      &reason,
		FeeMinor:           &feeMinor,
		FeeAsset:           &feeAsset,
		FeeDecimals:        &feeDecimals,
		CreatedAt:          time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
	}, approvals: []string{"op-1"}}
	server := transport.NewPayoutServer(decider, &fakeSaga{})

	response, err := server.ApprovePayout(context.Background(), &payoutv1.ApprovePayoutRequest{
		PayoutId:   "p-1",
		OperatorId: "op-1",
		Approve:    true,
		Reason:     "checked",
	})
	require.NoError(t, err)
	assert.Equal(t, "p-1", decider.gotID, "the arguments are passed through to the service")
	assert.Equal(t, "op-1", decider.gotOp)
	assert.True(t, decider.gotApprov)
	assert.Equal(t, "checked", decider.gotReason)
	payout := response.GetPayout()
	require.NotNil(t, payout)
	assert.Equal(t, "p-1", payout.GetPayoutId())
	assert.Equal(t, payoutv1.PayoutState_PAYOUT_STATE_APPROVED, payout.GetState())
	assert.Equal(t, uint64(2), payout.GetStateVersion())
	assert.Equal(t, int64(500_000), payout.GetAmount().GetMinorUnits())
	assert.Equal(t, "USDT", payout.GetAmount().GetAsset())
	assert.Equal(t, "tx-1", payout.GetTxid())
	assert.Equal(t, int64(50), payout.GetFee().GetMinorUnits())
	assert.Equal(t, "", payout.GetFailureReason())
	assert.Equal(t, []string{"op-1"}, payout.GetApprovedBy(), "the approval progress is exposed (cf-3.9)")
	assert.Equal(t, "2026-10-15T12:00:00Z", payout.GetCreatedAt().AsTime().UTC().Format(time.RFC3339))
}

func TestApprovePayoutErrorMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		err  error
		code codes.Code
	}{
		{storage.ErrNotFound, codes.NotFound},
		{usecase.ErrInvalidApprovalInput, codes.InvalidArgument},
		{usecase.ErrApprovalNotRequired, codes.FailedPrecondition},
		{usecase.ErrPayoutDecided, codes.FailedPrecondition},
		{usecase.ErrVoteLocked, codes.FailedPrecondition},
		{domain.ErrStateConflict, codes.Aborted},
		{context.Canceled, codes.Canceled},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{errors.New("boom"), codes.Internal},
	}
	for _, tc := range cases {
		server := transport.NewPayoutServer(&fakeDecider{err: tc.err}, &fakeSaga{})
		_, err := server.ApprovePayout(context.Background(), &payoutv1.ApprovePayoutRequest{PayoutId: "p-1", OperatorId: "op-1"})
		require.Error(t, err)
		assert.Equalf(t, tc.code, status.Code(err), "error %v", tc.err)
	}
}

func TestCreatePayoutValidationAndErrors(t *testing.T) {
	t.Parallel()

	server := transport.NewPayoutServer(&fakeDecider{}, &fakeSaga{})

	// Without amount it is InvalidArgument.
	_, err := server.CreatePayout(context.Background(), &payoutv1.CreatePayoutRequest{MerchantId: "m-1"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	// An idempotency key conflict is AlreadyExists.
	conflict := transport.NewPayoutServer(&fakeDecider{}, &fakeSaga{err: usecase.ErrIdempotencyConflict})
	_, err = conflict.CreatePayout(context.Background(), &payoutv1.CreatePayoutRequest{
		MerchantId: "m-1",
		Amount:     &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	// The saga is unavailable - FailedPrecondition.
	unavailable := transport.NewPayoutServer(&fakeDecider{}, &fakeSaga{err: usecase.ErrSagaUnavailable})
	_, err = unavailable.CreatePayout(context.Background(), &payoutv1.CreatePayoutRequest{
		MerchantId: "m-1",
		Amount:     &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2},
	})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	// GetPayout: the payout is not found.
	notFound := transport.NewPayoutServer(&fakeDecider{}, &fakeSaga{err: storage.ErrNotFound})
	_, err = notFound.GetPayout(context.Background(), &payoutv1.GetPayoutRequest{PayoutId: "p-1"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestCreatePayoutSuccessMapping(t *testing.T) {
	t.Parallel()

	refundInvoice := "01a13d53-6bf5-707f-a944-c0d7358654f0"
	saga := &fakeSaga{payout: storage.Payout{
		ID:              "p-1",
		State:           string(domain.StateSentToGateway),
		StateVersion:    4,
		RefundInvoiceID: &refundInvoice,
	}}
	server := transport.NewPayoutServer(&fakeDecider{}, saga)

	response, err := server.CreatePayout(context.Background(), &payoutv1.CreatePayoutRequest{
		IdempotencyKey:     "key-1",
		MerchantId:         "m-1",
		Amount:             &commonv1.Money{MinorUnits: 500_000, Asset: "USDT", Decimals: 2},
		Network:            "trc20",
		DestinationAddress: "Taddr",
		RefundInvoiceId:    refundInvoice,
	})
	require.NoError(t, err)
	assert.Equal(t, "p-1", response.GetPayoutId())
	assert.Equal(t, payoutv1.PayoutState_PAYOUT_STATE_SENT_TO_GATEWAY, response.GetState())
	require.NotNil(t, saga.gotInput)
	assert.Equal(t, refundInvoice, saga.gotInput.RefundInvoiceID, "the refund marker is passed through to the saga input")

	// GetPayout returns the refund marker (CBL-10.6) and the approval progress.
	getResponse, err := server.GetPayout(context.Background(), &payoutv1.GetPayoutRequest{PayoutId: "p-1"})
	require.NoError(t, err)
	assert.Equal(t, refundInvoice, getResponse.GetPayout().GetRefundInvoiceId())
	assert.Empty(t, getResponse.GetPayout().GetApprovedBy(), "no votes yet")
}
