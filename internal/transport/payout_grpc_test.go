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

	payoutv1 "github.com/cbl/cbl-protos/gen/go/billing/payout/v1"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/transport"
	"github.com/cbl/cbl-payout/internal/usecase"
)

// fakeDecider подменяет сервис апрува и фиксирует аргументы.
type fakeDecider struct {
	payout    storage.Payout
	err       error
	gotID     string
	gotOp     string
	gotApprov bool
	gotReason string
}

// Approve возвращает заданный результат.
func (f *fakeDecider) Approve(_ context.Context, payoutID, operatorID string, approve bool, reason string) (storage.Payout, error) {
	f.gotID = payoutID
	f.gotOp = operatorID
	f.gotApprov = approve
	f.gotReason = reason
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
	}}
	server := transport.NewPayoutServer(decider)

	response, err := server.ApprovePayout(context.Background(), &payoutv1.ApprovePayoutRequest{
		PayoutId:   "p-1",
		OperatorId: "op-1",
		Approve:    true,
		Reason:     "checked",
	})
	require.NoError(t, err)
	assert.Equal(t, "p-1", decider.gotID, "аргументы пробрасываются в сервис")
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
		server := transport.NewPayoutServer(&fakeDecider{err: tc.err})
		_, err := server.ApprovePayout(context.Background(), &payoutv1.ApprovePayoutRequest{PayoutId: "p-1", OperatorId: "op-1"})
		require.Error(t, err)
		assert.Equalf(t, tc.code, status.Code(err), "ошибка %v", tc.err)
	}
}

func TestPayoutServerUnimplementedUntilTransport(t *testing.T) {
	t.Parallel()

	server := transport.NewPayoutServer(&fakeDecider{})
	_, err := server.CreatePayout(context.Background(), &payoutv1.CreatePayoutRequest{MerchantId: "m-1"})
	assert.Equal(t, codes.Unimplemented, status.Code(err), "CreatePayout появится в 10.2")
	_, err = server.GetPayout(context.Background(), &payoutv1.GetPayoutRequest{PayoutId: "p-1"})
	assert.Equal(t, codes.Unimplemented, status.Code(err), "GetPayout появится в 10.2")
}
