package transport

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	payoutv1 "github.com/cbl/cbl-protos/gen/go/billing/payout/v1"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/usecase"
)

// ApprovalDecider describes the approval service needed by the gRPC transport.
type ApprovalDecider interface {
	// Approve records an operator vote for a payout.
	Approve(ctx context.Context, payoutID, operatorID string, approve bool, reason string) (storage.Payout, error)
}

// PayoutSaga describes the saga operations needed by the gRPC transport (10.2).
type PayoutSaga interface {
	// Create creates a payout request.
	Create(ctx context.Context, in usecase.CreatePayoutInput) (storage.Payout, error)
	// Get returns a payout by identifier.
	Get(ctx context.Context, payoutID string) (storage.Payout, error)
}

// PayoutServer implements billing.payout.v1.PayoutService: CreatePayout/GetPayout
// (10.2) and ApprovePayout (10.5).
type PayoutServer struct {
	payoutv1.UnimplementedPayoutServiceServer
	approval ApprovalDecider
	saga     PayoutSaga
}

// NewPayoutServer creates a PayoutServer.
func NewPayoutServer(approval ApprovalDecider, saga PayoutSaga) *PayoutServer {
	return &PayoutServer{approval: approval, saga: saga}
}

// CreatePayout creates a payout request: idempotent by merchant key; a small
// payout goes straight into the saga, a large one waits for operator approval
// (10.5).
func (s *PayoutServer) CreatePayout(ctx context.Context, req *payoutv1.CreatePayoutRequest) (*payoutv1.CreatePayoutResponse, error) {
	amount := req.GetAmount()
	if amount == nil {
		return nil, status.Error(codes.InvalidArgument, "amount is required")
	}
	payout, err := s.saga.Create(ctx, usecase.CreatePayoutInput{
		IdempotencyKey:     req.GetIdempotencyKey(),
		MerchantID:         req.GetMerchantId(),
		AmountMinor:        amount.GetMinorUnits(),
		Asset:              amount.GetAsset(),
		Decimals:           decimalsToInt(amount.GetDecimals()),
		Network:            req.GetNetwork(),
		DestinationAddress: req.GetDestinationAddress(),
		MerchantRef:        req.GetMerchantRef(),
		RefundInvoiceID:    req.GetRefundInvoiceId(),
	})
	if err != nil {
		return nil, payoutStatusError(err)
	}
	return &payoutv1.CreatePayoutResponse{PayoutId: payout.ID, State: payoutState(payout.State)}, nil
}

// GetPayout returns a payout by identifier.
func (s *PayoutServer) GetPayout(ctx context.Context, req *payoutv1.GetPayoutRequest) (*payoutv1.GetPayoutResponse, error) {
	payout, err := s.saga.Get(ctx, req.GetPayoutId())
	if err != nil {
		return nil, payoutStatusError(err)
	}
	return &payoutv1.GetPayoutResponse{Payout: payoutToProto(payout)}, nil
}

// ApprovePayout approves or rejects a payout by operator vote: once the
// number of distinct operators required by the policy is reached the payout
// becomes approved, on a rejection it becomes rejected (10.5).
func (s *PayoutServer) ApprovePayout(ctx context.Context, req *payoutv1.ApprovePayoutRequest) (*payoutv1.ApprovePayoutResponse, error) {
	payout, err := s.approval.Approve(ctx, req.GetPayoutId(), req.GetOperatorId(), req.GetApprove(), req.GetReason())
	if err != nil {
		return nil, payoutStatusError(err)
	}
	return &payoutv1.ApprovePayoutResponse{Payout: payoutToProto(payout)}, nil
}

// payoutStatusError maps service errors to gRPC codes; the texts are fixed
// and do not reveal internal details.
func payoutStatusError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, storage.ErrNotFound):
		return status.Error(codes.NotFound, "payout not found")
	case errors.Is(err, usecase.ErrInvalidApprovalInput), errors.Is(err, usecase.ErrInvalidPayoutInput):
		return status.Error(codes.InvalidArgument, "invalid request")
	case errors.Is(err, usecase.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "idempotency key reuse")
	case errors.Is(err, usecase.ErrApprovalNotRequired):
		return status.Error(codes.FailedPrecondition, "approval not required")
	case errors.Is(err, usecase.ErrPayoutDecided):
		return status.Error(codes.FailedPrecondition, "payout already decided")
	case errors.Is(err, usecase.ErrVoteLocked):
		return status.Error(codes.FailedPrecondition, "operator vote is immutable")
	case errors.Is(err, usecase.ErrSagaUnavailable), errors.Is(err, usecase.ErrLedgerUnavailable):
		return status.Error(codes.FailedPrecondition, "saga is unavailable")
	case errors.Is(err, domain.ErrStateConflict):
		return status.Error(codes.Aborted, "state conflict, retry")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

// decimalsToInt converts the contract scale to int (untrusted input).
func decimalsToInt(decimals uint32) int {
	if decimals > 18 {
		return 19
	}
	return int(decimals)
}

// payoutToProto converts a payout row to the CF-1 contract message.
func payoutToProto(payout storage.Payout) *payoutv1.Payout {
	message := &payoutv1.Payout{
		PayoutId:           payout.ID,
		MerchantId:         payout.MerchantID,
		State:              payoutState(payout.State),
		Amount:             &commonv1.Money{MinorUnits: payout.AmountMinor, Asset: payout.AmountAsset, Decimals: decimalsToUint32(payout.AmountDecimals)},
		Network:            payout.Network,
		DestinationAddress: payout.DestinationAddress,
		StateVersion:       uint64(payout.StateVersion), //nolint:gosec // CHECK >= 1
		CreatedAt:          timestamppb.New(payout.CreatedAt.UTC()),
	}
	if payout.TxID != nil {
		message.Txid = *payout.TxID
	}
	if payout.FailureReason != nil {
		message.FailureReason = *payout.FailureReason
	}
	if payout.RefundInvoiceID != nil {
		message.RefundInvoiceId = *payout.RefundInvoiceID
	}
	if payout.FeeMinor != nil && payout.FeeAsset != nil && payout.FeeDecimals != nil {
		message.Fee = &commonv1.Money{
			MinorUnits: *payout.FeeMinor,
			Asset:      *payout.FeeAsset,
			Decimals:   decimalsToUint32(*payout.FeeDecimals),
		}
	}
	return message
}

// payoutState converts the canonical state to the contract enum; an unknown
// value becomes UNSPECIFIED (impossible in the DB due to CHECK).
func payoutState(raw string) payoutv1.PayoutState {
	value, ok := payoutv1.PayoutState_value["PAYOUT_STATE_"+strings.ToUpper(raw)]
	if !ok {
		return payoutv1.PayoutState_PAYOUT_STATE_UNSPECIFIED
	}
	return payoutv1.PayoutState(value)
}

// decimalsToUint32 safely converts the scale to the contract uint32.
func decimalsToUint32(decimals int) uint32 {
	if decimals < 0 {
		return 0
	}
	return uint32(decimals) //nolint:gosec // CHECK 0 - 18
}
