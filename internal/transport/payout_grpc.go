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

// ApprovalDecider описывает сервис апрува, нужный gRPC-транспорту.
type ApprovalDecider interface {
	// Approve записывает голос оператора по выплате.
	Approve(ctx context.Context, payoutID, operatorID string, approve bool, reason string) (storage.Payout, error)
}

// PayoutServer реализует billing.payout.v1.PayoutService: в 10.5 доступен
// ApprovePayout; CreatePayout/GetPayout отвечают Unimplemented до 10.2.
type PayoutServer struct {
	payoutv1.UnimplementedPayoutServiceServer
	approval ApprovalDecider
}

// NewPayoutServer создает PayoutServer.
func NewPayoutServer(approval ApprovalDecider) *PayoutServer {
	return &PayoutServer{approval: approval}
}

// ApprovePayout подтверждает или отклоняет выплату голосом оператора: при
// наборе требуемого политикой числа разных операторов - approved, при
// отклонении - rejected (10.5).
func (s *PayoutServer) ApprovePayout(ctx context.Context, req *payoutv1.ApprovePayoutRequest) (*payoutv1.ApprovePayoutResponse, error) {
	payout, err := s.approval.Approve(ctx, req.GetPayoutId(), req.GetOperatorId(), req.GetApprove(), req.GetReason())
	if err != nil {
		return nil, approvalStatusError(err)
	}
	return &payoutv1.ApprovePayoutResponse{Payout: payoutToProto(payout)}, nil
}

// approvalStatusError переводит ошибки сервиса апрува в gRPC-коды; тексты
// фиксированы и не раскрывают внутренние детали.
func approvalStatusError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, storage.ErrNotFound):
		return status.Error(codes.NotFound, "payout not found")
	case errors.Is(err, usecase.ErrInvalidApprovalInput):
		return status.Error(codes.InvalidArgument, "invalid operator_id or reason")
	case errors.Is(err, usecase.ErrApprovalNotRequired):
		return status.Error(codes.FailedPrecondition, "approval not required")
	case errors.Is(err, usecase.ErrPayoutDecided):
		return status.Error(codes.FailedPrecondition, "payout already decided")
	case errors.Is(err, usecase.ErrVoteLocked):
		return status.Error(codes.FailedPrecondition, "operator vote is immutable")
	case errors.Is(err, domain.ErrStateConflict):
		return status.Error(codes.Aborted, "state conflict, retry")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

// payoutToProto переводит строку выплаты в контрактное сообщение CF-1.
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
	if payout.FeeMinor != nil && payout.FeeAsset != nil && payout.FeeDecimals != nil {
		message.Fee = &commonv1.Money{
			MinorUnits: *payout.FeeMinor,
			Asset:      *payout.FeeAsset,
			Decimals:   decimalsToUint32(*payout.FeeDecimals),
		}
	}
	return message
}

// payoutState переводит каноническое состояние в контрактный enum;
// неизвестное значение - UNSPECIFIED (в БД невозможно по CHECK).
func payoutState(raw string) payoutv1.PayoutState {
	value, ok := payoutv1.PayoutState_value["PAYOUT_STATE_"+strings.ToUpper(raw)]
	if !ok {
		return payoutv1.PayoutState_PAYOUT_STATE_UNSPECIFIED
	}
	return payoutv1.PayoutState(value)
}

// decimalsToUint32 безопасно переводит шкалу в uint32 контракта.
func decimalsToUint32(decimals int) uint32 {
	if decimals < 0 {
		return 0
	}
	return uint32(decimals) //nolint:gosec // CHECK 0 - 18
}
