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

// PayoutSaga описывает операции саги, нужные gRPC-транспорту (10.2).
type PayoutSaga interface {
	// Create создает заявку на выплату.
	Create(ctx context.Context, in usecase.CreatePayoutInput) (storage.Payout, error)
	// Get возвращает выплату по идентификатору.
	Get(ctx context.Context, payoutID string) (storage.Payout, error)
}

// PayoutServer реализует billing.payout.v1.PayoutService: CreatePayout/GetPayout
// (10.2) и ApprovePayout (10.5).
type PayoutServer struct {
	payoutv1.UnimplementedPayoutServiceServer
	approval ApprovalDecider
	saga     PayoutSaga
}

// NewPayoutServer создает PayoutServer.
func NewPayoutServer(approval ApprovalDecider, saga PayoutSaga) *PayoutServer {
	return &PayoutServer{approval: approval, saga: saga}
}

// CreatePayout создает заявку на выплату: идемпотентно по ключу мерчанта; малая
// выплата сразу идет в сагу, крупная ждет апрува операторов (10.5).
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
	})
	if err != nil {
		return nil, payoutStatusError(err)
	}
	return &payoutv1.CreatePayoutResponse{PayoutId: payout.ID, State: payoutState(payout.State)}, nil
}

// GetPayout возвращает выплату по идентификатору.
func (s *PayoutServer) GetPayout(ctx context.Context, req *payoutv1.GetPayoutRequest) (*payoutv1.GetPayoutResponse, error) {
	payout, err := s.saga.Get(ctx, req.GetPayoutId())
	if err != nil {
		return nil, payoutStatusError(err)
	}
	return &payoutv1.GetPayoutResponse{Payout: payoutToProto(payout)}, nil
}

// ApprovePayout подтверждает или отклоняет выплату голосом оператора: при
// наборе требуемого политикой числа разных операторов - approved, при
// отклонении - rejected (10.5).
func (s *PayoutServer) ApprovePayout(ctx context.Context, req *payoutv1.ApprovePayoutRequest) (*payoutv1.ApprovePayoutResponse, error) {
	payout, err := s.approval.Approve(ctx, req.GetPayoutId(), req.GetOperatorId(), req.GetApprove(), req.GetReason())
	if err != nil {
		return nil, payoutStatusError(err)
	}
	return &payoutv1.ApprovePayoutResponse{Payout: payoutToProto(payout)}, nil
}

// payoutStatusError переводит ошибки сервисов в gRPC-коды; тексты фиксированы
// и не раскрывают внутренние детали.
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

// decimalsToInt переводит шкалу контракта в int (недоверенный вход).
func decimalsToInt(decimals uint32) int {
	if decimals > 18 {
		return 19
	}
	return int(decimals)
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
