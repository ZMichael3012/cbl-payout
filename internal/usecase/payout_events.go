package usecase

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-kit/outbox"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
)

// Константы событий payout (CBL-003, раздел 6).
const (
	// ProducerPayout задает имя сервиса-продюсера событий выплаты.
	ProducerPayout = "cbl-payout"
	// TopicPayoutEvents задает топик жизненного цикла выплаты.
	TopicPayoutEvents = "payout.events"
	// TopicPayoutCommands задает топик команд выплаты для Connector.
	TopicPayoutCommands = "payout.commands"
	// SchemaVersion задает версию схем событий выплаты.
	SchemaVersion = "v1"
)

// PayoutEventSink собирает protobuf-конверты payout.events и пишет их в outbox
// в транзакции перехода (storage.TransitionEventSink).
type PayoutEventSink struct {
	store *outbox.Store
	log   *zap.Logger
}

// NewPayoutEventSink создает PayoutEventSink.
func NewPayoutEventSink(log *zap.Logger) *PayoutEventSink {
	if log == nil {
		log = zap.NewNop()
	}
	return &PayoutEventSink{store: outbox.NewStore(), log: log}
}

// AppendTransitionEvent собирает конверт события и записывает его в outbox.
// Переходы в состояния без типа в контракте (reserved, sent_to_gateway)
// пропускаются: события несут бизнес-вехи, а не внутренние шаги саги.
func (s *PayoutEventSink) AppendTransitionEvent(ctx context.Context, tx *gorm.DB, event storage.TransitionEvent) error {
	envelope, emit, err := BuildPayoutEventEnvelope(event)
	if err != nil {
		return err
	}
	if !emit {
		return nil
	}
	if err := s.store.Append(ctx, tx, envelope); err != nil {
		return fmt.Errorf("usecase: append payout event: %w", err)
	}
	s.log.Debug("payout event queued in outbox",
		zap.String("event_id", envelope.EventID),
		zap.String("event_type", envelope.Headers[outbox.HeaderEventType]),
		zap.String("payout_id", event.Payout.ID),
		zap.Int64("state_version", event.Payout.StateVersion))
	return nil
}

// BuildPayoutEventEnvelope собирает конверт payout.events для перехода:
// protobuf-тело billing.events.v1.PayoutEvent и заголовки конверта CBL.
// Второе значение false означает, что состояние не имеет типа в CF-2.
func BuildPayoutEventEnvelope(event storage.TransitionEvent) (outbox.Envelope, bool, error) {
	eventType, emit := payoutEventType(event.To)
	if !emit {
		return outbox.Envelope{}, false, nil
	}
	if event.OccurredAt.IsZero() {
		return outbox.Envelope{}, false, fmt.Errorf("usecase: transition %s has zero occurred_at", event.To)
	}
	payload := &eventsv1.PayoutEvent{
		PayoutId:        event.Payout.ID,
		MerchantId:      event.Payout.MerchantID,
		Type:            eventType,
		Amount:          &commonv1.Money{MinorUnits: event.Payout.AmountMinor, Asset: event.Payout.AmountAsset, Decimals: decimalsToUint32(event.Payout.AmountDecimals)},
		Txid:            derefString(event.Payout.TxID),
		Provider:        derefString(event.Payout.Provider),
		FailureReason:   derefString(event.Payout.FailureReason),
		StateVersion:    uint64(event.Payout.StateVersion), //nolint:gosec // CHECK >= 1
		OccurredAt:      timestamppb.New(event.OccurredAt.UTC()),
		RefundInvoiceId: derefString(event.Payout.RefundInvoiceID),
	}
	if fee := moneyFrom(event.Payout.FeeMinor, event.Payout.FeeAsset, event.Payout.FeeDecimals); fee != nil {
		payload.Fee = fee
	}
	body, err := proto.Marshal(payload)
	if err != nil {
		return outbox.Envelope{}, false, fmt.Errorf("usecase: marshal payout event: %w", err)
	}
	headers := map[string]string{
		outbox.HeaderProducer:      ProducerPayout,
		outbox.HeaderSchemaVersion: SchemaVersion,
		outbox.HeaderOccurredAt:    event.OccurredAt.UTC().Format(time.RFC3339Nano),
	}
	if event.Traceparent != "" {
		headers[outbox.HeaderTraceparent] = event.Traceparent
	}
	envelope, err := outbox.NewEnvelope("payout."+string(event.To), TopicPayoutEvents, event.Payout.ID, headers, body)
	if err != nil {
		return outbox.Envelope{}, false, fmt.Errorf("usecase: build payout envelope: %w", err)
	}
	return envelope, true, nil
}

// BuildPayoutCommandEnvelope собирает конверт команды payout.commands:
// protobuf-тело billing.events.v1.PayoutCommand с ключом идемпотентности
// команды для Connector.
func BuildPayoutCommandEnvelope(payout storage.Payout) (outbox.Envelope, error) {
	payload := &eventsv1.PayoutCommand{
		PayoutId: payout.ID,
		Amount: &commonv1.Money{
			MinorUnits: payout.AmountMinor,
			Asset:      payout.AmountAsset,
			Decimals:   decimalsToUint32(payout.AmountDecimals),
		},
		Network:        payout.Network,
		Address:        payout.DestinationAddress,
		IdempotencyKey: "payout:" + payout.ID + ":command",
	}
	body, err := proto.Marshal(payload)
	if err != nil {
		return outbox.Envelope{}, fmt.Errorf("usecase: marshal payout command: %w", err)
	}
	occurredAt := time.Now().UTC()
	headers := map[string]string{
		outbox.HeaderProducer:      ProducerPayout,
		outbox.HeaderSchemaVersion: SchemaVersion,
		outbox.HeaderOccurredAt:    occurredAt.Format(time.RFC3339Nano),
	}
	envelope, err := outbox.NewEnvelope("payout.command", TopicPayoutCommands, payout.ID, headers, body)
	if err != nil {
		return outbox.Envelope{}, fmt.Errorf("usecase: build payout command envelope: %w", err)
	}
	return envelope, nil
}

// payoutEventType переводит целевое состояние перехода в контрактный тип;
// состояния reserved и sent_to_gateway типа не имеют (внутренние шаги).
func payoutEventType(state domain.State) (eventsv1.PayoutEventType, bool) {
	switch state {
	case domain.StateRequested:
		return eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_REQUESTED, true
	case domain.StateApproved:
		return eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_APPROVED, true
	case domain.StateCompleted:
		return eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPLETED, true
	case domain.StateFailed:
		return eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_FAILED, true
	case domain.StateCompensated:
		return eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_COMPENSATED, true
	case domain.StateRejected:
		return eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_REJECTED, true
	default:
		return eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_UNSPECIFIED, false
	}
}

// moneyFrom собирает Money из необязательной тройки полей; неполная тройка
// (сумма без актива или шкалы) денег не образует и опускается.
func moneyFrom(amount *int64, asset *string, decimals *int) *commonv1.Money {
	if amount == nil || asset == nil || decimals == nil {
		return nil
	}
	return &commonv1.Money{
		MinorUnits: *amount,
		Asset:      *asset,
		Decimals:   decimalsToUint32(*decimals),
	}
}

// derefString разыменовывает необязательную строку (nil - пустая).
func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// decimalsToUint32 безопасно переводит шкалу в uint32 контракта.
func decimalsToUint32(decimals int) uint32 {
	if decimals < 0 {
		return 0
	}
	return uint32(decimals) //nolint:gosec // CHECK 0 - 18
}
