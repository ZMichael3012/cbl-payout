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

// Payout event constants (CBL-003, section 6).
const (
	// ProducerPayout sets the name of the payout event producer service.
	ProducerPayout = "cbl-payout"
	// TopicPayoutEvents sets the payout lifecycle topic.
	TopicPayoutEvents = "payout.events"
	// TopicPayoutCommands sets the payout command topic for Connector.
	TopicPayoutCommands = "payout.commands"
	// SchemaVersion sets the schema version of payout events.
	SchemaVersion = "v1"
)

// PayoutEventSink builds protobuf envelopes of payout.events and writes them
// to the outbox in the transition transaction (storage.TransitionEventSink).
type PayoutEventSink struct {
	store *outbox.Store
	log   *zap.Logger
}

// NewPayoutEventSink creates a PayoutEventSink.
func NewPayoutEventSink(log *zap.Logger) *PayoutEventSink {
	if log == nil {
		log = zap.NewNop()
	}
	return &PayoutEventSink{store: outbox.NewStore(), log: log}
}

// AppendTransitionEvent builds the event envelope and writes it to the outbox.
// Transitions to states without a contract type (reserved, sent_to_gateway)
// are skipped: events carry business milestones, not internal saga steps.
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

// BuildPayoutEventEnvelope builds the payout.events envelope for a transition:
// the protobuf body billing.events.v1.PayoutEvent and the CBL envelope headers.
// A false second value means the state has no type in CF-2.
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

// BuildPayoutCommandEnvelope builds the payout.commands command envelope: the
// protobuf body billing.events.v1.PayoutCommand with the command idempotency
// key for Connector.
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
	// A refund payout carries its source invoice so the Connector calls the
	// provider refunds API instead of a payout (17.12, cf-4.2).
	if payout.RefundInvoiceID != nil && *payout.RefundInvoiceID != "" {
		payload.RefundInvoiceId = *payout.RefundInvoiceID
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

// payoutEventType maps the transition target state to the contract type;
// the reserved and sent_to_gateway states have no type (internal steps).
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

// moneyFrom builds Money from the optional field triple; an incomplete triple
// (an amount without an asset or scale) does not form money and is omitted.
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

// derefString dereferences an optional string (nil means empty).
func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// decimalsToUint32 safely converts the scale to the contract uint32.
func decimalsToUint32(decimals int) uint32 {
	if decimals < 0 {
		return 0
	}
	return uint32(decimals) //nolint:gosec // CHECK 0 - 18
}
