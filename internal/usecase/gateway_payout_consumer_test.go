package usecase_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/ZMichael3012/cbl-kit/inbox"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/usecase"
)

// gatewayEventMessage builds an inbox message of a gateway event.
func gatewayEventMessage(t *testing.T, event *eventsv1.GatewayPayoutEvent) inbox.Message {
	t.Helper()
	body, err := proto.Marshal(event)
	require.NoError(t, err)
	return inbox.Message{
		Topic:   usecase.TopicGatewayPayoutEvents,
		Key:     event.GetPayoutId(),
		Headers: map[string]string{"event_id": uuid.Must(uuid.NewV7()).String()},
		Value:   body,
	}
}

// consumerFixture builds the consumer with a payout in the given state;
// gatewayID records the payout gateway_payout_id (verdict binding).
func consumerFixture(t *testing.T, state domain.State, reserveID, gatewayID *string) (*usecase.GatewayPayoutConsumer, *fakeSagaStore, *fakeLedger, storage.Payout) {
	t.Helper()
	store := newFakeSagaStore()
	id := uuid.Must(uuid.NewV7()).String()
	payout := storage.Payout{
		ID:              id,
		MerchantID:      "m-1",
		State:           string(state),
		StateVersion:    4,
		AmountMinor:     500_000,
		AmountAsset:     "USDT",
		AmountDecimals:  2,
		ReserveID:       reserveID,
		GatewayPayoutID: gatewayID,
	}
	store.payouts[id] = &payout
	ledger := &fakeLedger{}
	clock := func() time.Time { return time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC) }
	consumer := usecase.NewGatewayPayoutConsumer(store, ledger, zap.NewNop(), usecase.WithGatewayClock(clock))
	return consumer, store, ledger, payout
}

// ptrString returns a pointer to the string.
func ptrString(value string) *string { return &value }

func TestPlanGatewayEvent(t *testing.T) {
	t.Parallel()

	// The plan is pinned by the table: a too-early event is retried, a stale
	// one is acknowledged, working states yield saga actions.
	tests := []struct {
		state    domain.State
		event    eventsv1.GatewayPayoutEventType
		tooEarly bool
		stale    bool
	}{
		{domain.StateRequested, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED, true, false},
		{domain.StateApproved, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED, true, false},
		{domain.StateReserved, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED, true, false},
		{domain.StateSentToGateway, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED, false, false},
		{domain.StateSentToGateway, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED, false, false},
		{domain.StateSentToGateway, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED, false, false},
		{domain.StateFailed, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED, false, false},
		{domain.StateCompleted, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED, false, true},
		{domain.StateCompleted, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED, false, true},
		{domain.StateCompensated, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED, false, true},
		{domain.StateRejected, eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED, false, true},
	}
	for _, tc := range tests {
		// Binding: verdict gateway_payout_id matches the recorded one.
		consumer, _, _, payout := consumerFixture(t, tc.state, ptrString("reserve-1"), ptrString("gw-1"))
		err := consumer.Handle(context.Background(), nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
			PayoutId:        payout.ID,
			GatewayPayoutId: "gw-1",
			Type:            tc.event,
		}))
		switch {
		case tc.tooEarly:
			require.ErrorContainsf(t, err, "too early", "%s + %s", tc.state, tc.event)
		case tc.stale:
			require.NoErrorf(t, err, "%s + %s", tc.state, tc.event)
		default:
			require.NoErrorf(t, err, "%s + %s", tc.state, tc.event)
		}
	}
}

func TestGatewayConsumerSubmittedRecordsData(t *testing.T) {
	t.Parallel()

	consumer, store, _, payout := consumerFixture(t, domain.StateSentToGateway, nil, nil)
	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-1",
		Txid:            "tx-1",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	}
	require.NoError(t, consumer.Handle(context.Background(), nil, gatewayEventMessage(t, event)))

	updated := store.payouts[payout.ID]
	assert.Equal(t, string(domain.StateSentToGateway), updated.State)
	assert.Equal(t, int64(5), updated.StateVersion, "the self-transition records the submission data")
	require.NotNil(t, updated.GatewayPayoutID)
	assert.Equal(t, "gw-1", *updated.GatewayPayoutID)
}

func TestGatewayConsumerConfirmedCompletes(t *testing.T) {
	t.Parallel()

	consumer, store, ledger, payout := consumerFixture(t, domain.StateSentToGateway, ptrString("reserve-1"), ptrString("gw-1"))
	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-1",
		Txid:            "tx-9",
		Fee:             &commonv1.Money{MinorUnits: 250, Asset: "USDT", Decimals: 2},
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	}
	require.NoError(t, consumer.Handle(context.Background(), nil, gatewayEventMessage(t, event)))

	updated := store.payouts[payout.ID]
	assert.Equal(t, string(domain.StateCompleted), updated.State)
	require.Len(t, ledger.commits, 1)
	assert.Equal(t, "payout:"+payout.ID+":commit", ledger.commitKeys[0])
	assert.Equal(t, "reserve-1", ledger.commits[0])
	require.NotNil(t, updated.CompletedAt)
	require.NotNil(t, updated.FeeMinor)
	assert.Equal(t, int64(250), *updated.FeeMinor)
}

func TestGatewayConsumerFailedCompensates(t *testing.T) {
	t.Parallel()

	consumer, store, ledger, payout := consumerFixture(t, domain.StateSentToGateway, ptrString("reserve-2"), ptrString("gw-2"))
	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-2",
		Reason:          "address rejected",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED,
	}
	require.NoError(t, consumer.Handle(context.Background(), nil, gatewayEventMessage(t, event)))

	updated := store.payouts[payout.ID]
	assert.Equal(t, string(domain.StateCompensated), updated.State)
	require.Len(t, ledger.releases, 1)
	assert.Equal(t, "payout:"+payout.ID+":release", ledger.releaseKeys[0])
	assert.Equal(t, "reserve-2", ledger.releases[0])
	require.NotNil(t, updated.FailureReason)
	assert.Equal(t, "address rejected", *updated.FailureReason)
	assert.Equal(t, []domain.State{domain.StateFailed, domain.StateCompensated}, store.transitions)
}

func TestGatewayConsumerPreSubmissionFailedCompensates(t *testing.T) {
	t.Parallel()

	// The connector reports a business rejection before any provider payout
	// exists (publishFailed carries no gateway_payout_id): the hold must be
	// released and the payout compensated (CBL-17.26 increment D finding).
	consumer, store, ledger, payout := consumerFixture(t, domain.StateSentToGateway, ptrString("reserve-9"), nil)
	event := &eventsv1.GatewayPayoutEvent{
		PayoutId: payout.ID,
		Reason:   "provider rejected the payout",
		Provider: "btcpay",
		Type:     eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED,
	}
	require.NoError(t, consumer.Handle(context.Background(), nil, gatewayEventMessage(t, event)))

	updated := store.payouts[payout.ID]
	assert.Equal(t, string(domain.StateCompensated), updated.State)
	require.Len(t, ledger.releases, 1)
	assert.Equal(t, "payout:"+payout.ID+":release", ledger.releaseKeys[0])
	assert.Equal(t, "reserve-9", ledger.releases[0])
	require.NotNil(t, updated.FailureReason)
	assert.Equal(t, "provider rejected the payout", *updated.FailureReason)
	require.NotNil(t, updated.Provider, "the provider of the verdict is backfilled like the completion path")
	assert.Equal(t, "btcpay", *updated.Provider)
	assert.Equal(t, []domain.State{domain.StateFailed, domain.StateCompensated}, store.transitions)

	// The connector emits an empty provider for "no provider supports the
	// payout" and refund-mapping failures: the shape still compensates.
	consumer2, store2, ledger2, payout2 := consumerFixture(t, domain.StateSentToGateway, ptrString("reserve-10"), nil)
	require.NoError(t, consumer2.Handle(context.Background(), nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId: payout2.ID,
		Reason:   "no provider supports the payout",
		Type:     eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED,
	})))
	assert.Equal(t, string(domain.StateCompensated), store2.payouts[payout2.ID].State)
	require.Len(t, ledger2.releases, 1)
	assert.Nil(t, store2.payouts[payout2.ID].Provider)
}

func TestGatewayConsumerFailedReplayWithoutRelease(t *testing.T) {
	t.Parallel()

	reason := "gateway failed"
	consumer, store, ledger, payout := consumerFixture(t, domain.StateFailed, nil, ptrString("gw-3"))
	store.payouts[payout.ID].FailureReason = &reason

	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-3",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED,
	}
	require.NoError(t, consumer.Handle(context.Background(), nil, gatewayEventMessage(t, event)))

	assert.Equal(t, string(domain.StateCompensated), store.payouts[payout.ID].State)
	assert.Empty(t, ledger.releases, "release is not called without a reserve")
	assert.Empty(t, ledger.commits)
}

func TestGatewayConsumerLedgerUnavailableRetries(t *testing.T) {
	t.Parallel()

	consumer, store, ledger, payout := consumerFixture(t, domain.StateSentToGateway, ptrString("reserve-3"), ptrString("gw-4"))
	ledger.commitErr = usecase.ErrLedgerUnavailable

	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-4",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	}
	err := consumer.Handle(context.Background(), nil, gatewayEventMessage(t, event))
	require.ErrorIs(t, err, usecase.ErrLedgerUnavailable)
	assert.Equal(t, string(domain.StateSentToGateway), store.payouts[payout.ID].State, "a Ledger failure does not change the state")
}

func TestGatewayConsumerVerdictBinding(t *testing.T) {
	t.Parallel()

	// A verdict before submitted (no gateway_payout_id) is a DLQ error.
	consumer, _, ledger, payout := consumerFixture(t, domain.StateSentToGateway, ptrString("reserve-5"), nil)
	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-5",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	}
	err := consumer.Handle(context.Background(), nil, gatewayEventMessage(t, event))
	require.ErrorContains(t, err, "verdict before submitted")
	assert.Empty(t, ledger.commits, "Ledger is not called before binding")

	// A mismatching gateway_payout_id is a DLQ error.
	consumer2, _, ledger2, payout2 := consumerFixture(t, domain.StateSentToGateway, ptrString("reserve-6"), ptrString("gw-6"))
	event2 := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout2.ID,
		GatewayPayoutId: "gw-other",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED,
	}
	err = consumer2.Handle(context.Background(), nil, gatewayEventMessage(t, event2))
	require.ErrorContains(t, err, "gateway_payout_id mismatch")
	assert.Empty(t, ledger2.releases)

	// A confirmed verdict without gateway_payout_id is a DLQ error (a failed
	// verdict may omit it only against a never-submitted payout).
	err = consumer2.Handle(context.Background(), nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId: payout2.ID,
		Type:     eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	}))
	require.ErrorContains(t, err, "gateway_payout_id is required")

	// A failed verdict naming a gateway payout that was never submitted is a
	// DLQ error (the second half of the pre-submission exception).
	consumer4, _, ledger4, payout4 := consumerFixture(t, domain.StateSentToGateway, ptrString("reserve-8"), nil)
	err = consumer4.Handle(context.Background(), nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout4.ID,
		GatewayPayoutId: "gw-forged",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED,
	}))
	require.ErrorContains(t, err, "verdict before submitted")
	assert.Empty(t, ledger4.releases)

	// A failed verdict without a gateway_payout_id against an already submitted
	// payout is a mismatch (a forged pre-submission failure cannot kill it).
	err = consumer2.Handle(context.Background(), nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId: payout2.ID,
		Type:     eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED,
	}))
	require.ErrorContains(t, err, "gateway_payout_id mismatch")
	assert.Empty(t, ledger2.releases)

	// confirmed without a reserve is a DLQ error with no Ledger call.
	consumer3, _, ledger3, payout3 := consumerFixture(t, domain.StateSentToGateway, nil, ptrString("gw-7"))
	err = consumer3.Handle(context.Background(), nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout3.ID,
		GatewayPayoutId: "gw-7",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	}))
	require.ErrorContains(t, err, "confirmed without reserve")
	assert.Empty(t, ledger3.commits)
}

func TestGatewayConsumerInvalidEvents(t *testing.T) {
	t.Parallel()

	consumer, _, _, payout := consumerFixture(t, domain.StateSentToGateway, nil, nil)

	// Invalid payout_id.
	bad := &eventsv1.GatewayPayoutEvent{PayoutId: "not-a-uuid", Type: eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED}
	err := consumer.Handle(context.Background(), nil, gatewayEventMessage(t, bad))
	require.ErrorContains(t, err, "invalid gateway payout event")

	// Fee details rejection.
	mismatch := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		Fee:             &commonv1.Money{MinorUnits: 10, Asset: "BTC", Decimals: 8},
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
		GatewayPayoutId: "gw-x",
	}
	err = consumer.Handle(context.Background(), nil, gatewayEventMessage(t, mismatch))
	require.ErrorContains(t, err, "invalid gateway payout event")

	// Control characters in reason.
	control := &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-x",
		Reason:          "line\nbreak",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_FAILED,
	}
	err = consumer.Handle(context.Background(), nil, gatewayEventMessage(t, control))
	require.ErrorContains(t, err, "control characters")

	// An unknown non-empty enum is acknowledged with no effect (BACKWARD_TRANSITIVE).
	unknown := &eventsv1.GatewayPayoutEvent{PayoutId: payout.ID, Type: eventsv1.GatewayPayoutEventType(99)}
	require.NoError(t, consumer.Handle(context.Background(), nil, gatewayEventMessage(t, unknown)))
}

func TestGatewayConsumerUnknownPayoutRetries(t *testing.T) {
	t.Parallel()

	store := newFakeSagaStore()
	consumer := usecase.NewGatewayPayoutConsumer(store, &fakeLedger{}, zap.NewNop())
	event := &eventsv1.GatewayPayoutEvent{
		PayoutId:        uuid.Must(uuid.NewV7()).String(),
		GatewayPayoutId: "gw-unknown",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	}
	err := consumer.Handle(context.Background(), nil, gatewayEventMessage(t, event))
	require.ErrorIs(t, err, storage.ErrNotFound, "an event cannot outrun creation: retry")
	assert.False(t, errors.Is(err, usecase.ErrSagaUnavailable))
}
