package usecase_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/cbl/cbl-payout/internal/domain"
)

func TestGatewayConsumerSubmittedRecordsProvider(t *testing.T) {
	t.Parallel()
	consumer, store, _, payout := consumerFixture(t, domain.StateSentToGateway, nil, nil)
	require.NoError(t, consumer.Handle(context.Background(), nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-1",
		Provider:        "coingate",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	})))
	updated := store.payouts[payout.ID]
	require.NotNil(t, updated.Provider)
	assert.Equal(t, "coingate", *updated.Provider)
}

func TestGatewayConsumerSubmittedWithoutProviderTolerated(t *testing.T) {
	t.Parallel()
	consumer, store, _, payout := consumerFixture(t, domain.StateSentToGateway, nil, nil)
	require.NoError(t, consumer.Handle(context.Background(), nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-1",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	})))
	assert.Nil(t, store.payouts[payout.ID].Provider, "a legacy submission keeps the provider empty")
}

func TestGatewayConsumerRejectsInvalidProvider(t *testing.T) {
	t.Parallel()
	consumer, store, _, payout := consumerFixture(t, domain.StateSentToGateway, nil, nil)
	err := consumer.Handle(context.Background(), nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-1",
		Provider:        "Bad Provider!",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "provider")
	assert.Nil(t, store.payouts[payout.ID].GatewayPayoutID, "the invalid event changes nothing")
}

func TestGatewayConsumerVerdictProviderMismatch(t *testing.T) {
	t.Parallel()
	consumer, store, _, payout := consumerFixture(t, domain.StateSentToGateway, ptrString("reserve-1"), ptrString("gw-1"))
	store.payouts[payout.ID].Provider = ptrString("coingate")
	err := consumer.Handle(context.Background(), nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-1",
		Provider:        "bitpay",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "provider")
	assert.Equal(t, string(domain.StateSentToGateway), store.payouts[payout.ID].State)
}

func TestGatewayConsumerVerdictProviderBackfillsLegacy(t *testing.T) {
	t.Parallel()
	consumer, store, ledger, payout := consumerFixture(t, domain.StateSentToGateway, ptrString("reserve-1"), ptrString("gw-1"))
	require.NoError(t, consumer.Handle(context.Background(), nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-1",
		Txid:            "tx-1",
		Provider:        "coingate",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	})))
	updated := store.payouts[payout.ID]
	assert.Equal(t, string(domain.StateCompleted), updated.State)
	require.NotNil(t, updated.Provider)
	assert.Equal(t, "coingate", *updated.Provider)
	assert.Equal(t, []string{"reserve-1"}, ledger.commits)
}
