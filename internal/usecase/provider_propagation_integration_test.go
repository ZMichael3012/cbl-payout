//go:build integration

package usecase_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/events/v1"

	"github.com/ZMichael3012/cbl-kit/outbox"

	"github.com/cbl/cbl-payout/internal/domain"
	"github.com/cbl/cbl-payout/internal/storage"
	"github.com/cbl/cbl-payout/internal/usecase"
)

func TestIntegrationSagaProviderPropagation(t *testing.T) {
	db, repo := setupSagaDB(t)
	service, consumer, _ := sagaServiceAt(t, repo, `{"default":0}`)
	ctx := context.Background()

	payout, err := service.Create(ctx, smallPayoutInput())
	require.NoError(t, err)

	require.NoError(t, consumer.Handle(ctx, nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-provider",
		Provider:        "coingate",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	})))
	submitted, _, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	require.NotNil(t, submitted.Provider)
	assert.Equal(t, "coingate", *submitted.Provider)

	require.NoError(t, consumer.Handle(ctx, nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-provider",
		Txid:            "tx-provider",
		Provider:        "coingate",
		Fee:             &commonv1.Money{MinorUnits: 100, Asset: "USDT", Decimals: 2},
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	})))
	completed, _, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateCompleted), completed.State)
	require.NotNil(t, completed.Provider)
	assert.Equal(t, "coingate", *completed.Provider)

	var completedEvent eventsv1.PayoutEvent
	found := false
	for _, record := range outboxRows(t, db, usecase.TopicPayoutEvents) {
		if record.Headers[outbox.HeaderEventType] != "payout.completed" {
			continue
		}
		found = true
		require.NoError(t, proto.Unmarshal(record.Payload, &completedEvent))
	}
	require.True(t, found, "the completed event must be published")
	assert.Equal(t, "coingate", completedEvent.GetProvider())
}

func TestIntegrationSagaProviderMismatchRejected(t *testing.T) {
	_, repo := setupSagaDB(t)
	service, consumer, _ := sagaServiceAt(t, repo, `{"default":0}`)
	ctx := context.Background()
	payout, err := service.Create(ctx, smallPayoutInput())
	require.NoError(t, err)

	require.NoError(t, consumer.Handle(ctx, nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-provider",
		Provider:        "coingate",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	})))
	err = consumer.Handle(ctx, nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-provider",
		Provider:        "bitpay",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_CONFIRMED,
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "provider")
	after, _, err := repo.Get(ctx, payout.ID)
	require.NoError(t, err)
	assert.Equal(t, string(domain.StateSentToGateway), after.State)
}

func TestIntegrationSagaProviderImmutable(t *testing.T) {
	db, repo := setupSagaDB(t)
	service, consumer, _ := sagaServiceAt(t, repo, `{"default":0}`)
	ctx := context.Background()
	payout, err := service.Create(ctx, smallPayoutInput())
	require.NoError(t, err)
	require.NoError(t, consumer.Handle(ctx, nil, gatewayEventMessage(t, &eventsv1.GatewayPayoutEvent{
		PayoutId:        payout.ID,
		GatewayPayoutId: "gw-provider",
		Provider:        "coingate",
		Type:            eventsv1.GatewayPayoutEventType_GATEWAY_PAYOUT_EVENT_TYPE_SUBMITTED,
	})))

	err = db.Model(&storage.Payout{}).Where("id = ?", payout.ID).Update("provider", "bitpay").Error
	require.Error(t, err, "the provider is immutable once set")
	assert.Contains(t, err.Error(), "immutable")
}
